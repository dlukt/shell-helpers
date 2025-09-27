package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

const envFileContents = `VITE_HTTP_ENDPOINT=http://localhost:8081/query
VITE_OIDC_AUTHORITY=https://auth.icod.de/realms/dev
VITE_OIDC_CLIENT_ID=spa
VITE_OIDC_REDIRECT_URI=http://localhost:5173
VITE_OIDC_SCOPE='openid email profile roles'
VITE_FILES_PREFIX=http://localhost:8081/
`

const oidcConfigContents = `import {UserManagerSettings} from 'oidc-client-ts';

export const oidcConfig: UserManagerSettings = {
    authority: import.meta.env.VITE_OIDC_AUTHORITY,
    client_id: import.meta.env.VITE_OIDC_CLIENT_ID,
    redirect_uri: import.meta.env.VITE_OIDC_REDIRECT_URI,
    scope: import.meta.env.VITE_OIDC_SCOPE,
}
`

const relayEnvironmentContents = "import {\n    Environment,\n    type FetchFunction,\n    Network,\n    Observable,\n    RecordSource,\n    Store,\n    type SubscribeFunction,\n} from \"relay-runtime\";\nimport {createClient} from \"graphql-ws\";\nimport type {ExecutionResult} from \"graphql\";\nimport type {GraphQLResponse} from \"relay-runtime/lib/network/RelayNetworkTypes\";\nimport {oidcConfig} from \"./oidc-config.ts\";\nimport {User} from \"oidc-client-ts\";\n\nconst env = import.meta.env as ImportMetaEnv & { VITE_WS_ENDPOINT?: string };\nconst HTTP_ENDPOINT = env.VITE_HTTP_ENDPOINT;\nconst WS_ENDPOINT = env.VITE_WS_ENDPOINT ?? String(HTTP_ENDPOINT || \"\").replace(/^http/, \"ws\");\nconst file = \"file\"\nconst files = \"files\"\n\nfunction getUser() {\n  let oidcStorage = localStorage.getItem(`oidc.user:${oidcConfig.authority}:${oidcConfig.client_id}`)\n  if (!oidcStorage) {\n    oidcStorage = sessionStorage.getItem(`oidc.user:${oidcConfig.authority}:${oidcConfig.client_id}`)\n    if (!oidcStorage) {\n      return null;\n    }\n  }\n\n  return User.fromStorageString(oidcStorage);\n}\n\nconst fetchFn: FetchFunction = async (request, variables, _cacheConfig, uploadables) => {\n  const reqLabel = `[Relay] ${request.name}`\n  if (import.meta.env.DEV) {\n    try {\n      // Simple client-side trace for debugging stuck loads\n      console.debug(`${reqLabel} ->`, variables)\n    } catch {\n      // ignore\n    }\n  }\n  let payload\n  const user = getUser()\n  const access_token = user?.access_token\n\n  const headers = new Headers()\n  headers.set(\"Accept\", \"application/graphql-response+json; charset=utf-8, application/json; charset=utf-8\")\n  // headers.set(\"Content-Type\", \"application/json; charset=utf-8\")\n  if (access_token) {\n    headers.set(\"Authorization\", `Bearer ${access_token}`)\n  }\n\n  if (uploadables) {\n    if (!window.FormData) {\n      throw new Error(\"Uploading files without `FormData` not supported.\");\n    }\n    const formData = new FormData();\n    formData.append(\n        'operations',\n        JSON.stringify({\n          query: request.text,\n          variables: variables,\n        })\n    )\n    const map: Record<string, string[]> = {}\n    if (file in variables) {\n      for (const uploadable in uploadables) {\n        if (Object.prototype.hasOwnProperty.call(uploadables, uploadable)) {\n          map[uploadable] = ['variables.file']\n        }\n      }\n    } else if (files in variables && Array.isArray(variables.files)) {\n      Object.keys(uploadables).forEach((uploadKey, index) => {\n        map[uploadKey] = [`variables.files.${index}`]\n      })\n    } else {\n      console.error(\"uploadables provided, but no file or files in variables.\")\n      return\n    }\n\n    formData.append('map', JSON.stringify(map));\n\n    for (const uploadable in uploadables) {\n      if (Object.prototype.hasOwnProperty.call(uploadables, uploadable)) {\n        formData.append(uploadable, uploadables[uploadable])\n      }\n    }\n    payload = {\n      method: \"POST\",\n      headers: headers,\n      body: formData\n    }\n  } else {\n    headers.set(\"Content-Type\", \"application/json; charset=utf-8\")\n    payload = {\n      method: \"POST\",\n      headers: headers,\n      body: JSON.stringify({\n        query: request.text, // <-- The GraphQL document composed by Relay\n        variables,\n      }),\n    }\n  }\n\n  // Add a safety timeout so we can surface stuck requests as errors in dev\n  const controller = new AbortController()\n  const timeoutMs = 30000\n  const timeout = setTimeout(() => controller.abort(), timeoutMs)\n  try {\n    const resp = await fetch(HTTP_ENDPOINT, { ...payload, signal: controller.signal })\n    const isJson = resp.headers.get('content-type')?.includes('json')\n    const data = isJson ? await resp.json() : undefined\n    if (!resp.ok) {\n      if (import.meta.env.DEV) {\n        console.error(`${reqLabel} <- HTTP ${resp.status}`, data ?? await resp.text())\n      }\n      throw new Error(`GraphQL HTTP ${resp.status}`)\n    }\n    if (import.meta.env.DEV) {\n      try {\n        console.debug(`${reqLabel} <- ok`)\n      } catch {\n        // ignore\n      }\n    }\n    return data\n  } catch (err) {\n    if (import.meta.env.DEV) {\n      console.error(`${reqLabel} <- error`, err)\n    }\n    throw err\n  } finally {\n    clearTimeout(timeout)\n  }\n};\n\n\nconst subscribeFn: SubscribeFunction = (request, variables) => {\n  const user = getUser()\n  const access_token = user?.access_token\n  const client = createClient({\n    url: WS_ENDPOINT,\n    lazy: true,\n    connectionParams: access_token ? { headers: { Authorization: `Bearer ${access_token}` } } : undefined,\n  })\n  return Observable.create<GraphQLResponse>((sink) => {\n    const dispose = client.subscribe(\n      { query: request.text ?? \"\", variables },\n      {\n        next: (value: ExecutionResult) => {\n          const resp = { ...value } as GraphQLResponse & { data?: unknown | null };\n          if (Object.prototype.hasOwnProperty.call(resp, \"data\") && resp.data === null) {\n            delete resp.data;\n          }\n          sink.next(resp);\n        },\n        error: sink.error.bind(sink),\n        complete: sink.complete.bind(sink),\n      }\n    )\n    return () => dispose()\n  })\n}\n\nfunction createRelayEnvironment() {\n  return new Environment({\n    network: Network.create(fetchFn, subscribeFn),\n    store: new Store(new RecordSource()),\n  });\n}\n\nexport const RelayEnvironment = createRelayEnvironment();\n"

// spaCmd represents the spa command
var spaCmd = &cobra.Command{
	Use:   "spa [app-dir]",
	Short: "Scaffold a Relay SPA and wire OIDC integration",
	Args:  cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := exec.LookPath("pnpm"); err != nil {
			return fmt.Errorf("pnpm not found in PATH: %w", err)
		}

		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}
		ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
		defer cancel()

		appDirArg := "."
		if len(args) == 1 {
			trimmed := strings.TrimSpace(args[0])
			if trimmed == "" {
				return fmt.Errorf("app-dir argument cannot be empty")
			}
			appDirArg = trimmed
		}

		absAppDir, err := filepath.Abs(appDirArg)
		if err != nil {
			return fmt.Errorf("resolve app directory: %w", err)
		}
		absAppDir = filepath.Clean(absAppDir)
		parentDir := filepath.Dir(absAppDir)

		if err := ensureDirExists(parentDir); err != nil {
			return fmt.Errorf("ensure parent directory: %w", err)
		}

		projectDir, err := ensureProjectDir(ctx, absAppDir, parentDir)
		if err != nil {
			return err
		}

		fmt.Printf("Using project directory: %s\n", projectDir)

		fmt.Println("Installing packages...")
		baseDeps := []string{"react-oidc-context", "react-hook-form", "react-router", "zod", "date-fns", "graphql-ws"}
		if err := runCommand(ctx, projectDir, "pnpm", append([]string{"add"}, baseDeps...)...); err != nil {
			return fmt.Errorf("add base dependencies: %w", err)
		}

		pkgJSONPath := filepath.Join(projectDir, "package.json")
		pkgJSON, err := readPackageJSON(pkgJSONPath)
		if err != nil {
			return fmt.Errorf("read package.json: %w", err)
		}

		sanityDeps := []string{"oidc-client-ts", "relay-runtime", "graphql"}
		missing := missingDependencies(pkgJSON, sanityDeps)
		if len(missing) > 0 {
			fmt.Printf("Installing additional dependencies: %s\n", strings.Join(missing, ", "))
			if err := runCommand(ctx, projectDir, "pnpm", append([]string{"add"}, missing...)...); err != nil {
				return fmt.Errorf("add additional dependencies: %w", err)
			}
		}

		fmt.Println("Writing .env files...")
		if err := writeFile(filepath.Join(projectDir, ".env.development"), []byte(envFileContents)); err != nil {
			return fmt.Errorf("write .env.development: %w", err)
		}
		if err := writeFile(filepath.Join(projectDir, ".env.production"), []byte(envFileContents)); err != nil {
			return fmt.Errorf("write .env.production: %w", err)
		}

		fmt.Println("Writing src/oidc-config.ts...")
		if err := writeFile(filepath.Join(projectDir, "src", "oidc-config.ts"), []byte(oidcConfigContents)); err != nil {
			return fmt.Errorf("write oidc-config.ts: %w", err)
		}

		fmt.Println("Writing src/RelayEnvironment.ts...")
		if err := writeFile(filepath.Join(projectDir, "src", "RelayEnvironment.ts"), []byte(relayEnvironmentContents)); err != nil {
			return fmt.Errorf("write RelayEnvironment.ts: %w", err)
		}

		fmt.Println("Done. You can now run:")
		fmt.Println("   pnpm install")
		fmt.Println("   pnpm dev")

		return nil
	},
}

func init() {
	rootCmd.AddCommand(spaCmd)
}

func ensureProjectDir(ctx context.Context, absAppDir, parentDir string) (string, error) {
	info, err := os.Stat(absAppDir)
	if err == nil {
		if !info.IsDir() {
			return "", fmt.Errorf("%s exists and is not a directory", absAppDir)
		}
		return absAppDir, nil
	}

	if !os.IsNotExist(err) {
		return "", fmt.Errorf("check app directory: %w", err)
	}

	fmt.Println("Scaffolding app...")
	before, err := listSubdirs(parentDir)
	if err != nil {
		return "", fmt.Errorf("list directories before scaffolding: %w", err)
	}

	if err := runCommand(ctx, parentDir, "pnpm", "create", "@tobiastengler/relay-app"); err != nil {
		return "", fmt.Errorf("run pnpm create: %w", err)
	}

	after, err := listSubdirs(parentDir)
	if err != nil {
		return "", fmt.Errorf("list directories after scaffolding: %w", err)
	}

	if info, err := os.Stat(absAppDir); err == nil && info.IsDir() {
		return absAppDir, nil
	}

	newDirs := diffNewDirs(before, after)
	switch len(newDirs) {
	case 0:
		return "", fmt.Errorf("could not detect new project directory; please rerun with explicit <app-dir>")
	case 1:
		return filepath.Join(parentDir, newDirs[0]), nil
	default:
		return "", fmt.Errorf("multiple new directories detected (%s); please rerun with explicit <app-dir>", strings.Join(newDirs, ", "))
	}
}

func ensureDirExists(path string) error {
	info, err := os.Stat(path)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", path)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return os.MkdirAll(path, 0o755)
}

func runCommand(ctx context.Context, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func listSubdirs(dir string) (map[string]struct{}, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	dirs := make(map[string]struct{})
	for _, entry := range entries {
		if entry.IsDir() {
			dirs[entry.Name()] = struct{}{}
		}
	}
	return dirs, nil
}

func diffNewDirs(before, after map[string]struct{}) []string {
	var result []string
	for name := range after {
		if _, ok := before[name]; !ok {
			result = append(result, name)
		}
	}
	sort.Strings(result)
	return result
}

type packageJSON struct {
	Dependencies    map[string]any `json:"dependencies"`
	DevDependencies map[string]any `json:"devDependencies"`
}

func readPackageJSON(path string) (packageJSON, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return packageJSON{}, err
	}
	var pkg packageJSON
	if err := json.Unmarshal(data, &pkg); err != nil {
		return packageJSON{}, err
	}
	return pkg, nil
}

func missingDependencies(pkg packageJSON, names []string) []string {
	var missing []string
	for _, name := range names {
		if !hasDependency(pkg, name) {
			missing = append(missing, name)
		}
	}
	return missing
}

func hasDependency(pkg packageJSON, name string) bool {
	if pkg.Dependencies != nil {
		if _, ok := pkg.Dependencies[name]; ok {
			return true
		}
	}
	if pkg.DevDependencies != nil {
		if _, ok := pkg.DevDependencies[name]; ok {
			return true
		}
	}
	return false
}
