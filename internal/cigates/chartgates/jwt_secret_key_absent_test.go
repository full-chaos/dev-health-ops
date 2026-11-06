package chartgates_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The HS256 access-token key is retired: no deployment surface may carry it, by env name, Secret key or
// secretKeyRef key, in any spelling a service ever read (JWT_SECRET_KEY, AUTH_JWT_SECRET_KEY,
// GO_API_EDGE_JWT_SECRET, each with its _FILE form).
var retiredHS256Key = regexp.MustCompile(`(^|[^A-Z0-9_])((AUTH_)?JWT_SECRET_KEY|GO_API_EDGE_JWT_SECRET)(_FILE)?([^A-Z0-9_]|$)`)

// deploymentSurfaces are the deployment files read as text: the chart's templates and values, every
// compose file and override, the go-api deploy files, the Dockerfiles and the workflows.
func deploymentSurfaces(g *guard) []string {
	var out []string
	add := func(rel string) {
		if _, err := os.Stat(g.path(rel)); err == nil {
			out = append(out, rel)
		}
	}
	walk := func(dir string, keep func(string) bool) {
		root := g.path(dir)
		if _, err := os.Stat(root); err != nil {
			g.failf("deployment surface %s is missing: the gate would read nothing there", dir)
			return
		}
		_ = filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			rel, _ := filepath.Rel(g.root, p)
			rel = filepath.ToSlash(rel)
			if keep(rel) {
				out = append(out, rel)
			}
			return nil
		})
	}
	walk(chartFile("templates"), func(string) bool { return true })
	add(chartFile("values.yaml"))
	add(chartFile("values.schema.json"))
	for _, m := range globAll(g, "compose*.yml", "ci/*.yml", "ci/*/*.yml", "deploy/go-api/*", "docker/*Dockerfile*", ".github/workflows/*.yml") {
		add(m)
	}
	sort.Strings(out)
	return out
}

func globAll(g *guard, patterns ...string) []string {
	var out []string
	for _, p := range patterns {
		matches, _ := filepath.Glob(g.path(p))
		for _, m := range matches {
			rel, _ := filepath.Rel(g.root, m)
			out = append(out, filepath.ToSlash(rel))
		}
	}
	return out
}

func noDeploymentSurfaceNamesTheHS256Key(g *guard) {
	files := deploymentSurfaces(g)
	g.expect(len(files) > 20, "only %d deployment files found: the gate reads too little to mean anything", len(files))
	for _, rel := range files {
		for i, line := range strings.Split(g.read(rel), "\n") {
			if m := retiredHS256Key.FindStringSubmatch(line); m != nil {
				g.failf("%s:%d names the retired HS256 key %s", rel, i+1, m[2]+m[4])
			}
		}
	}
}

// hs256Renders are the chart renders the rendered check reads: the default, and go-api, query-api and
// auth-service at phases 1 and 2 (phase 2 mounts the session routes).
var hs256Renders = map[string][]string{
	"default": nil,
	"phase1": {
		"--set", "goApi.enabled=true", "--set", "queryApi.enabled=true",
		"--set", "authService.phase=1",
		"--set", "authService.database.secretName=auth-db",
		"--set", "authService.signingKey.secretName=auth-key",
		"--set", "authService.signingKey.keyId=auth-2026-10",
		"--set", "authService.migrate.secretName=auth-migrate-db",
		"--set", "authService.migrate.secretKey=MIGRATION_URI",
		"--set", "authService.migrate.sessionReaders.goApi=devhealth_api",
		"--set-json", `authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]`,
	},
}

func init() {
	hs256Renders["phase2"] = append(append([]string{}, hs256Renders["phase1"]...),
		"--set", "authService.phase=2",
		"--set", "authService.migrate.sessionReaders.queryApi=devhealth_query",
		"--set-json", `queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"}]`,
		"--set", "authService.sessionKeys.successor.secretName=auth-session",
		"--set", "authService.valkey.secretName=auth-valkey",
		"--set-json", `authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]`,
		"--set-json", `authService.trustedProxies=["10.8.0.0/28"]`,
	)
}

func noRenderedWorkloadCarriesTheHS256Key(g *guard) {
	names := make([]string, 0, len(hs256Renders))
	for n := range hs256Renders {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		docs := g.docs("dev-health", hs256Renders[n]...)
		g.expect(len(ofKind(docs, "Deployment")) > 0, "render %s has no Deployment: the rendered check reads nothing", n)
		for _, d := range docs {
			where := n + " " + kind(d) + "/" + name(d)
			for _, sec := range []string{"data", "stringData"} {
				for k := range mp(d[sec]) {
					if retiredHS256Key.MatchString(k) {
						g.failf("%s: Secret key %s is the retired HS256 key", where, k)
					}
				}
			}
			for _, c := range append(containers(d), initContainers(d)...) {
				for _, e := range list(c["env"]) {
					env := mp(e)
					if retiredHS256Key.MatchString(str(env["name"])) {
						g.failf("%s container %s: env %s is the retired HS256 key", where, str(c["name"]), str(env["name"]))
					}
					if key := str(at(env, "valueFrom", "secretKeyRef", "key")); retiredHS256Key.MatchString(key) {
						g.failf("%s container %s: env %s reads Secret key %s", where, str(c["name"]), str(env["name"]), key)
					}
				}
			}
		}
	}
}

func TestNoDeploymentCarriesTheHS256KeyRealTree(t *testing.T) {
	runReal(t,
		check{"no-deployment-surface-names-the-hs256-key", noDeploymentSurfaceNamesTheHS256Key},
		check{"no-rendered-workload-carries-the-hs256-key", noRenderedWorkloadCarriesTheHS256Key},
	)
}

func TestNoDeploymentCarriesTheHS256KeyPlantedDefects(t *testing.T) {
	const goAPI = "templates/go-api-deployment.yaml"
	surfaces := []string{chartRel, "compose.yml", "ci/auth_compose_proof.override.yml", ".github/workflows/go-auth-service.yml"}
	envBlock := "          env:\n"
	runPlanted(t, []planted{
		{"compose-regains-the-key", surfaces, appendText("compose.yml", "\n# JWT_SECRET_KEY=x\n"), noDeploymentSurfaceNamesTheHS256Key, "compose.yml"},
		{"workflow-regains-the-file-form", surfaces, appendText(".github/workflows/go-auth-service.yml", "\n# AUTH_JWT_SECRET_KEY_FILE\n"), noDeploymentSurfaceNamesTheHS256Key, "AUTH_JWT_SECRET_KEY_FILE"},
		{"query-api-edge-secret-in-a-compose-override", surfaces, appendText("ci/auth_compose_proof.override.yml", "\n# GO_API_EDGE_JWT_SECRET\n"), noDeploymentSurfaceNamesTheHS256Key, "GO_API_EDGE_JWT_SECRET"},
		{"go-api-template-regains-the-env", chartOnly,
			mut(chartFile(goAPI), envBlock, envBlock+"            - {name: JWT_SECRET_KEY, valueFrom: {secretKeyRef: {name: s, key: k}}}\n"),
			noRenderedWorkloadCarriesTheHS256Key, "env JWT_SECRET_KEY"},
		{"go-api-env-reads-the-secret-key", chartOnly,
			mut(chartFile(goAPI), envBlock, envBlock+"            - {name: SOME_KEY, valueFrom: {secretKeyRef: {name: s, key: JWT_SECRET_KEY}}}\n"),
			noRenderedWorkloadCarriesTheHS256Key, "reads Secret key JWT_SECRET_KEY"},
	})
}
