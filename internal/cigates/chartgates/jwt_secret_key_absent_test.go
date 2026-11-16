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

// The HS256 access-token key is retired: no go-api, auth-service or query-api reads it. No deployment surface
// (chart, compose, CI and bigboy scripts, .env.example) carries JWT_SECRET_KEY, AUTH_JWT_SECRET_KEY or the
// hs256Secret values block, and the chart renders no env row, Secret key or checksum for it. There is no allowlist.

var hs256KeyName = regexp.MustCompile(`\b(AUTH_)?JWT_SECRET_KEY\b|\bhs256Secret(Name)?\b|dev-health\.hs256SecretName|checksum/hs256`)

// hs256Surfaces are the globs, relative to the repo root, of every deployment surface the key must not appear in.
var hs256Surfaces = []string{
	chartRel + "/templates/*",
	chartRel + "/values*.yaml",
	chartRel + "/values.schema.json",
	"compose.yml",
	"ci/*.yml", "ci/*/*.yml",
	"ci/*.sh", "ci/*/*.sh",
	"deploy/scripts/*.sh",
	"scripts/*.sh", "scripts/*.yaml",
	".env.example",
}

func hs256SurfaceFiles(g *guard) []string {
	seen := map[string]bool{}
	for _, pat := range hs256Surfaces {
		files, err := filepath.Glob(g.path(pat))
		if err != nil {
			g.failf("surface glob %s: %v", pat, err)
			continue
		}
		for _, f := range files {
			if info, err := os.Stat(f); err == nil && !info.IsDir() && !strings.HasSuffix(f, "_test.go") {
				seen[f] = true
			}
		}
	}
	// A directory under the templates that cannot be read in full must fail by name, not shrink the walk.
	_ = filepath.WalkDir(g.path(chartRel+"/templates"), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			rel, _ := filepath.Rel(g.root, p)
			g.failf("cannot read the directory %s in full: %v", filepath.ToSlash(rel), err)
			return nil
		}
		if !d.IsDir() && !strings.HasSuffix(p, "_test.go") {
			seen[p] = true
		}
		return nil
	})
	var out []string
	for f := range seen {
		out = append(out, f)
	}
	sort.Strings(out)
	return out
}

func noDeploymentSurfaceNamesTheHS256Key(g *guard) {
	files := hs256SurfaceFiles(g)
	g.expect(len(files) > 20, "the surface walk found %d files: the globs measure nothing", len(files))
	for _, f := range files {
		rel, _ := filepath.Rel(g.root, f)
		b, err := os.ReadFile(f)
		if err != nil {
			g.failf("cannot read %s: %v", filepath.ToSlash(rel), err)
			continue
		}
		for i, line := range strings.Split(string(b), "\n") {
			if m := hs256KeyName.FindString(line); m != "" {
				g.failf("no-deployment-surface-names-the-hs256-key: %s:%d names %s", filepath.ToSlash(rel), i+1, m)
			}
		}
	}
}

// hs256RenderArgs renders go-api, query-api and auth-service at phase 2 (the phase that carried the key rows).
var hs256RenderArgs = join(
	set(
		"goApi.enabled=true", "queryApi.enabled=true", "authService.phase=2",
		"authService.database.secretName=auth-db", "authService.signingKey.secretName=auth-key",
		"authService.signingKey.keyId=auth-2026-10", "authService.migrate.secretName=auth-migrate-db",
		"authService.migrate.secretKey=MIGRATION_URI", "authService.sessionKeys.successor.secretName=auth-session",
		"authService.valkey.secretName=auth-valkey", "authService.migrate.sessionReaders.goApi=devhealth_api",
		"authService.migrate.sessionReaders.queryApi=devhealth_query",
	),
	setJSON(
		`authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]`,
		`authService.networkPolicy.egress.valkey.to=[{"podSelector":{"matchLabels":{"app":"auth-valkey"}}}]`,
		`authService.trustedProxies=["10.8.0.0/28"]`,
		`queryApi.extraEnv=[{"name":"QUERY_API_DATABASE_ROLE","value":"devhealth_query"}]`,
	),
)

var hs256ProdLikeArgs = join(hs256RenderArgs, set("secrets.create=false", "secrets.externalSecretName=shared-ext"))

func hs256RenderCarriesNoKey(g *guard, label string, args []string) {
	r := g.tpl("hs256", args...)
	if r.code != 0 {
		g.failf("%s: render failed: %s", label, firstLines(r.stderr, 3))
		return
	}
	docs, err := parseDocs(r.stdout)
	if err != nil {
		g.failf("%s: render is not YAML: %v", label, err)
		return
	}
	g.expect(len(ofKind(docs, "Deployment")) > 0, "%s: no Deployment rendered, the check measures nothing", label)
	for _, d := range docs {
		who := kind(d) + "/" + name(d)
		if kind(d) == "Secret" && (strings.HasSuffix(name(d), "-hs256") || at(d, "stringData", "JWT_SECRET_KEY") != nil || at(d, "data", "JWT_SECRET_KEY") != nil) {
			g.failf("%s: render-carries-no-hs256-key: %s is the HS256 key Secret", label, who)
		}
		for k := range mp(at(d, "metadata", "annotations")) {
			if k == "checksum/hs256" {
				g.failf("%s: render-carries-no-hs256-key: %s has the annotation %s", label, who, k)
			}
		}
		for k := range mp(at(d, "spec", "template", "metadata", "annotations")) {
			if k == "checksum/hs256" {
				g.failf("%s: render-carries-no-hs256-key: %s has the pod annotation %s", label, who, k)
			}
		}
		for _, c := range containers(d) {
			for _, e := range list(c["env"]) {
				n, key := str(at(e, "name")), str(at(e, "valueFrom", "secretKeyRef", "key"))
				if n == "JWT_SECRET_KEY" || n == "AUTH_JWT_SECRET_KEY" || key == "JWT_SECRET_KEY" {
					g.failf("%s: render-carries-no-hs256-key: %s container %s has env %s (key %q)", label, who, str(c["name"]), n, key)
				}
			}
		}
	}
}

func TestNoDeploymentCarriesTheHS256KeyRealTree(t *testing.T) {
	root := realRoot(t)
	t.Run("no-deployment-surface-names-the-hs256-key", func(t *testing.T) {
		g := &guard{root: root, tmp: t.TempDir()}
		noDeploymentSurfaceNamesTheHS256Key(g)
		if len(g.v) > 0 {
			t.Fatalf("%d violation(s):\n  %s", len(g.v), strings.Join(g.v, "\n  "))
		}
	})
	t.Run("render-carries-no-hs256-key", func(t *testing.T) {
		g := &guard{root: root, tmp: t.TempDir()}
		hs256RenderCarriesNoKey(g, "default", hs256RenderArgs)
		if len(g.v) > 0 {
			t.Fatalf("%d violation(s):\n  %s", len(g.v), strings.Join(g.v, "\n  "))
		}
	})
}

// A prod-shape render (external shared Secret, no hs256Secret, the key absent everywhere) must render.
func TestProdLikeRenderNeedsNoHS256Secret(t *testing.T) {
	g := &guard{root: realRoot(t), tmp: t.TempDir()}
	hs256RenderCarriesNoKey(g, "prod-like", hs256ProdLikeArgs)
	if len(g.v) > 0 {
		t.Fatalf("%d violation(s):\n  %s", len(g.v), strings.Join(g.v, "\n  "))
	}
}

func hs256Surface(g *guard) { noDeploymentSurfaceNamesTheHS256Key(g) }

func TestNoDeploymentCarriesTheHS256KeyPlanted(t *testing.T) {
	surfaces := []string{chartRel, "compose.yml", "ci", "deploy/scripts", "scripts", ".env.example"}
	const key = "JWT_SECRET_KEY"
	cases := []planted{
		{"key-line-in-a-chart-template", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, chartRel+"/templates/zz-planted.yaml", "      env:\n        - name: "+key+"\n")
		}, hs256Surface, "zz-planted.yaml:2"},
		{"auth-key-line-in-a-chart-template", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, chartRel+"/templates/zz-planted.yaml", "- name: AUTH_"+key+"\n")
		}, hs256Surface, "names AUTH_JWT_SECRET_KEY"},
		{"hs256secret-block-in-values", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, chartRel+"/values.zz.yaml", "hs256Secret:\n  externalSecretName: x\n")
		}, hs256Surface, "values.zz.yaml:1"},
		{"key-line-in-compose", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "compose.yml", "x:\n  "+key+": ${"+key+"}\n")
		}, hs256Surface, "compose.yml:2"},
		{"key-line-in-a-ci-yml", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "ci/zz.yml", "e: ${"+key+"}\n")
		}, hs256Surface, "ci/zz.yml:1"},
		{"key-line-in-a-ci-bigboy-yml", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "ci/bigboy/compose.bigboy.yml", "e: ${"+key+"}\n")
		}, hs256Surface, "ci/bigboy/compose.bigboy.yml:1"},
		{"key-line-in-a-ci-sh", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "ci/zz.sh", "export "+key+"=x\n")
		}, hs256Surface, "ci/zz.sh:1"},
		{"key-line-in-a-ci-subdir-sh", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "ci/bigboy/zz.sh", "export "+key+"=x\n")
		}, hs256Surface, "ci/bigboy/zz.sh:1"},
		{"key-line-in-env-example", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, ".env.example", "# "+key+"=x\n")
		}, hs256Surface, ".env.example:1"},
		{"key-line-in-a-scripts-sh", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, "scripts/zz.sh", "echo "+key+"\n")
		}, hs256Surface, "scripts/zz.sh:1"},
		{"checksum-annotation-in-a-template", surfaces, func(t *testing.T, root string) {
			writeFile(t, root, chartRel+"/templates/zz-planted.yaml", "checksum/hs256: x\n")
		}, hs256Surface, "names checksum/hs256"},
		{"empty-tree-measures-nothing", nil, func(t *testing.T, root string) {}, hs256Surface, "measure nothing"},
	}
	runPlanted(t, cases)
}

func TestNoDeploymentCarriesTheHS256KeyUnreadableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root: a mode-0 directory is still readable")
	}
	root := t.TempDir()
	copyPath(t, realRoot(t), root, chartRel+"/templates")
	dir := filepath.Join(root, filepath.FromSlash(chartRel+"/templates/zz-closed"))
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	g := &guard{root: root, tmp: t.TempDir()}
	hs256SurfaceFiles(g)
	for _, v := range g.v {
		if strings.Contains(v, "zz-closed") {
			return
		}
	}
	t.Fatalf("an unreadable templates directory was not reported by name: %v", g.v)
}
