package workflows_test

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-10486: setup-go's own cache key (setup-go-<os>-<arch>-go-<version>-<go.sum hash>) is shared by every workflow,
// and the first job to finish on main saves it. A job that compiled nothing saved 7 KB, and every GitHub-hosted go-quality
// leg then "hit" that empty entry, compiled everything cold, never saved, and internal/cigates/testshard ran up to its
// 600 s go test timeout. The go-quality legs therefore restore a cache of their own (one per leg, keyed on the leg, the
// runner, the Go version and go.sum), saved only by a push to main on a key miss; the jobs that compile little or nothing
// do not use setup-go's cache at all.

var cacheLayoutRE = regexp.MustCompile(`^go-quality-v([0-9]+)-`)

const minCacheLayout = 2

var goCacheWorkflows = []string{
	".github/workflows/go-quality.yml",
	".github/workflows/go-auth-service.yml",
	".github/workflows/go.yml",
	".github/workflows/docs-cloudflare.yml",
	".github/workflows/auth-k8s-proof.yml",
}

// noSetupGoCacheJobs are the jobs (workflow file, job) whose setup-go must not use its shared cache.
var noSetupGoCacheJobs = [][2]string{
	{"go-auth-service.yml", "auth-changes"},
	{"go.yml", "graphql-wire-parity"},
	{"docs-cloudflare.yml", "preview-or-build"},
	{"docs-cloudflare.yml", "deploy-production"},
	{"auth-k8s-proof.yml", "auth-k8s-proof"},
}

func usesAction(step yamlMap, action string) bool {
	return strings.HasPrefix(asString(step["uses"]), action+"@")
}

func goCacheViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	for _, jf := range noSetupGoCacheJobs {
		steps := stepsOf(jobsOf(loadWf(t, root, jf[0]))[jf[1]])
		found := false
		for _, s := range steps {
			if !usesAction(s, "actions/setup-go") {
				continue
			}
			found = true
			if c, ok := asMap(s["with"])["cache"].(bool); !ok || c {
				add("%s:%s: setup-go must set `cache: false` (its shared key is the one an empty job saved)", jf[0], jf[1])
			}
		}
		if !found {
			add("%s:%s: no setup-go step", jf[0], jf[1])
		}
	}

	steps := stepsOf(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	setupAt, restoreAt, legAt, saveAt := -1, -1, -1, -1
	var restore, save yamlMap
	for i, s := range steps {
		switch {
		case usesAction(s, "actions/setup-go"):
			setupAt = i
			if c, ok := asMap(s["with"])["cache"].(bool); !ok || c {
				add("go-quality.yml:go-quality-leg: setup-go must set `cache: false`")
			}
		case usesAction(s, "actions/cache/restore"):
			restoreAt, restore = i, s
		case usesAction(s, "actions/cache/save"):
			saveAt, save = i, s
		case strings.Contains(asString(s["run"]), "bash ci/check_go.sh ci-leg ${{ matrix.args }}"):
			legAt = i
		}
	}
	if setupAt < 0 || legAt < 0 {
		add("go-quality.yml:go-quality-leg: the setup-go step or the leg step is missing")
		return out
	}
	paths := func(s yamlMap) string { return asString(asMap(s["with"])["path"]) }
	if restore == nil {
		add("go-quality.yml:go-quality-leg: no actions/cache/restore step (the hosted legs restore their own Go cache)")
	} else {
		w := asMap(restore["with"])
		key, rk, cond := asString(w["key"]), asString(w["restore-keys"]), asString(restore["if"])
		if restoreAt < setupAt || restoreAt > legAt {
			add("go-quality.yml: the cache restore must run after setup-go and before the leg")
		}
		for _, part := range []string{"go-quality-", "${{ matrix.name }}", "${{ runner.os }}", "${{ runner.arch }}", "steps.setup-go.outputs.go-version", "hashFiles('**/go.sum')"} {
			if !strings.Contains(key, part) {
				add("go-quality.yml: the restore key %q does not hold %s", key, part)
			}
		}
		// A key is immutable and the save runs only on a key miss, so a main entry built into the wrong directory (before
		// DEV_HEALTH_GO_CACHE was exported) would be restored for ever and never re-saved. The layout segment retires it:
		// bump it whenever the cached directory layout changes. v1 is the /tmp layout; the first fixed layout is v2.
		if m := cacheLayoutRE.FindStringSubmatch(key); m == nil {
			add("go-quality.yml: the restore key %q has no layout-version segment go-quality-v<N>-", key)
		} else if n, _ := strconv.Atoi(m[1]); n < minCacheLayout {
			add("go-quality.yml: the restore key %q has layout-version v%d, want v%d or later (earlier entries were built into /tmp)", key, n, minCacheLayout)
		}
		if prefix, _, ok := strings.Cut(key, "${{ hashFiles"); !ok || strings.TrimSpace(rk) != prefix {
			add("go-quality.yml: restore-keys must be the key without its go.sum hash (got %q)", rk)
		}
		if !strings.Contains(cond, "runner.environment == 'github-hosted'") {
			add("go-quality.yml: the cache restore must run on GitHub-hosted legs only (if: %q)", cond)
		}
		if asString(restore["id"]) == "" {
			add("go-quality.yml: the cache restore needs an id (the save reads its primary key)")
		}
		for _, p := range []string{"~/.cache/go-build", "~/go/pkg/mod"} {
			if !strings.Contains(paths(restore), p) {
				add("go-quality.yml: the cache restore does not hold %s", p)
			}
		}
	}
	// check_go.sh builds into DEV_HEALTH_GO_CACHE, else an inherited GOCACHE, else a configured one, else /tmp/dev-health-go-build-cache.
	// A hosted runner's GOCACHE is the unset default (~/.cache/go-build), so without an export the restored cache is never read.
	exportAt := -1
	var export yamlMap
	for i, s := range steps {
		if strings.Contains(shellCode(asString(s["run"])), "DEV_HEALTH_GO_CACHE=") {
			exportAt, export = i, s
		}
	}
	if restore != nil {
		if export == nil {
			add("go-quality.yml:go-quality-leg: no step exports DEV_HEALTH_GO_CACHE: check_go.sh builds into /tmp/dev-health-go-build-cache and never reads the restored cache")
		} else {
			run, cond := shellCode(asString(export["run"])), asString(export["if"])
			if exportAt > legAt {
				add("go-quality.yml: DEV_HEALTH_GO_CACHE is exported after the leg step, so check_go.sh never sees it")
			}
			if !strings.Contains(cond, "runner.environment == 'github-hosted'") {
				add("go-quality.yml: the DEV_HEALTH_GO_CACHE export must run on GitHub-hosted legs only (if: %q); the pool keeps its hostPath", cond)
			}
			if !strings.Contains(run, `>> "${GITHUB_ENV}"`) {
				add("go-quality.yml: the DEV_HEALTH_GO_CACHE export must append to GITHUB_ENV so the leg step inherits it")
			}
			for _, p := range strings.Fields(paths(restore)) {
				if !strings.HasSuffix(p, "/.cache/go-build") {
					continue
				}
				want := "DEV_HEALTH_GO_CACHE=${HOME}" + strings.TrimPrefix(p, "~")
				if strings.Contains(run, "DEV_HEALTH_GO_CACHE=~") {
					add("go-quality.yml: DEV_HEALTH_GO_CACHE is a literal ~ the shell does not expand: use ${HOME}")
				} else if !strings.Contains(run, want+`"`) {
					add("go-quality.yml: DEV_HEALTH_GO_CACHE is not the restored path %s (want %s)", p, want)
				}
			}
		}
	}
	if save == nil {
		add("go-quality.yml:go-quality-leg: no actions/cache/save step (a push to main saves the leg's cache)")
	} else {
		cond := asString(save["if"])
		if saveAt < legAt {
			add("go-quality.yml: the cache save must run after the leg")
		}
		for _, part := range []string{"!cancelled()", "github.event_name == 'push'", "github.ref == 'refs/heads/main'", "runner.environment == 'github-hosted'", ".outputs.cache-hit != 'true'"} {
			if !strings.Contains(cond, part) {
				add("go-quality.yml: the cache save condition %q does not hold %s", cond, part)
			}
		}
		if restore != nil {
			id := asString(restore["id"])
			if got := asString(asMap(save["with"])["key"]); got != "${{ steps."+id+".outputs.cache-primary-key }}" {
				add("go-quality.yml: the cache save key %q is not the restore step's primary key", got)
			}
			if paths(save) != paths(restore) {
				add("go-quality.yml: the cache save and restore paths differ")
			}
			if !strings.Contains(cond, "steps."+id+".outputs.cache-hit") {
				add("go-quality.yml: the cache save does not read the restore step's cache-hit")
			}
		}
	}
	return out
}

func TestGoQualityLegsUseTheirOwnGoCacheAndNoJobSavesTheSharedKeyEmpty(t *testing.T) {
	expectNoViolations(t, goCacheViolations(t, rootOf(t)))
}

func TestGoCacheGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	cases := []struct {
		name string
		m    mutation
		want string
	}{
		{"go-quality back on setup-go's cache", mutation{gq, "          cache: false\n\n      - name: Restore this leg's", "          cache: true\n\n      - name: Restore this leg's"}, "go-quality-leg: setup-go must set `cache: false`"},
		{"the key loses the leg", mutation{gq, "key: go-quality-v2-${{ matrix.name }}-", "key: go-quality-v2-"}, "${{ matrix.name }}"},
		{"the key loses go.sum", mutation{gq, "-${{ hashFiles('**/go.sum') }}", ""}, "hashFiles"},
		{"restore on self-hosted too", mutation{gq, "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted'\n        uses: actions/cache/restore", "steps.relevance.outputs.relevant == 'true'\n        uses: actions/cache/restore"}, "GitHub-hosted legs only"},
		{"pull requests save", mutation{gq, "&& github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.go-cache", "&& steps.go-cache"}, "github.event_name == 'push'"},
		{"save on a hit", mutation{gq, " && steps.go-cache.outputs.cache-hit != 'true'", ""}, "cache-hit"},
		{"save a different key", mutation{gq, "key: ${{ steps.go-cache.outputs.cache-primary-key }}", "key: go-quality-other"}, "not the restore step's primary key"},
		{"save loses the module cache", mutation{gq, "            ~/go/pkg/mod\n          key: ${{ steps.go-cache", "          key: ${{ steps.go-cache"}, "paths differ"},
		{"no step exports the restored dir", mutation{gq, `echo "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build" >> "${GITHUB_ENV}"`, `echo "X=1" >> "${GITHUB_ENV}"`}, "no step exports DEV_HEALTH_GO_CACHE"},
		{"the export names another dir", mutation{gq, "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build", "DEV_HEALTH_GO_CACHE=${RUNNER_TEMP}/go-build"}, "is not the restored path"},
		{"the export is a literal tilde", mutation{gq, "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build", "DEV_HEALTH_GO_CACHE=~/.cache/go-build"}, "literal ~"},
		{"the export is on the pool path only", mutation{gq, "&& runner.environment == 'github-hosted'\n        run: echo \"DEV_HEALTH_GO_CACHE", "&& vars.SELF_HOSTED_RUNNERS == 'enabled'\n        run: echo \"DEV_HEALTH_GO_CACHE"}, "GitHub-hosted legs only"},
		{"the export is set after check_go.sh", mutation{gq, "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n", "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n\n      - name: late export\n        if: runner.environment == 'github-hosted'\n        run: echo \"DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build\" >> \"${GITHUB_ENV}\"\n"}, "after the leg step"},
		{"the layout segment is back to v1", mutation{gq, "go-quality-v2-", allMarker + "go-quality-v1-"}, "layout-version v1"},
		{"the key has no layout segment", mutation{gq, "go-quality-v2-", allMarker + "go-quality-"}, "no layout-version segment"},
		{"restore-keys keeps the old layout", mutation{gq, "restore-keys: |\n            go-quality-v2-", "restore-keys: |\n            go-quality-"}, "restore-keys must be the key without its go.sum hash"},
		{"auth-changes back on the shared cache", mutation{".github/workflows/go-auth-service.yml", "          cache: false", "          cache: true"}, "go-auth-service.yml:auth-changes"},
		{"graphql-wire-parity back on the shared cache", mutation{".github/workflows/go.yml", "empty entry.\n          cache: false\n\n      - uses: pnpm/action-setup", "empty entry.\n          cache: true\n\n      - uses: pnpm/action-setup"}, "go.yml:graphql-wire-parity"},
		{"docs preview back on the shared cache", mutation{".github/workflows/docs-cloudflare.yml", "          cache: false", "          cache: true"}, "docs-cloudflare.yml:preview-or-build"},
		{"auth-k8s-proof back on the shared cache", mutation{".github/workflows/auth-k8s-proof.yml", "          cache: false", "          cache: true"}, "auth-k8s-proof.yml:auth-k8s-proof"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			planted := plantTree(t, root, goCacheWorkflows, c.m)
			expectViolation(t, goCacheViolations(t, planted), c.want)
		})
	}
}
