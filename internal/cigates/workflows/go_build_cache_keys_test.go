package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-10486: setup-go's own cache key (setup-go-<os>-<arch>-go-<version>-<go.sum hash>) is shared by every workflow,
// and the first job to finish on main saves it. A job that compiled nothing saved 7 KB, and every GitHub-hosted go-quality
// leg then "hit" that empty entry, compiled everything cold, never saved, and internal/cigates/testshard ran up to its
// 600 s go test timeout. The go-quality legs therefore restore a cache of their own (one per leg, keyed on the leg, the
// runner, the Go version and go.sum), saved only by a push to main on a key miss; the jobs that compile little or nothing
// do not use setup-go's cache at all.

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
		{"the key loses the leg", mutation{gq, "key: go-quality-${{ matrix.name }}-", "key: go-quality-"}, "${{ matrix.name }}"},
		{"the key loses go.sum", mutation{gq, "-${{ hashFiles('**/go.sum') }}", ""}, "hashFiles"},
		{"restore on self-hosted too", mutation{gq, "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted'\n        uses: actions/cache/restore", "steps.relevance.outputs.relevant == 'true'\n        uses: actions/cache/restore"}, "GitHub-hosted legs only"},
		{"pull requests save", mutation{gq, "&& github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.go-cache", "&& steps.go-cache"}, "github.event_name == 'push'"},
		{"save on a hit", mutation{gq, " && steps.go-cache.outputs.cache-hit != 'true'", ""}, "cache-hit"},
		{"save a different key", mutation{gq, "key: ${{ steps.go-cache.outputs.cache-primary-key }}", "key: go-quality-other"}, "not the restore step's primary key"},
		{"save loses the module cache", mutation{gq, "            ~/go/pkg/mod\n          key: ${{ steps.go-cache", "          key: ${{ steps.go-cache"}, "paths differ"},
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
