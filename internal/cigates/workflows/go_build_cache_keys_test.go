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
// 600 s go test timeout. The jobs that compile little or nothing do not use setup-go's cache at all.
//
// CHAOS-13718: one cache per leg (18 legs x ~1.5 GB) passed the 10 GB Actions cache cap, so the legs evicted each other on
// main. The go-quality Go cache is now two families: build (static, plan-reads, the test legs) and race (the race legs),
// one entry per family, go.sum and UTC day. The legs only restore; the go-quality-cache job is the only writer of a
// go-quality- key, on a push to main, and its key is the legs' key with the family as its matrix value.

var cacheLayoutRE = regexp.MustCompile(`^go-quality-v([0-9]+)-`)

const minCacheLayout = 3

const hostedOnlyIf = "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted'"

const cacheActionSHA = "55cc8345863c7cc4c66a329aec7e433d2d1c52a9"

// legFamily is the leg's cache family; saverFamily is the same family as the saver job's matrix value.
const (
	legFamily        = "${{ startsWith(matrix.name, 'race-') && 'race' || 'build' }}"
	saverFamily      = "${{ matrix.family }}"
	legV2Fallback    = "go-quality-v2-${{ startsWith(matrix.name, 'race-') && 'race' || 'test' }}-"
	cacheDayRef      = "${{ needs.go-quality-plan.outputs.cache_day }}"
	saverJob         = "go-quality-cache"
	saverIf          = "github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'"
	saverFillRun     = "bash ci/check_go.sh ci-leg-warm all ${{ matrix.family }}"
	goSumHashSegment = "${{ hashFiles('**/go.sum') }}"
)

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
	{"docs-cloudflare.yml", "smoke-production"},
	{"auth-k8s-proof.yml", "auth-k8s-proof"},
}

var goVersionStepRef = regexp.MustCompile(`steps\.([A-Za-z0-9_-]+)\.outputs\.go-version`)

var primaryKeyRef = regexp.MustCompile(`^\$\{\{ steps\.([A-Za-z0-9_-]+)\.outputs\.cache-primary-key \}\}$`)

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

	pinned := func(action string, s yamlMap) {
		if got := asString(s["uses"]); got != action+"@"+cacheActionSHA {
			add("go-quality.yml: %s must be pinned to %s (got %q)", action, cacheActionSHA, got)
		}
	}
	paths := func(s yamlMap) string { return asString(asMap(s["with"])["path"]) }
	isReach := func(s yamlMap) bool { return strings.Contains(paths(s), "plan-reads-reach") }
	rkLines := func(s yamlMap) []string {
		var out []string
		for _, l := range strings.Split(asString(asMap(s["with"])["restore-keys"]), "\n") {
			if l = strings.TrimSpace(l); l != "" {
				out = append(out, l)
			}
		}
		return out
	}

	// No job but the saver writes a go-quality- key, in any workflow (a save keyed on a restore step's primary key is
	// judged by that restore step's key). The legs restore only: one save per leg is what passed the 10 GB cap.
	for _, wf := range workflowNames(t, root) {
		jobs := jobsOf(loadWf(t, root, wf))
		for _, job := range sortedKeys(jobs) {
			steps := stepsOf(jobs[job])
			ids := map[string]yamlMap{}
			for _, s := range steps {
				if id := asString(s["id"]); id != "" {
					ids[id] = s
				}
			}
			for _, s := range steps {
				if !usesAction(s, "actions/cache/save") && !usesAction(s, "actions/cache") {
					continue
				}
				key := asString(asMap(s["with"])["key"])
				if m := primaryKeyRef.FindStringSubmatch(key); m != nil {
					key = asString(asMap(ids[m[1]]["with"])["key"])
				}
				if !strings.HasPrefix(key, "go-quality-") {
					continue
				}
				if wf != "go-quality.yml" || job != saverJob {
					add("%s: job %s writes the go-quality Go cache key %q: only go-quality.yml job %s saves the go-quality families", wf, job, key, saverJob)
				}
			}
		}
	}

	doc := loadWf(t, root, "go-quality.yml")
	if asString(asMap(asMap(jobsOf(doc)["go-quality-plan"])["outputs"])["cache_day"]) != "${{ steps.cache-day.outputs.day }}" {
		add("go-quality.yml:go-quality-plan: no output cache_day = ${{ steps.cache-day.outputs.day }} (the UTC day that names the cache generation)")
	}
	steps := stepsOf(jobsOf(doc)["go-quality-leg"])
	setupAt, restoreAt, legAt := -1, -1, -1
	var restore yamlMap
	for i, s := range steps {
		switch {
		case usesAction(s, "actions/setup-go"):
			setupAt = i
			if c, ok := asMap(s["with"])["cache"].(bool); !ok || c {
				add("go-quality.yml:go-quality-leg: setup-go must set `cache: false`")
			}
		case usesAction(s, "actions/cache/restore") && !isReach(s):
			restoreAt, restore = i, s
			pinned("actions/cache/restore", s)
		case usesAction(s, "actions/cache/save") && !isReach(s):
			add("go-quality.yml:go-quality-leg: a leg saves a Go cache (only go-quality-cache saves the go-quality families)")
		case strings.Contains(asString(s["run"]), "bash ci/check_go.sh ci-leg ${{ matrix.args }}"):
			legAt = i
		}
	}
	if setupAt < 0 || legAt < 0 {
		add("go-quality.yml:go-quality-leg: the setup-go step or the leg step is missing")
		return out
	}
	checkGoVersionRef := func(job string, steps []yamlMap, key string) {
		m := goVersionStepRef.FindStringSubmatch(key)
		if m == nil {
			add("go-quality.yml:%s: the cache key %q does not hold steps.<id>.outputs.go-version", job, key)
			return
		}
		found := false
		for _, st := range steps {
			if asString(st["id"]) != m[1] {
				continue
			}
			found = true
			if !usesAction(st, "actions/setup-go") {
				add("go-quality.yml: step id %q is not actions/setup-go (uses %q)", m[1], asString(st["uses"]))
			}
		}
		if !found {
			add("go-quality.yml: the restore key reads steps.%s.outputs.go-version but there is no step with id %q", m[1], m[1])
		}
	}
	legKey := ""
	if restore == nil {
		add("go-quality.yml:go-quality-leg: no actions/cache/restore step (the hosted legs restore their family Go cache)")
	} else {
		w := asMap(restore["with"])
		key, cond := asString(w["key"]), asString(restore["if"])
		legKey = key
		if restoreAt < setupAt || restoreAt > legAt {
			add("go-quality.yml: the cache restore must run after setup-go and before the leg")
		}
		if strings.Contains(key, "${{ matrix.name }}") {
			add("go-quality.yml: the restore key %q is per leg (${{ matrix.name }}): the legs restore a family key", key)
		}
		for _, part := range []string{"go-quality-", legFamily, "${{ runner.os }}", "${{ runner.arch }}", goSumHashSegment, cacheDayRef} {
			if !strings.Contains(key, part) {
				add("go-quality.yml: the restore key %q does not hold %s", key, part)
			}
		}
		// A key is immutable, so a layout change needs a new segment. v1 was the /tmp layout, v2 one entry per leg, v3 the families.
		if m := cacheLayoutRE.FindStringSubmatch(key); m == nil {
			add("go-quality.yml: the restore key %q has no layout-version segment go-quality-v<N>-", key)
		} else if n, _ := strconv.Atoi(m[1]); n < minCacheLayout {
			add("go-quality.yml: the restore key %q has layout-version v%d, want v%d or later (v2 was one entry per leg)", key, n, minCacheLayout)
		}
		checkGoVersionRef("go-quality-leg", steps, key)
		// restore-keys: the key without its day (newest generation of this go.sum), then without its go.sum hash (newest
		// generation of any go.sum), then optionally the v2 per-leg fallback of the same paths (the switch to v3).
		day, _, okDay := strings.Cut(key, cacheDayRef)
		sum, _, okSum := strings.Cut(key, goSumHashSegment)
		rk := rkLines(restore)
		if !okDay || len(rk) < 1 || rk[0] != day {
			add("go-quality.yml: restore-keys line 1 must be the key without its day %q (got %q)", day, rk)
		}
		if !okSum || len(rk) < 2 || rk[1] != sum {
			add("go-quality.yml: restore-keys line 2 must be the key without its go.sum hash %q (got %q)", sum, rk)
		}
		if len(rk) > 3 || (len(rk) == 3 && rk[2] != legV2Fallback) {
			add("go-quality.yml: restore-keys may end only with the v2 transition fallback %q (got %q)", legV2Fallback, rk)
		}
		if strings.TrimSpace(cond) != hostedOnlyIf {
			add("go-quality.yml: the cache restore must run on GitHub-hosted legs only, if: exactly %q (got %q)", hostedOnlyIf, cond)
		}
		if asString(restore["id"]) == "" {
			add("go-quality.yml: the cache restore needs an id")
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
			if strings.TrimSpace(cond) != hostedOnlyIf {
				add("go-quality.yml: the DEV_HEALTH_GO_CACHE export must run on GitHub-hosted legs only, if: exactly %q (got %q); the pool keeps its hostPath", hostedOnlyIf, cond)
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

	// The saver: one entry per family and day, on a push to main only, never on a same-day hit, only after a whole fill.
	saver := asMap(jobsOf(doc)[saverJob])
	if saver == nil {
		add("go-quality.yml: no job %s (the only writer of the go-quality families)", saverJob)
		return out
	}
	if got := strings.TrimSpace(asString(saver["if"])); got != saverIf {
		add("go-quality.yml:%s: the saver job if must be exactly %q (got %q)", saverJob, saverIf, got)
	}
	if asString(saver["runs-on"]) != "ubuntu-latest" {
		add("go-quality.yml:%s: runs-on must be ubuntu-latest (the entries serve the GitHub-hosted legs)", saverJob)
	}
	if c, ok := saver["continue-on-error"].(bool); !ok || !c {
		add("go-quality.yml:%s: must be continue-on-error: true (not a gate)", saverJob)
	}
	var fams []string
	for _, f := range asList(asMap(asMap(saver["strategy"])["matrix"])["family"]) {
		fams = append(fams, asString(f))
	}
	if strings.Join(fams, ",") != "build,race" {
		add("go-quality.yml:%s: the matrix family must be exactly [build, race] (got %v): two families keep the go-quality caches under the cap", saverJob, fams)
	}
	needsPlan := false
	for _, n := range asList(saver["needs"]) {
		needsPlan = needsPlan || asString(n) == "go-quality-plan"
	}
	if !needsPlan {
		add("go-quality.yml:%s: must need go-quality-plan (its cache_day output)", saverJob)
	}
	sSteps := stepsOf(saver)
	var lookup, sRestore, fill, sSave, sExport yamlMap
	at := map[string]int{}
	for i, s := range sSteps {
		switch {
		case usesAction(s, "actions/setup-go"):
			at["setup"] = i
			if c, ok := asMap(s["with"])["cache"].(bool); !ok || c {
				add("go-quality.yml:%s: setup-go must set `cache: false`", saverJob)
			}
		case usesAction(s, "actions/cache/restore") && asString(s["id"]) == "lookup":
			lookup, at["lookup"] = s, i
			pinned("actions/cache/restore", s)
		case usesAction(s, "actions/cache/restore"):
			sRestore, at["restore"] = s, i
			pinned("actions/cache/restore", s)
		case usesAction(s, "actions/cache/save"):
			sSave, at["save"] = s, i
			pinned("actions/cache/save", s)
		case strings.Contains(asString(s["run"]), "ci-leg-warm"):
			fill, at["fill"] = s, i
		case strings.Contains(shellCode(asString(s["run"])), "DEV_HEALTH_GO_CACHE="):
			sExport, at["export"] = s, i
		}
	}
	if lookup == nil || sRestore == nil || fill == nil || sSave == nil || sExport == nil {
		add("go-quality.yml:%s: needs a lookup (id lookup), a restore, the DEV_HEALTH_GO_CACHE export, the fill and a save step", saverJob)
		return out
	}
	if !(at["setup"] < at["lookup"] && at["lookup"] < at["restore"] && at["restore"] < at["fill"] && at["export"] < at["fill"] && at["fill"] < at["save"]) {
		add("go-quality.yml:%s: the steps must run setup-go, lookup, restore, fill, save (export before the fill)", saverJob)
	}
	wantKey := strings.Replace(legKey, legFamily, saverFamily, 1)
	for _, s := range []yamlMap{lookup, sRestore} {
		if got := asString(asMap(s["with"])["key"]); got != wantKey {
			add("go-quality.yml:%s: the key %q is not the key the legs restore with the family as the matrix value (%q)", saverJob, got, wantKey)
		}
		if paths(s) != paths(restore) {
			add("go-quality.yml:%s: the cache paths differ from the legs' paths", saverJob)
		}
	}
	checkGoVersionRef(saverJob, sSteps, asString(asMap(lookup["with"])["key"]))
	if lo, ok := asMap(lookup["with"])["lookup-only"].(bool); !ok || !lo {
		add("go-quality.yml:%s: the lookup step must be lookup-only: true (a same-day entry ends the job without a download)", saverJob)
	}
	if len(rkLines(lookup)) != 0 {
		add("go-quality.yml:%s: the lookup step must have no restore-keys (only the exact same-day key ends the job)", saverJob)
	}
	legRK := rkLines(restore)
	sRK := rkLines(sRestore)
	if len(legRK) < 2 || len(sRK) != 2 || sRK[0] != strings.Replace(legRK[0], legFamily, saverFamily, 1) || sRK[1] != strings.Replace(legRK[1], legFamily, saverFamily, 1) {
		add("go-quality.yml:%s: restore-keys must be the legs' first two lines with the family as the matrix value (got %q)", saverJob, sRK)
	}
	for name, s := range map[string]yamlMap{"restore": sRestore, "fill": fill, "export": sExport} {
		if !strings.Contains(asString(s["if"]), "steps.lookup.outputs.cache-hit != 'true'") {
			add("go-quality.yml:%s: the %s step must skip on a same-day lookup hit", saverJob, name)
		}
	}
	if strings.TrimSpace(asString(fill["run"])) != saverFillRun || asString(fill["id"]) == "" {
		add("go-quality.yml:%s: the fill step must run exactly %q and have an id", saverJob, saverFillRun)
	}
	if !strings.Contains(shellCode(asString(sExport["run"])), `DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build" >> "${GITHUB_ENV}"`) {
		add("go-quality.yml:%s: the export must append DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build to GITHUB_ENV", saverJob)
	}
	cond := asString(sSave["if"])
	rid, fid := asString(sRestore["id"]), asString(fill["id"])
	for _, part := range []string{"github.event_name == 'push'", "github.ref == 'refs/heads/main'", "steps.lookup.outputs.cache-hit != 'true'", "steps." + fid + ".outcome == 'success'", "steps." + rid + ".outputs.cache-hit != 'true'"} {
		if !strings.Contains(cond, part) {
			add("go-quality.yml:%s: the save condition %q does not hold %s", saverJob, cond, part)
		}
	}
	if got := asString(asMap(sSave["with"])["key"]); got != "${{ steps."+rid+".outputs.cache-primary-key }}" {
		add("go-quality.yml:%s: the save key %q is not the restore step's primary key", saverJob, got)
	}
	if paths(sSave) != paths(sRestore) {
		add("go-quality.yml:%s: the save and restore paths differ", saverJob)
	}
	return out
}

func TestGoQualityLegsRestoreAFamilyCacheAndOnlyTheSaverJobSavesIt(t *testing.T) {
	expectNoViolations(t, goCacheViolations(t, rootOf(t)))
}

// legSaveStep is the per-leg save CHAOS-13718 removed, planted back as the breach the guard must refuse.
const legSaveStep = `      - name: Save this leg's Go build and module cache (main pushes, GitHub-hosted legs)
        if: github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.go-cache.outputs.cache-hit != 'true'
        uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0
        with:
          path: |
            ~/.cache/go-build
            ~/go/pkg/mod
          key: ${{ steps.go-cache.outputs.cache-primary-key }}

`

func TestGoCacheGuardPlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	cases := []struct {
		name string
		m    mutation
		want string
	}{
		{"go-quality back on setup-go's cache", mutation{gq, "          cache: false\n\n      - name: Restore this leg's", "          cache: true\n\n      - name: Restore this leg's"}, "go-quality-leg: setup-go must set `cache: false`"},
		{"a leg saves its cache again", mutation{gq, "      - name: Install pinned shellcheck\n", legSaveStep + "      - name: Install pinned shellcheck\n"}, "a leg saves a Go cache"},
		{"another workflow writes a go-quality key", mutation{".github/workflows/go-auth-service.yml", "key: go-shared-v1-", allMarker + "key: go-quality-v3-build-"}, "only go-quality.yml job go-quality-cache saves"},
		{"the key is per leg again", mutation{gq, "key: go-quality-v3-${{ startsWith(matrix.name, 'race-') && 'race' || 'build' }}-", "key: go-quality-v3-${{ matrix.name }}-"}, "is per leg"},
		{"the key loses go.sum", mutation{gq, "-${{ hashFiles('**/go.sum') }}", allMarker}, "hashFiles"},
		{"the key loses the day", mutation{gq, "-${{ needs.go-quality-plan.outputs.cache_day }}", allMarker}, "cache_day"},
		{"the plan job loses cache_day", mutation{gq, "      cache_day: ${{ steps.cache-day.outputs.day }}\n", ""}, "no output cache_day"},
		{"restore on self-hosted too", mutation{gq, "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted'\n        uses: actions/cache/restore", "steps.relevance.outputs.relevant == 'true'\n        uses: actions/cache/restore"}, "GitHub-hosted legs only, if: exactly"},
		{"no step exports the restored dir", mutation{gq, `echo "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build" >> "${GITHUB_ENV}"`, `echo "X=1" >> "${GITHUB_ENV}"`}, "no step exports DEV_HEALTH_GO_CACHE"},
		{"the export names another dir", mutation{gq, "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build", "DEV_HEALTH_GO_CACHE=${RUNNER_TEMP}/go-build"}, "is not the restored path"},
		{"the export is a literal tilde", mutation{gq, "DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build", "DEV_HEALTH_GO_CACHE=~/.cache/go-build"}, "literal ~"},
		{"the export is on the pool path only", mutation{gq, "&& runner.environment == 'github-hosted'\n        run: echo \"DEV_HEALTH_GO_CACHE", "&& vars.SELF_HOSTED_RUNNERS == 'enabled'\n        run: echo \"DEV_HEALTH_GO_CACHE"}, "GitHub-hosted legs only, if: exactly"},
		{"the export is set after check_go.sh", mutation{gq, "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n", "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n\n      - name: late export\n        if: runner.environment == 'github-hosted'\n        run: echo \"DEV_HEALTH_GO_CACHE=${HOME}/.cache/go-build\" >> \"${GITHUB_ENV}\"\n"}, "after the leg step"},
		{"the layout segment is back to v2", mutation{gq, "go-quality-v3-", allMarker + "go-quality-v2-"}, "layout-version v2"},
		{"the key has no layout segment", mutation{gq, "go-quality-v3-", allMarker + "go-quality-"}, "no layout-version segment"},
		{"restore-keys line 1 keeps the day", mutation{gq, "restore-keys: |\n            go-quality-v3-${{ startsWith(matrix.name, 'race-') && 'race' || 'build' }}-${{ runner.os }}-${{ runner.arch }}-go${{ steps.setup-go.outputs.go-version }}-${{ hashFiles('**/go.sum') }}-\n", "restore-keys: |\n            go-quality-v3-${{ startsWith(matrix.name, 'race-') && 'race' || 'build' }}-${{ runner.os }}-${{ runner.arch }}-go${{ steps.setup-go.outputs.go-version }}-${{ hashFiles('**/go.sum') }}-${{ needs.go-quality-plan.outputs.cache_day }}\n"}, "restore-keys line 1"},
		{"restore-keys loses the any-go.sum line", mutation{gq, "            go-quality-v3-${{ startsWith(matrix.name, 'race-') && 'race' || 'build' }}-${{ runner.os }}-${{ runner.arch }}-go${{ steps.setup-go.outputs.go-version }}-\n", ""}, "restore-keys line 2"},
		{"the v2 fallback names another leg kind", mutation{gq, "            go-quality-v2-${{ startsWith(matrix.name, 'race-') && 'race' || 'test' }}-\n", "            go-quality-v2-${{ startsWith(matrix.name, 'race-') && 'race' || 'static' }}-\n"}, "v2 transition fallback"},
		{"a fourth restore-keys line", mutation{gq, "            go-quality-v2-${{ startsWith(matrix.name, 'race-') && 'race' || 'test' }}-\n", "            go-quality-v2-${{ startsWith(matrix.name, 'race-') && 'race' || 'test' }}-\n            go-quality-\n"}, "v2 transition fallback"},
		{"the saver runs on pull requests", mutation{gq, "    if: " + saverIf + "\n", "    if: github.event_name != 'merge_group'\n"}, "saver job if must be exactly"},
		{"the saver is a gate", mutation{gq, "    timeout-minutes: 60\n    continue-on-error: true\n", "    timeout-minutes: 60\n"}, "continue-on-error: true"},
		{"a third family", mutation{gq, "        family: [build, race]", "        family: [build, race, static]"}, "exactly [build, race]"},
		{"the saver key drifts from the legs' key", mutation{gq, "go-quality-v3-${{ matrix.family }}-", allMarker + "go-quality-v3-f-${{ matrix.family }}-"}, "is not the key the legs restore"},
		{"the lookup downloads", mutation{gq, "          lookup-only: true\n", ""}, "lookup-only: true"},
		{"the fill runs on a same-day hit", mutation{gq, "        id: fill\n        if: steps.lookup.outputs.cache-hit != 'true'\n", "        id: fill\n"}, "the fill step must skip"},
		{"the fill names one family", mutation{gq, "run: " + saverFillRun, "run: bash ci/check_go.sh ci-leg-warm all build"}, "the fill step must run exactly"},
		{"the saver saves on any event", mutation{gq, "          && github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.go-cache.outputs.cache-hit != 'true'\n        uses: actions/cache/save", "          && steps.go-cache.outputs.cache-hit != 'true'\n        uses: actions/cache/save"}, "github.event_name == 'push'"},
		{"the saver saves after a failed fill", mutation{gq, " && steps.fill.outcome == 'success'", ""}, "steps.fill.outcome == 'success'"},
		{"the saver saves on a same-day hit", mutation{gq, "          steps.lookup.outputs.cache-hit != 'true' && steps.fill.outcome", "          steps.fill.outcome"}, "steps.lookup.outputs.cache-hit != 'true'"},
		{"the saver saves a different key", mutation{gq, "            ~/go/pkg/mod\n          key: ${{ steps.go-cache.outputs.cache-primary-key }}", "            ~/go/pkg/mod\n          key: go-quality-other"}, "not the restore step's primary key"},
		{"the saver save loses the module cache", mutation{gq, "            ~/go/pkg/mod\n          key: ${{ steps.go-cache.outputs.cache-primary-key }}", "          key: ${{ steps.go-cache.outputs.cache-primary-key }}"}, "save and restore paths differ"},
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

func TestGoCacheGuardPinsTheCacheActionsAndTheSetupGoStep(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	const wrongSHA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cases := []struct {
		name string
		muts []mutation
		want string
	}{
		{"restore pinned to another SHA", []mutation{{gq, "actions/cache/restore@" + cacheActionSHA, allMarker + "actions/cache/restore@" + wrongSHA}}, "actions/cache/restore must be pinned to"},
		{"save pinned to another SHA", []mutation{{gq, "actions/cache/save@" + cacheActionSHA + " # v6.1.0\n        with:\n          path: |", allMarker + "actions/cache/save@" + wrongSHA + " # v6.1.0\n        with:\n          path: |"}}, "actions/cache/save must be pinned to"},
		{"restore pinned to a tag", []mutation{{gq, "actions/cache/restore@" + cacheActionSHA + " # v6.1.0", allMarker + "actions/cache/restore@v6.1.0"}}, "actions/cache/restore must be pinned to"},
		{"save pinned to a tag", []mutation{{gq, "actions/cache/save@" + cacheActionSHA + " # v6.1.0\n        with:\n          path: |", allMarker + "actions/cache/save@v6.1.0\n        with:\n          path: |"}}, "actions/cache/save must be pinned to"},
		{"restore also runs on the pool", []mutation{{gq, "relevant == 'true' && runner.environment == 'github-hosted'\n        uses: actions/cache/restore", "relevant == 'true' && runner.environment == 'github-hosted' || matrix.pool\n        uses: actions/cache/restore"}}, "GitHub-hosted legs only, if: exactly"},
		{"export also runs on the pool", []mutation{{gq, "runner.environment == 'github-hosted'\n        run: echo \"DEV_HEALTH_GO_CACHE", "runner.environment == 'github-hosted' || matrix.pool\n        run: echo \"DEV_HEALTH_GO_CACHE"}}, "GitHub-hosted legs only, if: exactly"},
		{"the setup-go id is renamed", []mutation{{gq, "        id: setup-go\n", allMarker + "        id: setup-golang\n"}}, "no step with id \"setup-go\""},
		{"the setup-go id moves to another step", []mutation{
			{gq, "        id: setup-go\n", "        id: setup-golang\n"},
			{gq, "      - name: Build into the restored Go cache (GitHub-hosted legs)\n", "      - name: Build into the restored Go cache (GitHub-hosted legs)\n        id: setup-go\n"},
		}, "step id \"setup-go\" is not actions/setup-go"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			planted := plantTree(t, root, goCacheWorkflows, c.muts...)
			expectViolation(t, goCacheViolations(t, planted), c.want)
		})
	}
}
