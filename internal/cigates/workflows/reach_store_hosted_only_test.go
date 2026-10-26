package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-11421: the plan-reads call-graph result is kept in actions/cache on the GitHub-hosted static leg only, and only a
// pull request READS it. The self-hosted pool shares a hostPath that any pull request job can write, so no store lives
// there: a self-hosted leg never restores or saves one and recomputes every time. A merge-queue run and a push to main
// always compute fresh (PLAN_READS_REACH_FRESH), and only a push to main saves, so the stored result is a fresh compute.
// The store is never an authenticated source: the digest in its header catches accident, not a writer.

const reachStoreHostedIf = "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted' && matrix.name == 'static'"

const reachKeyOutput = "${{ steps.reach-key.outputs.key }}"

func reachStoreViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, "go-quality.yml: "+fmt.Sprintf(f, a...)) }
	steps := stepsOf(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	var restore, save, hash yamlMap
	exports, hashes := 0, 0
	for _, s := range steps {
		w := asMap(s["with"])
		path := asString(w["path"])
		ifc := strings.Join(strings.Fields(asString(s["if"])), " ")
		isCache := usesAction(s, "actions/cache") || usesAction(s, "actions/cache/restore") || usesAction(s, "actions/cache/save")
		if isCache && strings.Contains(path, "plan-reads-reach") {
			if !strings.Contains(ifc, reachStoreHostedIf) {
				add("the reach store step %q must run on GitHub-hosted static legs only (if: %q), got %q", asString(s["name"]), reachStoreHostedIf, ifc)
			}
			switch {
			case usesAction(s, "actions/cache/restore"):
				restore = s
			case usesAction(s, "actions/cache/save"):
				save = s
			default:
				add("the reach store must use actions/cache/restore and actions/cache/save, not %q", asString(s["uses"]))
			}
			if _, ok := w["restore-keys"]; ok {
				add("the reach store restores an exact key: restore-keys would reuse a result of other inputs")
			}
			if strings.Contains(path, "/mnt/") || strings.Contains(path, "go-cache") {
				add("the reach store path %q is on the pool's shared hostPath", path)
			}
		}
		if strings.Contains(asString(s["run"]), "PLAN_READS_REACH_CACHE=") {
			exports++
			hash = s
			if ifc != reachStoreHostedIf {
				add("the step that exports PLAN_READS_REACH_CACHE must run on GitHub-hosted static legs only (if: %q), got %q", reachStoreHostedIf, ifc)
			}
		}
		if asString(s["id"]) == "reach-key" {
			hashes++
		}
		if _, ok := asMap(s["env"])["PLAN_READS_REACH_CACHE"]; ok {
			add("PLAN_READS_REACH_CACHE is set as a step env of %q: it may only be exported by the hosted-only step", asString(s["name"]))
		}
		if _, ok := asMap(s["env"])["PLAN_READS_REACH_FRESH"]; ok {
			add("PLAN_READS_REACH_FRESH is set as a step env of %q: it may only be exported by the hosted-only step", asString(s["name"]))
		}
	}
	if _, ok := asMap(asMap(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])["env"])["PLAN_READS_REACH_CACHE"]; ok {
		add("PLAN_READS_REACH_CACHE is set at job level: every leg, the self-hosted ones too, would use a store")
	}
	if restore == nil || save == nil || hash == nil || exports != 1 || hashes != 1 {
		add("want one key/export step, one restore and one save of the reach store (got restore=%v save=%v export=%v exports=%d keys=%d)", restore != nil, save != nil, hash != nil, exports, hashes)
		return out
	}
	// A merge-queue run and a push to main must not read a store: the restore is pull_request only, and the tool is told to
	// compute fresh on every other event.
	if ifc := strings.Join(strings.Fields(asString(restore["if"])), " "); ifc != reachStoreHostedIf+" && github.event_name == 'pull_request'" {
		add("the reach store restore must run on pull_request only, if: %q + \" && github.event_name == 'pull_request'\", got %q", reachStoreHostedIf, ifc)
	}
	run := asString(hash["run"])
	if !strings.Contains(run, `if [ "${EVENT_NAME}" != "pull_request" ]; then`) || !strings.Contains(run, `echo "PLAN_READS_REACH_FRESH=1" >> "${GITHUB_ENV}"`) {
		add("the key/export step must set PLAN_READS_REACH_FRESH on every event but pull_request (a merge-queue run and a push to main compute fresh)")
	}
	if got := asString(asMap(hash["env"])["EVENT_NAME"]); got != "${{ github.event_name }}" {
		add("the key/export step must read EVENT_NAME from github.event_name (got %q)", got)
	}
	if got := asString(asMap(hash["env"])["REACH_KEY"]); !strings.HasPrefix(got, "plan-reads-reach-v") || !strings.Contains(got, "hashFiles(") {
		add("the coarse key %q must be plan-reads-reach-v<N>-${{ hashFiles(...) }}", got)
	}
	for name, st := range map[string]yamlMap{"restore": restore, "save": save} {
		if got := asString(asMap(st["with"])["key"]); got != reachKeyOutput {
			add("the reach store %s key %q must be %s", name, got, reachKeyOutput)
		}
		want := "actions/cache/" + name + "@" + cacheActionSHA
		if got := asString(st["uses"]); got != want {
			add("the reach store %s step must be pinned to %s (got %q)", name, want, got)
		}
	}
	cond := asString(save["if"])
	for _, part := range []string{"success()", "github.event_name == 'push'", "github.ref == 'refs/heads/main'", "runner.environment == 'github-hosted'"} {
		if !strings.Contains(cond, part) {
			add("the reach store save condition %q does not hold %s", cond, part)
		}
	}
	if strings.Contains(cond, "pull_request") || strings.Contains(cond, "merge_group") {
		add("the reach store save condition %q lets a pull request or the merge queue write it", cond)
	}
	if strings.Contains(cond, "reach-store") {
		add("the reach store save condition %q must not read the restore step: a push to main never restores", cond)
	}
	if !strings.Contains(cond, "github.repository == 'full-chaos/dev-health-ops'") {
		add("the reach store save condition %q does not name the repository", cond)
	}
	// Only the key/export step, the restore and the save may name the store: no other step of any workflow writes there.
	for _, name := range workflowNames(t, root) {
		for job, jv := range jobsOf(loadWf(t, root, name)) {
			for _, st := range stepsOf(jv) {
				text := fmt.Sprint(st)
				if !strings.Contains(text, "plan-reads-reach") && !strings.Contains(text, "PLAN_READS_REACH") {
					continue
				}
				id := asString(st["id"])
				if name == "go-quality.yml" && job == "go-quality-leg" && (id == "reach-key" || id == "reach-store" || usesAction(st, "actions/cache/save")) {
					continue
				}
				add("step %q of %s:%s names the reach store: only the key step, the restore and the save may", asString(st["name"]), name, job)
			}
		}
	}
	return out
}

func TestReachStoreIsHostedOnly(t *testing.T) {
	expectNoViolations(t, reachStoreViolations(t, rootOf(t)))
}

func TestReachStorePlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	const gq = ".github/workflows/go-quality.yml"
	files := []string{gq}
	const prOnly = " && github.event_name == 'pull_request'"
	const looseHosted = "steps.relevance.outputs.relevant == 'true' && matrix.name == 'static'"
	const restoreTail = "          key: ${{ steps.reach-key.outputs.key }}\n\n      - name: Compile this leg's packages"
	cases := []struct {
		name string
		m    mutation
		want string
	}{
		{"restore also on the pool", mutation{gq, "        id: reach-store\n        if: " + reachStoreHostedIf + prOnly, "        id: reach-store\n        if: " + looseHosted + prOnly}, "must run on GitHub-hosted static legs only"},
		{"restore on every event", mutation{gq, reachStoreHostedIf + prOnly + "\n        uses: actions/cache/restore", reachStoreHostedIf + "\n        uses: actions/cache/restore"}, "must run on pull_request only"},
		{"restore also in the merge queue", mutation{gq, reachStoreHostedIf + prOnly + "\n        uses: actions/cache/restore", reachStoreHostedIf + " && (github.event_name == 'pull_request' || github.event_name == 'merge_group')\n        uses: actions/cache/restore"}, "must run on pull_request only"},
		{"save also on the pool", mutation{gq, "success() && " + reachStoreHostedIf, "success() && " + looseHosted}, "must run on GitHub-hosted static legs only"},
		{"the export also on the pool", mutation{gq, "        id: reach-key\n        if: " + reachStoreHostedIf, "        id: reach-key\n        if: steps.relevance.outputs.relevant == 'true'"}, "must run on GitHub-hosted static legs only"},
		{"no fresh flag outside pull requests", mutation{gq, "          if [ \"${EVENT_NAME}\" != \"pull_request\" ]; then\n            echo \"PLAN_READS_REACH_FRESH=1\" >> \"${GITHUB_ENV}\"\n          fi\n", ""}, "must set PLAN_READS_REACH_FRESH"},
		{"the fresh flag only on pull requests", mutation{gq, "if [ \"${EVENT_NAME}\" != \"pull_request\" ]; then", "if [ \"${EVENT_NAME}\" = \"pull_request\" ]; then"}, "must set PLAN_READS_REACH_FRESH"},
		{"restore-keys", mutation{gq, restoreTail, "          key: ${{ steps.reach-key.outputs.key }}\n          restore-keys: plan-reads-reach-v3-\n\n      - name: Compile this leg's packages"}, "restore-keys would reuse"},
		{"pull requests save", mutation{gq, "          && github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'\n        uses: actions/cache/save", "          && github.repository == 'full-chaos/dev-health-ops'\n        uses: actions/cache/save"}, "github.event_name == 'push'"},
		{"save reads the restore step", mutation{gq, " && github.repository == 'full-chaos/dev-health-ops'\n        uses: actions/cache/save", " && github.repository == 'full-chaos/dev-health-ops' && steps.reach-store.outputs.cache-hit != 'true'\n        uses: actions/cache/save"}, "must not read the restore step"},
		{"hostPath store", mutation{gq, "path: ${{ runner.temp }}/plan-reads-reach\n" + restoreTail, "path: /mnt/go-cache/plan-reads-reach\n" + restoreTail}, "shared hostPath"},
		{"another step writes the store", mutation{gq, "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n", "        run: bash ci/check_go.sh ci-leg ${{ matrix.args }}\n\n      - name: poison\n        run: echo x > \"${RUNNER_TEMP}/plan-reads-reach/reach-x.tsv\"\n"}, "names the reach store"},
		{"the restore pinned to a tag", mutation{gq, "actions/cache/restore@" + cacheActionSHA + " # v6.1.0\n        with:\n          path: ${{ runner.temp }}/plan-reads-reach", "actions/cache/restore@v6.1.0\n        with:\n          path: ${{ runner.temp }}/plan-reads-reach"}, "must be pinned to"},
		{"the save pinned to another sha", mutation{gq, "actions/cache/save@" + cacheActionSHA + " # v6.1.0\n        with:\n          path: ${{ runner.temp }}/plan-reads-reach", "actions/cache/save@aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n        with:\n          path: ${{ runner.temp }}/plan-reads-reach"}, "must be pinned to"},
		{"the save names no repository", mutation{gq, " && github.repository == 'full-chaos/dev-health-ops'\n        uses: actions/cache/save", "\n        uses: actions/cache/save"}, "does not name the repository"},
		{"the coarse key has no version prefix", mutation{gq, "REACH_KEY: plan-reads-reach-v3-", "REACH_KEY: "}, "plan-reads-reach-v<N>"},
		{"the variable at job level", mutation{gq, "      # The default 600 s per-package go test timeout is too small for the pool.\n", "      PLAN_READS_REACH_CACHE: /mnt/go-cache/plan-reads\n      # The default 600 s per-package go test timeout is too small for the pool.\n"}, "set at job level"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			planted := plantTree(t, root, files, c.m)
			expectViolation(t, reachStoreViolations(t, planted), c.want)
		})
	}
}
