package workflows_test

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-11421: the plan-reads call-graph result is kept in actions/cache on the GitHub-hosted static leg only. The
// self-hosted pool shares a hostPath that any pull request job can write, and a workflow condition is no boundary for a
// pull request that edits the workflow, so no store lives there: a self-hosted leg never restores or saves one and
// recomputes every time. The key is exact (no restore-keys) and only a push to main saves.

const reachStoreHostedIf = "steps.relevance.outputs.relevant == 'true' && runner.environment == 'github-hosted' && matrix.name == 'static'"

func reachStoreViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, "go-quality.yml: "+fmt.Sprintf(f, a...)) }
	steps := stepsOf(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])
	var restore, save yamlMap
	exports, keys := 0, 0
	for _, s := range steps {
		w := asMap(s["with"])
		path := asString(w["path"])
		isCache := usesAction(s, "actions/cache") || usesAction(s, "actions/cache/restore") || usesAction(s, "actions/cache/save")
		if isCache && strings.Contains(path, "plan-reads-reach") {
			ifc := strings.Join(strings.Fields(asString(s["if"])), " ")
			if usesAction(s, "actions/cache/save") && !strings.Contains(ifc, reachStoreHostedIf) || !usesAction(s, "actions/cache/save") && ifc != reachStoreHostedIf {
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
			if strings.TrimSpace(asString(s["if"])) != reachStoreHostedIf {
				add("the step that exports PLAN_READS_REACH_CACHE must run on GitHub-hosted static legs only (if: %q), got %q", reachStoreHostedIf, asString(s["if"]))
			}
		}
		if strings.Contains(asString(s["run"]), "--print-reach-key") {
			keys++
		}
		if _, ok := asMap(s["env"])["PLAN_READS_REACH_CACHE"]; ok {
			add("PLAN_READS_REACH_CACHE is set as a step env of %q: it may only be exported by the hosted-only step", asString(s["name"]))
		}
	}
	if _, ok := asMap(asMap(jobsOf(loadWf(t, root, "go-quality.yml"))["go-quality-leg"])["env"])["PLAN_READS_REACH_CACHE"]; ok {
		add("PLAN_READS_REACH_CACHE is set at job level: every leg, the self-hosted ones too, would use a store")
	}
	if restore == nil || save == nil || exports != 1 || keys != 1 {
		add("want one hash step, one restore, one save and one export of the reach store (got restore=%v save=%v exports=%d keys=%d)", restore != nil, save != nil, exports, keys)
		return out
	}
	if got := asString(asMap(restore["with"])["key"]); !strings.HasPrefix(got, "plan-reads-reach-v") || !strings.HasSuffix(got, "${{ steps.reach-key.outputs.key }}") {
		add("the restore key %q must be plan-reads-reach-v<N>-<hash of the inputs>", got)
	}
	cond := asString(save["if"])
	for _, part := range []string{"success()", "github.event_name == 'push'", "github.ref == 'refs/heads/main'", "runner.environment == 'github-hosted'", "steps.reach-store.outputs.cache-hit != 'true'"} {
		if !strings.Contains(cond, part) {
			add("the reach store save condition %q does not hold %s", cond, part)
		}
	}
	if strings.Contains(cond, "pull_request") || strings.Contains(cond, "merge_group") {
		add("the reach store save condition %q lets a pull request or the merge queue write it", cond)
	}
	if got := asString(asMap(save["with"])["key"]); got != "${{ steps.reach-store.outputs.cache-primary-key }}" {
		add("the reach store save key %q is not the restore step's primary key", got)
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
	cases := []struct {
		name string
		m    mutation
		want string
	}{
		{"restore also on the pool", mutation{gq, "        id: reach-store\n        if: " + reachStoreHostedIf, "        id: reach-store\n        if: steps.relevance.outputs.relevant == 'true' && matrix.name == 'static'"}, "must run on GitHub-hosted static legs only"},
		{"save also on the pool", mutation{gq, "success() && " + reachStoreHostedIf, "success() && steps.relevance.outputs.relevant == 'true' && matrix.name == 'static'"}, "must run on GitHub-hosted static legs only"},
		{"the export also on the pool", mutation{gq, "        id: reach-key\n        if: " + reachStoreHostedIf, "        id: reach-key\n        if: steps.relevance.outputs.relevant == 'true'"}, "must run on GitHub-hosted static legs only"},
		{"restore-keys", mutation{gq, "          key: plan-reads-reach-v1-${{ steps.reach-key.outputs.key }}\n", "          key: plan-reads-reach-v1-${{ steps.reach-key.outputs.key }}\n          restore-keys: plan-reads-reach-v1-\n"}, "restore-keys would reuse"},
		{"pull requests save", mutation{gq, "          && github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.reach-store", "          && steps.reach-store"}, "github.event_name == 'push'"},
		{"save on a hit", mutation{gq, " && steps.reach-store.outputs.cache-hit != 'true'", ""}, "cache-hit"},
		{"hostPath store", mutation{gq, "path: ${{ runner.temp }}/plan-reads-reach\n          key: plan-reads-reach-v1-", "path: /mnt/go-cache/plan-reads-reach\n          key: plan-reads-reach-v1-"}, "shared hostPath"},
		{"the variable at job level", mutation{gq, "      # The default 600 s per-package go test timeout is too small for the pool.\n", "      PLAN_READS_REACH_CACHE: /mnt/go-cache/plan-reads\n      # The default 600 s per-package go test timeout is too small for the pool.\n"}, "set at job level"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			planted := plantTree(t, root, files, c.m)
			expectViolation(t, reachStoreViolations(t, planted), c.want)
		})
	}
}
