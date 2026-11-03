package affected_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A merge_group run and a main push run every integration shard: the planner plans FULL for both events whatever the
// changed paths, and the unfiltered shard plan holds a leg for each package that builds or holds ClickHouse query text.
// Only a pull request is judged by path (and, without a label, skips the shards: ci/integration_pr_scope.sh).
type queryTextChange struct {
	name, changed string
	// shard: the plan must assign this package ("" for none); target: a split matrix target the plan must hold.
	pkg, target string
}

var queryTextChanges = []queryTextChange{
	{"liverow", "internal/storage/clickhouse/liverow/liverow.go", "internal/storage/clickhouse/liverow", ""},
	{"rowview", "internal/queryapi/rowview/rowview.go", "", "queryapiserver"},
	{"storage_clickhouse", "internal/storage/clickhouse/authorization.go", "internal/storage/clickhouse", ""},
	{"chmigrate", "internal/chmigrate/apply.go", "internal/chmigrate", ""},
	{"metrics_remaining", "internal/jobs/metrics/remaining/capacity_native_clickhouse.go", "", "remaining"},
}

type shardPlanOut struct {
	shardOf map[string]int // package -> shard of a "SHARD n pkg" line
	matrix  []struct {
		Target string `json:"target"`
		Shard  int    `json:"shard"`
	}
}

var (
	shardLine    = regexp.MustCompile(`(?m)^  SHARD (\d+) (\S+) weight=`)
	shardPlanMu  sync.Mutex
	shardPlanFor = map[string]*shardPlanOut{}
)

// unfilteredShardPlan runs ci/check_go.sh integration-shard-plan as the plan job does for an event that sets no
// INTEGRATION_AFFECTED_FILE.
func unfilteredShardPlan(t *testing.T, event string) *shardPlanOut {
	t.Helper()
	shardPlanMu.Lock()
	defer shardPlanMu.Unlock()
	if p := shardPlanFor[event]; p != nil {
		return p
	}
	root := repoRoot(t)
	out := filepath.Join(t.TempDir(), "output")
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "INTEGRATION_AFFECTED_FILE=") && !strings.HasPrefix(e, "GITHUB_EVENT_NAME=") && !strings.HasPrefix(e, "GITHUB_OUTPUT=") {
			env = append(env, e)
		}
	}
	cmd := exec.Command("bash", "ci/check_go.sh", "integration-shard-plan")
	cmd.Dir = root
	cmd.Env = append(env, "GITHUB_EVENT_NAME="+event, "GITHUB_OUTPUT="+out)
	stdout, err := cmd.Output()
	if err != nil {
		t.Fatalf("integration-shard-plan for %s: %v", event, err)
	}
	p := &shardPlanOut{shardOf: map[string]int{}}
	for _, m := range shardLine.FindAllStringSubmatch(string(stdout), -1) {
		n, _ := strconv.Atoi(m[1])
		p.shardOf[m[2]] = n
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if body, ok := strings.CutPrefix(line, "matrix="); ok {
			var m struct {
				Include []struct {
					Target string `json:"target"`
					Shard  int    `json:"shard"`
				} `json:"include"`
			}
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				t.Fatalf("matrix output: %v", err)
			}
			p.matrix = m.Include
		}
	}
	shardPlanFor[event] = p
	return p
}

func (p *shardPlanOut) hasLeg(target string, shard int) bool {
	for _, l := range p.matrix {
		if l.Target == target && (shard == 0 || l.Shard == shard) {
			return true
		}
	}
	return false
}

func TestQueryTextChangeRunsItsIntegrationShardsOnMergeGroupAndPush(t *testing.T) {
	t.Parallel()
	for _, event := range []string{"merge_group", "push"} {
		for _, c := range queryTextChanges {
			t.Run(event+"/"+c.name, func(t *testing.T) {
				t.Parallel()
				lines := realPlanEvent(t, event, c.changed)
				if want := "FULL event " + event; !strings.HasPrefix(lines[0], want) {
					t.Fatalf("a change to %s on %s plans %q, want a line starting %q", c.changed, event, lines[0], want)
				}
				plan := unfilteredShardPlan(t, event)
				if c.pkg != "" {
					n, ok := plan.shardOf[c.pkg]
					if !ok {
						t.Fatalf("the %s shard plan holds no shard for %s", event, c.pkg)
					}
					if !plan.hasLeg("packages", n) {
						t.Fatalf("the %s matrix has no packages leg %d, which holds %s", event, n, c.pkg)
					}
				}
				if c.target != "" && !plan.hasLeg(c.target, 0) {
					t.Fatalf("the %s matrix has no %s leg", event, c.target)
				}
			})
		}
	}
}

// A docs-only change is not judged by the planner on these events: it plans the same FULL line a query-text change does
// (the docs-only skip is a workflow step, ci/codeql_docs_only.sh, pinned in internal/cigates/workflows).
func TestDocsOnlyChangePlansLikeAQueryTextChangeOnMergeGroupAndPush(t *testing.T) {
	t.Parallel()
	for _, event := range []string{"merge_group", "push"} {
		t.Run(event, func(t *testing.T) {
			t.Parallel()
			docs := realPlanEvent(t, event, "docs/README.md")
			query := realPlanEvent(t, event, queryTextChanges[0].changed)
			if docs[0] != query[0] || !strings.HasPrefix(docs[0], "FULL event "+event) || len(docs) != 1 {
				t.Fatalf("docs-only plans %q, a query-text change plans %q", docs, query)
			}
		})
	}
}
