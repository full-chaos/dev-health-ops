//go:build integration

package issueprlinks_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// TestLoadPullRequestsWindowBoundsBindAgainstRealDateTime64Column is the
// red-first regression test for a live-stack outage: EVERY workgraph.build
// failed permanently in the native pre-step because loadPullRequests bound
// window.From/To (raw time.Time values) against `git_pull_requests.created_at`,
// a real DateTime64(3, 'UTC') column, via a `{from_ts:DateTime64(3)}`/
// `{to_ts:DateTime64(3)}` placeholder. clickhouse-go renders a bound
// time.Time as a `toDateTime(...)` expression, which ClickHouse refuses to
// parse against a DateTime64(3)-typed placeholder (code 457,
// "cannot be parsed as DateTime64(3)"). Every existing test for this loader
// either used a stub `conn` that only records the query text (service_test.go
// -- proves construction, never touches a real server) or called `Load` with
// an unbounded Window{} (every real-ClickHouse integration test in this
// package) -- so the bind path that actually breaks was never exercised
// against the real column type before this test.
func TestLoadPullRequestsWindowBoundsBindAgainstRealDateTime64Column(t *testing.T) {
	ctx := context.Background()
	conn := connect(ctx, t)

	org := "org-window-bind-" + uuid.NewString()
	repo := uuid.New()

	// Load returns early (never calling loadPullRequests at all) when the
	// org has zero work_item_dependencies rows -- seed one so the bind path
	// under test actually runs.
	mustExec(ctx, t, conn, fmt.Sprintf(
		`INSERT INTO work_item_dependencies (
			source_work_item_id, target_work_item_id, relationship_type,
			relationship_type_raw, last_synced, org_id
		) VALUES ('gh:1', 'gh:2', 'relates_to', 'relates_to', '2026-08-01 00:00:00.000', '%s')`,
		org))

	// Three PRs straddling the window: before, inside, after. All in the
	// SAME repo so a wrong result can't be explained by the repo filter.
	for _, row := range []struct {
		number    int
		createdAt string
	}{
		{1, "2026-07-30 00:00:00.000"}, // before the window
		{2, "2026-08-01 12:00:00.000"}, // inside the window
		{3, "2026-08-03 00:00:00.000"}, // after the window
	} {
		mustExec(ctx, t, conn, fmt.Sprintf(
			`INSERT INTO git_pull_requests (repo_id, number, org_id, created_at, last_synced)
			 VALUES ('%s', %d, '%s', '%s', '%s')`,
			repo, row.number, org, row.createdAt, row.createdAt))
	}

	from := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)

	loader, err := issueprlinks.NewLoader(conn)
	if err != nil {
		t.Fatalf("new loader: %v", err)
	}
	inputs, err := loader.Load(ctx, org, issueprlinks.Window{From: &from, To: &to})
	if err != nil {
		t.Fatalf("Load with a bounded window: %v -- this is the live-stack outage: "+
			"binding a raw time.Time against a DateTime64(3) placeholder fails with "+
			"ClickHouse code 457, \"cannot be parsed as DateTime64(3)\"", err)
	}

	if len(inputs.PullRequests) != 1 {
		t.Fatalf("got %d pull requests in the window, want exactly 1 (number=2): %+v",
			len(inputs.PullRequests), inputs.PullRequests)
	}
	if got := inputs.PullRequests[0].Number; got != 2 {
		t.Fatalf("the pull request the window admitted has number=%d, want 2 -- "+
			"the bound bind may have SUCCEEDED without erroring but still filtered "+
			"the wrong rows, which a bare nil-error check would miss", got)
	}
}
