//go:build integration

package providersync

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// legacyLinksCutConn is the store with the read of the legacy links table cut:
// every other statement goes to the real store.
type legacyLinksCutConn struct{ driver.Conn }

func (conn legacyLinksCutConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, "jira_project_ops_team_links") {
		return nil, errors.New("the legacy links read is cut")
	}
	return conn.Conn.Query(ctx, query, args...)
}

// TestAJiraCatalogRunWithACutLegacyLinksReadNamesTheTerm pins the legacy
// links term of the Jira catalog snapshot through the real collector: a run
// whose legacy links read did not finish keeps the open row and names
// legacy_links on both counters. The control run before it (the read works)
// reports nothing. Not parallel: it reads process-wide counters.
func TestAJiraCatalogRunWithACutLegacyLinksReadNamesTheTerm(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	lease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
	orgID := uuid.NewString()
	seeded := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO jira_project_ops_team_links (org_id, project_key, ops_team_id, project_name, ops_team_name, updated_at) VALUES (?, ?, ?, ?, ?, ?)`,
		orgID, "OPS", "ops-team-a", "Ops Project", "Ops Team", seeded); err != nil {
		t.Fatal(err)
	}
	live := `{"values":[{"id":"10001","key":"OPS","name":"Ops Project"}],"isLast":true,"total":1}`
	run := func(store driver.Conn, at time.Time) TeamCatalogResult {
		t.Helper()
		result, err := JiraTeamCatalogCollector{
			Handler: JiraTeamCatalogRouteHandler{}, Sink: JiraTeamCatalogClickHouseEffects{Conn: store, Lease: lease},
			ScopeCensus: staticScopeCensus{},
		}.CollectTeamCatalog(ctx, TeamCatalogReference{OrgID: orgID, SyncRunID: "run", IntegrationID: "integration-a"},
			providerfoundation.Credential{Provider: "jira"},
			jiraTeamCatalogTestClient(t, fakehttp.Client(&jiraTeamCatalogFixtureDoer{t: t, byURI: map[string]jiraTeamCatalogFixtureResponse{
				"/rest/api/3/project/OPS":               {body: `{"projectTypeKey":"business"}`},
				jiraTeamCatalogProjectSearchURI:         {body: live},
				jiraTeamCatalogArchivedProjectSearchURI: {body: `{"values":[],"isLast":true}`},
			}})),
			TeamCatalogSelections{Projects: true}, at)
		if err != nil {
			t.Fatalf("team catalog sync: %v", err)
		}
		return result
	}
	open := func() int {
		t.Helper()
		return len(openGitLabOwnership(ctx, t, conn, orgID, "jira"))
	}
	moved := func(before map[string]int64) map[string]int64 {
		out := map[string]int64{}
		for reason, n := range jiraSnapshotIncompleteCounts(t) {
			if d := n - before[reason]; d != 0 {
				out[reason] = d
			}
		}
		return out
	}
	first := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)

	jira, shared := jiraSnapshotIncompleteCounts(t), snapshotAbandonedCounts(t)
	if result := run(conn, first); result.OwnershipSnapshotIncomplete || open() != 1 {
		t.Fatalf("control: incomplete=%v open=%d, want a complete run with one open legacy row", result.OwnershipSnapshotIncomplete, open())
	}
	if got := moved(jira); len(got) != 0 {
		t.Fatalf("control: a run whose reads finished moved %s: %v", jiraOwnershipSnapshotIncompleteName, got)
	}
	if got := snapshotAbandonedMoved(t, shared); len(got) != 0 {
		t.Fatalf("control: a run whose reads finished moved %s: %v", snapshotCloseAbandonedName, got)
	}

	jira, shared = jiraSnapshotIncompleteCounts(t), snapshotAbandonedCounts(t)
	result := run(legacyLinksCutConn{conn}, first.Add(time.Hour))
	if !result.OwnershipSnapshotIncomplete || result.OwnershipRetracted != 0 || open() != 1 {
		t.Fatalf("incomplete=%v retracted=%d open=%d, want an incomplete run that keeps the open legacy row",
			result.OwnershipSnapshotIncomplete, result.OwnershipRetracted, open())
	}
	// The cut read gives no link, so the answer is empty too: both reasons are said.
	if got, want := moved(jira), map[string]int64{"legacy_links": 1, "no_live_ownership": 1}; !reflect.DeepEqual(got, want) {
		t.Errorf("%s moved %v, want %v", jiraOwnershipSnapshotIncompleteName, got, want)
	}
	want := map[string]int64{
		"jira/jira_legacy_ownership/" + jiraSnapshotLegacyLinks: 1,
		"jira/jira_legacy_ownership/" + SnapshotEmptyAnswer:     1,
	}
	if got := snapshotAbandonedMoved(t, shared); !reflect.DeepEqual(got, want) {
		t.Errorf("%s moved %v, want %v", snapshotCloseAbandonedName, got, want)
	}
}
