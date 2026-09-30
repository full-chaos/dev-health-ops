//go:build integration

package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/google/uuid"
)

// INVARIANT (CHAOS-7265): a row written by the real adapters for project_membership_transitions and
// work_items gets ingested_at = the server's time at the write, while the client-stamped last_synced
// (the RMT version) keeps the writer's own old value. The view exposes the ingested_at.
func TestMembershipAndWorkItemAdaptersLeaveIngestedAtToTheServer(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	orgID := workItemTestOrgID(t)
	clientStamp := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)

	membership := projectmembership.Row{
		OrgID: orgID, RepoID: uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"),
		SubjectKind: projectmembership.SubjectPullRequest, SubjectID: "42", Provider: "github",
		ToProjectID: "ghprojv2:acme:3", ToProjectKey: "3", OccurredAt: clientStamp, LastSynced: clientStamp,
	}
	membership.EventID = projectmembership.EventID(membership)
	workItem := workItemTestRow(orgID, clientStamp)

	before := time.Now().UTC().Add(-time.Second)
	identity, effect := workItemEffect(t, "project_membership_transitions", membership)
	if err := (GitHubProjectMembershipClickHouseAdapter{Conn: conn}).WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("membership write: %v", err)
	}
	identity, effect = workItemEffect(t, "work_items", workItem)
	if err := (GitHubWorkItemsClickHouseAdapter{Conn: conn}).WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		t.Fatalf("work_items write: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	for _, table := range []string{"project_membership_transitions", "work_items"} {
		var ingested, synced int64
		query := `SELECT toUnixTimestamp64Milli(max(ingested_at)), toUnixTimestamp64Milli(max(last_synced)) FROM ` + table + ` FINAL WHERE org_id = ?`
		if err := conn.QueryRow(ctx, query, orgID).Scan(&ingested, &synced); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		if got := time.UnixMilli(ingested).UTC(); got.Before(before) || got.After(after) {
			t.Fatalf("%s: ingested_at = %s, want the server write time between %s and %s", table, got, before, after)
		}
		if synced == ingested {
			t.Fatalf("%s: last_synced (the RMT version) was replaced by the ingest time", table)
		}
	}

	var viewSynced int64
	var source string
	if err := conn.QueryRow(ctx, `SELECT toUnixTimestamp64Milli(last_synced), source FROM project_membership_presence WHERE org_id = ? AND subject_kind = 'pull_request'`, orgID).Scan(&viewSynced, &source); err != nil {
		t.Fatalf("view: %v", err)
	}
	if got := time.UnixMilli(viewSynced).UTC(); got.Before(before) || got.After(after) || source != "transition" {
		t.Fatalf("view last_synced = %s source = %s, want the server write time between %s and %s", got, source, before, after)
	}
}
