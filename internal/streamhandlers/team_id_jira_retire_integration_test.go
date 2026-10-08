//go:build integration

package streamhandlers

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/google/uuid"
)

func TestAPushedJiraTeamSurvivesTheJiraProjectAsTeamRetire(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "jira", "acme"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	records := []externalSinkRecord{
		{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "jira:platform", "name": "Platform", "updatedAt": "2026-10-01T00:00:00Z"}},
		{Index: 1, Kind: "team.v1", ExternalID: "t2", Payload: map[string]any{"id": "payments", "name": "Payments", "updatedAt": "2026-10-01T00:00:00Z"}},
	}
	if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff"), Records: records}); err != nil {
		t.Fatalf("push: %v", err)
	}
	nativeOf := func(id string) (native string, active uint8) {
		t.Helper()
		if err := conn.QueryRow(ctx, `SELECT ifNull(native_team_key, '<NULL>'), is_active FROM teams FINAL WHERE org_id = ? AND id = ?`,
			projectMembershipTestOrg, id).Scan(&native, &active); err != nil {
			t.Fatalf("read team %q: %v", id, err)
		}
		return native, active
	}
	if native, _ := nativeOf("jira:platform"); native != "platform" {
		t.Fatalf("native_team_key of jira:platform = %q, want platform", native)
	}
	if native, _ := nativeOf("jira:payments"); native != "payments" {
		t.Fatalf("native_team_key of jira:payments = %q, want payments", native)
	}
	outcome, err := providersync.RetireJiraProjectAsTeamRows(ctx, conn, projectMembershipTestOrg, time.Now().UTC(), false)
	if err != nil {
		t.Fatalf("retire: %v", err)
	}
	if outcome.TeamsRetired != 0 {
		t.Errorf("the Jira project-as-team retire deactivated %d pushed team(s)", outcome.TeamsRetired)
	}
	for _, id := range []string{"jira:platform", "jira:payments"} {
		if _, active := nativeOf(id); active != 1 {
			t.Errorf("pushed team %q is_active = %d after the retire, want 1", id, active)
		}
	}
}
