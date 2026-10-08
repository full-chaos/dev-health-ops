//go:build integration

package streamhandlers

import (
	"testing"

	"github.com/google/uuid"
)

// A team.v1 push carries the bare team before it writes: the admin's manual
// member reaches the prefixed row (the push keeps the manual members of the
// id it writes) and the bare row goes inactive.
func TestATeamV1PushCarriesTheBareTeamFirst(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES ('platform', generateUUIDv4(), 'Platform', [], ['m@example.com'], [], [], 1, '2026-09-01 00:00:00', ?, 'custom')`, projectMembershipTestOrg); err != nil {
		t.Fatal(err)
	}
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	records := []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "platform", "name": "Platform", "updatedAt": "2026-10-08T00:00:00Z"}}}
	if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.MustParse("9749bda0-fc9f-4076-b19d-7b26c4f306ff"), Records: records}); err != nil {
		t.Fatalf("push: %v", err)
	}
	for id, want := range map[string]string{"platform": "0|m@example.com", "custom:platform": "1|m@example.com"} {
		var got string
		if err := conn.QueryRow(ctx, `SELECT concat(toString(is_active), '|', arrayStringConcat(manual_members, ',')) FROM teams FINAL WHERE org_id = ? AND id = ?`, projectMembershipTestOrg, id).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", id, err)
		}
		if got != want {
			t.Errorf("team %s = %q, want %q", id, got, want)
		}
	}
}
