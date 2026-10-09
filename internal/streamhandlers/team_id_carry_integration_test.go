//go:build integration

package streamhandlers

import (
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
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

// A team.v1 push of the custom system whose id an admin team holds (the
// carry gives an admin's own team custom:<id>) is refused like a team.v1
// id CheckPushed refuses: the team.v1 kind fails and the admin team stays;
// the batch's other kinds are written as on any team.v1 failure.
func TestATeamV1PushOfAnAdminsCustomIDIsRefused(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES ('eng', generateUUIDv4(), 'Admin Eng', [], ['admin@example.com'], [], [], 1, '2026-09-01 00:00:00', ?, '')`, projectMembershipTestOrg); err != nil {
		t.Fatal(err)
	}
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	records := []externalSinkRecord{
		{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": "Pushed Eng", "updatedAt": "2026-10-08T00:00:00Z"}},
		{Index: 1, Kind: "identity.v1", ExternalID: "u1", Payload: map[string]any{"canonicalId": "ada@example.test", "updatedAt": "2026-10-08T00:00:00Z", "teamIds": []any{"squad"}}},
	}
	_, err = sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records})
	if !errors.Is(err, providersync.ErrTeamIDCustomHeld) {
		t.Fatalf("write error = %v, want the custom id conflict", err)
	}
	var got string
	if err := conn.QueryRow(ctx, `SELECT concat(provider, '|', name, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active)) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`, projectMembershipTestOrg).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "|Admin Eng|admin@example.com|1" {
		t.Errorf("custom:eng = %q, want the admin team unchanged", got)
	}
	var identities uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM identities FINAL WHERE org_id = ? AND canonical_id = 'ada@example.test'`, projectMembershipTestOrg).Scan(&identities); err != nil {
		t.Fatal(err)
	}
	if identities != 1 {
		t.Errorf("identity.v1 rows = %d, want 1 (the other kind is written)", identities)
	}
}
