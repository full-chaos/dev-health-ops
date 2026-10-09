//go:build integration

package streamhandlers

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
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

func teamRowSummary(t *testing.T, conn driver.Conn, ctx context.Context, id string) string {
	t.Helper()
	var got string
	if err := conn.QueryRow(ctx, `SELECT concat(provider, '|', name, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active)) FROM teams FINAL WHERE org_id = ? AND id = ?`, projectMembershipTestOrg, id).Scan(&got); err != nil {
		return "none"
	}
	return got
}

// An older admin edit of a pushed custom team (it stored provider "")
// leaves that team the source's: the next push of the source writes it
// (keeping the admin's manual member) and every other team of the batch.
func TestAPushAfterAnAdminEditOfAPushedCustomTeamUpdatesIt(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	push := func(name, at string) error {
		records := []externalSinkRecord{
			{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": name, "updatedAt": at}},
			{Index: 1, Kind: "team.v1", ExternalID: "t2", Payload: map[string]any{"id": "ops", "name": "Ops " + name, "updatedAt": at}},
		}
		_, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records})
		return err
	}
	if err := push("Pushed Eng", "2026-10-01T00:00:00Z"); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES ('custom:eng', generateUUIDv4(), 'Pushed Eng', [], ['admin@example.com'], [], [], 1, '2026-10-02 00:00:00', ?, '')`, projectMembershipTestOrg); err != nil {
		t.Fatal(err)
	}
	if err := push("Pushed Eng v2", "2026-10-08T00:00:00Z"); err != nil {
		t.Fatalf("push after the admin edit: %v", err)
	}
	for id, want := range map[string]string{
		"custom:eng": "|Pushed Eng v2|admin@example.com|1",
		"custom:ops": "|Ops Pushed Eng v2||1",
	} {
		if got := teamRowSummary(t, conn, ctx, id); got != want {
			t.Errorf("%s = %q, want %q", id, got, want)
		}
	}
}

// An admin's own team "eng" and a pushed custom-system team "eng" are one
// custom team: the push carries the bare admin team to custom:eng and
// writes over it (the last write wins), keeping the admin's manual member;
// no second team appears.
func TestAnAdminTeamAndAPushedCustomTeamOfOneIDAreOneTeam(t *testing.T) {
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
	records := []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": "Pushed Eng", "updatedAt": "2026-10-08T00:00:00Z"}}}
	if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records}); err != nil {
		t.Fatalf("push: %v", err)
	}
	var active string
	if err := conn.QueryRow(ctx, `SELECT arrayStringConcat(groupArray(id), ',') FROM (SELECT id FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY id)`, projectMembershipTestOrg).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != "custom:eng" {
		t.Errorf("active = %q, want custom:eng only", active)
	}
	if got := teamRowSummary(t, conn, ctx, "custom:eng"); got != "|Pushed Eng|admin@example.com|1" {
		t.Errorf("custom:eng = %q, want the pushed write over the admin team, member kept", got)
	}
}

// A pushed custom team is stored like an admin team, with no provider, so
// the attribution cascade takes it for a project key of an item of every
// provider, as it takes an admin team.
func TestAPushedCustomTeamHoldsAProjectKeyForEveryProvider(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	records := []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": "Eng", "projectKeys": []any{"ENGKEY"}, "updatedAt": "2026-10-08T00:00:00Z"}}}
	if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records}); err != nil {
		t.Fatalf("push: %v", err)
	}
	teams, err := teamattribution.ClickHouseFactSource{Conn: conn}.LoadTeams(ctx, projectMembershipTestOrg)
	if err != nil {
		t.Fatal(err)
	}
	derived := teamattribution.NewGitHubWorkItemDerivationContext(teamattribution.GithubWorkItemDerivationFacts{Teams: teams})
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		key := "ENGKEY"
		candidates := derived.IssueProjectCandidates(teamattribution.GithubWorkItemDerivationSubject{Provider: provider, ProjectKey: &key})
		if len(candidates) != 1 || candidates[0].TeamID == nil || *candidates[0].TeamID != "custom:eng" {
			t.Errorf("%s item: candidates = %+v, want custom:eng", provider, candidates)
		}
	}
}

// A bare admin team beside a custom:eng an earlier push already wrote (with
// provider custom) is one team: the push carries first, the kept custom:eng
// takes the admin's manual member, and the push writes over it and keeps
// that member.
func TestAPushAfterAnAdminTeamBesideAKeyedCustomTeamKeepsTheAdminsMember(t *testing.T) {
	ctx, conn := newProjectMembershipConn(t)
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('custom:eng', generateUUIDv4(), 'Pushed Eng', [], [], [], [], 1, '2026-09-01 00:00:00', ?, 'custom', 'eng')`, projectMembershipTestOrg); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES ('eng', generateUUIDv4(), 'Admin Eng', [], ['admin@example.com'], [], [], 1, '2026-09-02 00:00:00', ?, '')`, projectMembershipTestOrg); err != nil {
		t.Fatal(err)
	}
	pointer := projectMembershipPointer()
	pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
	sink, err := NewClickHouseExternalBatchSink(conn)
	if err != nil {
		t.Fatal(err)
	}
	records := []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": "Pushed Eng v2", "updatedAt": "2026-10-08T00:00:00Z"}}}
	if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records}); err != nil {
		t.Fatalf("push: %v", err)
	}
	for id, want := range map[string]string{"custom:eng": "|Pushed Eng v2|admin@example.com|1", "eng": "|Admin Eng|admin@example.com|0"} {
		if got := teamRowSummary(t, conn, ctx, id); got != want {
			t.Errorf("%s = %q, want %q", id, got, want)
		}
	}
}

// A push stamped at the same version as the kept custom:eng, with a bare
// admin team beside it, writes its rename: the carry's fold takes the
// admin's member at the stored version, and the push, inserted after it at an
// equal or newer version, wins.
func TestAPushAtTheKeptRowsVersionAfterAFoldWritesItsRename(t *testing.T) {
	for _, c := range []struct{ name, updatedAt string }{
		{"push equal to stored", "2026-10-01T00:00:00Z"},
		{"push newer than stored", "2026-10-01T00:00:01Z"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, conn := newProjectMembershipConn(t)
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('custom:eng', generateUUIDv4(), 'Pushed Eng', [], [], [], [], 1, '2026-10-01 00:00:00', ?, '', 'eng')`, projectMembershipTestOrg); err != nil {
				t.Fatal(err)
			}
			if err := conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider) VALUES ('eng', generateUUIDv4(), 'Admin Eng', [], ['admin@example.com'], [], [], 1, '2026-10-02 00:00:00', ?, '')`, projectMembershipTestOrg); err != nil {
				t.Fatal(err)
			}
			pointer := projectMembershipPointer()
			pointer.SourceSystem, pointer.SourceInstance = "custom", "acme"
			sink, err := NewClickHouseExternalBatchSink(conn)
			if err != nil {
				t.Fatal(err)
			}
			records := []externalSinkRecord{{Index: 0, Kind: "team.v1", ExternalID: "t1", Payload: map[string]any{"id": "eng", "name": "Renamed", "updatedAt": c.updatedAt}}}
			if _, err := sink.Write(ctx, externalSinkBatch{Pointer: pointer, SourceID: uuid.New(), Records: records}); err != nil {
				t.Fatal(err)
			}
			if got := teamRowSummary(t, conn, ctx, "custom:eng"); got != "|Renamed|admin@example.com|1" {
				t.Errorf("custom:eng = %q, want the push's name with the admin's member", got)
			}
		})
	}
}
