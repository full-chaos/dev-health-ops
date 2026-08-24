//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// newReadbackIntegrationConn applies the REAL ClickHouse migration chain
// (not a hand-authored CREATE TABLE copy) before opening the Go connection,
// the same convention github_deployments_effects_integration_test.go uses:
// a local DDL copy would only prove the copy, not the deployed schema the
// team-autoimport populators (still Python-side, CHAOS-4198) actually write
// to.
func newReadbackIntegrationConn(t *testing.T) (context.Context, driver.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ctx, conn
}

const readbackTestOrg = "00000000-0000-4000-8000-0000000003a1"

func insertReadbackTeam(t *testing.T, ctx context.Context, conn driver.Conn, provider, nativeTeamKey string) {
	t.Helper()
	now := time.Now().UTC()
	if err := conn.Exec(ctx, `
INSERT INTO teams (id, team_uuid, name, description, members, updated_at, last_synced, org_id, provider, native_team_key)
VALUES (generateUUIDv4(), generateUUIDv4(), ?, NULL, [], ?, ?, ?, ?, ?)`,
		nativeTeamKey, now, now, readbackTestOrg, provider, nativeTeamKey); err != nil {
		t.Fatal(err)
	}
}

func insertReadbackSprint(t *testing.T, ctx context.Context, conn driver.Conn, provider, sprintID string) {
	t.Helper()
	now := time.Now().UTC()
	if err := conn.Exec(ctx, `
INSERT INTO sprints (provider, sprint_id, name, state, started_at, ended_at, completed_at, last_synced, org_id, native_team_key)
VALUES (?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, '')`,
		provider, sprintID, sprintID, now, readbackTestOrg); err != nil {
		t.Fatal(err)
	}
}

// TestClickHouseReadbackVerifierAgainstMigratedSchema pins
// _missing_team_keys/_missing_sprint_ids's exact query shape against the
// real deployed schema: a written row is visible, an unwritten one is
// reported missing, and a same-key row from another org or provider is
// correctly excluded (the WHERE clause's org_id/provider fence, not just
// the IN-list membership, is load-bearing).
func TestClickHouseReadbackVerifierAgainstMigratedSchema(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	insertReadbackTeam(t, ctx, conn, "linear", "ENG")
	insertReadbackSprint(t, ctx, conn, "linear", "sprint-1")
	// Same natural keys, different tenant/provider -- must not satisfy the
	// readback check for readbackTestOrg/linear.
	if err := conn.Exec(ctx, `
INSERT INTO teams (id, team_uuid, name, description, members, updated_at, last_synced, org_id, provider, native_team_key)
VALUES (generateUUIDv4(), generateUUIDv4(), 'ENG', NULL, [], now64(6), now64(6), 'foreign-org', 'linear', 'ENG')`); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(ctx, `
INSERT INTO sprints (provider, sprint_id, name, state, started_at, ended_at, completed_at, last_synced, org_id, native_team_key)
VALUES ('jira', 'sprint-1', 'sprint-1', NULL, NULL, NULL, NULL, now64(3), ?, '')`, readbackTestOrg); err != nil {
		t.Fatal(err)
	}

	verifier, err := NewClickHouseReadbackVerifier(conn)
	if err != nil {
		t.Fatal(err)
	}

	missingTeams, err := verifier.MissingTeamKeys(ctx, readbackTestOrg, "linear", []string{"ENG", "PLATFORM"})
	if err != nil {
		t.Fatalf("MissingTeamKeys: %v", err)
	}
	if len(missingTeams) != 1 || missingTeams[0] != "PLATFORM" {
		t.Fatalf("missingTeams=%v want=[PLATFORM]", missingTeams)
	}

	missingSprints, err := verifier.MissingSprintIDs(ctx, readbackTestOrg, "linear", []string{"sprint-1", "sprint-2"})
	if err != nil {
		t.Fatalf("MissingSprintIDs: %v", err)
	}
	if len(missingSprints) != 1 || missingSprints[0] != "sprint-2" {
		t.Fatalf("missingSprints=%v want=[sprint-2]", missingSprints)
	}

	// The foreign-org/foreign-provider rows above must not satisfy the
	// tenant/provider fence for readbackTestOrg/linear.
	stillMissingTeams, err := verifier.MissingTeamKeys(ctx, readbackTestOrg, "linear", []string{"ENG"})
	if err != nil {
		t.Fatal(err)
	}
	if len(stillMissingTeams) != 0 {
		t.Fatalf("stillMissingTeams=%v want=[] (own-tenant row is visible)", stillMissingTeams)
	}
}

// TestReferenceReadbackVerifierEndToEnd exercises ReferenceReadbackVerifier's
// poll loop against the real ClickHouseReadbackVerifier: a key that appears
// AFTER the first poll (simulating eventual-consistency lag between the
// populate write and this read) must still succeed within the deadline.
func TestReferenceReadbackVerifierEndToEnd(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	checker, err := NewClickHouseReadbackVerifier(conn)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewReferenceReadbackVerifier(checker)
	if err != nil {
		t.Fatal(err)
	}
	verifier.timeout = 2 * time.Second

	polls := 0
	verifier.sleep = func(time.Duration) {
		polls++
		if polls == 1 {
			insertReadbackTeam(t, ctx, conn, "linear", "ENG")
		}
	}

	err = verifier.Verify(ctx, readbackTestOrg, "linear", map[string]any{
		"reference_team_keys": []any{"ENG"},
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if polls < 1 {
		t.Fatalf("polls=%d want>=1 (the row was not there on the first check)", polls)
	}
}
