//go:build integration

package teamattribution

import (
	"context"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// These three tests are the executed proof that LoadProjects, LoadRepos and
// LoadProviderMembers return the NEWEST version's value for their one
// Nullable(String)/Nullable(UUID) projected column, never a stale non-null
// value from an older version. Each seeds two physical rows for the same
// dedup identity: an OLDER row with a real value and a NEWER row (by
// (updated_at, valid_from)) with that column NULL. Before the tuple-wrap
// fix, argMax(col, version) silently skips the NULL and returns the older
// value; after the fix, (argMax(tuple(col), version)).1 returns NULL.

func openTeamAttributionSchema(ctx context.Context, t *testing.T) stdclickhouse.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse test dependency: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = instance.Close(closeCtx)
	})
	options, err := stdclickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatalf("parse ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("open ClickHouse connection: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Minimal schema matching migrations 002_teams.sql (as amended by
	// 051_team_attribution_dimensions.sql) and 051 itself, restricted to the
	// columns these three readers actually project.
	statements := []string{
		`CREATE TABLE teams (
			org_id String, id String, name String,
			updated_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC')
		) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, id)`,
		`CREATE TABLE team_project_ownership (
			org_id String, provider String, team_id String, project_id String,
			project_key Nullable(String),
			source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
			is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
			valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')),
			updated_at DateTime64(3, 'UTC')
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (org_id, provider, project_id, team_id, source, valid_from)`,
		`CREATE TABLE team_repo_ownership (
			org_id String, provider String, team_id String,
			repo_id Nullable(UUID), repo_full_name String,
			match_type Enum8('exact' = 1, 'pattern' = 2),
			source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
			is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
			valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')),
			updated_at DateTime64(3, 'UTC')
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (org_id, provider, repo_full_name, team_id, source, valid_from)`,
		`CREATE TABLE team_memberships (
			org_id String, provider String, team_id String, member_id String,
			raw_provider_user_id Nullable(String), raw_email Nullable(String),
			identity_facets Array(String) DEFAULT [],
			source Enum8('native' = 1, 'jira_legacy' = 2, 'provider_access' = 3, 'manual' = 4, 'inferred' = 5),
			is_primary UInt8 DEFAULT 0, specificity UInt16 DEFAULT 0, priority Int32 DEFAULT 0,
			valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')),
			updated_at DateTime64(3, 'UTC')
		) ENGINE = ReplacingMergeTree(updated_at)
		ORDER BY (org_id, provider, team_id, member_id, source, valid_from)`,
	}
	for _, stmt := range statements {
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("create schema: %v\nstatement: %s", err, stmt)
		}
	}
	return conn
}

func TestLoadProjectsReturnsNewestNullProjectKeyNotStaleValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	staleKey := "PROJ-STALE"
	insertOwnership := `INSERT INTO team_project_ownership
		(org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, ?, ?, ?, 'manual', 1, 1, 1, ?, NULL, ?)`
	if err := conn.Exec(ctx, insertOwnership, "org-4547", "github", "team-a", "proj-1", staleKey, older, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := conn.Exec(ctx, insertOwnership, "org-4547", "github", "team-a", "proj-1", nil, newer, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	source := ClickHouseFactSource{Conn: conn}
	facts, err := source.LoadProjects(ctx, "org-4547", asOf)
	if err != nil {
		t.Fatalf("LoadProjects: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("LoadProjects returned %d facts, want 1: %+v", len(facts), facts)
	}
	if facts[0].ProjectKey != nil {
		t.Fatalf("ProjectKey = %q, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %q instead",
			*facts[0].ProjectKey, staleKey)
	}
}

func TestLoadReposReturnsNewestNullRepoIDNotStaleValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	staleRepoID := "11111111-1111-4111-8111-111111111111"
	insertOwnership := `INSERT INTO team_repo_ownership
		(org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, ?, ?, ?, 'exact', 'manual', 1, 1, 1, ?, NULL, ?)`
	if err := conn.Exec(ctx, insertOwnership, "org-4547", "github", "team-a", staleRepoID, "acme/api", older, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := conn.Exec(ctx, insertOwnership, "org-4547", "github", "team-a", nil, "acme/api", newer, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	source := ClickHouseFactSource{Conn: conn}
	facts, err := source.LoadRepos(ctx, "org-4547", asOf)
	if err != nil {
		t.Fatalf("LoadRepos: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("LoadRepos returned %d facts, want 1: %+v", len(facts), facts)
	}
	if facts[0].RepoID != nil {
		t.Fatalf("RepoID = %q, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %q instead",
			*facts[0].RepoID, staleRepoID)
	}
}

func TestLoadProviderMembersReturnsNewestNullFieldsNotStaleValues(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	conn := openTeamAttributionSchema(ctx, t)

	older := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	asOf := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

	staleUserID := "provider-user-stale"
	staleEmail := "stale@example.com"
	insertMembership := `INSERT INTO team_memberships
		(org_id, provider, team_id, member_id, raw_provider_user_id, raw_email, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, 'manual', 1, 1, 1, ?, NULL, ?)`
	if err := conn.Exec(ctx, insertMembership, "org-4547", "github", "team-a", "member-1", staleUserID, staleEmail, older, older); err != nil {
		t.Fatalf("insert older row: %v", err)
	}
	if err := conn.Exec(ctx, insertMembership, "org-4547", "github", "team-a", "member-1", nil, nil, newer, newer); err != nil {
		t.Fatalf("insert newer NULL row: %v", err)
	}

	source := ClickHouseFactSource{Conn: conn}
	facts, err := source.LoadProviderMembers(ctx, "org-4547", asOf)
	if err != nil {
		t.Fatalf("LoadProviderMembers: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("LoadProviderMembers returned %d facts, want 1: %+v", len(facts), facts)
	}
	if facts[0].RawProviderUserID != nil {
		t.Fatalf("RawProviderUserID = %q, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %q instead",
			*facts[0].RawProviderUserID, staleUserID)
	}
	if facts[0].RawEmail != nil {
		t.Fatalf("RawEmail = %q, want nil (newest row's NULL) -- argMax skipped the NULL and returned the stale %q instead",
			*facts[0].RawEmail, staleEmail)
	}
}
