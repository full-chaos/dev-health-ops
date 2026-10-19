//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A GitLab grant a store wrote before ownership was keyed by the catalog id
// holds the project's path as project_id. The next run keys the same grant by
// the catalog id with the grant's first instant, and the path row becomes an
// empty-interval tombstone, so at every time exactly one row of the grant is
// valid. A path row of a project the run does not list is not
// touched by the carry.
func TestGitLabOwnershipPathRowMovesToTheCatalogIDWithItsValidFrom(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org, fake := "gl-catalog-carry", newGitLabSnapshotServer(t)
	fake.set("", "", nil, map[string][]string{"org": {"org/api"}})
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	granted := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	run := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

	legacy := mustGitLabOwnershipRow(org, "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/api")
	unlisted := mustGitLabOwnershipRow(org, "gl:org", "org/gone", gitlabTeamCatalogBaseSpecificity, granted)
	unlisted.ProjectID = testPID("org/gone")
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{legacy, unlisted}); err != nil {
		t.Fatal(err)
	}

	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run); err != nil {
		t.Fatal(err)
	}

	type version struct {
		projectID, key string
		validFrom      time.Time
		validTo        *time.Time
	}
	read := func() map[string]version {
		t.Helper()
		result, err := conn.Query(ctx, `SELECT project_id, ifNull(project_key, ''), valid_from, valid_to FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org'`, org)
		if err != nil {
			t.Fatal(err)
		}
		defer result.Close()
		out := map[string]version{}
		for result.Next() {
			var row version
			if err := result.Scan(&row.projectID, &row.key, &row.validFrom, &row.validTo); err != nil {
				t.Fatal(err)
			}
			out[row.projectID] = row
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
		return out
	}
	rows := read()
	catalogID := org + ":gitlab:" + testGitLabNativeProjectID("org/api")
	moved, ok := rows[catalogID]
	if !ok || moved.key != "org/api" || !moved.validFrom.Equal(granted) || moved.validTo != nil {
		t.Fatalf("catalog-keyed row = %+v (present %v), want project_key org/api, valid_from %s (the grant's first instant), open", moved, ok, granted)
	}
	path := rows["org/api"]
	if path.validTo == nil || !path.validTo.Equal(granted) {
		t.Fatalf("path-keyed row valid_to = %v, want the empty-interval tombstone at %s", path.validTo, granted)
	}
	gone, present := rows["org/gone"]
	if !present || (gone.validTo != nil && !gone.validTo.After(granted)) {
		t.Fatalf("the unlisted path row = %+v (present %v): the carry must not replace a row no listed project matches", gone, present)
	}

	var validMidway uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org' AND ifNull(project_key, '') = 'org/api'
  AND valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)`, org, granted.Add(24*time.Hour), granted.Add(24*time.Hour)).Scan(&validMidway); err != nil {
		t.Fatal(err)
	}
	if validMidway != 1 {
		t.Fatalf("rows of the grant valid a day after it began = %d, want exactly 1", validMidway)
	}

	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if again := read()[catalogID]; !again.validFrom.Equal(granted) || again.validTo != nil {
		t.Fatalf("after a second run the catalog-keyed row = %+v, want unchanged", again)
	}
}

// A grant written under a project's old path, for a project renamed before the
// run: the repos row of the old path names the project's numeric id
// (provider_repo_id), so the grant moves to the catalog id at the new path with
// its first instant. The repos read is the real one.
func TestGitLabOwnershipPathRowOfARenamedProjectIsCarriedByID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org, fake := "gl-catalog-rename", newGitLabSnapshotServer(t)
	fake.set("", "", nil, map[string][]string{"org": {"org/new"}})
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	granted := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	run := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	nativeID := testGitLabNativeProjectID("org/new")
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, provider_repo_id, created_at, last_synced) VALUES (generateUUIDv4(), 'org/old', ?, 'gitlab', ?, ?, ?)`,
		org, nativeID, granted, granted); err != nil {
		t.Fatal(err)
	}
	legacy := mustGitLabOwnershipRow(org, "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/old")
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{legacy}); err != nil {
		t.Fatal(err)
	}
	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run); err != nil {
		t.Fatal(err)
	}
	var validFrom time.Time
	var validTo *time.Time
	if err := conn.QueryRow(ctx, `SELECT valid_from, valid_to FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org' AND project_id = ?`, org, org+":gitlab:"+nativeID).Scan(&validFrom, &validTo); err != nil {
		t.Fatal(err)
	}
	if !validFrom.Equal(granted) || validTo != nil {
		t.Fatalf("renamed project's row = [%s, %v), want the grant's first instant (%s) and open", validFrom, validTo, granted)
	}
}

// The same rename after CHAOS-3383: the project keeps one repos row, whose
// repo is the new path and whose identity_path is the old path it was minted
// at. That old path still names the project, so the grant is carried by id.
func TestGitLabOwnershipPathRowOfAProjectRenamedUnderAStableRepoIDIsCarriedByID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org, fake := "gl-catalog-stable-rename", newGitLabSnapshotServer(t)
	fake.set("", "", nil, map[string][]string{"org": {"org/new"}})
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	granted := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	run := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	nativeID := testGitLabNativeProjectID("org/new")
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, identity_path, org_id, provider, provider_repo_id, created_at, last_synced) VALUES (generateUUIDv4(), 'org/new', 'org/old', ?, 'gitlab', ?, ?, ?)`,
		org, nativeID, granted, granted); err != nil {
		t.Fatal(err)
	}
	legacy := mustGitLabOwnershipRow(org, "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/old")
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{legacy}); err != nil {
		t.Fatal(err)
	}
	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run); err != nil {
		t.Fatal(err)
	}
	var validFrom time.Time
	var validTo *time.Time
	if err := conn.QueryRow(ctx, `SELECT valid_from, valid_to FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org' AND project_id = ?`, org, org+":gitlab:"+nativeID).Scan(&validFrom, &validTo); err != nil {
		t.Fatal(err)
	}
	if !validFrom.Equal(granted) || validTo != nil {
		t.Fatalf("renamed project's row = [%s, %v), want the grant's first instant (%s) and open", validFrom, validTo, granted)
	}
}

// A repos row from before migration 134 names its project only in
// settings.project_id. The carry names the row's project by the same rule as
// the identity resolver, so a grant under that row's path is carried by id.
func TestGitLabOwnershipPathRowOfALegacyReposRowIsCarriedBySettingsProjectID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org, fake := "gl-catalog-legacy-settings", newGitLabSnapshotServer(t)
	fake.set("", "", nil, map[string][]string{"org": {"org/new"}})
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	granted := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	run := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	nativeID := testGitLabNativeProjectID("org/new")
	if err := conn.Exec(ctx, `INSERT INTO repos (id, repo, org_id, provider, settings, created_at, last_synced) VALUES (generateUUIDv4(), 'org/old', ?, 'gitlab', ?, ?, ?)`,
		org, `{"source":"gitlab","project_id":`+nativeID+`}`, granted, granted); err != nil {
		t.Fatal(err)
	}
	legacy := mustGitLabOwnershipRow(org, "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/old")
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{legacy}); err != nil {
		t.Fatal(err)
	}
	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run); err != nil {
		t.Fatal(err)
	}
	var validFrom time.Time
	var validTo *time.Time
	if err := conn.QueryRow(ctx, `SELECT valid_from, valid_to FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org' AND project_id = ?`, org, org+":gitlab:"+nativeID).Scan(&validFrom, &validTo); err != nil {
		t.Fatal(err)
	}
	if !validFrom.Equal(granted) || validTo != nil {
		t.Fatalf("legacy project's row = [%s, %v), want the grant's first instant (%s) and open", validFrom, validTo, granted)
	}
}

// A path the projects catalog names for another (deleted) project was taken
// over by the listed project: the listed project's grant starts at the run, and
// the old path row ends at the run. The projects read is the real one.
func TestGitLabOwnershipPathRowOfATakenOverPathIsNotCarried(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org, fake := "gl-catalog-takeover", newGitLabSnapshotServer(t)
	fake.set("", "", nil, map[string][]string{"org": {"org/api"}})
	sink := GitLabTeamCatalogClickHouseEffects{Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}
	granted := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	run := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

	deleted, ok := normalizeGitLabProjectCatalogRow(org, gitlabTeamCatalogProjectPayload{ID: json.Number("1"), PathWithNamespace: "org/api", Name: "api"}, granted)
	if !ok {
		t.Fatal("the catalog normalizer refused the old project")
	}
	claim := Claim{Unit: Unit{OrgID: org, Provider: gitlabTeamCatalogProvider}}
	if err := sink.writeProjects(ctx, claim, []gitlabTeamCatalogProjectRow{deleted}); err != nil {
		t.Fatal(err)
	}
	legacy := mustGitLabOwnershipRow(org, "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/api")
	if err := sink.writeOwnership(ctx, []gitlabTeamCatalogOwnershipRow{legacy}); err != nil {
		t.Fatal(err)
	}
	if _, err := gitlabSnapshotRun(ctx, t, conn, org, fake, TeamCatalogSelections{Projects: true}, false, run); err != nil {
		t.Fatal(err)
	}
	result, err := conn.Query(ctx, `SELECT project_id, valid_from, valid_to FROM team_project_ownership FINAL
WHERE org_id = ? AND provider = 'gitlab' AND team_id = 'gl:org'`, org)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	seen := 0
	for result.Next() {
		var projectID string
		var validFrom time.Time
		var validTo *time.Time
		if err := result.Scan(&projectID, &validFrom, &validTo); err != nil {
			t.Fatal(err)
		}
		seen++
		switch projectID {
		case "org/api":
			if validTo == nil || !validTo.Equal(run) {
				t.Errorf("old path row valid_to = %v, want the run %s", validTo, run)
			}
		case org + ":gitlab:" + testGitLabNativeProjectID("org/api"):
			if !validFrom.Equal(run) || validTo != nil {
				t.Errorf("the new project's row = [%s, %v), want it to start at the run and stay open", validFrom, validTo)
			}
		default:
			t.Errorf("unexpected row %s", projectID)
		}
	}
	if err := result.Err(); err != nil {
		t.Fatal(err)
	}
	if seen != 2 {
		t.Fatalf("rows = %d, want the ended path row and the new project's row", seen)
	}
}
