//go:build integration

package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// A GitLab grant a store wrote before ownership was keyed by the catalog id
// holds the project's path as project_id. The next run keys the same grant by
// the catalog id and keeps the first valid_from; the path row ends where it
// began, so at every time exactly one row of the grant is valid. A path row of
// a project the run does not list is not touched by the carry.
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
		t.Fatalf("catalog-keyed row = %+v (present %v), want project_key org/api, valid_from %s, open", moved, ok, granted)
	}
	path := rows["org/api"]
	if path.validTo == nil || !path.validTo.Equal(granted.Add(time.Millisecond)) {
		t.Fatalf("path-keyed row valid_to = %v, want it to end where it began (%s + 1ms)", path.validTo, granted)
	}
	gone, present := rows["org/gone"]
	if !present || (gone.validTo != nil && !gone.validTo.After(granted.Add(time.Millisecond))) {
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
