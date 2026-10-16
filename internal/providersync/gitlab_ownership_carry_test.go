package providersync

import (
	"testing"
	"time"
)

func gitlabCarryRows(t *testing.T, catalogAtPath map[string][]string) (fresh, path gitlabTeamCatalogOwnershipRow, out []gitlabTeamCatalogOwnershipRow, at time.Time) {
	t.Helper()
	granted := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	at = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	fresh = mustGitLabOwnershipRow("org", "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, at)
	path = mustGitLabOwnershipRow("org", "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, granted)
	path.ProjectID = testPID("org/api")
	out, _ = gitlabOwnershipSnapshot([]gitlabTeamCatalogOwnershipRow{fresh}, []gitlabTeamCatalogOwnershipRow{path}, catalogAtPath, at, KindSnapshot[OwnershipSnapshotRow]{})
	return fresh, path, out, at
}

func validAt(row gitlabTeamCatalogOwnershipRow, instant time.Time) bool {
	return !row.ValidFrom.After(instant) && (row.ValidTo == nil || row.ValidTo.After(instant))
}

// The carried grant has exactly one valid row at every instant: the path row
// holds the grant's first instant and the catalog-keyed row starts where it
// ends.
func TestGitLabOwnershipCarryLeavesExactlyOneValidRowAtEveryInstant(t *testing.T) {
	fresh, path, out, _ := gitlabCarryRows(t, nil)
	if len(out) != 2 {
		t.Fatalf("rows written = %d, want the catalog row and the ended path row: %+v", len(out), out)
	}
	for _, instant := range []time.Time{path.ValidFrom, path.ValidFrom.Add(time.Millisecond), path.ValidFrom.Add(24 * time.Hour), fresh.ValidFrom} {
		valid := 0
		for _, row := range out {
			if validAt(row, instant) {
				valid++
			}
		}
		if valid != 1 {
			t.Errorf("rows of the grant valid at %s = %d, want exactly 1: %+v", instant, valid, out)
		}
	}
	for _, row := range out {
		if row.ProjectID == fresh.ProjectID && !row.ValidFrom.Equal(path.ValidFrom.Add(time.Millisecond)) {
			t.Errorf("catalog row valid_from = %s, want the path row's end %s", row.ValidFrom, path.ValidFrom.Add(time.Millisecond))
		}
	}
}

// A path the projects catalog also names for another project id was taken
// over: the new project's grant starts at the run, and the old path row ends
// at the run. The new project does not inherit the old project's history.
func TestGitLabOwnershipCarryDoesNotGiveATakenOverPathTheOldHistory(t *testing.T) {
	_, path, out, at := gitlabCarryRows(t, map[string][]string{"org/api": {"org:gitlab:1"}})
	for _, row := range out {
		switch row.ProjectID.String() {
		case path.ProjectID.String():
			if row.ValidTo == nil || !row.ValidTo.Equal(at) {
				t.Errorf("old path row valid_to = %v, want the run %s", row.ValidTo, at)
			}
		default:
			if !row.ValidFrom.Equal(at) {
				t.Errorf("new project's row valid_from = %s, want the run %s (no inherited history)", row.ValidFrom, at)
			}
		}
	}
	if len(out) != 2 {
		t.Fatalf("rows written = %d, want 2: %+v", len(out), out)
	}
}

// The catalog naming only the fresh project at the path is the same project:
// the grant is carried.
func TestGitLabOwnershipCarryKeepsTheHistoryWhenTheCatalogNamesTheSameProject(t *testing.T) {
	fresh, path, out, _ := gitlabCarryRows(t, map[string][]string{"org/api": {"org:gitlab:" + testGitLabNativeProjectID("org/api")}})
	for _, row := range out {
		if row.ProjectID == fresh.ProjectID && !row.ValidFrom.Equal(path.ValidFrom.Add(time.Millisecond)) {
			t.Errorf("catalog row valid_from = %s, want the carried start %s", row.ValidFrom, path.ValidFrom.Add(time.Millisecond))
		}
	}
}
