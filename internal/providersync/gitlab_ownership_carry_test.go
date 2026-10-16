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

// The carried grant has exactly one valid row at every instant: the
// catalog-keyed row takes the grant's first instant and the path row it
// replaces is an empty-interval tombstone.
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
		if row.ProjectID == fresh.ProjectID && !row.ValidFrom.Equal(path.ValidFrom) {
			t.Errorf("catalog row valid_from = %s, want the grant's first instant %s", row.ValidFrom, path.ValidFrom)
		}
		if row.ProjectID == path.ProjectID && (row.ValidTo == nil || !row.ValidTo.Equal(path.ValidFrom)) {
			t.Errorf("path row valid_to = %v, want the tombstone at its own valid_from %s", row.ValidTo, path.ValidFrom)
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

// A grant written under the project's old path, for a project renamed before
// this run: the store knows the old path was the same numeric project (repos
// history), so the grant is carried to the project's catalog id at its new
// path with its first instant.
func TestGitLabOwnershipCarryFollowsARenameTheStoreKnows(t *testing.T) {
	granted := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	fresh := mustGitLabOwnershipRow("org", "gl:org", "org/new", gitlabTeamCatalogBaseSpecificity, at)
	legacy := mustGitLabOwnershipRow("org", "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/old")
	out, _ := gitlabOwnershipSnapshot([]gitlabTeamCatalogOwnershipRow{fresh}, []gitlabTeamCatalogOwnershipRow{legacy},
		map[string][]string{"org/old": {fresh.ProjectID.String()}}, at, KindSnapshot[OwnershipSnapshotRow]{})
	start := granted
	for _, row := range out {
		switch row.ProjectID {
		case fresh.ProjectID:
			if !row.ValidFrom.Equal(start) {
				t.Errorf("renamed project's row valid_from = %s, want the carried start %s", row.ValidFrom, start)
			}
		case legacy.ProjectID:
			if row.ValidTo == nil || !row.ValidTo.Equal(start) {
				t.Errorf("old path row valid_to = %v, want %s", row.ValidTo, start)
			}
		}
	}
	if len(out) != 2 {
		t.Fatalf("rows written = %d, want the carried row and the ended path row: %+v", len(out), out)
	}
}

// The project the store saw at the old path was renamed and a new project
// took the path: the grant follows the renamed project by id, and the new
// project at the path starts at the run.
func TestGitLabOwnershipCarryFollowsTheRenamedProjectNotThePathsNewOwner(t *testing.T) {
	granted := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	renamed := mustGitLabOwnershipRow("org", "gl:org", "org/new", gitlabTeamCatalogBaseSpecificity, at)
	newcomer := mustGitLabOwnershipRow("org", "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, at)
	legacy := mustGitLabOwnershipRow("org", "gl:org", "org/old", gitlabTeamCatalogBaseSpecificity, granted)
	legacy.ProjectID = testPID("org/old")
	out, _ := gitlabOwnershipSnapshot([]gitlabTeamCatalogOwnershipRow{renamed, newcomer}, []gitlabTeamCatalogOwnershipRow{legacy},
		map[string][]string{"org/old": {renamed.ProjectID.String()}}, at, KindSnapshot[OwnershipSnapshotRow]{})
	for _, row := range out {
		switch row.ProjectID {
		case renamed.ProjectID:
			if !row.ValidFrom.Equal(granted) {
				t.Errorf("renamed project's row valid_from = %s, want the carried start", row.ValidFrom)
			}
		case newcomer.ProjectID:
			if !row.ValidFrom.Equal(at) {
				t.Errorf("the path's new project starts at %s, want the run %s", row.ValidFrom, at)
			}
		}
	}
	if len(out) != 3 {
		t.Fatalf("rows written = %d, want 3: %+v", len(out), out)
	}
}

// Two open path rows of one grant (an older store's duplicate): the grant is
// carried from the earlier one, and both path rows become tombstones, so no
// instant has two valid rows of the grant.
func TestGitLabOwnershipCarryTombstonesADuplicatePathRow(t *testing.T) {
	granted := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	later := granted.Add(48 * time.Hour)
	at := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	fresh := mustGitLabOwnershipRow("org", "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, at)
	first := mustGitLabOwnershipRow("org", "gl:org", "org/api", gitlabTeamCatalogBaseSpecificity, later)
	first.ProjectID = testPID("org/api")
	second := first
	second.ValidFrom = granted
	out, _ := gitlabOwnershipSnapshot([]gitlabTeamCatalogOwnershipRow{fresh}, []gitlabTeamCatalogOwnershipRow{first, second}, nil, at, KindSnapshot[OwnershipSnapshotRow]{})
	for _, instant := range []time.Time{granted, later, later.Add(time.Millisecond), at} {
		valid := 0
		for _, row := range out {
			if validAt(row, instant) {
				valid++
			}
		}
		if valid != 1 {
			t.Errorf("rows valid at %s = %d, want exactly 1: %+v", instant, valid, out)
		}
	}
	if len(out) != 3 {
		t.Fatalf("rows written = %d, want the carried row and two tombstones: %+v", len(out), out)
	}
}

// The catalog naming only the fresh project at the path is the same project:
// the grant is carried.
func TestGitLabOwnershipCarryKeepsTheHistoryWhenTheCatalogNamesTheSameProject(t *testing.T) {
	fresh, path, out, _ := gitlabCarryRows(t, map[string][]string{"org/api": {"org:gitlab:" + testGitLabNativeProjectID("org/api")}})
	for _, row := range out {
		if row.ProjectID == fresh.ProjectID && !row.ValidFrom.Equal(path.ValidFrom) {
			t.Errorf("catalog row valid_from = %s, want the carried start %s", row.ValidFrom, path.ValidFrom)
		}
	}
}
