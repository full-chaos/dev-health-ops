package providersync

import (
	"testing"
	"time"
)

// testPID builds a ProjectID of any text for a test that needs a stored id
// the constructors do not make (a legacy or foreign form). It lives in a test
// file only: no production file may build a ProjectID by hand
// (TestProjectIDConstructionCensus).
func testPID(value string) ProjectID { return ProjectID{value} }

// mustProjectID unwraps a constructor result in a test.
func mustProjectID(t testing.TB) func(ProjectID, bool) ProjectID {
	t.Helper()
	return func(id ProjectID, ok bool) ProjectID {
		t.Helper()
		if !ok {
			t.Fatalf("project id constructor refused its input")
		}
		return id
	}
}

// mustGitLabOwnershipRow is normalizeGitLabOwnershipRow for a test whose path
// is a valid one.
func mustGitLabOwnershipRow(orgID, teamID, projectPath string, specificity uint16, at time.Time) gitlabTeamCatalogOwnershipRow {
	row, ok := normalizeGitLabOwnershipRow(orgID, teamID, projectPath, specificity, at)
	if !ok {
		panic("mustGitLabOwnershipRow: blank project path")
	}
	return row
}
