package restorefence

import (
	"strings"
	"testing"
)

func TestInitStatementsWriteOneInitialRowForTheTimelineOnly(t *testing.T) {
	sql := initInsertSQL(testSchema(t))
	for _, want := range []string{`INSERT INTO "auth".restore_fence_state`, "'initial'", "'init'"} {
		if !strings.Contains(sql, want) {
			t.Errorf("init insert lacks %q", want)
		}
	}
	if strings.Contains(sql, "ON CONFLICT") {
		t.Error("init refuses an existing row; it never merges into one")
	}
	if got := initExistsSQL(testSchema(t)); !strings.Contains(got, "WHERE timeline_id = $1") || strings.Contains(got, "manifest_sha256") {
		t.Errorf("a row of ANY kind for the timeline refuses init: %s", got)
	}
}

func TestInitHelpSaysItIsOnlyForADatabaseThatIsNotARestore(t *testing.T) {
	if !strings.Contains(InitHelp, "only on a database that is NOT a restore") {
		t.Error("the init help must carry the warning")
	}
}
