package operationalordering

import (
	"errors"
	"strings"
	"testing"
)

func TestCheckForReadAcceptsOnlyUnsetOrTwo(t *testing.T) {
	for name, tc := range map[string]struct {
		value   string
		set     bool
		refused bool
	}{
		"unset": {"", false, false},
		"two":   {"2", true, false},
		"one":   {"1", true, true},
		"blank": {"", true, true},
		"zero":  {"0", true, true},
		"three": {"3", true, true},
		"pad":   {" 2", true, true},
		"typo":  {"two", true, true},
	} {
		err := CheckForRead(func(string) (string, bool) { return tc.value, tc.set })
		if (err != nil) != tc.refused {
			t.Errorf("%s: err = %v, want refused=%v", name, err, tc.refused)
		}
		var unsupported UnsupportedError
		if tc.refused && !errors.As(err, &unsupported) {
			t.Errorf("%s: err %v is not an UnsupportedError", name, err)
		}
	}
}

func TestCurrentRowSelectionIsRevisionOrderedAndFiltersComeAfterIt(t *testing.T) {
	query := RevisionCurrentRows("operational_incidents", "org_id = {org_id:String}", []string{"is_deleted = 0"})
	for _, want := range []string{
		"FROM operational_incidents",
		"ORDER BY org_id, id, source_revision DESC, source_conflict_key DESC, ingest_revision DESC",
		"LIMIT 1 BY org_id, id",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("missing %q in\n%s", want, query)
		}
	}
	if strings.Contains(query, "FINAL") {
		t.Errorf("a contract-2 read must not use FINAL:\n%s", query)
	}
	if strings.Index(query, "WHERE is_deleted = 0") < strings.Index(query, "LIMIT 1 BY") {
		t.Errorf("the post-selection filter must come after the row selection:\n%s", query)
	}
	if got := LatestRevisionRow("a,b", "t", "org_id = ?"); got !=
		"SELECT a,b FROM t WHERE org_id = ? ORDER BY source_revision DESC, source_conflict_key DESC, ingest_revision DESC LIMIT 1" {
		t.Errorf("LatestRevisionRow = %q", got)
	}
}
