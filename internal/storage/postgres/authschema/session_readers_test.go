package authschema

import (
	"reflect"
	"testing"
)

// The session readers' grant is the reviewed one, written out here
// independently of SessionReaderColumns.
func TestSessionReaderColumnsAreTheReviewedOnes(t *testing.T) {
	want := ColumnGrant{Table: "sessions", Columns: []string{"id", "principal_id", "revoked_at"}}
	if got := SessionReaderColumns(); !reflect.DeepEqual(got, want) {
		t.Fatalf("SessionReaderColumns() = %+v, want %+v", got, want)
	}
}

func TestSessionReaderRolesAreValidated(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "auth_runtime"}
	for name, readers := range map[string][]string{
		"runtime role":   {"auth_runtime"},
		"migration role": {"auth_migrator"},
		"duplicate":      {"devhealth_api", "devhealth_api"},
		"bad name":       {"Bad-Name"},
	} {
		options := base
		options.SessionReaderRoles = readers
		if err := options.Validate("auth_migrator"); err == nil {
			t.Errorf("%s: %v accepted", name, readers)
		}
	}
	options := base
	options.SessionReaderRoles = []string{"devhealth_api", "devhealth_query"}
	if err := options.Validate("auth_migrator"); err != nil {
		t.Errorf("two distinct readers: %v", err)
	}
}
