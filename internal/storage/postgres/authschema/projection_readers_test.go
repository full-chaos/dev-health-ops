package authschema

import (
	"reflect"
	"testing"
)

// The projection reader's grant is the reviewed one, written out here independently of ProjectionReaderReads: the two
// filters of the read, the keyset column and the three columns it returns (CHAOS-11199). No fingerprint, scope, source,
// owner or principal column.
func TestProjectionReaderReadsAreTheReviewedColumns(t *testing.T) {
	want := []ColumnGrant{{Table: "credential_projection", Columns: []string{
		"domain_owner", "organization_id", "external_reference", "revision", "status", "expires_at"}}}
	if got := ProjectionReaderReads(); !reflect.DeepEqual(got, want) {
		t.Fatalf("ProjectionReaderReads() = %+v, want %+v", got, want)
	}
}

func TestProjectionReaderRolesAreValidated(t *testing.T) {
	base := Options{Schema: "auth", RuntimeRole: "auth_runtime"}
	for name, set := range map[string]func(*Options){
		"runtime role":   func(o *Options) { o.ProjectionReaderRoles = []string{"auth_runtime"} },
		"migration role": func(o *Options) { o.ProjectionReaderRoles = []string{"auth_migrator"} },
		"duplicate":      func(o *Options) { o.ProjectionReaderRoles = []string{"devhealth_worker", "devhealth_worker"} },
		"bad name":       func(o *Options) { o.ProjectionReaderRoles = []string{"Bad-Name"} },
		"a session reader": func(o *Options) {
			o.SessionReaderRoles = []string{"devhealth_api"}
			o.ProjectionReaderRoles = []string{"devhealth_api"}
		},
		"the schema owner": func(o *Options) { o.SchemaOwnerRole = "auth_owner"; o.ProjectionReaderRoles = []string{"auth_owner"} },
	} {
		options := base
		set(&options)
		if err := options.Validate("auth_migrator"); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	options := base
	options.ProjectionReaderRoles = []string{"devhealth_worker"}
	if err := options.Validate("auth_migrator"); err != nil {
		t.Errorf("one distinct projection reader: %v", err)
	}
}

// A projection reader that loses a declared column grant is a narrowing the operator must acknowledge, named by the
// reader; a grant it never declared is not counted.
func TestProjectionReaderNarrowingNamesTheLostDeclaredColumns(t *testing.T) {
	grant := func(column string) GrantRecord {
		return GrantRecord{Kind: "column", Object: "credential_projection", Column: column, Privilege: "SELECT"}
	}
	before := []GrantRecord{grant("status"), grant("revision"), {Kind: "column", Object: "sessions", Column: "id", Privilege: "SELECT"}}
	after := []GrantRecord{grant("revision")}
	got := projectionReaderNarrowing("devhealth_worker", before, after)
	want := []GrantRecord{{Kind: "column", Object: "credential_projection", Column: "status", Privilege: "SELECT", Role: "devhealth_worker"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("projectionReaderNarrowing = %+v, want %+v", got, want)
	}
	if got := projectionReaderNarrowing("devhealth_worker", after, after); len(got) != 0 {
		t.Fatalf("an unchanged grant narrowed: %+v", got)
	}
}
