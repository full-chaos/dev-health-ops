package rivermigrate

import (
	"errors"
	"reflect"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func noNames(t *testing.T) func([]postgresstore.PostureGap) ([]aclEntry, error) {
	return func([]postgresstore.PostureGap) ([]aclEntry, error) {
		t.Helper()
		t.Fatal("the grantor read ran without an excess")
		return nil, nil
	}
}

func TestAPIExcessMissingGapsNeverFail(t *testing.T) {
	gaps := []postgresstore.PostureGap{
		{TableName: "organizations", Missing: []string{"SELECT"}},
		{TableName: "internal_service_credential_audits", TableMissing: true},
		{TableName: "users", ColumnName: "email", Missing: []string{"UPDATE"}},
	}
	for _, gaps := range [][]postgresstore.PostureGap{nil, gaps} {
		if got := apiExcessOutcome(gaps, nil, noNames(t)); !got.ok || got.unavailable || got.lines != nil {
			t.Errorf("apiExcessOutcome(%v) = %+v, want ok", gaps, got)
		}
	}
}

func TestAPIExcessReadErrorFailsClosed(t *testing.T) {
	got := apiExcessOutcome(nil, postgresstore.ErrUnavailable, noNames(t))
	if got.ok || !got.unavailable {
		t.Errorf("apiExcessOutcome(read error) = %+v, want a failure (unavailable)", got)
	}
}

func TestAPIExcessFailsAndNamesEachEntry(t *testing.T) {
	gaps := []postgresstore.PostureGap{
		{TableName: "organizations", Missing: []string{"SELECT"}},
		{TableName: "users", Excess: []string{"SELECT WITH GRANT OPTION"}},
		{TableName: "users", ColumnName: "saml_nameid_format", Excess: []string{"INSERT (saml_nameid_format)"}},
		{TableName: "sync_dispatch_transport_routes", Excess: []string{"SELECT"}},
		{TableName: "saml_assertion_replays", Excess: []string{"DELETE"}},
	}
	var asked []postgresstore.PostureGap
	names := func(excess []postgresstore.PostureGap) ([]aclEntry, error) {
		asked = excess
		return []aclEntry{
			{Table: "users", Grantee: "devhealth_api", Grantor: "devhealth", Privilege: "SELECT"},
			{Table: "users", Grantee: "devhealth_api", Grantor: "devhealth_auth_migrator", Privilege: "SELECT", Grantable: true},
			{Table: "users", Column: "saml_nameid_format", Grantee: "devhealth_api", Grantor: "devhealth_auth_migrator", Privilege: "INSERT"},
			{Table: "users", Column: "email", Grantee: "devhealth_api", Grantor: "devhealth", Privilege: "UPDATE"},
			{Table: "sync_dispatch_transport_routes", Grantee: "PUBLIC", Grantor: "devhealth", Privilege: "SELECT"},
		}, nil
	}
	got := apiExcessOutcome(gaps, nil, names)
	if got.ok || got.unavailable {
		t.Fatalf("apiExcessOutcome = %+v, want an excess failure", got)
	}
	if !reflect.DeepEqual(asked, gaps[1:]) {
		t.Errorf("grantor read asked for %v, want only the excess gaps %v", asked, gaps[1:])
	}
	want := []string{
		"users: excess privileges [SELECT WITH GRANT OPTION] beyond the declared posture (SELECT via devhealth_api granted by devhealth_auth_migrator with grant option)",
		"users.saml_nameid_format: excess privileges [INSERT (saml_nameid_format)] beyond the declared posture (INSERT via devhealth_api granted by devhealth_auth_migrator)",
		"sync_dispatch_transport_routes: excess privileges [SELECT] beyond the declared posture (SELECT via PUBLIC granted by devhealth)",
		"saml_assertion_replays: excess privileges [DELETE] beyond the declared posture (no ACL entry on the object reaches the role)",
	}
	if !reflect.DeepEqual(got.lines, want) {
		t.Errorf("lines =\n%q\nwant\n%q", got.lines, want)
	}
}

func TestAPIExcessGrantorReadFailureStillFails(t *testing.T) {
	gaps := []postgresstore.PostureGap{{TableName: "users", Excess: []string{"INSERT"}}}
	names := func([]postgresstore.PostureGap) ([]aclEntry, error) { return nil, errors.New("read failed") }
	got := apiExcessOutcome(gaps, nil, names)
	want := []string{"users: excess privileges [INSERT] beyond the declared posture (grantor read unavailable)"}
	if got.ok || got.unavailable || !reflect.DeepEqual(got.lines, want) {
		t.Errorf("apiExcessOutcome(names fail) = %+v, want a failure with lines %q", got, want)
	}
}

func TestAPIExcessTableGapNamesColumnEntries(t *testing.T) {
	gaps := []postgresstore.PostureGap{{TableName: "users", Excess: []string{"INSERT"}}}
	entries := []aclEntry{{Table: "users", Column: "is_superuser", Grantee: "cell_group", Grantor: "devhealth", Privilege: "INSERT"}}
	got := apiExcessLines(gaps, entries, nil)
	want := []string{"users: excess privileges [INSERT] beyond the declared posture (INSERT on column is_superuser via cell_group granted by devhealth)"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lines = %q, want %q", got, want)
	}
}
