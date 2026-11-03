package riverstore

import "testing"

// CHAOS-12614: a migration login that cannot grant in the auth schema (NOSUPERUSER, not its owner) leaves the query-api
// session read to auth-grants and revokes nothing there; a superuser or the auth owner keeps today's leg.
func TestResolveAuthLeg(t *testing.T) {
	t.Parallel()
	withRead := func() MigrationOptions {
		options := queryAPIOptions()
		options.QueryAPISessionRead = &SessionReadGrant{Schema: "auth2", Table: "sessions", Columns: []string{"id"}}
		return options
	}
	kept, skipped := resolveAuthLeg(withRead(), true)
	if skipped || kept.QueryAPISessionRead == nil || kept.revokeSkipSchema != "" {
		t.Fatalf("a login that can grant in auth: skipped=%v read=%v skip=%q", skipped, kept.QueryAPISessionRead, kept.revokeSkipSchema)
	}
	dropped, skipped := resolveAuthLeg(withRead(), false)
	if !skipped || dropped.QueryAPISessionRead != nil || dropped.revokeSkipSchema != "auth2" {
		t.Fatalf("a login that cannot grant in auth: skipped=%v read=%v skip=%q", skipped, dropped.QueryAPISessionRead, dropped.revokeSkipSchema)
	}
	noRead := queryAPIOptions()
	noRead.AuthSchema = "auth3"
	got, skipped := resolveAuthLeg(noRead, false)
	if skipped || got.revokeSkipSchema != "auth3" {
		t.Fatalf("no session read: skipped=%v skip=%q", skipped, got.revokeSkipSchema)
	}
	none := apiOptions()
	if got, skipped := resolveAuthLeg(none, false); skipped || got.revokeSkipSchema != "" {
		t.Fatalf("no query-api role: skipped=%v skip=%q", skipped, got.revokeSkipSchema)
	}
	if (MigrationOptions{}).authSchema() != "auth" {
		t.Fatal("the default auth schema is auth")
	}
	bad := queryAPIOptions()
	bad.AuthSchema = `au"th`
	if ValidateMigrationOptions(bad) == nil {
		t.Fatal("an unusable auth schema name was accepted")
	}
}
