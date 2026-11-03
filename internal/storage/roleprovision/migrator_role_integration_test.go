//go:build integration

package roleprovision

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/roleacl"
)

// CHAOS-12614: Apply creates the ops migration login LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
// NOBYPASSRLS with CONNECT on the database and nothing else; once it owns the database (after the owner move) a rerun
// changes nothing and Verify still passes it.
func TestApplyCreatesTheMigratorLoginWithConnectOnly(t *testing.T) {
	t.Parallel()
	s := startSide(t)
	ctx := context.Background()
	options := testOptions(false)
	options.Migrator = Role{"parity_migrator", "parity-migrator-pass"}
	s.runGo(t, options)

	var login, plain bool
	var database string
	if err := s.admin.QueryRow(ctx, `SELECT rolcanlogin, NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls), current_database()
		FROM pg_roles WHERE rolname = $1`, options.Migrator.Name).Scan(&login, &plain, &database); err != nil {
		t.Fatal(err)
	}
	if !login || !plain {
		t.Fatalf("the migrator login: login=%v plain=%v", login, plain)
	}
	grants, err := roleacl.Enumerate(ctx, s.admin, options.Migrator.Name)
	if err != nil {
		t.Fatal(err)
	}
	var own []string
	for _, grant := range grants {
		if !grant.ViaPublic {
			own = append(own, grant.String())
		}
	}
	if len(own) != 1 || own[0] != "CONNECT on database "+database {
		t.Fatalf("the migrator login holds %v in its own name, want only CONNECT on the database", own)
	}
	var ownsNothing, memberFree bool
	if err := s.admin.QueryRow(ctx, `SELECT `+roleacl.OwnsNothingSQL+`, `+roleacl.MembershipFreeSQL, options.Migrator.Name).Scan(&ownsNothing, &memberFree); err != nil {
		t.Fatal(err)
	}
	if !ownsNothing || !memberFree {
		t.Fatalf("ownsNothing=%v memberFree=%v", ownsNothing, memberFree)
	}
	if problems, warnings, err := Verify(ctx, s.admin, options); err != nil || len(problems) != 0 {
		t.Fatalf("Verify: %v %v %v", problems, warnings, err)
	}

	if _, err := s.admin.Exec(ctx, "ALTER DATABASE "+ident(database)+" OWNER TO "+ident(options.Migrator.Name)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = s.admin.Exec(context.Background(), "ALTER DATABASE "+ident(database)+" OWNER TO CURRENT_USER")
	})
	s.runGo(t, options)
	if problems, _, err := Verify(ctx, s.admin, options); err != nil || len(problems) != 0 {
		t.Fatalf("Verify with the migrator owning the database: %v %v", problems, err)
	}
}
