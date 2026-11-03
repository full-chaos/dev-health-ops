//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// newNologinRole creates a role that cannot log in and drops it at cleanup.
func newNologinRole(t *testing.T, ctx context.Context, env *authFixture, base string) string {
	t.Helper()
	role, err := containers.RoleName(base, env.instance)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.admin.Exec(ctx, fmt.Sprintf("CREATE ROLE %s NOLOGIN", Quote(mustValidatedIdentifier(role)))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(env.admin, role, t.Logf) })
	return role
}

func adminExec(t *testing.T, ctx context.Context, env *authFixture, statement string) {
	t.Helper()
	if _, err := env.admin.Exec(ctx, statement); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
}

// A reader that is not an owner but keeps an undeclared privilege through a
// membership is refused by the posture check, and the refusal rolls the
// reconcile back: the reader's direct grant is still there afterwards.
func TestSessionReaderRefusedByPostureKeepsEveryACL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := newAuthFixture(t, ctx)
	extra := newNologinRole(t, ctx, env, "auth_extra_reads")
	reader, _ := newSessionReader(t, ctx, env, "auth_posture_reader")
	extraID, readerID := Quote(mustValidatedIdentifier(extra)), Quote(mustValidatedIdentifier(reader))
	adminExec(t, ctx, env, fmt.Sprintf(`GRANT USAGE ON SCHEMA auth TO %s`, extraID))
	adminExec(t, ctx, env, fmt.Sprintf(`GRANT SELECT ON auth.principals TO %s`, extraID))
	adminExec(t, ctx, env, fmt.Sprintf(`GRANT %s TO %s`, extraID, readerID))
	adminExec(t, ctx, env, fmt.Sprintf(`GRANT SELECT ON auth.sessions TO %s`, readerID))
	before := authACLs(t, ctx, env)
	if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); !errors.Is(err, ErrRuntimeRoleCanEscalate) {
		t.Fatalf("Apply with a reader that keeps an undeclared privilege: err = %v, want ErrRuntimeRoleCanEscalate", err)
	}
	if after := authACLs(t, ctx, env); !reflect.DeepEqual(before, after) {
		t.Errorf("the auth ACLs changed under a refused non-owner reader:\nbefore %v\nafter  %v", before, after)
	}
}

// A reader that owns, or inherits the owner of, one auth table (or one auth
// sequence) alone, with the schema owned elsewhere, is refused before any ACL
// change.
func TestSessionReaderThatInheritsAnOwnerOfOneObjectKeepsEveryACL(t *testing.T) {
	for _, tc := range []struct {
		name, create, alter string
	}{
		{"table", `CREATE TABLE auth.owner_cell_table (id int)`, `ALTER TABLE auth.owner_cell_table OWNER TO %s`},
		{"sequence", `CREATE SEQUENCE auth.owner_cell_sequence`, `ALTER SEQUENCE auth.owner_cell_sequence OWNER TO %s`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
			defer cancel()
			env := newAuthFixture(t, ctx)
			owner := newNologinRole(t, ctx, env, "auth_object_owner")
			reader, _ := newSessionReader(t, ctx, env, "auth_object_reader")
			ownerID, readerID := Quote(mustValidatedIdentifier(owner)), Quote(mustValidatedIdentifier(reader))
			adminExec(t, ctx, env, tc.create)
			adminExec(t, ctx, env, fmt.Sprintf(tc.alter, ownerID))
			adminExec(t, ctx, env, fmt.Sprintf(`GRANT %s TO %s`, ownerID, readerID))
			before := authACLs(t, ctx, env)
			if _, err := Apply(ctx, env.migration, readerOptions(env, reader)); !errors.Is(err, ErrInvalidOptions) {
				t.Fatalf("%s owner reader: err = %v, want ErrInvalidOptions", tc.name, err)
			}
			if after := authACLs(t, ctx, env); !reflect.DeepEqual(before, after) {
				t.Errorf("%s owner reader: the auth ACLs changed:\nbefore %v\nafter  %v", tc.name, before, after)
			}
		})
	}
}
