//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// legacyLinkGrantor is who granted the runtime role its old purge grant on the two link-token tables in prod
// (fcsup read, RECORD-13362-tokens-acl-1, live release 606): devhealth_auth_migrator, which holds DELETE WITH GRANT
// OPTION, REFERENCES and SELECT(id) WITH GRANT OPTION granted by the owner (devhealth); go-api's login
// (devhealth_api) holds SELECT, INSERT, DELETE granted by the owner and nothing else holds a privilege.
const legacyLinkGrantor = "migrator"

func runtimeHoldsAnythingOn(t *testing.T, ctx context.Context, env *ownerMoveEnv, table string) bool {
	t.Helper()
	var held bool
	if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, format('public.%I', $2::text), 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
		OR has_any_column_privilege($1, format('public.%I', $2::text), 'SELECT,INSERT,UPDATE,REFERENCES')`, env.runtimeRole, table).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

// Prod shape: real PostgreSQL, the two tables owned by the owner role, the migrator on the anchor set this PR leaves
// (the two tables absent from the grant set, so it holds at most the anchor on them), the grants pass run AS THE MIGRATOR. A database that
// never gave the runtime role the old purge grant passes with nothing issued; one that did (granted by the grantor
// above) is refused as a narrowing with the pinned digest, then passes with it and leaves the runtime role nothing
// on either table.
func TestAMigratorOnTheNewAnchorSetRevokesTheOldLinkTokenGrantOrIssuesNothing(t *testing.T) {
	for _, held := range []bool{false, true} {
		t.Run(fmt.Sprintf("runtime role holds the old grant %v", held), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			env := newOwnerMoveEnv(t, ctx, false)
			env.move(t, ctx, env.moveOptions(true))
			for _, table := range retiredLinkTokenTables {
				if env.migratorHoldsMoreThanTheAnchorOn(t, ctx, table) {
					t.Fatalf("the migrator holds a privilege beyond the anchor on public.%s after the move", table)
				}
			}
			options := env.options(true)
			if !held {
				if _, err := Apply(ctx, env.migrator, options); err != nil {
					t.Fatalf("Apply as the migrator, runtime role holding nothing on the link-token tables: %v", err)
				}
				return
			}
			if _, err := Apply(ctx, env.migrator, options); err != nil {
				t.Fatalf("first Apply as the migrator: %v", err)
			}
			api := env.addGoAPIRole(t, ctx)
			migrator := Quote(mustValidatedIdentifier(env.migratorRole))
			runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
			apiRole := Quote(mustValidatedIdentifier(api))
			for _, table := range retiredLinkTokenTables {
				name := Quote(mustValidatedIdentifier(table))
				// The owner's grants, exactly as the prod ACL shows them.
				for _, statement := range []string{
					"GRANT DELETE ON public." + name + " TO " + migrator + " WITH GRANT OPTION",
					"GRANT REFERENCES ON public." + name + " TO " + migrator,
					"GRANT SELECT (id) ON public." + name + " TO " + migrator + " WITH GRANT OPTION",
					"GRANT SELECT, INSERT, DELETE ON public." + name + " TO " + apiRole,
				} {
					if _, err := env.old.Exec(ctx, statement); err != nil {
						t.Fatalf("%s: %v", statement, err)
					}
				}
				// The runtime role's grants, made BY THE MIGRATOR (its grantor).
				for _, statement := range []string{
					"GRANT DELETE ON public." + name + " TO " + runtime,
					"GRANT SELECT (id) ON public." + name + " TO " + runtime,
				} {
					if _, err := env.migrator.Exec(ctx, statement); err != nil {
						t.Fatalf("as the grantor %s: %s: %v", legacyLinkGrantor, statement, err)
					}
				}
				if !runtimeHoldsAnythingOn(t, ctx, env, table) {
					t.Fatalf("setup: the runtime role holds nothing on public.%s", table)
				}
			}
			// Question 2: the owner-move of this tip does not take the migrator's own grants away (the tables are no
			// longer in the grant set it walks), so the later REVOKE still has its grant option.
			env.move(t, ctx, env.moveOptions(true))
			for _, table := range retiredLinkTokenTables {
				env.requireMigratorLinkGrants(t, ctx, table)
			}
			_, err := Apply(ctx, env.migrator, options)
			var narrowed *NarrowingError
			if !errors.As(err, &narrowed) {
				t.Fatalf("Apply over the old grant without the acknowledgement: err = %v, want the narrowing refusal", err)
			}
			if narrowed.Digest != retiredLinkTokensAckDigest || len(narrowed.Lost) != len(retiredLinkTokensLost()) {
				t.Fatalf("narrowing = %s, %d lost; want %s, %d lost:\n%v", narrowed.Digest, len(narrowed.Lost), retiredLinkTokensAckDigest, len(retiredLinkTokensLost()), narrowed)
			}
			acknowledged := options
			acknowledged.AcknowledgeNarrowing = retiredLinkTokensAckDigest
			if _, err := Apply(ctx, env.migrator, acknowledged); err != nil {
				t.Fatalf("Apply over the old grant with the acknowledgement: %v", err)
			}
			for _, table := range retiredLinkTokenTables {
				// Question 1: the migrator is the grantor, so its REVOKE ALL removes the runtime role's grants.
				if runtimeHoldsAnythingOn(t, ctx, env, table) {
					t.Errorf("the runtime role still holds a privilege on public.%s after the acknowledged Apply", table)
				}
				// Question 2: the grants pass does not revoke the migrator's own grants either.
				env.requireMigratorLinkGrants(t, ctx, table)
				// Question 3: go-api's owner-granted SELECT, INSERT, DELETE are not touched by the grants pass (its
				// posture keeps naming the tables until the drop).
				for _, privilege := range []string{"SELECT", "INSERT", "DELETE"} {
					var held bool
					if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, format('public.%I', $2::text), $3)`, api, table, privilege).Scan(&held); err != nil {
						t.Fatal(err)
					}
					if !held {
						t.Errorf("go-api's login lost %s on public.%s: the grants pass must not touch it", privilege, table)
					}
				}
			}
			if _, err := Apply(ctx, env.migrator, options); err != nil {
				t.Fatalf("Apply after the revoke, without the acknowledgement: %v", err)
			}
		})
	}
}

func (e *ownerMoveEnv) migratorHoldsMoreThanTheAnchorOn(t *testing.T, ctx context.Context, table string) bool {
	t.Helper()
	var held bool
	if err := e.admin.QueryRow(ctx, `SELECT has_table_privilege($1, format('public.%I', $2::text), 'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,TRIGGER')
		OR has_any_column_privilege($1, format('public.%I', $2::text), 'SELECT,INSERT,UPDATE')`, e.migratorRole, table).Scan(&held); err != nil {
		t.Fatal(err)
	}
	return held
}

// addGoAPIRole creates go-api's login stand-in (NOLOGIN: only its catalog privileges matter here).
func (e *ownerMoveEnv) addGoAPIRole(t *testing.T, ctx context.Context) string {
	t.Helper()
	role, err := containers.RoleName("om_goapi", e.instance)
	if err != nil {
		t.Fatalf("derive role om_goapi: %v", err)
	}
	if _, err := e.admin.Exec(ctx, "CREATE ROLE "+Quote(mustValidatedIdentifier(role))+" NOLOGIN"); err != nil {
		t.Fatalf("create go-api role: %v", err)
	}
	t.Cleanup(func() { containers.DropRole(e.admin, role, t.Logf) })
	return role
}

// requireMigratorLinkGrants checks the migrator still holds what prod shows: DELETE WITH GRANT OPTION, REFERENCES and
// SELECT(id) WITH GRANT OPTION on public.<table>.
func (e *ownerMoveEnv) requireMigratorLinkGrants(t *testing.T, ctx context.Context, table string) {
	t.Helper()
	var del, ref, sel bool
	if err := e.admin.QueryRow(ctx, `SELECT has_table_privilege($1, format('public.%I', $2::text), 'DELETE WITH GRANT OPTION'),
		has_table_privilege($1, format('public.%I', $2::text), 'REFERENCES'),
		has_column_privilege($1, format('public.%I', $2::text), 'id', 'SELECT WITH GRANT OPTION')`, e.migratorRole, table).Scan(&del, &ref, &sel); err != nil {
		t.Fatal(err)
	}
	if !del || !ref || !sel {
		t.Fatalf("the migrator's own grants on public.%s changed: DELETE* %v, REFERENCES %v, SELECT(id)* %v", table, del, ref, sel)
	}
}
