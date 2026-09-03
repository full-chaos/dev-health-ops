//go:build integration

package authschema

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// insufficientPrivilege is PostgreSQL's SQLSTATE for a refused permission.
// Asserting on the CODE rather than the message is what keeps this suite from
// passing on an unrelated failure -- a syntax error, a missing table, a dead
// connection -- which would otherwise look identical to "permission denied"
// through a plain `err != nil` check. That distinction is the whole difference
// between proving the posture and proving that something went wrong.
const insufficientPrivilege = "42501"
const undefinedTable = "42P01"

// asPGCode returns the SQLSTATE of err, or "" when it is not a PgError.
func asPGCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// dsnAs rewrites an admin DSN to authenticate as another role.
func dsnAs(t *testing.T, adminURI, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(adminURI)
	if err != nil {
		t.Fatalf("parse admin URI: %v", err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

// TestRuntimeRolePostureAgainstLivePostgres is CHAOS-4882's executed proof.
//
// The ticket asks for a live-PostgreSQL posture test that connects AS the
// runtime role and shows DDL rejected and access outside the auth schema
// rejected. This does that, and then does the thing that makes those
// rejections mean something: it proves the runtime role CAN still perform the
// operations it is supposed to. A role granted nothing at all passes every
// negative assertion here, so without the positive half this suite would be
// strongest exactly when the schema was most broken.
func TestRuntimeRolePostureAgainstLivePostgres(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	// Role names are derived per call: CREATE ROLE is cluster-scoped, not
	// database-scoped, so a hard-coded name collides with a second run of this
	// suite and with any concurrent lane pointed at the same server
	// (CHAOS-4661).
	migrationRole, err := containers.RoleName("auth_migrator", instance)
	if err != nil {
		t.Fatalf("derive migration role name: %v", err)
	}
	runtimeRole, err := containers.RoleName("auth_runtime", instance)
	if err != nil {
		t.Fatalf("derive runtime role name: %v", err)
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatalf("read database name: %v", err)
	}

	const password = "posture-test-not-a-real-credential"
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", migrationRole, password),
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", runtimeRole, password),
		// The migration role needs CREATE on the database to make the schema.
		// The runtime role is deliberately given NOTHING here: every privilege
		// it ends up with must come from ApplyRuntimeGrants, or this test
		// would be proving the setup rather than the code.
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %q TO %q", database, migrationRole),
		fmt.Sprintf("GRANT CONNECT ON DATABASE %q TO %q", database, runtimeRole),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() {
		containers.DropRole(admin, runtimeRole, t.Logf)
		containers.DropRole(admin, migrationRole, t.Logf)
	})

	// A table OUTSIDE the auth schema, created by the admin. The runtime role
	// must not be able to reach it: ACP-ADR-04's "no cross-schema access".
	if _, err := admin.Exec(ctx, `CREATE TABLE public.ops_owned_table (id int PRIMARY KEY)`); err != nil {
		t.Fatalf("create the out-of-schema table: %v", err)
	}

	// --- Apply the lineage AS THE MIGRATION ROLE -------------------------
	migrationPool, err := pgxpool.New(ctx, dsnAs(t, instance.URI, migrationRole, password))
	if err != nil {
		t.Fatalf("migration pool: %v", err)
	}
	t.Cleanup(migrationPool.Close)

	const schema = "auth"
	result, err := Apply(ctx, migrationPool, Options{Schema: schema, RuntimeRole: runtimeRole})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	head, err := HeadVersion()
	if err != nil {
		t.Fatalf("HeadVersion: %v", err)
	}
	if result.CurrentVersion != head || len(result.AppliedVersions) != head {
		t.Fatalf(
			"Apply from empty = version %d with %d applied, want %d and %d",
			result.CurrentVersion, len(result.AppliedVersions), head, head,
		)
	}
	t.Logf("$ auth-migrate  (from empty) -> applied %d migrations, current version %d",
		len(result.AppliedVersions), result.CurrentVersion)

	// Idempotence: a second run applies nothing and still reports current.
	second, err := Apply(ctx, migrationPool, Options{Schema: schema, RuntimeRole: runtimeRole})
	if err != nil {
		t.Fatalf("second Apply: %v", err)
	}
	if len(second.AppliedVersions) != 0 || second.CurrentVersion != head {
		t.Fatalf("second Apply = %+v, want nothing applied and version %d", second, head)
	}
	if current, checkHead, err := Check(ctx, migrationPool, schema); err != nil {
		t.Fatalf("Check after Apply = %v (current %d, head %d)", err, current, checkHead)
	}
	t.Logf("$ auth-migrate --check -> auth schema current at version %d", head)

	// --- Connect AS THE RUNTIME ROLE -------------------------------------
	runtimePool, err := pgxpool.New(ctx, dsnAs(t, instance.URI, runtimeRole, password))
	if err != nil {
		t.Fatalf("runtime pool: %v", err)
	}
	t.Cleanup(runtimePool.Close)

	var whoami string
	if err := runtimePool.QueryRow(ctx, `SELECT current_user`).Scan(&whoami); err != nil {
		t.Fatalf("read current_user as the runtime role: %v", err)
	}
	if whoami != runtimeRole {
		t.Fatalf("connected as %q, want %q; the negative assertions below would prove nothing", whoami, runtimeRole)
	}
	t.Logf("connected as current_user = %s", whoami)

	// --- POSITIVE CONTROL, FIRST -----------------------------------------
	// Everything below this point asserts a refusal, and a role with no
	// privileges satisfies all of them. Proving the granted capabilities
	// first is what makes the refusals evidence rather than a tautology.
	t.Run("positive control: the runtime role can do its job", func(t *testing.T) {
		var principalID string
		if err := runtimePool.QueryRow(ctx, `
			INSERT INTO auth.principals (kind, display_name)
			VALUES ('user', 'posture-control') RETURNING id`,
		).Scan(&principalID); err != nil {
			t.Fatalf("INSERT into auth.principals was refused: %v", err)
		}
		var count int
		if err := runtimePool.QueryRow(ctx, `SELECT count(*) FROM auth.principals`).Scan(&count); err != nil {
			t.Fatalf("SELECT from auth.principals was refused: %v", err)
		}
		if count != 1 {
			t.Fatalf("principals count = %d, want 1", count)
		}
		// The audit insert exercises the SEQUENCE grant as well as the table
		// grant: without USAGE on security_audit_events_id_seq this fails on
		// nextval even though the INSERT privilege is present.
		if _, err := runtimePool.Exec(ctx, `
			INSERT INTO auth.security_audit_events (event_type, outcome)
			VALUES ('posture.control', 'allowed')`,
		); err != nil {
			t.Fatalf("INSERT into auth.security_audit_events was refused: %v", err)
		}
		if _, err := runtimePool.Exec(ctx, `SELECT * FROM auth.signing_keys`); err != nil {
			t.Fatalf("SELECT from auth.signing_keys was refused: %v", err)
		}
		t.Log("runtime role can INSERT/SELECT principals, append an audit event, and read signing keys")
	})

	// --- NEGATIVE: no DDL ------------------------------------------------
	t.Run("DDL is refused", func(t *testing.T) {
		for _, statement := range []string{
			`CREATE TABLE auth.attacker_table (id int)`,
			`ALTER TABLE auth.principals ADD COLUMN attacker_column text`,
			`DROP TABLE auth.sessions`,
			`CREATE INDEX attacker_idx ON auth.principals (kind)`,
			`ALTER TABLE auth.security_audit_events DISABLE TRIGGER ALL`,
			`CREATE SCHEMA attacker_schema`,
			`TRUNCATE auth.security_audit_events`,
		} {
			_, err := runtimePool.Exec(ctx, statement)
			if err == nil {
				t.Errorf("DDL was ALLOWED: %s", statement)
				continue
			}
			code := asPGCode(err)
			if code != insufficientPrivilege {
				t.Errorf("%s -> SQLSTATE %s (%v), want %s", statement, code, err, insufficientPrivilege)
				continue
			}
			t.Logf("$ %s\n    ERROR: %s (SQLSTATE %s)", statement, pgMessage(err), code)
		}
	})

	// --- NEGATIVE: nothing outside the auth schema -----------------------
	t.Run("access outside the auth schema is refused", func(t *testing.T) {
		for _, statement := range []string{
			`SELECT * FROM public.ops_owned_table`,
			`INSERT INTO public.ops_owned_table (id) VALUES (1)`,
			`CREATE TABLE public.attacker_table (id int)`,
		} {
			_, err := runtimePool.Exec(ctx, statement)
			if err == nil {
				t.Errorf("out-of-schema access was ALLOWED: %s", statement)
				continue
			}
			code := asPGCode(err)
			if code != insufficientPrivilege {
				t.Errorf("%s -> SQLSTATE %s (%v), want %s", statement, code, err, insufficientPrivilege)
				continue
			}
			t.Logf("$ %s\n    ERROR: %s (SQLSTATE %s)", statement, pgMessage(err), code)
		}
	})

	// --- NEGATIVE: the append-only and read-only postures hold live -------
	t.Run("append-only and read-only tables refuse mutation", func(t *testing.T) {
		for _, statement := range []string{
			`UPDATE auth.security_audit_events SET outcome = 'allowed'`,
			`DELETE FROM auth.security_audit_events`,
			`UPDATE auth.policy_revisions SET revision = 2`,
			`DELETE FROM auth.entitlement_snapshots`,
			`INSERT INTO auth.signing_keys (kid, algorithm, public_key_jwk, custody_ref, custody_kind)
			   VALUES ('attacker', 'EdDSA', '{}'::jsonb, 'ref', 'file')`,
			`UPDATE auth.roles SET name = 'attacker'`,
			`INSERT INTO auth.actions (action_key, resource_kind) VALUES ('attacker.act', 'thing')`,
		} {
			_, err := runtimePool.Exec(ctx, statement)
			if err == nil {
				t.Errorf("mutation was ALLOWED: %s", strings.TrimSpace(statement))
				continue
			}
			code := asPGCode(err)
			if code != insufficientPrivilege {
				t.Errorf("%s -> SQLSTATE %s (%v), want %s", strings.TrimSpace(statement), code, err, insufficientPrivilege)
				continue
			}
			t.Logf("$ %s\n    ERROR: %s (SQLSTATE %s)", strings.TrimSpace(statement), pgMessage(err), code)
		}
	})

	// --- NEGATIVE: the lineage table is not the runtime's ------------------
	t.Run("the migration bookkeeping table is unreachable", func(t *testing.T) {
		_, err := runtimePool.Exec(ctx, `SELECT * FROM auth.schema_migrations`)
		if err == nil {
			t.Fatal("the runtime role can read the migration lineage table; it is declared unreachable")
		}
		if code := asPGCode(err); code != insufficientPrivilege && code != undefinedTable {
			t.Fatalf("reading schema_migrations -> SQLSTATE %s (%v)", code, err)
		}
		t.Logf("$ SELECT * FROM auth.schema_migrations\n    ERROR: %s", pgMessage(err))
	})
}

// TestApplyRefusesWhenTheMigrationRoleIsTheRuntimeRole proves ACP-ADR-04's
// central rule holds against a real server, not only in Options.Validate:
// running the migration as the runtime role would make that role the OWNER of
// every object and therefore a permanent holder of DDL.
func TestApplyRefusesWhenTheMigrationRoleIsTheRuntimeRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})

	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	role, err := containers.RoleName("auth_selfmigrate", instance)
	if err != nil {
		t.Fatalf("derive role name: %v", err)
	}
	database, err := containers.DatabaseName(instance.URI)
	if err != nil {
		t.Fatalf("read database name: %v", err)
	}
	const password = "posture-test-not-a-real-credential"
	for _, statement := range []string{
		fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", role, password),
		fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %q TO %q", database, role),
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup %q: %v", statement, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	pool, err := pgxpool.New(ctx, dsnAs(t, instance.URI, role, password))
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	_, err = Apply(ctx, pool, Options{Schema: "auth", RuntimeRole: role})
	if !errors.Is(err, ErrInvalidOptions) {
		t.Fatalf("Apply = %v, want a refusal because the migration role IS the runtime role", err)
	}
	t.Logf("$ auth-migrate  (migration role == runtime role)\n    ERROR: %v", err)

	// And nothing was created: a refusal that had already made the schema
	// would leave the forbidden ownership behind.
	var present bool
	if err := admin.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'auth')`,
	).Scan(&present); err != nil {
		t.Fatalf("look for the auth schema: %v", err)
	}
	if present {
		t.Fatal("the refused run still created the auth schema")
	}
	t.Log("no schema was created by the refused run")
}

// TestApplyFailsWhenTheRuntimeRoleCanEscalate reproduces codex round 1's P1
// and proves the repair.
//
// Both escalations below survived a SUCCESSFUL reapply before the fix: the
// grant manifest revokes object-level privileges inside the schema, and
// neither a role membership nor a database-level grant is one of those. The
// reviewer executed both against a live server — after `GRANT <migration> TO
// <runtime>` the runtime could `SET ROLE` to the schema's owner and CREATE a
// table, and with `GRANT CREATE ON DATABASE` it could create a schema outside
// auth. Apply now re-derives the posture from the database and fails.
//
// Each case also asserts the ESCALATION ITSELF worked before the fix would
// have caught it, so this test cannot pass merely because the setup was
// ineffective — a refusal for the wrong reason would otherwise look identical.
func TestApplyFailsWhenTheRuntimeRoleCanEscalate(t *testing.T) {
	cases := []struct {
		name string
		// escalate runs as the admin, granting the runtime role something the
		// object-level manifest cannot revoke.
		escalate func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, database, migrationRole, runtimeRole string)
		wantKind string
	}{
		{
			name: "membership in the object-owning migration role",
			escalate: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, database, migrationRole, runtimeRole string) {
				if _, err := admin.Exec(ctx, fmt.Sprintf("GRANT %q TO %q", migrationRole, runtimeRole)); err != nil {
					t.Fatalf("grant membership: %v", err)
				}
			},
			wantKind: "role_membership",
		},
		{
			name: "CREATE on the database",
			escalate: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, database, migrationRole, runtimeRole string) {
				if _, err := admin.Exec(ctx, fmt.Sprintf("GRANT CREATE ON DATABASE %q TO %q", database, runtimeRole)); err != nil {
					t.Fatalf("grant database CREATE: %v", err)
				}
			},
			wantKind: "database_privilege",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			instance, err := containers.StartPostgres(ctx)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer closeCancel()
				if err := instance.Close(closeCtx); err != nil {
					t.Errorf("terminate PostgreSQL: %v", err)
				}
			})

			admin, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatalf("admin pool: %v", err)
			}
			t.Cleanup(admin.Close)

			migrationRole, err := containers.RoleName("auth_esc_migrator", instance)
			if err != nil {
				t.Fatalf("derive migration role: %v", err)
			}
			runtimeRole, err := containers.RoleName("auth_esc_runtime", instance)
			if err != nil {
				t.Fatalf("derive runtime role: %v", err)
			}
			database, err := containers.DatabaseName(instance.URI)
			if err != nil {
				t.Fatalf("read database name: %v", err)
			}

			const password = "posture-test-not-a-real-credential"
			for _, statement := range []string{
				fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", migrationRole, password),
				fmt.Sprintf("CREATE ROLE %q LOGIN PASSWORD '%s'", runtimeRole, password),
				fmt.Sprintf("GRANT CREATE, CONNECT ON DATABASE %q TO %q", database, migrationRole),
				fmt.Sprintf("GRANT CONNECT ON DATABASE %q TO %q", database, runtimeRole),
			} {
				if _, err := admin.Exec(ctx, statement); err != nil {
					t.Fatalf("setup %q: %v", statement, err)
				}
			}
			t.Cleanup(func() {
				containers.DropRole(admin, runtimeRole, t.Logf)
				containers.DropRole(admin, migrationRole, t.Logf)
			})

			testCase.escalate(t, ctx, admin, database, migrationRole, runtimeRole)

			migrationPool, err := pgxpool.New(ctx, dsnAs(t, instance.URI, migrationRole, password))
			if err != nil {
				t.Fatalf("migration pool: %v", err)
			}
			t.Cleanup(migrationPool.Close)

			options := Options{Schema: "auth", RuntimeRole: runtimeRole}
			_, applyErr := Apply(ctx, migrationPool, options)
			if !errors.Is(applyErr, ErrRuntimeRoleCanEscalate) {
				t.Fatalf("Apply = %v, want ErrRuntimeRoleCanEscalate", applyErr)
			}
			if !strings.Contains(applyErr.Error(), testCase.wantKind) {
				t.Fatalf("error %q does not name the %s path", applyErr, testCase.wantKind)
			}
			t.Logf("$ auth-migrate  (%s)\n    ERROR: %v", testCase.name, applyErr)

			// The escalation is REAL, not a mislabelled refusal: prove the
			// runtime role can actually exercise it. Without this the test
			// would pass just as well against a setup that granted nothing and
			// a verifier that reported a phantom path.
			runtimePool, err := pgxpool.New(ctx, dsnAs(t, instance.URI, runtimeRole, password))
			if err != nil {
				t.Fatalf("runtime pool: %v", err)
			}
			t.Cleanup(runtimePool.Close)

			switch testCase.wantKind {
			case "role_membership":
				if _, err := runtimePool.Exec(ctx, fmt.Sprintf("SET ROLE %q", migrationRole)); err != nil {
					t.Fatalf("the granted membership is not usable, so the finding is mislabelled: %v", err)
				}
				t.Log("confirmed: the runtime role really can SET ROLE to the schema owner")
			case "database_privilege":
				var held bool
				if err := runtimePool.QueryRow(ctx,
					`SELECT has_database_privilege(current_user, current_database(), 'CREATE')`,
				).Scan(&held); err != nil {
					t.Fatalf("read database privilege: %v", err)
				}
				if !held {
					t.Fatal("the granted database CREATE is not held, so the finding is mislabelled")
				}
				t.Log("confirmed: the runtime role really does hold CREATE on the database")
			}
		})
	}
}

func pgMessage(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Message
	}
	return err.Error()
}
