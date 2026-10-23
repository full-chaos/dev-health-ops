//go:build integration

package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The three runtime logins (domain, queue-control, coordinator) are the DB
// side of worker-class isolation. Each one is read by its own check; this
// matrix plants one defect at a time in the real grant harness and asserts
// three things per cell: the affected login's real check refuses it
// (ErrPostureRefused, not merely "unavailable"), the other two logins'
// checks still pass, and undoing the defect makes the affected login pass
// again. Every cell runs as the login itself over its own pool.

type isolationLogin struct {
	class string
	name  string
	pool  *pgxpool.Pool
	check func(ctx context.Context, pool *pgxpool.Pool, role string) error
}

// isolationEnv holds what a defect row needs to render its statements.
type isolationEnv struct {
	logins      map[string]isolationLogin
	database    string
	adminRole   string
	riverSchema string
	riverTable  string
	// Per class: a table that belongs to another class's posture and to none
	// of this class's, and the first table this class's own posture requires.
	foreignTable map[string]string
	ownTable     map[string]string
}

type isolationDefect struct {
	name string
	// classes lists the logins the row is planted on; empty means all three.
	classes []string
	// shared marks a defect that cannot be confined to one login (a PUBLIC
	// grant): every login must refuse it and every login must pass after undo.
	shared bool
	apply  func(env isolationEnv, login isolationLogin) []string
	undo   func(env isolationEnv, login isolationLogin) []string
}

func isolationIdent(parts ...string) string { return pgx.Identifier(parts).Sanitize() }

const (
	isolationProbeTable   = "isolation_probe_table"
	isolationProbeColumn  = "note"
	isolationProbeOwned   = "isolation_probe_owned"
	isolationProbeSeq     = "isolation_probe_seq"
	isolationProbeFn      = "isolation_probe_fn"
	isolationOwnedSchema  = "isolation_owned_schema"
	isolationOwnedRevoked = "isolation_owned_revoked"
)

func isolationDefects() []isolationDefect {
	statements := func(list ...string) []string { return list }
	var defects []isolationDefect
	for _, attribute := range []string{"SUPERUSER", "CREATEROLE", "CREATEDB", "BYPASSRLS", "REPLICATION"} {
		defects = append(defects, isolationDefect{
			name: "role attribute " + attribute,
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("ALTER ROLE " + isolationIdent(login.name) + " " + attribute)
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("ALTER ROLE " + isolationIdent(login.name) + " NO" + attribute)
			},
		})
	}
	for _, target := range []string{"domain", "queue", "coordinator"} {
		defects = append(defects, isolationDefect{
			name:    "member of the " + target + " login",
			classes: without([]string{"domain", "queue", "coordinator"}, target),
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT " + isolationIdent(env.logins[target].name) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE " + isolationIdent(env.logins[target].name) + " FROM " + isolationIdent(login.name))
			},
		})
	}
	for _, builtin := range []string{"pg_read_all_data", "pg_write_all_data", "pg_monitor"} {
		defects = append(defects, isolationDefect{
			name: "member of " + builtin,
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT " + builtin + " TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE " + builtin + " FROM " + isolationIdent(login.name))
			},
		})
	}
	for _, privilege := range []string{"CREATE", "TEMPORARY"} {
		defects = append(defects, isolationDefect{
			name: privilege + " on the database",
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT " + privilege + " ON DATABASE " + isolationIdent(env.database) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE " + privilege + " ON DATABASE " + isolationIdent(env.database) + " FROM " + isolationIdent(login.name))
			},
		})
	}
	defects = append(defects,
		isolationDefect{
			name: "CREATE on the public schema",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT CREATE ON SCHEMA public TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE CREATE ON SCHEMA public FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "CREATE on the river schema",
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT CREATE ON SCHEMA " + isolationIdent(env.riverSchema) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE CREATE ON SCHEMA " + isolationIdent(env.riverSchema) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name:    "SELECT on a river table",
			classes: []string{"domain", "coordinator"},
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT SELECT ON TABLE " + isolationIdent(env.riverSchema, env.riverTable) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE SELECT ON TABLE " + isolationIdent(env.riverSchema, env.riverTable) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "SELECT on a table of another class",
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT SELECT ON TABLE " + isolationIdent("public", env.foreignTable[login.class]) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE SELECT ON TABLE " + isolationIdent("public", env.foreignTable[login.class]) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "TRUNCATE on a table of its own posture",
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT TRUNCATE ON TABLE " + isolationIdent("public", env.ownTable[login.class]) + " TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE TRUNCATE ON TABLE " + isolationIdent("public", env.ownTable[login.class]) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "SELECT on an undeclared table",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT SELECT ON TABLE " + isolationIdent("public", isolationProbeTable) + " TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE SELECT ON TABLE " + isolationIdent("public", isolationProbeTable) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name:   "SELECT on an undeclared table granted to PUBLIC",
			shared: true,
			apply: func(_ isolationEnv, _ isolationLogin) []string {
				return statements("GRANT SELECT ON TABLE " + isolationIdent("public", isolationProbeTable) + " TO PUBLIC")
			},
			undo: func(_ isolationEnv, _ isolationLogin) []string {
				return statements("REVOKE SELECT ON TABLE " + isolationIdent("public", isolationProbeTable) + " FROM PUBLIC")
			},
		},
		isolationDefect{
			name: "column SELECT on an undeclared table",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT SELECT (" + isolationIdent(isolationProbeColumn) + ") ON TABLE " + isolationIdent("public", isolationProbeTable) + " TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE SELECT (" + isolationIdent(isolationProbeColumn) + ") ON TABLE " + isolationIdent("public", isolationProbeTable) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "USAGE on an undeclared sequence",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT USAGE ON SEQUENCE " + isolationIdent("public", isolationProbeSeq) + " TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE USAGE ON SEQUENCE " + isolationIdent("public", isolationProbeSeq) + " FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "EXECUTE on a public function",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("GRANT EXECUTE ON FUNCTION " + isolationIdent("public", isolationProbeFn) + "() TO " + isolationIdent(login.name))
			},
			undo: func(_ isolationEnv, login isolationLogin) []string {
				return statements("REVOKE EXECUTE ON FUNCTION " + isolationIdent("public", isolationProbeFn) + "() FROM " + isolationIdent(login.name))
			},
		},
		isolationDefect{
			name: "owns a table",
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements("ALTER TABLE " + isolationIdent("public", isolationProbeOwned) + " OWNER TO " + isolationIdent(login.name))
			},
			undo: func(env isolationEnv, _ isolationLogin) []string {
				return statements("ALTER TABLE " + isolationIdent("public", isolationProbeOwned) + " OWNER TO " + isolationIdent(env.adminRole))
			},
		},
		isolationDefect{
			name:    "owns a schema with its own privileges revoked",
			classes: []string{"domain", "coordinator"},
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements(
					"CREATE SCHEMA "+isolationIdent(isolationOwnedSchema)+" AUTHORIZATION "+isolationIdent(login.name),
					"REVOKE ALL ON SCHEMA "+isolationIdent(isolationOwnedSchema)+" FROM "+isolationIdent(login.name),
				)
			},
			undo: func(_ isolationEnv, _ isolationLogin) []string {
				return statements("DROP SCHEMA " + isolationIdent(isolationOwnedSchema))
			},
		},
		isolationDefect{
			name:    "owns a table with its own privileges revoked",
			classes: []string{"domain", "coordinator"},
			apply: func(_ isolationEnv, login isolationLogin) []string {
				return statements(
					"CREATE TABLE "+isolationIdent("public", isolationOwnedRevoked)+" (id integer)",
					"ALTER TABLE "+isolationIdent("public", isolationOwnedRevoked)+" OWNER TO "+isolationIdent(login.name),
					"REVOKE ALL ON TABLE "+isolationIdent("public", isolationOwnedRevoked)+" FROM "+isolationIdent(login.name),
				)
			},
			undo: func(_ isolationEnv, _ isolationLogin) []string {
				return statements("DROP TABLE " + isolationIdent("public", isolationOwnedRevoked))
			},
		},
		isolationDefect{
			name:    "SELECT held with grant option on a table of its own posture",
			classes: []string{"domain", "coordinator"},
			apply: func(env isolationEnv, login isolationLogin) []string {
				return statements("GRANT SELECT ON TABLE " + isolationIdent("public", env.ownTable[login.class]) + " TO " + isolationIdent(login.name) + " WITH GRANT OPTION")
			},
			undo: func(env isolationEnv, login isolationLogin) []string {
				return statements("REVOKE GRANT OPTION FOR SELECT ON TABLE " + isolationIdent("public", env.ownTable[login.class]) + " FROM " + isolationIdent(login.name))
			},
		},
	)
	return defects
}

func without(list []string, drop string) []string {
	var kept []string
	for _, item := range list {
		if item != drop {
			kept = append(kept, item)
		}
	}
	return kept
}

// runIsolationCheck answers a login's real check, trying again only when the database did not answer
// (a timeout under load), never when it answered 'no'.
func runIsolationCheck(ctx context.Context, login isolationLogin) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		err = login.check(ctx, login.pool, login.name)
		if err == nil || errors.Is(err, ErrPostureRefused) {
			return err
		}
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

func isolationQueryString(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) string {
	t.Helper()
	var value string
	if err := pool.QueryRow(ctx, query).Scan(&value); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return value
}

func isolationExec(t *testing.T, ctx context.Context, admin *pgxpool.Pool, list []string) {
	t.Helper()
	for _, statement := range list {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
}

func TestRuntimeLoginIsolationMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)

	logins := map[string]isolationLogin{
		"domain": {class: "domain", name: roles.domain, pool: connectAs(t, ctx, uri, roles.domain, grantDomainPass),
			check: func(ctx context.Context, pool *pgxpool.Pool, role string) error {
				return CheckDomainAuthorization(ctx, pool, role, grantSchema)
			}},
		"queue": {class: "queue", name: roles.queue, pool: connectAs(t, ctx, uri, roles.queue, grantQueuePass),
			check: func(ctx context.Context, pool *pgxpool.Pool, role string) error {
				return CheckQueueAuthorization(ctx, pool, role, grantSchema)
			}},
		"coordinator": {class: "coordinator", name: roles.coordinator, pool: connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass),
			check: func(ctx context.Context, pool *pgxpool.Pool, role string) error {
				return CheckCoordinatorAuthorization(ctx, pool, role, grantSchema)
			}},
	}
	classes := []string{"domain", "queue", "coordinator"}

	isolationExec(t, ctx, admin, []string{
		"CREATE TABLE public." + isolationProbeTable + " (id integer, " + isolationProbeColumn + " text)",
		"CREATE TABLE public." + isolationProbeOwned + " (id integer)",
		"CREATE SEQUENCE public." + isolationProbeSeq,
		"CREATE FUNCTION public." + isolationProbeFn + "() RETURNS integer LANGUAGE sql AS 'SELECT 1'",
		"REVOKE ALL ON FUNCTION public." + isolationProbeFn + "() FROM PUBLIC",
	})

	env := isolationEnv{
		logins:      logins,
		database:    isolationQueryString(t, ctx, admin, "SELECT current_database()"),
		adminRole:   isolationQueryString(t, ctx, admin, "SELECT current_user"),
		riverSchema: grantSchema,
		riverTable: isolationQueryString(t, ctx, admin,
			"SELECT table_name FROM information_schema.tables WHERE table_schema = '"+grantSchema+"' AND table_type = 'BASE TABLE' ORDER BY table_name LIMIT 1"),
		foreignTable: map[string]string{
			"domain":      firstExclusivePostureTable(t, coordinatorPosture(), domainPosture()),
			"coordinator": firstExclusivePostureTable(t, domainPosture(), coordinatorPosture()),
			"queue":       firstExclusivePostureTable(t, domainPosture(), queuePosture()),
		},
		ownTable: map[string]string{
			"domain":      domainPosture().RequiredTables[0].TableName,
			"coordinator": coordinatorPosture().RequiredTables[0].TableName,
			"queue":       queuePosture().RequiredTables[0].TableName,
		},
	}

	checkAll := func(t *testing.T, label string) {
		t.Helper()
		for _, class := range classes {
			login := logins[class]
			if err := runIsolationCheck(ctx, login); err != nil {
				t.Fatalf("%s: %s login's check refused: %v", label, class, err)
			}
		}
	}
	checkAll(t, "baseline before any defect")

	planted := map[string]int{}
	detected := map[string]int{}
	for _, defect := range isolationDefects() {
		targets := defect.classes
		if len(targets) == 0 {
			targets = classes
		}
		if defect.shared {
			targets = targets[:1]
		}
		for _, class := range targets {
			login := logins[class]
			t.Run(defect.name+"/"+class, func(t *testing.T) {
				isolationExec(t, ctx, admin, defect.apply(env, login))
				restored := false
				restore := func() {
					if !restored {
						restored = true
						isolationExec(t, ctx, admin, defect.undo(env, login))
					}
				}
				defer restore()

				for _, other := range classes {
					affected := other == class || defect.shared
					peer := logins[other]
					err := runIsolationCheck(ctx, peer)
					if affected {
						planted[other]++
					}
					switch {
					case affected && !errors.Is(err, ErrPostureRefused):
						t.Errorf("%s login's check = %v, want ErrPostureRefused", other, err)
					case affected:
						detected[other]++
					case err != nil:
						t.Errorf("%s login's check = %v while the defect sits on the %s login, want it to pass", other, err, class)
					}
				}

				restore()
				for _, other := range classes {
					peer := logins[other]
					if err := runIsolationCheck(ctx, peer); err != nil {
						t.Errorf("%s login's check = %v after the defect was undone, want it to pass", other, err)
					}
				}
			})
		}
	}

	// The planted rows are the detector's own proof of life: every class must
	// have been planted several times, and every planted row must have been
	// reported by the check of the login that carries it.
	for _, class := range classes {
		if planted[class] < 12 {
			t.Errorf("only %d defects were planted on the %s login; the matrix shrank", planted[class], class)
		}
		if detected[class] < planted[class] {
			t.Errorf("the %s login's check reported %d of %d planted defects", class, detected[class], planted[class])
		}
		t.Logf("matrix %s: planted=%d reported=%d", class, planted[class], detected[class])
	}
}
