//go:build integration

package config

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// liveIdentifierCells is the axis the character-only list missed: the
// same reserved character behaves differently depending on WHERE in the
// identifier it sits. A database named "/app" assembles into a URL path
// of "//app", which pgconn reads back as "app" -- a successful LIVE
// connection to a different existing database. Each cell below is
// executed against a REAL server, for database, user and password at
// once, on both drivers.
var liveIdentifierCells = func() []struct{ name, db, user, password string } {
	chars := []struct{ name, char string }{
		{"hash", "#"}, {"question", "?"}, {"slash", "/"}, {"space", " "}, {"percent", "%"},
	}
	positions := []struct{ name, format string }{
		{"leading", "%sbase"}, {"middle", "ba%sse"}, {"trailing", "base%s"}, {"only", "%s"},
	}
	var cells []struct{ name, db, user, password string }
	for _, c := range chars {
		for _, p := range positions {
			infix := fmt.Sprintf(p.format, c.char)
			cells = append(cells, struct{ name, db, user, password string }{
				name:     c.name + "-" + p.name,
				db:       "d" + infix + "b",
				user:     "u" + infix + "r",
				password: "p" + infix + "w",
			})
		}
	}
	return cells
}()

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// TestComponentFormReservedCharacterClassesConnectLivePostgreSQL is the
// live proof: pgx must CONNECT (not merely parse) to a database, as a
// user, with a password, each containing one of the five reserved-
// character classes, entirely assembled through the component form. The
// telemetry identifier (ComponentDatabaseName) must match the configured
// database name verbatim, never derived by parsing a DSN.
func TestComponentFormReservedCharacterClassesConnectLivePostgreSQL(t *testing.T) {
	ctx := context.Background()
	inst, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(ctx) })

	adminPool, err := pgxpool.New(ctx, inst.URI)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)

	adminURL, err := url.Parse(inst.URI)
	if err != nil {
		t.Fatalf("parse admin URI: %v", err)
	}
	host := adminURL.Hostname()
	port := adminURL.Port()

	for _, cell := range liveIdentifierCells {
		t.Run(cell.name, func(t *testing.T) {
			db, role, password := cell.db, cell.user, cell.password

			if _, err := adminPool.Exec(ctx, fmt.Sprintf(
				"CREATE ROLE %s LOGIN PASSWORD %s", quoteIdent(role), quoteLiteral(password),
			)); err != nil {
				t.Fatalf("create role %q: %v", role, err)
			}
			t.Cleanup(func() {
				_, _ = adminPool.Exec(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdent(db)))
				_, _ = adminPool.Exec(ctx, fmt.Sprintf("DROP ROLE IF EXISTS %s", quoteIdent(role)))
			})
			if _, err := adminPool.Exec(ctx, fmt.Sprintf(
				"CREATE DATABASE %s OWNER %s", quoteIdent(db), quoteIdent(role),
			)); err != nil {
				t.Fatalf("create database %q: %v", db, err)
			}

			env := map[string]string{
				"DEV_HEALTH_PG_DOMAIN_HOST":     host,
				"DEV_HEALTH_PG_DOMAIN_PORT":     port,
				"DEV_HEALTH_PG_DOMAIN_USER":     role,
				"DEV_HEALTH_PG_DOMAIN_PASSWORD": password,
				"DEV_HEALTH_PG_DB":              db,
			}
			value, used, resolveErr := ResolveDSNFromComponents(lookup(env), DomainDatabaseSpec)
			if !used {
				t.Fatal("expected used=true once HOST is set")
			}
			if resolveErr != nil {
				// A refused cell is not a skipped cell: prove LIVE that
				// the refusal prevents a real mis-connection. The DSN the
				// assembly would have produced is built here and used
				// against the same server; it must reach a DIFFERENT
				// database than the one configured, which is precisely
				// what the resolver now refuses to let happen.
				assertRefusalPreventsAWrongLiveDatabase(ctx, t, adminPool,
					assembleWithNetURL("postgresql", host, port, role, password, db), db, resolveErr)
				return
			}

			conn, connErr := pgx.Connect(ctx, value.Reveal())
			if connErr != nil {
				t.Fatalf("LIVE connect failed: %v", connErr)
			}
			t.Cleanup(func() { _ = conn.Close(ctx) })

			var gotDB, gotUser string
			if scanErr := conn.QueryRow(ctx, "SELECT current_database(), current_user").Scan(&gotDB, &gotUser); scanErr != nil {
				t.Fatalf("query failed: %v", scanErr)
			}
			if gotDB != db {
				t.Fatalf("connected to WRONG database: got %q, want %q", gotDB, db)
			}
			if gotUser != role {
				t.Fatalf("connected as WRONG role: got %q, want %q", gotUser, role)
			}

			if got := ComponentDatabaseIdentity(DomainDatabaseSpec.Scheme, value); got != gotDB {
				t.Fatalf("telemetry reports %q but the connection reached %q", got, gotDB)
			}
		})
	}
}

// TestComponentFormReservedCharacterClassesConnectLiveClickHouse mirrors
// the PostgreSQL proof above for the ClickHouse driver and its own
// database/user/password grammar.
func TestComponentFormReservedCharacterClassesConnectLiveClickHouse(t *testing.T) {
	ctx := context.Background()
	inst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = inst.Close(ctx) })

	adminOptions, err := clickhouse.ParseDSN(inst.URI)
	if err != nil {
		t.Fatalf("parse admin DSN: %v", err)
	}
	adminConn, err := clickhouse.Open(adminOptions)
	if err != nil {
		t.Fatalf("admin open: %v", err)
	}
	t.Cleanup(func() { _ = adminConn.Close() })

	host := adminOptions.Addr[0]

	for _, cell := range liveIdentifierCells {
		t.Run(cell.name, func(t *testing.T) {
			db, user, password := cell.db, cell.user, cell.password

			if err := adminConn.Exec(ctx, fmt.Sprintf(
				"CREATE USER %s IDENTIFIED WITH plaintext_password BY %s",
				quoteIdent(user), quoteLiteral(password),
			)); err != nil {
				t.Fatalf("create user %q: %v", user, err)
			}
			t.Cleanup(func() {
				_ = adminConn.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdent(db)))
				_ = adminConn.Exec(context.Background(), fmt.Sprintf("DROP USER IF EXISTS %s", quoteIdent(user)))
			})
			if err := adminConn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", quoteIdent(db))); err != nil {
				t.Fatalf("create database %q: %v", db, err)
			}
			if err := adminConn.Exec(ctx, fmt.Sprintf("GRANT ALL ON %s.* TO %s", quoteIdent(db), quoteIdent(user))); err != nil {
				t.Fatalf("grant to %q: %v", user, err)
			}

			// splitHostPort avoids net.SplitHostPort's colon-shaped-host
			// escaping concerns entirely -- ClickHouse's own admin Addr
			// entry is already host:port from a live container, never an
			// IPv6 literal in this harness.
			hostOnly, portOnly, splitErr := splitHostPort(host)
			if splitErr != nil {
				t.Fatalf("split admin addr %q: %v", host, splitErr)
			}

			value, used, resolveErr := ResolveDSNFromComponents(lookup(map[string]string{
				"DEV_HEALTH_CH_HOST":     hostOnly,
				"DEV_HEALTH_CH_PORT":     portOnly,
				"DEV_HEALTH_CH_USER":     user,
				"DEV_HEALTH_CH_PASSWORD": password,
				"DEV_HEALTH_CH_DB":       db,
			}), ClickHouseSpec)
			if !used {
				t.Fatal("expected used=true once HOST is set")
			}
			if resolveErr != nil {
				// Same as the PostgreSQL side: a refused cell proves its
				// refusal, it does not skip. The database the driver
				// would have reached must differ from the configured one.
				assembled := assembleWithNetURL("clickhouse", hostOnly, portOnly, user, password, db)
				wouldBe, parseWouldErr := clickhouse.ParseDSN(assembled)
				if parseWouldErr != nil {
					return // the driver refuses it outright; nothing silent to prevent
				}
				if wouldBe.Auth.Database == db {
					t.Fatalf("database %q was refused (%v) but the driver reads it back unchanged -- the refusal is wrong",
						db, resolveErr)
				}
				if !strings.Contains(resolveErr.Error(), "DEV_HEALTH_CH_DB") {
					t.Fatalf("the refusal does not name the setting responsible: %v", resolveErr)
				}
				return
			}

			options, parseErr := clickhouse.ParseDSN(value.Reveal())
			if parseErr != nil {
				t.Fatalf("parse assembled DSN: %v", parseErr)
			}
			conn, openErr := clickhouse.Open(options)
			if openErr != nil {
				t.Fatalf("LIVE open failed: %v", openErr)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if pingErr := conn.Ping(ctx); pingErr != nil {
				t.Fatalf("LIVE ping/connect failed: %v", pingErr)
			}

			var gotDB, gotUser string
			row := conn.QueryRow(ctx, "SELECT currentDatabase(), currentUser()")
			if scanErr := row.Scan(&gotDB, &gotUser); scanErr != nil {
				t.Fatalf("query failed: %v", scanErr)
			}
			if gotDB != db {
				t.Fatalf("connected to WRONG database: got %q, want %q", gotDB, db)
			}
			if gotUser != user {
				t.Fatalf("connected as WRONG user: got %q, want %q", gotUser, user)
			}

			if got := ComponentDatabaseIdentity(ClickHouseSpec.Scheme, value); got != gotDB {
				t.Fatalf("telemetry reports %q but the connection reached %q", got, gotDB)
			}
		})
	}
}

func splitHostPort(hostPort string) (host, port string, err error) {
	idx := strings.LastIndex(hostPort, ":")
	if idx < 0 {
		return "", "", fmt.Errorf("no port in address %q", hostPort)
	}
	return hostPort[:idx], hostPort[idx+1:], nil
}

// assertRefusalPreventsAWrongLiveDatabase is the live half of a refused
// cell. It creates BOTH the configured database and the one the driver
// would have rewritten it to, connects with the DSN the assembly would
// have produced, and requires that connection to land on the rewritten
// name -- the silent wrong-database connection the refusal exists to
// prevent, demonstrated against a real server rather than argued.
func assertRefusalPreventsAWrongLiveDatabase(
	ctx context.Context, t *testing.T, adminPool *pgxpool.Pool, assembled, configured string, refusal error,
) {
	t.Helper()

	if !strings.Contains(refusal.Error(), "DEV_HEALTH_PG_DB") {
		t.Fatalf("the refusal does not name the setting responsible: %v", refusal)
	}
	cfg, parseErr := pgconn.ParseConfig(assembled)
	if parseErr != nil {
		return // the driver refuses it outright; there is nothing silent to prevent
	}
	if cfg.Database == configured {
		t.Fatalf("database %q was refused (%v) but the driver reads it back unchanged -- the refusal is wrong",
			configured, refusal)
	}
	if _, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", quoteIdent(cfg.Database))); err != nil {
		t.Fatalf("create the rewritten database %q: %v", cfg.Database, err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", quoteIdent(cfg.Database)))
	})

	conn, connErr := pgx.Connect(ctx, assembled)
	if connErr != nil {
		t.Fatalf("the assembled DSN was expected to connect (to the WRONG database): %v", connErr)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	var reached string
	if scanErr := conn.QueryRow(ctx, "SELECT current_database()").Scan(&reached); scanErr != nil {
		t.Fatalf("query failed: %v", scanErr)
	}
	if reached == configured {
		t.Fatalf("expected the assembled DSN to reach a DIFFERENT database than %q, but it reached it", configured)
	}
}
