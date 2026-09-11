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
	"github.com/jackc/pgx/v5/pgxpool"
)

// reservedIdentifierClasses names five proof characters for the rule
// that a component is never refused on character grounds when the
// assembler can encode it. Each class is executed against a REAL,
// live-created identifier of that shape -- not merely a driver's DSN
// parser -- for all three of database, user, and password, on both
// PostgreSQL and ClickHouse.
var reservedIdentifierClasses = []struct {
	name string
	char string
}{
	{"hash", "#"},
	{"question", "?"},
	{"slash", "/"},
	{"space", " "},
	{"percent", "%"},
}

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

	for _, class := range reservedIdentifierClasses {
		t.Run(class.name, func(t *testing.T) {
			db := "app" + class.char + "db"
			role := "app" + class.char + "role"
			password := "app" + class.char + "pass"

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

			value, used, resolveErr := ResolveDSNFromComponents(lookup(map[string]string{
				"DEV_HEALTH_PG_DOMAIN_HOST":     host,
				"DEV_HEALTH_PG_DOMAIN_PORT":     port,
				"DEV_HEALTH_PG_DOMAIN_USER":     role,
				"DEV_HEALTH_PG_DOMAIN_PASSWORD": password,
				"DEV_HEALTH_PG_DB":              db,
			}), DomainDatabaseSpec)
			if !used || resolveErr != nil {
				t.Fatalf("used=%v err=%v", used, resolveErr)
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

			if got := ComponentDatabaseName(lookup(map[string]string{"DEV_HEALTH_PG_DB": db}), DomainDatabaseSpec); got != db {
				t.Fatalf("telemetry identifier mismatch: got %q, want %q", got, db)
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

	for _, class := range reservedIdentifierClasses {
		t.Run(class.name, func(t *testing.T) {
			db := "app" + class.char + "db"
			user := "app" + class.char + "user"
			password := "app" + class.char + "pass"

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
			if !used || resolveErr != nil {
				t.Fatalf("used=%v err=%v", used, resolveErr)
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

			if got := ComponentDatabaseName(lookup(map[string]string{"DEV_HEALTH_CH_DB": db}), ClickHouseSpec); got != db {
				t.Fatalf("telemetry identifier mismatch: got %q, want %q", got, db)
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
