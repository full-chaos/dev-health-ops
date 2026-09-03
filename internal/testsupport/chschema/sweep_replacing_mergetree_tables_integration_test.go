//go:build integration

package chschema

import (
	"context"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestSweepReplacingMergeTreeTablesMatchesTheAuthoritativeCount applies the
// full migration chain and reads system.tables. This is the AUTHORITATIVE
// population for CHAOS-4902 -- the engine after all migrations apply, rather
// than a regex over migration text.
//
// A prior regex-over-migration-text instrument answered 88; this schema read
// answers 85. The 3-table diff was the instrument's own error list -- the
// count asserted here is the number the CHAOS-4902 dedup sweep was actually
// scoped against, not the regex's guess. A future migration that adds or
// drops a ReplacingMergeTree table without updating that scope should fail
// this test, not silently change coverage.
func TestSweepReplacingMergeTreeTablesMatchesTheAuthoritativeCount(t *testing.T) {
	const wantCount = 85

	ctx := context.Background()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	Apply(ctx, t, instance)

	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
	// HTTP protocol EXPLICITLY. The native client against an 8123 endpoint
	// reports "[handshake] unexpected packet [72] from server" -- 72 is 'H',
	// the first byte of an HTTP response. That error reads like a schema or
	// auth problem and is neither.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.HTTP,
		Addr:     []string{parsed.Host},
		Auth: clickhouse.Auth{
			Database: strings.TrimPrefix(parsed.Path, "/"),
			Username: parsed.User.Username(),
			Password: password,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.tables WHERE database = currentDatabase() AND engine LIKE 'Replacing%' ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(names)

	// A CONTROL, because "the schema did not apply" and "the schema has no
	// Replacing tables" produce the same empty answer, and this whole ticket
	// is about two states that look identical from outside.
	var total uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = currentDatabase()").Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total == 0 {
		t.Fatal("zero tables of ANY engine after Apply -- the migration chain did not run, so a wrong Replacing count would mean nothing")
	}

	if len(names) != wantCount {
		t.Fatalf("found %d ReplacingMergeTree table(s), want the CHAOS-4902 authoritative count %d -- "+
			"a migration added or dropped a Replacing* table without updating the dedup sweep's scope; tables: %v",
			len(names), wantCount, names)
	}
}
