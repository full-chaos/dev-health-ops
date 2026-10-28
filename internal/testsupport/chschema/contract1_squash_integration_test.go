//go:build integration

package chschema

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

func loadArchive(t *testing.T) (chmigrate.Baseline, []chmigrate.ChainFile) {
	t.Helper()
	// The archive the head baseline replaced (CHAOS-11751).
	raw, err := os.ReadFile(filepath.Join("..", "..", "chmigrate", "archive", "baseline-098", "head.json"))
	if err != nil {
		t.Fatal(err)
	}
	var baseline chmigrate.Baseline
	if err := json.Unmarshal(raw, &baseline); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join("..", "..", "chmigrate", "archive", "sql", "*.sql"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("the archived chain holds %d files (%v)", len(paths), err)
	}
	sort.Strings(paths)
	var chain []chmigrate.ChainFile
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		chain = append(chain, chmigrate.ChainFile{Version: filepath.Base(path), SQL: string(body)})
	}
	return baseline, chain
}

// contract1Database builds a fresh database on the instance's server with baseline and chain and returns its
// objects (name, engine, CREATE with the database name removed) and versions.
func contract1Database(ctx context.Context, t *testing.T, admin driver.Conn, uri, name string, baseline chmigrate.Baseline, chain []chmigrate.ChainFile) []string {
	t.Helper()
	if err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" SYNC") })
	options, err := clickhouse.ParseDSN(uri)
	if err != nil {
		t.Fatal(err)
	}
	options.Auth.Database = name
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	var out []string
	rows, err := conn.Query(ctx, "SELECT name, engine, create_table_query FROM system.tables WHERE database = ? AND name NOT LIKE '.inner%' ORDER BY name", name)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var object, engine, create string
		if err := rows.Scan(&object, &engine, &create); err != nil {
			t.Fatal(err)
		}
		out = append(out, fmt.Sprintf("%s|%s|%s", object, engine, chmigrate.StripDatabase(create, name)))
	}
	rows.Close()
	versions, err := conn.Query(ctx, "SELECT version FROM schema_migrations ORDER BY version")
	if err != nil {
		t.Fatal(err)
	}
	for versions.Next() {
		var version string
		if err := versions.Scan(&version); err != nil {
			t.Fatal(err)
		}
		out = append(out, "version|"+version)
	}
	versions.Close()
	return out
}

// The contract-1 shape a test asks for is the one it was before the baseline moved to the chain head: the
// overlay on the archived 098 baseline, then the archived chain, builds the database the overlay on the head
// baseline builds (no archived chain file touches an operational table).
func TestContract1OverTheHeadBaselineEqualsTheArchivedChain(t *testing.T) {
	ctx := context.Background()
	instance := startInstance(ctx, t)
	options, err := clickhouse.ParseDSN(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	options.Auth.Database = ""
	admin, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	archivedBaseline, archivedChain := loadArchive(t)
	for _, file := range archivedChain {
		if strings.Contains(file.SQL, "operational_") {
			t.Errorf("archived chain file %s names an operational table", file.Version)
		}
	}
	old, err := overlayContract1(archivedBaseline)
	if err != nil {
		t.Fatal(err)
	}
	head, err := LoadContract1Baseline()
	if err != nil {
		t.Fatal(err)
	}
	live, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	suffix := make([]byte, 6)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatal(err)
	}
	a := contract1Database(ctx, t, admin, instance.URI, "dho_contract1_archive_"+hex.EncodeToString(suffix), old, archivedChain)
	b := contract1Database(ctx, t, admin, instance.URI, "dho_contract1_head_"+hex.EncodeToString(suffix), head, live)
	if !reflect.DeepEqual(a, b) {
		for index := 0; index < len(a) || index < len(b); index++ {
			var x, y string
			if index < len(a) {
				x = a[index]
			}
			if index < len(b) {
				y = b[index]
			}
			if x != y {
				t.Fatalf("contract 1: the archived chain and the head baseline differ at %d:\n  archive %.300s\n  head    %.300s", index, x, y)
			}
		}
	}
}
