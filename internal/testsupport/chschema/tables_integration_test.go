//go:build integration

package chschema

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

// TestCreateTablesMatchesTheMigrator builds every table the chain changes twice: once by the migrator (Apply), once
// by CreateTables in another database, and reads both back from the server. Each table must have the same columns
// (name, position, type, default, comment, codec), the same engine and keys, and the same data skipping indices.
func TestCreateTablesMatchesTheMigrator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	tables := ChainTables(chain)
	if len(tables) == 0 {
		t.Fatal("the chain changes no table: nothing would be compared")
	}

	migrated := startInstance(ctx, t)
	Apply(ctx, t, migrated)
	built := startInstance(ctx, t)
	conn := open(ctx, t, built.URI)
	CreateTables(ctx, t, conn, tables...)

	want := open(ctx, t, migrated.URI)
	for _, table := range tables {
		if got, wantShape := shape(ctx, t, conn, table), shape(ctx, t, want, table); !reflect.DeepEqual(got, wantShape) {
			t.Errorf("%s:\nCreateTables %#v\nmigrator     %#v", table, got, wantShape)
		}
	}
}

type tableShape struct {
	Engine, SortingKey, PrimaryKey, PartitionKey string
	Columns                                      []columnShape
	Indices                                      []string
}

type columnShape struct {
	Name, Type, DefaultKind, DefaultExpression, Comment, Codec string
	Position                                                   uint64
}

func open(ctx context.Context, t *testing.T, uri string) driver.Conn {
	t.Helper()
	options, err := clickhouse.ParseDSN(uri)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := clickhouse.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	return conn
}

func shape(ctx context.Context, t *testing.T, conn driver.Conn, table string) tableShape {
	t.Helper()
	var result tableShape
	if err := conn.QueryRow(ctx, `SELECT engine_full, sorting_key, primary_key, partition_key FROM system.tables
WHERE database = currentDatabase() AND name = ?`, table).Scan(&result.Engine, &result.SortingKey, &result.PrimaryKey, &result.PartitionKey); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	rows, err := conn.Query(ctx, `SELECT name, type, default_kind, default_expression, comment, compression_codec, position
FROM system.columns WHERE database = currentDatabase() AND table = ? ORDER BY position`, table)
	if err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var column columnShape
		if err := rows.Scan(&column.Name, &column.Type, &column.DefaultKind, &column.DefaultExpression, &column.Comment, &column.Codec, &column.Position); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		result.Columns = append(result.Columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	if len(result.Columns) == 0 {
		t.Fatalf("%s has no columns", table)
	}
	indices, err := conn.Query(ctx, `SELECT concat(name, ' ', type_full, ' ', expr, ' ', toString(granularity))
FROM system.data_skipping_indices WHERE database = currentDatabase() AND table = ? ORDER BY name`, table)
	if err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	defer indices.Close()
	for indices.Next() {
		var index string
		if err := indices.Scan(&index); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
		result.Indices = append(result.Indices, index)
	}
	if err := indices.Err(); err != nil {
		t.Fatalf("%s: %v", table, err)
	}
	return result
}
