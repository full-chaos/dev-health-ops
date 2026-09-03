//go:build integration

package authschema

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// schemaShape is what the catalog says about the auth schema at one instant:
// every relation, and for every column its nullability and type.
//
// This is the replacement for scanning migration text. The retired regexp was
// walked through six times across two review rounds — a dynamic DO block, a
// quoted name, an UNLOGGED table, a one-line column, an indentation rule, and
// `ALTER COLUMN ... TYPE ... USING` which erases every value while containing
// no destructive token at all. Each was a legal way to write SQL the scanner
// did not understand. A shape read from the catalog cannot be evaded by how
// the statement was spelled, because it is measured AFTER the statement ran.
type schemaShape struct {
	relations map[string]string      // name -> relkind
	columns   map[string]columnShape // "table.column" -> shape
}

type columnShape struct {
	dataType string
	nullable bool
}

func snapshotSchema(ctx context.Context, conn *pgx.Conn, schema string) (schemaShape, error) {
	shape := schemaShape{
		relations: map[string]string{},
		columns:   map[string]columnShape{},
	}

	rows, err := conn.Query(ctx, `
		SELECT c.relname, c.relkind::text
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f','S')`, schema)
	if err != nil {
		return shape, fmt.Errorf("snapshot relations: %w", err)
	}
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			rows.Close()
			return shape, err
		}
		shape.relations[name] = kind
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return shape, err
	}

	rows, err = conn.Query(ctx, `
		SELECT table_name, column_name, data_type, is_nullable
		FROM information_schema.columns WHERE table_schema = $1`, schema)
	if err != nil {
		return shape, fmt.Errorf("snapshot columns: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var table, column, dataType, nullable string
		if err := rows.Scan(&table, &column, &dataType, &nullable); err != nil {
			return shape, err
		}
		shape.columns[table+"."+column] = columnShape{dataType: dataType, nullable: nullable == "YES"}
	}
	return shape, rows.Err()
}

// diffIsAdditive reports every way `after` is not a superset of `before`.
//
// "Additive" is given a precise meaning here rather than left to a reader's
// intuition: nothing disappears, nothing that was mandatory becomes optional,
// and nothing changes type. The middle clause is the one that matters most and
// the one no text pattern caught — `ALTER COLUMN x DROP NOT NULL` followed by
// `ALTER COLUMN x TYPE text USING NULL::text` erases every value in the column
// while adding no keyword any destructive-statement regexp looks for.
func diffIsAdditive(before, after schemaShape) []string {
	var regressions []string
	for name, kind := range before.relations {
		afterKind, present := after.relations[name]
		if !present {
			regressions = append(regressions, fmt.Sprintf("relation %s disappeared", name))
			continue
		}
		if afterKind != kind {
			regressions = append(regressions,
				fmt.Sprintf("relation %s changed kind %s -> %s", name, kind, afterKind))
		}
	}
	for key, was := range before.columns {
		now, present := after.columns[key]
		if !present {
			regressions = append(regressions, fmt.Sprintf("column %s disappeared", key))
			continue
		}
		if now.dataType != was.dataType {
			regressions = append(regressions,
				fmt.Sprintf("column %s changed type %s -> %s", key, was.dataType, now.dataType))
		}
		if now.nullable && !was.nullable {
			regressions = append(regressions,
				fmt.Sprintf("column %s lost NOT NULL, so existing values can be erased", key))
		}
	}
	sort.Strings(regressions)
	return regressions
}

// TestEveryMigrationStepIsAdditive applies the lineage ONE MIGRATION AT A TIME
// against a real database, snapshotting the catalog around each step, and
// asserts every step only adds.
//
// This is the property CHAOS-4882's scope states ("additive-only
// schema/migrations, no existing row moves"), checked where it is actually
// true or false — in the database the migrations built — instead of inferred
// from the text that built it.
func TestEveryMigrationStepIsAdditive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	env := newAuthMigrationOnly(t, ctx)
	migrations, err := Migrations()
	if err != nil {
		t.Fatalf("Migrations: %v", err)
	}

	steps := 0
	for _, migration := range migrations {
		before, err := snapshotSchema(ctx, env.conn, env.schema)
		if err != nil {
			t.Fatalf("snapshot before %04d: %v", migration.Version, err)
		}
		if err := applyOne(ctx, env.conn, env.options, migration); err != nil {
			t.Fatalf("apply %04d_%s: %v", migration.Version, migration.Name, err)
		}
		after, err := snapshotSchema(ctx, env.conn, env.schema)
		if err != nil {
			t.Fatalf("snapshot after %04d: %v", migration.Version, err)
		}
		if regressions := diffIsAdditive(before, after); len(regressions) > 0 {
			t.Errorf("migration %04d_%s is not additive:\n  %s",
				migration.Version, migration.Name, strings.Join(regressions, "\n  "))
		}
		steps++
		t.Logf("%04d_%s: +%d relation(s), +%d column(s)", migration.Version, migration.Name,
			len(after.relations)-len(before.relations), len(after.columns)-len(before.columns))
	}
	if steps != len(migrations) {
		t.Fatalf("checked %d step(s) of %d", steps, len(migrations))
	}
	if steps == 0 {
		t.Fatal("no migrations were stepped through; this proves nothing")
	}
}

// TestAdditiveDiffCatchesDestructiveMigrations is the control. Each synthetic
// migration below is one a real reviewer found the retired text guard
// accepting, applied for real, and the diff must catch every one.
func TestAdditiveDiffCatchesDestructiveMigrations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cases := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "ALTER COLUMN erasure (codex round 2, missed by the regexp)",
			sql: `ALTER TABLE principals ALTER COLUMN display_name DROP NOT NULL;
			      ALTER TABLE principals ALTER COLUMN display_name TYPE text USING NULL::text;`,
			want: "lost NOT NULL",
		},
		{
			name: "dropped column",
			sql:  `ALTER TABLE principals DROP COLUMN display_name;`,
			want: "disappeared",
		},
		{
			name: "dropped table",
			sql:  `DROP TABLE refresh_credentials;`,
			want: "disappeared",
		},
		{
			name: "dynamic SQL drop (codex round 1, missed by the regexp)",
			sql:  `DO $$ BEGIN EXECUTE 'DROP ' || 'TABLE refresh_credentials'; END $$;`,
			want: "disappeared",
		},
		{
			name: "narrowed type",
			sql:  `ALTER TABLE principals ALTER COLUMN display_name TYPE varchar(8);`,
			want: "changed type",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			env := newAuthMigrationOnly(t, ctx)
			migrations, err := Migrations()
			if err != nil {
				t.Fatalf("Migrations: %v", err)
			}
			for _, migration := range migrations {
				if err := applyOne(ctx, env.conn, env.options, migration); err != nil {
					t.Fatalf("apply %04d: %v", migration.Version, err)
				}
			}

			before, err := snapshotSchema(ctx, env.conn, env.schema)
			if err != nil {
				t.Fatalf("snapshot before: %v", err)
			}
			hostile := Migration{Version: len(migrations) + 1, Name: "hostile", SQL: testCase.sql}
			if err := applyOne(ctx, env.conn, env.options, hostile); err != nil {
				t.Fatalf("the hostile migration did not apply, so this proves nothing: %v", err)
			}
			after, err := snapshotSchema(ctx, env.conn, env.schema)
			if err != nil {
				t.Fatalf("snapshot after: %v", err)
			}

			regressions := diffIsAdditive(before, after)
			if len(regressions) == 0 {
				t.Fatalf("the additive diff accepted a destructive migration:\n%s", testCase.sql)
			}
			var matched bool
			for _, regression := range regressions {
				if strings.Contains(regression, testCase.want) {
					matched = true
					t.Logf("caught: %s", regression)
				}
			}
			if !matched {
				t.Fatalf("caught %v, but none mentions %q", regressions, testCase.want)
			}
		})
	}
}
