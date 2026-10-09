//go:build integration

package daily

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/teamkeytables"
	"github.com/google/uuid"
)

// The stale-key rule against every table of StaleTeamKeyTables, on the schema
// of the migration chain. Each table gets the same case, built from its
// declaration alone, so a table that is added to the set is covered with no
// new test:
//
//   - a key under the old team id holds a measure; the run produces the same
//     key under the new team id;
//   - the rule writes ONE row of zeros over the old key: 0 in every count,
//     NULL in every Nullable measure, the column default in every other
//     column;
//   - a key of another unit (outside the run's scope) keeps its measure;
//   - a second run writes nothing, and a key that the run produced is never
//     superseded;
//   - a row that the declaration's fixed predicate excludes is not touched.

const staleKeyRuleOrg = "00000000-0000-4000-8000-00000000e0b1"

var staleKeyRuleDay = time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)

// staleKeyRuleFixedValues are the key values that a table's fixed predicate
// needs.
var staleKeyRuleFixedValues = map[string]map[string]string{
	"compounding_risk_daily": {"scope": "team"},
}

// staleKeyRuleKey builds one key of a table: the team column holds team, the
// scope columns hold a value of the unit, the other columns a fixed value.
func staleKeyRuleKey(table StaleKeyTable, team, unit string) staleKey {
	inScope := map[string]bool{}
	for _, name := range table.Scope {
		inScope[name] = true
	}
	key := make(staleKey, 0, len(table.Keys))
	for _, column := range table.Keys {
		fixed, isFixed := staleKeyRuleFixedValues[table.Table][column.Name]
		switch {
		case isFixed:
			key = append(key, fixed)
		case column.Name == table.TeamColumn:
			key = append(key, team)
		case column.Kind == teamkeytables.KeyUUID || column.Kind == teamkeytables.KeyNullableUUID:
			seed := column.Name
			if inScope[column.Name] {
				seed += ":" + unit
			}
			key = append(key, uuid.NewSHA1(uuid.NameSpaceURL, []byte(seed)).String())
		case inScope[column.Name]:
			key = append(key, column.Name+"-"+unit)
		default:
			key = append(key, column.Name+"-value")
		}
	}
	return key
}

// insertStaleKeyRuleLiveRow stores a row that holds 1 in every measure.
func insertStaleKeyRuleLiveRow(
	t *testing.T, ctx context.Context, conn driver.Conn, table StaleKeyTable, key staleKey, computedAt time.Time,
) {
	t.Helper()
	columns := []string{"org_id", table.DayColumn}
	values := []any{staleKeyRuleOrg, staleKeyRuleDay}
	for index, column := range table.Keys {
		stored, err := column.StoredValue(key[index])
		if err != nil {
			t.Fatalf("%s key value: %v", table.Table, err)
		}
		// A statement binds a NULL as a nil value, not as a nil pointer.
		switch pointer := stored.(type) {
		case *string:
			if pointer == nil {
				stored = nil
			}
		case *uuid.UUID:
			if pointer == nil {
				stored = nil
			}
		}
		columns = append(columns, column.Name)
		values = append(values, stored)
	}
	for _, measure := range append(append([]string{}, table.Measures...), table.NullableMeasures...) {
		columns = append(columns, measure)
		values = append(values, 1)
	}
	columns = append(columns, "computed_at")
	values = append(values, computedAt)
	query := "INSERT INTO " + table.Table + " (" + strings.Join(columns, ", ") + ") VALUES (" +
		strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", ") + ")"
	if err := conn.Exec(ctx, query, values...); err != nil {
		t.Fatalf("insert a live row of %s: %v", table.Table, err)
	}
}

// staleKeyRuleStored is what a reader of the newest row of one key sees.
type staleKeyRuleStored struct {
	versions       uint64
	measureSum     float64 // the sum of the counts and values
	nullMeasures   uint64  // the Nullable measures that hold NULL
	nonDefaultRest uint64  // the label and configuration columns that hold a value
}

func readStaleKeyRuleStored(
	t *testing.T, ctx context.Context, conn driver.Conn, table StaleKeyTable, key staleKey,
) staleKeyRuleStored {
	t.Helper()
	predicates := []string{"org_id = ?", table.DayColumn + " = ?"}
	args := []any{staleKeyRuleOrg, staleKeyRuleDay}
	for index, column := range table.Keys {
		predicates = append(predicates, column.ReadExpression()+" = ?")
		args = append(args, key[index])
	}
	where := strings.Join(predicates, " AND ")
	var stored staleKeyRuleStored
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table.Table+" WHERE "+where, args...).Scan(&stored.versions); err != nil {
		t.Fatalf("count the versions of a %s key: %v", table.Table, err)
	}
	sum, nulls, rest := []string{"0"}, []string{"0"}, []string{"0"}
	for _, measure := range table.Measures {
		sum = append(sum, "toFloat64("+measure+")")
	}
	for _, measure := range table.NullableMeasures {
		sum = append(sum, "toFloat64(ifNull("+measure+", 0))")
		nulls = append(nulls, "isNull("+measure+")")
	}
	for _, column := range append(append([]string{}, table.Labels...), table.Configuration...) {
		rest = append(rest, "toUInt64(toString("+column+") NOT IN ('', '0', 'unknown'))")
	}
	query := "SELECT toFloat64(sum(" + strings.Join(sum, " + ") + ")), toUInt64(sum(" + strings.Join(nulls, " + ") +
		")), toUInt64(sum(" + strings.Join(rest, " + ") + ")) FROM " + table.Table + " FINAL WHERE " + where
	if err := conn.QueryRow(ctx, query, args...).Scan(&stored.measureSum, &stored.nullMeasures, &stored.nonDefaultRest); err != nil {
		t.Fatalf("read the newest row of a %s key: %v", table.Table, err)
	}
	return stored
}

func TestTheStaleKeyRuleWritesARowOfZerosOverEachSupersededKey(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	first := staleKeyRuleDay.Add(30 * time.Hour)

	for _, table := range StaleTeamKeyTables() {
		t.Run(table.Table, func(t *testing.T) {
			measures := float64(len(table.Measures) + len(table.NullableMeasures))
			oldKey := staleKeyRuleKey(table, "platform", "unit-a")
			newKey := staleKeyRuleKey(table, "github:platform", "unit-a")
			outside := staleKeyRuleKey(table, "platform", "unit-b")
			insertStaleKeyRuleLiveRow(t, ctx, conn, table, oldKey, first)
			insertStaleKeyRuleLiveRow(t, ctx, conn, table, newKey, first.Add(time.Hour))
			scoped := len(table.Scope) > 0
			var scope staleKeyScope
			if scoped {
				insertStaleKeyRuleLiveRow(t, ctx, conn, table, outside, first)
				scope = staleKeyScope{table.ScopeTuple(oldKey): {}}
			}
			if table.Where != "" {
				// A row that the fixed predicate excludes: the same team id
				// under the other scope value of the table.
				if err := conn.Exec(ctx, `INSERT INTO compounding_risk_daily
    (org_id, day, scope, scope_id, compounding_risk, computed_at) VALUES (?, ?, 'repo', 'platform', 0.5, ?)`,
					staleKeyRuleOrg, staleKeyRuleDay, first); err != nil {
					t.Fatalf("insert the excluded row: %v", err)
				}
			}

			second := first.Add(10 * time.Hour)
			written, err := supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, scope, []staleKey{newKey}, second)
			if err != nil {
				t.Fatalf("the rule: %v", err)
			}
			if written != 1 {
				t.Fatalf("the rule wrote %d row(s), want 1: the old key only", written)
			}
			old := readStaleKeyRuleStored(t, ctx, conn, table, oldKey)
			if old.versions != 2 || old.measureSum != 0 || old.nullMeasures != uint64(len(table.NullableMeasures)) || old.nonDefaultRest != 0 {
				t.Errorf("the old key holds %+v, want 2 versions, 0 in every count, NULL in all %d Nullable measures, defaults elsewhere",
					old, len(table.NullableMeasures))
			}
			if kept := readStaleKeyRuleStored(t, ctx, conn, table, newKey); kept.versions != 1 || kept.measureSum != measures {
				t.Errorf("the key that the run produced holds %+v, want its 1 version with every measure", kept)
			}
			if scoped {
				if other := readStaleKeyRuleStored(t, ctx, conn, table, outside); other.versions != 1 || other.measureSum != measures {
					t.Errorf("the key of another unit holds %+v, want its 1 version with every measure", other)
				}
			}
			if table.Where != "" {
				var versions uint64
				var score *float64
				if err := conn.QueryRow(ctx, `SELECT count(), max(compounding_risk) FROM compounding_risk_daily
WHERE org_id = ? AND day = ? AND scope = 'repo' AND scope_id = 'platform'`, staleKeyRuleOrg, staleKeyRuleDay).Scan(&versions, &score); err != nil {
					t.Fatalf("read the excluded row: %v", err)
				}
				if versions != 1 || score == nil || *score != 0.5 {
					t.Errorf("the row that the fixed predicate excludes holds %d version(s), score %v; want 1 and 0.5", versions, score)
				}
			}

			// The predicate for readers agrees with the writer's read: both
			// forms hold the keys with a measure and leave out the key with
			// the row of zeros.
			liveWant := uint64(1)
			if scoped {
				liveWant = 2
			}
			where := "org_id = ? AND " + table.DayColumn + " = ?"
			if table.Where != "" {
				where += " AND (" + table.Where + ")"
			}
			qualified := make([]string, 0, len(table.Keys))
			for _, column := range table.Keys {
				qualified = append(qualified, "t."+column.Name)
			}
			for form, query := range map[string]string{
				"every key":         "SELECT count() FROM " + table.Table + " FINAL WHERE " + where,
				"LiveRow":           "SELECT count() FROM " + table.Table + " FINAL WHERE " + where + " AND " + table.LiveRow(""),
				"LiveRow qualified": "SELECT count() FROM " + table.Table + " AS t FINAL WHERE " + where + " AND " + table.LiveRow("t."),
				"LiveHaving": "SELECT count() FROM (SELECT 1 FROM " + table.Table + " AS t WHERE " + where +
					" GROUP BY " + strings.Join(qualified, ", ") + " HAVING " + table.LiveHaving("t.") + ")",
				"LiveKeysQuery": "SELECT count() FROM (" + table.LiveKeysQuery() + ")",
			} {
				var count uint64
				if err := conn.QueryRow(ctx, query, staleKeyRuleOrg, staleKeyRuleDay).Scan(&count); err != nil {
					t.Fatalf("%s: %v\n%s", form, err, query)
				}
				want := liveWant
				if form == "every key" {
					want = liveWant + 1
				}
				if count != want {
					t.Errorf("%s holds %d key(s), want %d", form, count, want)
				}
			}

			// A second run writes nothing: the newest row of the old key is a
			// row of zeros.
			third := second.Add(10 * time.Hour)
			written, err = supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, scope, []staleKey{newKey}, third)
			if err != nil || written != 0 {
				t.Errorf("a second run wrote %d row(s) (err %v), want 0", written, err)
			}

			// A key that holds a measure again and that the run produced is
			// not superseded.
			insertStaleKeyRuleLiveRow(t, ctx, conn, table, oldKey, third.Add(time.Hour))
			written, err = supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, scope,
				[]staleKey{newKey, oldKey}, third.Add(2*time.Hour))
			if err != nil || written != 0 {
				t.Errorf("a run that produced both keys wrote %d row(s) (err %v), want 0", written, err)
			}
			if again := readStaleKeyRuleStored(t, ctx, conn, table, oldKey); again.measureSum != measures {
				t.Errorf("the key that the run produced again holds %+v, want every measure", again)
			}

			// A run with no unit holds no key to supersede.
			if scoped {
				written, err = supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, nil, nil, third.Add(3*time.Hour))
				if err != nil || written != 0 {
					t.Errorf("a run with no unit wrote %d row(s) (err %v), want 0", written, err)
				}
			}
		})
	}
}

// A key whose Nullable key column holds NULL is superseded with NULL stored
// again, so the row of zeros lands on the same key and not on a new one.
func TestTheStaleKeyRuleStoresANullKeyColumnAsNull(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	first := staleKeyRuleDay.Add(30 * time.Hour)
	for _, table := range StaleTeamKeyTables() {
		nullable := false
		key := staleKeyRuleKey(table, "platform", "unit-a")
		for index, column := range table.Keys {
			switch column.Kind {
			case teamkeytables.KeyNullableString:
				key[index], nullable = "", true
			case teamkeytables.KeyNullableUUID:
				key[index], nullable = uuid.Nil.String(), true
			}
		}
		if !nullable {
			continue
		}
		t.Run(table.Table, func(t *testing.T) {
			insertStaleKeyRuleLiveRow(t, ctx, conn, table, key, first)
			var scope staleKeyScope
			if len(table.Scope) > 0 {
				scope = staleKeyScope{table.ScopeTuple(key): {}}
			}
			written, err := supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, scope, nil, first.Add(time.Hour))
			if err != nil || written != 1 {
				t.Fatalf("the rule wrote %d row(s) (err %v), want 1", written, err)
			}
			if stored := readStaleKeyRuleStored(t, ctx, conn, table, key); stored.versions != 2 || stored.measureSum != 0 {
				t.Errorf("the key with a NULL column holds %+v, want 2 versions and 0 in every measure", stored)
			}
			// Both stored versions hold NULL in the column, not the empty
			// value the sorting key reads it as: a read of the column itself
			// finds the row of zeros where it found the row it supersedes.
			for _, column := range table.Keys {
				if column.Kind != teamkeytables.KeyNullableString && column.Kind != teamkeytables.KeyNullableUUID {
					continue
				}
				var nulls uint64
				if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT countIf(isNull(%s)) FROM %s WHERE org_id = ? AND %s = ?",
					column.Name, table.Table, table.DayColumn), staleKeyRuleOrg, staleKeyRuleDay).Scan(&nulls); err != nil {
					t.Fatalf("count the NULL values of %s: %v", column.Name, err)
				}
				if nulls != 2 {
					t.Errorf("%d stored version(s) hold NULL in %s, want 2: the first row and the row of zeros", nulls, column.Name)
				}
			}
			var total uint64
			if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s FINAL WHERE org_id = ? AND %s = ?", table.Table, table.DayColumn),
				staleKeyRuleOrg, staleKeyRuleDay).Scan(&total); err != nil {
				t.Fatalf("count the keys of %s: %v", table.Table, err)
			}
			if total != 1 {
				t.Errorf("%s holds %d key(s) for the day, want 1: the row of zeros must land on the key it supersedes", table.Table, total)
			}
		})
	}
}
