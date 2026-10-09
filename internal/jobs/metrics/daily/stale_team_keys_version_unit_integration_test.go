//go:build integration

package daily

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// The version of a row of zeros is at most one unit of the table's
// computed_at column after the row it supersedes. A larger step puts the row
// of zeros ahead of the clock, and a real row that the next compute writes
// for the same key at its own clock then reads as superseded.

// The declared unit of each table is the unit of its column in the schema of
// the migration chain.
func TestTheVersionStepOfEachStaleKeyTableIsOneUnitOfItsComputedAtColumn(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	tables := StaleTeamKeyTables()
	if len(tables) == 0 || len(staleKeyVersionSteps) != len(tables) {
		t.Fatalf("%d table(s) of the rule and %d declared unit(s): each table has one", len(tables), len(staleKeyVersionSteps))
	}
	for _, table := range tables {
		var columnType string
		if err := conn.QueryRow(ctx, `SELECT type FROM system.columns
WHERE database = currentDatabase() AND table = ? AND name = 'computed_at'`, table.Table).Scan(&columnType); err != nil {
			t.Fatalf("read the computed_at type of %s: %v", table.Table, err)
		}
		var unit time.Duration
		switch {
		case strings.HasPrefix(columnType, "DateTime64(3"):
			unit = time.Millisecond
		case strings.HasPrefix(columnType, "DateTime64(6"):
			unit = time.Microsecond
		case columnType == "DateTime" || strings.HasPrefix(columnType, "DateTime("):
			unit = time.Second
		default:
			t.Fatalf("%s.computed_at is %q: the test knows no unit for it", table.Table, columnType)
		}
		if declared := staleKeyVersionSteps[table.Table]; declared != unit {
			t.Errorf("%s: the declared unit is %s, its computed_at column (%s) keeps %s", table.Table, declared, columnType, unit)
		}
	}
}

// A clock that is three units after the old row is late enough: the row of
// zeros takes the clock, in every table.
func TestARowOfZerosIsNotAheadOfAClockThatIsLaterThanTheOldRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	stored := staleKeyRuleDay.Add(31 * time.Hour)
	for _, table := range StaleTeamKeyTables() {
		t.Run(table.Table, func(t *testing.T) {
			unit := staleKeyVersionSteps[table.Table]
			if unit <= 0 {
				t.Fatalf("no declared unit")
			}
			clock := stored.Add(3 * unit)
			key := staleKeyRuleKey(table, "platform-unit", "unit")
			insertStaleKeyRuleLiveRow(t, ctx, conn, table, key, stored)
			var scope staleKeyScope
			if len(table.Scope) > 0 {
				scope = staleKeyScope{table.ScopeTuple(key): {}}
			}
			if written, err := supersedeStaleTeamKeys(ctx, conn, table, staleKeyRuleOrg, staleKeyRuleDay, scope, nil, clock); err != nil || written < 1 {
				t.Fatalf("the rule wrote %d row(s) (err %v), want the row of zeros of the key", written, err)
			}
			predicates := []string{"org_id = ?", table.DayColumn + " = ?"}
			args := []any{staleKeyRuleOrg, staleKeyRuleDay}
			for index, column := range table.Keys {
				predicates = append(predicates, column.ReadExpression()+" = ?")
				args = append(args, key[index])
			}
			var newest time.Time
			if err := conn.QueryRow(ctx, "SELECT max(computed_at) FROM "+table.Table+" WHERE "+strings.Join(predicates, " AND "),
				args...).Scan(&newest); err != nil {
				t.Fatalf("read the newest row of the key: %v", err)
			}
			if !newest.Equal(clock) {
				t.Errorf("the row of zeros is at %s, the clock was %s (the old row at %s): it is %s ahead of its clock",
					newest.UTC().Format(time.RFC3339Nano), clock.Format(time.RFC3339Nano), stored.Format(time.RFC3339Nano), newest.Sub(clock))
			}
		})
	}
}

// One host, one clock, no skew: a team's key is superseded, and 300 ms later
// the team is back and the family computes the day again. The real row of
// that compute is what the key holds.
func TestAKeyThatComesBackInAFinalizeFamilyTableReadsItsRealRow(t *testing.T) {
	ctx := context.Background()
	conn := workItemAttributionLinkedIssueMigratedClickHouse(t, ctx)
	const org = "00000000-0000-4000-8000-0000007f0001"
	day := earlyReturnDay
	repo := uuid.MustParse("00000000-0000-4000-8000-0000000007c1")
	t0 := day.Add(-72 * time.Hour)
	exec := func(what, query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	team := func(at time.Time, active uint8) {
		t.Helper()
		exec("insert team", `INSERT INTO teams
    (id, team_uuid, name, members, repo_patterns, updated_at, last_synced, org_id, provider, is_active)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			"platform", uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:platform")), "Platform", []string{"dev@example.com"},
			[]string{"acme/*"}, at, at, org, "github", active)
	}
	team(t0, 1)
	exec("insert repo", "INSERT INTO repos (id, repo, org_id, provider, last_synced) VALUES (?, ?, ?, ?, ?)",
		repo, "acme/api", org, "github", t0)
	exec("insert ownership", `INSERT INTO team_repo_ownership
    (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, valid_from, updated_at)
    VALUES (?, 'github', 'platform', ?, 'acme/api', 'exact', 'native', 1, 10, ?, ?)`, org, repo, t0, t0)
	exec("insert repository complexity", `INSERT INTO repo_complexity_daily
    (repo_id, day, loc_total, cyclomatic_total, cyclomatic_per_kloc, high_complexity_functions, very_high_complexity_functions, computed_at, org_id)
    VALUES (?, ?, 1000, 200, 200, 4, 1, ?, ?)`, repo, day, t0, org)

	base := day.Add(40 * time.Hour) // a whole second
	compute := func(at time.Time) {
		t.Helper()
		executor, err := NewTeamComplexityExecutor(conn)
		if err != nil {
			t.Fatal(err)
		}
		executor.nowUTC = func() time.Time { return at }
		if _, err := executor.ComputeFinalizeFamily(ctx, Run{ID: uuid.NewString(), OrganizationID: org, TargetDay: day}); err != nil {
			t.Fatalf("team_complexity at %s: %v", at.Format(time.RFC3339Nano), err)
		}
	}
	held := func() uint64 {
		t.Helper()
		var loc uint64
		if err := conn.QueryRow(ctx, `SELECT toUInt64(sum(loc_total)) FROM team_complexity_daily FINAL
WHERE org_id = ? AND day = ? AND team_id = 'platform'`, org, day).Scan(&loc); err != nil {
			t.Fatalf("read the team's row: %v", err)
		}
		return loc
	}

	compute(base)
	if loc := held(); loc != 1000 {
		t.Fatalf("the first compute stored %d lines for the team, want 1000: the case is not set", loc)
	}
	team(t0.Add(time.Hour), 0)
	compute(base.Add(300 * time.Millisecond))
	if loc := held(); loc != 0 {
		t.Fatalf("the key of the inactive team holds %d lines, want its row of zeros: the case is not set", loc)
	}
	team(t0.Add(2*time.Hour), 1)
	compute(base.Add(600 * time.Millisecond))
	if loc := held(); loc != 1000 {
		t.Errorf("300 ms after its row of zeros the key holds %d lines, want the 1000 of the real row", loc)
	}
}
