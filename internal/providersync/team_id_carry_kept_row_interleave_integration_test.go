//go:build integration

package providersync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// keptRowReadInterleaver commits another writer's version of a kept row right
// after the carry's read of the kept rows: the window between that read and
// the carry's own teams write.
type keptRowReadInterleaver struct {
	driver.Conn
	other func()
	fired bool
}

func (c *keptRowReadInterleaver) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	rows, err := c.Conn.Query(ctx, query, args...)
	if err == nil && !c.fired && query == teamIDCarryKeyedQuery {
		c.fired = true
		c.other()
	}
	return rows, err
}

// CHAOS-10099: the fold of a moved team's manual members into the kept row,
// and the activation of an inactive kept row (D6023), write the kept row in
// the statement that reads it. A writer's update of the kept row at the SAME
// version, committed after the carry read the kept rows, is kept: the rename
// and the added member survive the carry and a merge.
func TestCarryTeamIDsKeepsAnEqualVersionUpdateOfTheKeptRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, c := range []struct {
		name       string
		keptActive uint8
	}{
		{name: "fold into an active kept row", keptActive: 1},
		{name: "activation of an inactive kept row", keptActive: 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
			f.team("", "eng", nil, nil, 1, carryOld.Add(time.Minute), []string{"admin@example.com"}, nil)
			f.team("", "custom:eng", carryPtr("eng"), nil, c.keptActive, carryOld, []string{"kept@example.com"}, nil)

			interleaved := &keptRowReadInterleaver{Conn: conn, other: func() {
				f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('custom:eng', ?, 'renamed eng', ['kept@example.com', 'late@example.com'], [], [], ?, ?, ?, ?, '', 'eng')`,
					uuid.New(), c.keptActive, carryOld, carryOld, f.orgID)
			}}
			outcome, err := CarryTeamIDs(ctx, interleaved, f.orgID, carryAt, false)
			if err != nil {
				t.Fatal(err)
			}
			if outcome.KeptTeamsNotRestated != 0 {
				t.Errorf("outcome = %+v; want every planned kept row written", outcome)
			}
			if !interleaved.fired {
				t.Fatal("the carry did not read the kept rows: the interleaving did not happen")
			}

			want := "custom:eng|renamed eng|kept@example.com,late@example.com,admin@example.com|1"
			check := func(when string) {
				t.Helper()
				got := f.str(`SELECT concat(id, '|', name, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active)) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`)
				if got != want {
					t.Errorf("%s: kept row = %q, want %q", when, got, want)
				}
			}
			check("before a merge")
			f.exec(`OPTIMIZE TABLE teams FINAL`)
			check("after OPTIMIZE TABLE teams FINAL")
			if got := f.str(`SELECT toString(count()) FROM teams FINAL WHERE org_id = ? AND is_active = 1`); got != "1" {
				t.Errorf("active teams = %s, want 1 (%s)", got, strings.ToLower(c.name))
			}
		})
	}
}

// A kept row an admin deletes after the carry read it is not written again
// live: the delete stands. The carry counts it (outcome, Info line, metric)
// from the rows the server reports written.
func TestCarryTeamIDsLeavesAKeptRowDeletedAfterItsReadDeleted(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "eng", nil, nil, 1, carryOld.Add(time.Minute), []string{"admin@example.com"}, nil)
	f.team("", "custom:eng", carryPtr("eng"), nil, 0, carryOld, []string{"kept@example.com"}, nil)
	deleted := carryOld.Add(time.Hour)
	interleaved := &keptRowReadInterleaver{Conn: conn, other: func() {
		f.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, deleted_at) VALUES ('custom:eng', ?, 'team custom:eng', ['kept@example.com'], [], [], 0, ?, ?, ?, '', 'eng', ?)`,
			uuid.New(), deleted, deleted, f.orgID, deleted)
	}}
	before := keptNotRestatedCount(t)
	outcome, err := CarryTeamIDs(ctx, interleaved, f.orgID, carryAt, false)
	if err != nil {
		t.Fatal(err)
	}
	if !interleaved.fired {
		t.Fatal("the carry did not read the kept rows: the interleaving did not happen")
	}
	if outcome.KeyedTeamsActivated != 1 || outcome.KeptTeamsNotRestated != 1 {
		t.Errorf("outcome = %+v; want 1 activation planned and 1 kept row not restated", outcome)
	}
	if got := keptNotRestatedCount(t) - before; got != 1 {
		t.Errorf("%s grew by %d, want 1", teamIDCarryKeptNotRestatedName, got)
	}
	if got := f.str(`SELECT concat(toString(is_active), '|', if(deleted_at IS NULL, 'live', 'deleted')) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`); got != "0|deleted" {
		t.Errorf("kept row = %q, want it still deleted", got)
	}
}

func keptNotRestatedCount(t *testing.T) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != teamIDCarryKeptNotRestatedName {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				total += point.Value
			}
		}
	}
	return total
}
