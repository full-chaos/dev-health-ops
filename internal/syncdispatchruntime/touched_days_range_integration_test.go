//go:build integration

package syncdispatchruntime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The days between a late event and the day its row is written show the item
// in the wrong state until the row is written (CHAOS-8855). Every case runs the
// real record on real ClickHouse, with its own organization on one connection.

func rangeDay(month time.Month, d int) time.Time {
	return time.Date(2026, month, d, 9, 30, 0, 0, time.UTC)
}

// rangePreviousRecord is the earlier sync of the repository: a 'touched' event
// of the given time on one day and the 'dispatched' event that ended it, so the
// record itself leaves nothing pending.
func rangePreviousRecord(t *testing.T, ctx context.Context, conn driver.Conn, org string, repo uuid.UUID, at time.Time) {
	t.Helper()
	for i, kind := range []string{"touched", "dispatched"} {
		if err := conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate(?), ?, ?, fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
			org, at.Format("2006-01-02"), repo, kind, at.Add(time.Duration(i)*time.Minute).UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
}

func rangeKeys(repo uuid.UUID, from, to time.Time) []string {
	var keys []string
	for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
		keys = append(keys, d.Format("2006-01-02")+"|"+repo.String())
	}
	return keys
}

func rangeRecord(t *testing.T, ctx context.Context, conn driver.Conn, org string, since time.Time) []string {
	t.Helper()
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecordTouched(ctx, org, since, nil); err != nil {
		t.Fatal(err)
	}
	return pendingTouchedTestKeys(t, ctx, conn, org)
}

func TestTouchedDaysRecordRangeFromANewEventToItsWriteDay(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	repo := touchedTestRepoA
	since := rangeDay(8, 20).Truncate(24 * time.Hour)
	previous := rangeDay(8, 5)
	org := func(n int) string { return fmt.Sprintf("00000000-0000-4000-8000-00000000c0%02d", n) }
	at := func(month time.Month, d int) *time.Time { v := rangeDay(month, d); return &v }

	t.Run("the late event of the ticket: created 08-10, started 08-12, completed 08-15, written 08-20", func(t *testing.T) {
		o := org(1)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#late", provider: "github",
			created: rangeDay(8, 10), started: at(8, 12), completed: at(8, 15), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		if want := rangeKeys(repo, rangeDay(8, 10), rangeDay(8, 20)); !reflect.DeepEqual(got, want) || len(got) != 11 {
			t.Fatalf("pending keys (%d)\n got %v\nwant %v", len(got), got, want)
		}
	})

	t.Run("an old item that is only written again adds no day", func(t *testing.T) {
		o := org(2)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#old", provider: "github",
			created: rangeDay(6, 1), completed: at(6, 20), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(6, 1).Format("2006-01-02") + "|" + repo.String(), rangeDay(6, 20).Format("2006-01-02") + "|" + repo.String()}
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("an old item adds only its own event days\n got %v\nwant %v", got, want)
		}
	})

	t.Run("a gap of several syncs: every new event of the gap gives its range, items and transitions", func(t *testing.T) {
		o := org(3)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		insertTouchedTestItems(t, ctx, conn,
			touchedTestItem{org: o, repo: repo, id: "gh:acme/api#a", provider: "github", created: rangeDay(7, 1), completed: at(8, 8), synced: rangeDay(8, 20)},
			touchedTestItem{org: o, repo: repo, id: "gh:acme/api#b", provider: "github", created: rangeDay(7, 2), completed: at(8, 12), synced: rangeDay(8, 20)})
		insertTouchedTestTransition(t, ctx, conn, o, repo, "gh:acme/api#c", "github", rangeDay(8, 6), rangeDay(8, 20))
		got := rangeRecord(t, ctx, conn, o, since)
		want := append(rangeKeys(repo, rangeDay(8, 6), rangeDay(8, 20)),
			rangeDay(7, 1).Format("2006-01-02")+"|"+repo.String(), rangeDay(7, 2).Format("2006-01-02")+"|"+repo.String())
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("pending keys\n got %v\nwant %v", got, want)
		}
	})

	t.Run("a repository with no previous record (first sync): the event days only", func(t *testing.T) {
		o := org(4)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#first", provider: "github",
			created: rangeDay(8, 10), completed: at(8, 15), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(8, 10).Format("2006-01-02") + "|" + repo.String(), rangeDay(8, 15).Format("2006-01-02") + "|" + repo.String()}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("a first sync adds no range (its unit windows mark its days)\n got %v\nwant %v", got, want)
		}
	})

	t.Run("NAMED LIMIT: an event delivered late with an event time older than the previous record is not new", func(t *testing.T) {
		o := org(5)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#older", provider: "github",
			created: rangeDay(8, 1), completed: at(8, 4), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(8, 1).Format("2006-01-02") + "|" + repo.String(), rangeDay(8, 4).Format("2006-01-02") + "|" + repo.String()}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("limit: only the event days of an event older than the previous record\n got %v\nwant %v", got, want)
		}
	})

	t.Run("the range covers at most 366 day keys: the write day and the 365 days before it", func(t *testing.T) {
		o := org(6)
		// the oldest event that can be new: after a previous record that is still
		// inside the 366 days of `at` before the run (2025-08-19 00:30), on the day
		// 366 days before the write day
		old := time.Date(2025, 8, 19, 9, 30, 0, 0, time.UTC)
		if err := conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate('2026-08-01'), ?, 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
			o, repo, time.Date(2025, 8, 19, 0, 30, 0, 0, time.UTC).UnixMilli()); err != nil {
			t.Fatal(err)
		}
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#ancient", provider: "github",
			created: old, synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		first := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).AddDate(0, 0, -365)
		want := append(rangeKeys(repo, first, time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)), old.Format("2006-01-02")+"|"+repo.String())
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("pending %d keys, want %d (the event day and 366 range days, the 367th is not touched)", len(got), len(want))
		}
	})

	t.Run("the previous record is the NEWEST touched record of the repository", func(t *testing.T) {
		o := org(8)
		// two records: 08-01 and 08-12. An event at 08-10 is older than the newest
		// record, so it is not new (min would call it new).
		rangePreviousRecord(t, ctx, conn, o, repo, rangeDay(8, 1))
		rangePreviousRecord(t, ctx, conn, o, repo, rangeDay(8, 12))
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#between", provider: "github",
			created: rangeDay(7, 1), completed: at(8, 10), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(7, 1).Format("2006-01-02") + "|" + repo.String(), rangeDay(8, 10).Format("2006-01-02") + "|" + repo.String()}
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("an event older than the newest previous record is not new\n got %v\nwant %v", got, want)
		}
	})

	t.Run("a dispatched event is not a previous record: only touched events count", func(t *testing.T) {
		o := org(9)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		// a 'dispatched' event NEWER than the event below: if it counted, the event
		// would not be new.
		if err := conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate('2026-08-16'), ?, 'dispatched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
			o, repo, rangeDay(8, 18).UnixMilli()); err != nil {
			t.Fatal(err)
		}
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#late2", provider: "github",
			created: rangeDay(8, 12), completed: at(8, 14), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		for _, key := range rangeKeys(repo, rangeDay(8, 12), rangeDay(8, 20)) {
			if !contains(got, key) {
				t.Fatalf("day %s of the range is missing: %v", key, got)
			}
		}
	})

	t.Run("a previous record older than the 366 days before the run is not seen", func(t *testing.T) {
		o := org(10)
		// the only record is on a day more than 366 days before the run's lower bound
		rangePreviousRecord(t, ctx, conn, o, repo, time.Date(2025, 6, 1, 9, 0, 0, 0, time.UTC))
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#dormant", provider: "github",
			created: rangeDay(8, 10), completed: at(8, 15), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(8, 10).Format("2006-01-02") + "|" + repo.String(), rangeDay(8, 15).Format("2006-01-02") + "|" + repo.String()}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("no previous record is seen in the window: event days only\n got %v\nwant %v", got, want)
		}
	})

	t.Run("an event at exactly the time of the previous record is not new", func(t *testing.T) {
		o := org(11)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#equal", provider: "github",
			created: rangeDay(7, 1), completed: &previous, synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		want := []string{rangeDay(7, 1).Format("2006-01-02") + "|" + repo.String(), previous.Format("2006-01-02") + "|" + repo.String()}
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("new means strictly later than the previous record\n got %v\nwant %v", got, want)
		}
	})

	t.Run("the previous record is found by its time, not by the day it touched", func(t *testing.T) {
		o := org(12)
		// the last syncs of the repository wrote only OLD items: the record has a
		// recent `at` (08-05) and a day far in the past (2024-01-01)
		for i, kind := range []string{"touched", "dispatched"} {
			if err := conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate('2024-01-01'), ?, ?, fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
				o, repo, kind, previous.Add(time.Duration(i)*time.Minute).UnixMilli()); err != nil {
				t.Fatal(err)
			}
		}
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#oldday", provider: "github",
			created: rangeDay(8, 10), completed: at(8, 15), synced: rangeDay(8, 20)})
		got := rangeRecord(t, ctx, conn, o, since)
		for _, key := range rangeKeys(repo, rangeDay(8, 10), rangeDay(8, 20)) {
			if !contains(got, key) {
				t.Fatalf("day %s of the range is missing although the previous record is recent: %v", key, got)
			}
		}
	})

	t.Run("two syncs of one repository: the record of the later run never hides the event of the earlier one", func(t *testing.T) {
		o := org(7)
		rangePreviousRecord(t, ctx, conn, o, repo, previous)
		store, err := NewClickHouseTouchedDaysStore(conn)
		if err != nil {
			t.Fatal(err)
		}
		// run A records first (its record is newer than the start of run B)
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#a", provider: "github",
			created: rangeDay(7, 1), completed: at(8, 18), synced: rangeDay(8, 20)})
		if _, err := store.RecordTouched(ctx, o, since, nil); err != nil {
			t.Fatal(err)
		}
		// run B started before that record; its row has an older event than A's record
		insertTouchedTestItems(t, ctx, conn, touchedTestItem{org: o, repo: repo, id: "gh:acme/api#b", provider: "github",
			created: rangeDay(7, 2), completed: at(8, 15), synced: rangeDay(8, 20)})
		if _, err := store.RecordTouched(ctx, o, since, nil); err != nil {
			t.Fatal(err)
		}
		got := pendingTouchedTestKeys(t, ctx, conn, o)
		for _, day := range rangeKeys(repo, rangeDay(8, 15), rangeDay(8, 20)) {
			if !contains(got, day) {
				t.Fatalf("day %s of the range of run B is missing: %v", day, got)
			}
		}
	})
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
