//go:build integration

package syncdispatchruntime

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

const (
	touchedTestOrg      = "00000000-0000-4000-8000-0000000008a1"
	touchedTestOtherOrg = "00000000-0000-4000-8000-0000000008a2"
)

var (
	touchedTestRepoA = uuid.MustParse("00000000-0000-4000-8000-00000000a001")
	touchedTestRepoB = uuid.MustParse("00000000-0000-4000-8000-00000000b002")
	// touchedTestSince is the lower bound the record reads from: the start of
	// the sync run minus the clock margin.
	touchedTestSince = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
)

func touchedTestDay(month time.Month, day int) time.Time {
	return time.Date(2026, month, day, 9, 30, 0, 0, time.UTC)
}

type touchedTestItem struct {
	org       string
	repo      uuid.UUID
	id        string
	provider  string
	created   time.Time
	started   *time.Time
	completed *time.Time
	closed    *time.Time
	synced    time.Time
}

func insertTouchedTestItems(t *testing.T, ctx context.Context, conn driver.Conn, items ...touchedTestItem) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, type, status, created_at, started_at, completed_at, closed_at, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if err := batch.Append(item.repo, item.id, item.provider, "issue", "done",
			item.created, item.started, item.completed, item.closed, item.org, item.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func insertTouchedTestTransition(
	t *testing.T, ctx context.Context, conn driver.Conn,
	org string, repo uuid.UUID, id, provider string, occurred, synced time.Time,
) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_item_transitions (
		repo_id, work_item_id, occurred_at, provider, from_status, to_status, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(repo, id, occurred, provider, "todo", "done", org, synced); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

// pendingTouchedTestKeys reads the pending keys of the organization by the
// reader contract of the table, sorted, as "2006-01-02|repository".
func pendingTouchedTestKeys(t *testing.T, ctx context.Context, conn driver.Conn, org string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT toString(day), toString(repo_id) FROM (`+pendingTouchedKeysSQL+`)`, org)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	keys := []string{}
	for rows.Next() {
		var day, repo string
		if err := rows.Scan(&day, &repo); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, day+"|"+repo)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	return keys
}

func touchedTestKey(month time.Month, day int, repo uuid.UUID) string {
	return touchedTestDay(month, day).Format("2006-01-02") + "|" + repo.String()
}

// seedTouchedTestRows writes the raw rows of one sync run of every provider
// shape, with rows on both sides of the lower bound of the read.
func seedTouchedTestRows(t *testing.T, ctx context.Context, conn driver.Conn) []string {
	t.Helper()
	at := func(month time.Month, day int) *time.Time {
		value := touchedTestDay(month, day)
		return &value
	}
	inRun := touchedTestSince.Add(time.Hour)
	beforeRun := touchedTestSince.Add(-time.Millisecond)
	insertTouchedTestItems(t, ctx, conn,
		// A row written exactly at the lower bound belongs to the read.
		touchedTestItem{org: touchedTestOrg, repo: touchedTestRepoA, id: "gh:acme/api#1", provider: "github",
			created: touchedTestDay(8, 1), started: at(8, 2), completed: at(8, 3), closed: at(8, 4), synced: touchedTestSince},
		touchedTestItem{org: touchedTestOrg, repo: touchedTestRepoB, id: "gitlab:acme/web#7", provider: "gitlab",
			created: touchedTestDay(7, 1), synced: inRun},
		touchedTestItem{org: touchedTestOrg, repo: touchedTestRepoB, id: "gitlab:acme/web#8", provider: "gitlab",
			created: touchedTestDay(8, 1), synced: inRun},
		touchedTestItem{org: touchedTestOrg, repo: uuid.Nil, id: "jira:OPS-2", provider: "jira",
			created: touchedTestDay(6, 1), completed: at(6, 5), synced: inRun},
		touchedTestItem{org: touchedTestOrg, repo: uuid.Nil, id: "linear:OPS-1", provider: "linear",
			created: touchedTestDay(5, 1), synced: inRun},
		// One millisecond before the lower bound: a row of an earlier run.
		touchedTestItem{org: touchedTestOrg, repo: touchedTestRepoA, id: "gh:acme/api#old", provider: "github",
			created: touchedTestDay(4, 1), synced: beforeRun},
		touchedTestItem{org: touchedTestOtherOrg, repo: touchedTestRepoA, id: "gh:other/api#1", provider: "github",
			created: touchedTestDay(2, 1), synced: inRun},
	)
	// A github transition carries the nil repository; its item has a real one.
	insertTouchedTestTransition(t, ctx, conn, touchedTestOrg, uuid.Nil, "gh:acme/api#old", "github", touchedTestDay(3, 10), inRun)
	// A transition whose item has no stored row keeps its own repository.
	insertTouchedTestTransition(t, ctx, conn, touchedTestOrg, touchedTestRepoB, "gitlab:acme/web#ghost", "gitlab", touchedTestDay(3, 11), inRun)
	insertTouchedTestTransition(t, ctx, conn, touchedTestOrg, touchedTestRepoA, "gh:acme/api#1", "github", touchedTestDay(3, 12), beforeRun)
	want := []string{
		touchedTestKey(8, 1, touchedTestRepoA), touchedTestKey(8, 2, touchedTestRepoA),
		touchedTestKey(8, 3, touchedTestRepoA), touchedTestKey(8, 4, touchedTestRepoA),
		touchedTestKey(7, 1, touchedTestRepoB), touchedTestKey(8, 1, touchedTestRepoB),
		touchedTestKey(6, 1, uuid.Nil), touchedTestKey(6, 5, uuid.Nil), touchedTestKey(5, 1, uuid.Nil),
		touchedTestKey(3, 10, touchedTestRepoA), touchedTestKey(3, 11, touchedTestRepoB),
	}
	sort.Strings(want)
	return want
}

// The record holds every (day, repository) that a raw row of the run belongs
// to, for each provider shape and for the nil repository, and no key of a row
// written before the lower bound or of another organization.
func TestTouchedDaysRecordHoldsEveryDayOfTheRawRowsOfTheRun(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	want := seedTouchedTestRows(t, ctx, conn)

	recorded, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince)
	if err != nil {
		t.Fatal(err)
	}
	if recorded != uint64(len(want)) {
		t.Fatalf("recorded keys = %d, want %d", recorded, len(want))
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys\n got %v\nwant %v", got, want)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOtherOrg); len(got) != 0 {
		t.Fatalf("the other organization was not recorded, so it has no key; got %v", got)
	}
}

// A dispatched event ends a key only when it is not older than the touched
// event, a listed day ends only its listed keys, and a second record with no
// new raw row leaves nothing pending.
func TestTouchedDaysMarkEndsOnlyWhatWasDispatched(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	all := seedTouchedTestRows(t, ctx, conn)
	if _, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}

	bounded, err := store.PendingDays(ctx, touchedTestOrg, 3)
	if err != nil {
		t.Fatal(err)
	}
	wantNewest := []time.Time{utcDay(touchedTestDay(8, 4)), utcDay(touchedTestDay(8, 3)), utcDay(touchedTestDay(8, 2))}
	if !reflect.DeepEqual(bounded.Days, wantNewest) || !bounded.Truncated {
		t.Fatalf("bounded read = %v truncated=%v, want %v truncated", bounded.Days, bounded.Truncated, wantNewest)
	}
	first, err := store.PendingDays(ctx, touchedTestOrg, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Days) != 10 || first.Truncated {
		t.Fatalf("pending days = %d truncated=%v, want 10 days", len(first.Days), first.Truncated)
	}
	capped, err := store.PendingRepositories(ctx, touchedTestOrg, []time.Time{touchedTestDay(8, 1), touchedTestDay(6, 1)}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(capped["2026-08-01"]) != 1 || !reflect.DeepEqual(capped["2026-06-01"], []string{uuid.Nil.String()}) || len(capped) != 2 {
		t.Fatalf("capped repositories = %v", capped)
	}

	// A second record after the read: its events are newer than first.TakenAt.
	time.Sleep(20 * time.Millisecond)
	if _, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatched(ctx, touchedTestOrg, first.TakenAt, first.Days, nil); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, all) {
		t.Fatalf("a key touched after the read was ended by the mark\n got %v\nwant %v", got, all)
	}

	second, err := store.PendingDays(ctx, touchedTestOrg, 100)
	if err != nil {
		t.Fatal(err)
	}
	// 2026-08-01 is a listed day: only repository A is listed, B stays.
	// 2026-06-01 is a day of every repository.
	if err := store.MarkDispatched(ctx, touchedTestOrg, second.TakenAt,
		[]time.Time{touchedTestDay(6, 1)},
		[]TouchedDayKey{{Day: touchedTestDay(8, 1), RepositoryID: touchedTestRepoA.String()}},
	); err != nil {
		t.Fatal(err)
	}
	want := []string{}
	for _, key := range all {
		if key != touchedTestKey(8, 1, touchedTestRepoA) && key != touchedTestKey(6, 1, uuid.Nil) {
			want = append(want, key)
		}
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending keys after the mark\n got %v\nwant %v", got, want)
	}

	if err := store.MarkDispatched(ctx, touchedTestOrg, second.TakenAt, second.Days, nil); err != nil {
		t.Fatal(err)
	}
	recorded, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince.AddDate(1, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); recorded != 0 || len(got) != 0 {
		t.Fatalf("a record with no new raw row: recorded=%d pending=%v, want none", recorded, got)
	}
}

// The mark of listed keys carries the time of the read, as the mark of whole
// days does: a listed key that a later record touched again stays pending.
func TestTouchedDaysMarkOfListedKeysLeavesAKeyTouchedAfterTheRead(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	all := seedTouchedTestRows(t, ctx, conn)
	if _, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	read, err := store.PendingDays(ctx, touchedTestOrg, 100)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := store.RecordTouched(ctx, touchedTestOrg, touchedTestSince); err != nil {
		t.Fatal(err)
	}
	listed := []TouchedDayKey{
		{Day: touchedTestDay(8, 1), RepositoryID: touchedTestRepoA.String()},
		{Day: touchedTestDay(6, 1), RepositoryID: uuid.Nil.String()},
	}
	if err := store.MarkDispatched(ctx, touchedTestOrg, read.TakenAt, nil, listed); err != nil {
		t.Fatal(err)
	}
	if got := pendingTouchedTestKeys(t, ctx, conn, touchedTestOrg); !reflect.DeepEqual(got, all) {
		t.Fatalf("a listed key touched after the read was ended by the mark\n got %v\nwant %v", got, all)
	}
}

// A transition is of the run when it was written at or after the lower bound
// of the read, in the organization of the run: a transition written exactly
// at the bound is recorded, one written a millisecond earlier is not, and a
// transition of another organization is not.
func TestTouchedDaysRecordTakesATransitionWrittenAtTheBound(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	const org, otherOrg = "touched-days-transition-bound", "touched-days-transition-other"
	insertTouchedTestTransition(t, ctx, conn, org, touchedTestRepoA, "gh:acme/api#1", "github", touchedTestDay(3, 20), touchedTestSince)
	insertTouchedTestTransition(t, ctx, conn, org, touchedTestRepoA, "gh:acme/api#2", "github", touchedTestDay(3, 21), touchedTestSince.Add(-time.Millisecond))
	insertTouchedTestTransition(t, ctx, conn, otherOrg, touchedTestRepoA, "gh:other/api#1", "github", touchedTestDay(3, 22), touchedTestSince.Add(time.Hour))

	recorded, err := store.RecordTouched(ctx, org, touchedTestSince)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{touchedTestKey(3, 20, touchedTestRepoA)}
	if got := pendingTouchedTestKeys(t, ctx, conn, org); recorded != 1 || !reflect.DeepEqual(got, want) {
		t.Fatalf("recorded=%d pending=%v, want the one transition written at the bound: %v", recorded, got, want)
	}
}

// A key whose touched event carries exactly the time of the read is ended by
// the mark of that read: the run the fan-out started computes the day after
// the raw rows of that event were stored. Only an event newer than the read
// keeps the key pending.
func TestTouchedDaysMarkEndsAKeyTouchedAtTheTimeOfTheRead(t *testing.T) {
	ctx, conn := newReadbackIntegrationConn(t)
	store, err := NewClickHouseTouchedDaysStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	const org = "touched-days-tie"
	read, err := store.PendingDays(ctx, org, 10)
	if err != nil {
		t.Fatal(err)
	}
	day := utcDay(touchedTestDay(3, 25))
	// The times go in as milliseconds: a bound time.Time loses them.
	for repo, at := range map[uuid.UUID]time.Time{
		touchedTestRepoA: read.TakenAt, touchedTestRepoB: read.TakenAt.Add(time.Millisecond),
	} {
		if err := conn.Exec(ctx, `
INSERT INTO daily_metrics_touched_days (org_id, day, repo_id, kind, at)
SELECT ?, toDate(?), toUUID(?), 'touched', fromUnixTimestamp64Milli(toInt64(?), 'UTC')`,
			org, day.Format("2006-01-02"), repo.String(), at.UnixMilli()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.MarkDispatched(ctx, org, read.TakenAt, []time.Time{day}, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{touchedTestKey(3, 25, touchedTestRepoB)}
	if got := pendingTouchedTestKeys(t, ctx, conn, org); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending after the mark = %v, want only the key touched after the read: %v", got, want)
	}
}
