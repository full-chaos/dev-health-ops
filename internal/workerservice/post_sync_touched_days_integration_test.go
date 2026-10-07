//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

const touchedRigRouteGeneration = 3

type touchedStubRemaining struct{}

func (touchedStubRemaining) StartRunTx(context.Context, pgx.Tx, string, syncdispatchruntime.PostSyncPlan, string) (string, error) {
	return "", nil
}

type touchedStubWorkGraph struct{}

func (touchedStubWorkGraph) StartRequestTx(context.Context, pgx.Tx, string, syncdispatchruntime.PostSyncPlan, string) (string, error) {
	return "", nil
}

// touchedStubPublish is the last writer of the fan-out transaction. With err
// set it fails after the daily runs were staged, so the transaction rolls back.
type touchedStubPublish struct{ err error }

func (stub touchedStubPublish) PublishTx(context.Context, pgx.Tx, syncdispatchruntime.PostSyncPlan) error {
	return stub.err
}

// touchedFaultStore is the real ClickHouse store with one step made to fail.
type touchedFaultStore struct {
	syncdispatchruntime.TouchedDaysStore
	failRecord bool
	failMark   bool
}

func (store touchedFaultStore) RecordTouched(ctx context.Context, org string, since time.Time) (uint64, error) {
	if store.failRecord {
		return 0, syncdispatchruntime.ErrTouchedDaysUnavailable
	}
	return store.TouchedDaysStore.RecordTouched(ctx, org, since)
}

func (store touchedFaultStore) MarkDispatched(ctx context.Context, org string, at time.Time, days []time.Time, keys []syncdispatchruntime.TouchedDayKey) error {
	if store.failMark {
		return syncdispatchruntime.ErrTouchedDaysUnavailable
	}
	return store.TouchedDaysStore.MarkDispatched(ctx, org, at, days, keys)
}

type touchedRig struct {
	*nilPartitionRig
	touched *syncdispatchruntime.ClickHouseTouchedDaysStore
}

func newTouchedRig(t *testing.T, ctx context.Context) *touchedRig {
	t.Helper()
	rig := newNilPartitionRig(t, ctx)
	touched, err := syncdispatchruntime.NewClickHouseTouchedDaysStore(rig.conn)
	if err != nil {
		t.Fatal(err)
	}
	if got := pgseed.SyncTransportRoute(ctx, t, rig.pool, "post_sync", "river", touchedRigRouteGeneration, false, "celery"); got != touchedRigRouteGeneration {
		t.Fatalf("post_sync route generation = %d, want %d", got, touchedRigRouteGeneration)
	}
	return &touchedRig{nilPartitionRig: rig, touched: touched}
}

// service builds the fan-out with the production daily writer and the given
// touched-day store; lastWriterErr fails the last writer of the transaction.
func (rig *touchedRig) service(t *testing.T, store syncdispatchruntime.TouchedDaysStore, lastWriterErr error) *syncdispatchruntime.NativePostSyncService {
	t.Helper()
	writer := dailyPostSyncWriter{store: rig.store, publisher: nilPartitionPublisher{}}
	service, err := syncdispatchruntime.NewNativePostSyncService(
		rig.pool, writer, touchedStubRemaining{}, touchedStubWorkGraph{},
		touchedStubPublish{}, touchedStubPublish{err: lastWriterErr}, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetTouchedDays(store, writer); err != nil {
		t.Fatal(err)
	}
	return service
}

// seedSync seeds one finished sync run with one successful unit of dataset
// over [day, day] and returns the arguments of its post_sync job.
func (rig *touchedRig) seedSync(t *testing.T, ctx context.Context, orgID, dataset string, day time.Time) syncdispatchruntime.PostSyncArgs {
	t.Helper()
	runID, outboxID, integrationID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	pgseed.EnsureSyncRun(ctx, t, rig.pool, pgseed.SyncRun{ID: runID, OrgID: orgID, IntegrationID: integrationID})
	pgseed.SyncDispatchOutbox(ctx, t, rig.pool, outboxID, runID, orgID, "post_sync", "dispatched", "river", touchedRigRouteGeneration)
	pgseed.InsertSyncRunUnit(ctx, t, rig.pool, pgseed.SyncRunUnit{
		ID: uuid.NewString(), RunID: runID, OrgID: orgID, IntegrationID: integrationID, SourceID: uuid.NewString(),
		DatasetKey: dataset, Status: "success", SinceAt: &day, BeforeAt: &day,
	})
	return syncdispatchruntime.PostSyncArgs{TransportArgs: syncdispatchruntime.TransportArgs{
		Version: syncdispatchruntime.ContractVersionV1, OrgID: orgID, RunID: runID,
		DispatchOutbox: outboxID, RouteGeneration: touchedRigRouteGeneration,
	}}
}

func (rig *touchedRig) setRunStart(t *testing.T, ctx context.Context, args syncdispatchruntime.PostSyncArgs, startedAt time.Time) {
	t.Helper()
	if _, err := rig.pool.Exec(ctx, `UPDATE sync_runs SET started_at = $2 WHERE id = $1::uuid`, args.SyncRunID(), startedAt); err != nil {
		t.Fatal(err)
	}
}

type touchedRun struct {
	id      string
	fullOrg bool
	repos   string
}

// runsOf returns the daily runs of the generation of one sync run, by day.
func (rig *touchedRig) runsOf(t *testing.T, ctx context.Context, orgID string, args syncdispatchruntime.PostSyncArgs) map[string]touchedRun {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `
SELECT run.id::text, run.target_day::text, run.full_org,
       COALESCE((SELECT string_agg(repo, ',' ORDER BY repo)
                 FROM public.daily_metrics_partitions AS part, json_array_elements_text(part.repo_ids) AS repo
                 WHERE part.run_id = run.id), '')
FROM public.daily_metrics_runs AS run
WHERE run.org_id = $1::uuid AND run.generation = $2`, orgID, "post-sync:"+args.SyncRunID())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	runs := map[string]touchedRun{}
	for rows.Next() {
		var run touchedRun
		var day string
		if err := rows.Scan(&run.id, &day, &run.fullOrg, &run.repos); err != nil {
			t.Fatal(err)
		}
		runs[day] = run
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return runs
}

func touchedRunDays(runs map[string]touchedRun) []string {
	days := make([]string, 0, len(runs))
	for day := range runs {
		days = append(days, day)
	}
	sort.Strings(days)
	return days
}

func (rig *touchedRig) pendingDays(t *testing.T, ctx context.Context, orgID string) []string {
	t.Helper()
	pending, err := rig.touched.PendingDays(ctx, orgID, 5000)
	if err != nil {
		t.Fatal(err)
	}
	days := make([]string, 0, len(pending.Days))
	for _, day := range pending.Days {
		days = append(days, day.Format("2006-01-02"))
	}
	sort.Strings(days)
	return days
}

type touchedItem struct {
	repo      uuid.UUID
	id        string
	provider  string
	day       time.Time
	completed bool
	synced    time.Time
}

func insertTouchedItems(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, items ...touchedItem) {
	t.Helper()
	batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
		repo_id, work_item_id, provider, type, status, created_at, completed_at, org_id, last_synced)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		var completed *time.Time
		status := "todo"
		if item.completed {
			value := item.day.Add(10 * time.Hour)
			completed, status = &value, "done"
		}
		if err := batch.Append(item.repo, item.id, item.provider, "issue", status, item.day.Add(9*time.Hour), completed, orgID, item.synced); err != nil {
			t.Fatal(err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func insertTouchedRepo(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, repo uuid.UUID, name, provider string) {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, ?, ?, ?, ?)`,
		repo, orgID, name, provider, at, at); err != nil {
		t.Fatal(err)
	}
}

// derivedDaySnapshot is the newest row of every key of the work-item daily
// tables for one day, without computed_at: a count and a hash for each table.
func derivedDaySnapshot(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) map[string]string {
	t.Helper()
	sources := map[string]string{
		"work_item_metrics_daily":         `work_item_metrics_daily FINAL WHERE org_id = ? AND day = ?`,
		"work_item_user_metrics_daily":    `work_item_user_metrics_daily FINAL WHERE org_id = ? AND day = ?`,
		"work_item_state_durations_daily": `work_item_state_durations_daily FINAL WHERE org_id = ? AND day = ?`,
		"estimate_coverage_metrics_daily": `estimate_coverage_metrics_daily FINAL WHERE org_id = ? AND day = ?`,
		"issue_type_metrics_daily": `(SELECT * FROM issue_type_metrics_daily WHERE org_id = ? AND day = ?
			ORDER BY computed_at DESC LIMIT 1 BY repo_id, provider, team_id, issue_type_norm)`,
		"investment_metrics_daily": `(SELECT * FROM investment_metrics_daily WHERE org_id = ? AND day = ?
			ORDER BY computed_at DESC LIMIT 1 BY repo_id, team_id, investment_area, project_stream)`,
	}
	snapshot := map[string]string{}
	for table, source := range sources {
		var count, hash uint64
		if err := conn.QueryRow(ctx, `SELECT count(), groupBitXor(cityHash64(* EXCEPT (computed_at))) FROM `+source, orgID, day).Scan(&count, &hash); err != nil {
			t.Fatalf("snapshot %s: %v", table, err)
		}
		snapshot[table] = fmt.Sprintf("%d rows, hash %d", count, hash)
	}
	return snapshot
}

// storedVersions counts every stored row version of one day in the two tables
// that keep each version until a merge.
func storedVersions(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) [2]uint64 {
	t.Helper()
	var versions [2]uint64
	for index, table := range []string{"work_item_metrics_daily", "issue_type_metrics_daily"} {
		if err := conn.QueryRow(ctx, `SELECT count() FROM `+table+` WHERE org_id = ? AND day = ?`, orgID, day).Scan(&versions[index]); err != nil {
			t.Fatal(err)
		}
	}
	return versions
}

func (rig *touchedRig) fullRecompute(t *testing.T, ctx context.Context, orgID string, day time.Time) {
	t.Helper()
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
		}, nilPartitionPublisher{})
	})
	rig.dispatchAndRun(t, ctx, run)
}

// A sync whose unit window is one day writes raw rows of two older days. After
// the fan-out and the daily runs it started, the derived rows of both days
// equal a recompute of every repository, and a day that no raw row touched
// gets no new row version.
func TestPostSyncFanoutRecomputesEveryDayTheRawRowsTouched(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	near, far, untouched := target.AddDate(0, 0, -3), target.AddDate(0, 0, -40), target.AddDate(0, 0, -20)
	repoA, repoB := uuid.New(), uuid.New()
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoA, "acme/api", "github")
	insertTouchedRepo(t, ctx, rig.conn, orgID, repoB, "acme/web", "gitlab")
	earlier := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	insertTouchedItems(t, ctx, rig.conn, orgID,
		touchedItem{repo: repoA, id: "gh:acme/api#1", provider: "github", day: near, completed: true, synced: earlier},
		touchedItem{repo: repoA, id: "gh:acme/api#2", provider: "github", day: far, completed: true, synced: earlier},
		touchedItem{repo: uuid.Nil, id: "linear:OPS-1", provider: "linear", day: far, completed: true, synced: earlier},
		touchedItem{repo: repoB, id: "gitlab:acme/web#1", provider: "gitlab", day: untouched, completed: true, synced: earlier},
	)
	for _, day := range []time.Time{near, far, untouched} {
		rig.fullRecompute(t, ctx, orgID, day)
	}
	untouchedBefore := storedVersions(t, ctx, rig.conn, orgID, untouched)
	staleFar := derivedDaySnapshot(t, ctx, rig.conn, orgID, far)

	// The sync: its unit covers only the target day, its rows belong to the
	// two older days. One row for each provider shape.
	now := time.Now().UTC()
	insertTouchedItems(t, ctx, rig.conn, orgID,
		touchedItem{repo: repoA, id: "gh:acme/api#3", provider: "github", day: near, completed: true, synced: now},
		touchedItem{repo: uuid.Nil, id: "jira:OPS-2", provider: "jira", day: far, completed: true, synced: now},
		touchedItem{repo: uuid.Nil, id: "linear:OPS-3", provider: "linear", day: far, completed: true, synced: now},
		touchedItem{repo: repoB, id: "gitlab:acme/web#9", provider: "gitlab", day: far, completed: true, synced: now},
		// A row of the target day: the window run computes it, so the fan-out
		// starts no second run for the day and ends its key.
		touchedItem{repo: repoA, id: "gh:acme/api#4", provider: "github", day: target, completed: true, synced: now},
	)
	args := rig.seedSync(t, ctx, orgID, "work-items", target)
	service := rig.service(t, rig.touched, nil)
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	runs := rig.runsOf(t, ctx, orgID, args)
	nearKey, farKey, targetKey := near.Format("2006-01-02"), far.Format("2006-01-02"), target.Format("2006-01-02")
	if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{farKey, nearKey, targetKey}) {
		t.Fatalf("days with a run = %v, want the target day and the two touched days", got)
	}
	if !runs[targetKey].fullOrg || runs[nearKey].fullOrg || runs[farKey].fullOrg {
		t.Fatalf("runs = %+v: the target day is a run of every repository, a touched day a run of its repositories", runs)
	}
	wantFar := []string{uuid.Nil.String(), repoB.String()}
	sort.Strings(wantFar)
	if runs[nearKey].repos != repoA.String() || runs[farKey].repos != strings.Join(wantFar, ",") {
		t.Fatalf("repositories of the touched days = %q and %q", runs[nearKey].repos, runs[farKey].repos)
	}
	if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
		t.Fatalf("pending days after the fan-out = %v, want none", got)
	}
	for _, run := range runs {
		rig.dispatchAndRun(t, ctx, daily.Run{ID: run.id, OrganizationID: orgID})
	}

	if got := completedByProvider(t, ctx, rig.conn, orgID, far); !reflect.DeepEqual(got, map[string]uint64{"github": 1, "linear": 2, "jira": 1, "gitlab": 1}) {
		t.Fatalf("items completed on the far day = %v", got)
	}
	if got := completedByProvider(t, ctx, rig.conn, orgID, near); !reflect.DeepEqual(got, map[string]uint64{"github": 2}) {
		t.Fatalf("items completed on the near day = %v", got)
	}
	afterFanout := map[string]map[string]string{
		nearKey: derivedDaySnapshot(t, ctx, rig.conn, orgID, near),
		farKey:  derivedDaySnapshot(t, ctx, rig.conn, orgID, far),
	}
	if reflect.DeepEqual(afterFanout[farKey], staleFar) {
		t.Fatalf("the far day still holds the rows of before the sync: %v", staleFar)
	}
	if got := storedVersions(t, ctx, rig.conn, orgID, untouched); got != untouchedBefore || got[0] == 0 || got[1] == 0 {
		t.Fatalf("row versions of the untouched day = %v, before the sync %v: want the same, not zero", got, untouchedBefore)
	}
	for _, day := range []time.Time{near, far} {
		rig.fullRecompute(t, ctx, orgID, day)
		key := day.Format("2006-01-02")
		if got := derivedDaySnapshot(t, ctx, rig.conn, orgID, day); !reflect.DeepEqual(got, afterFanout[key]) {
			t.Fatalf("day %s after the fan-out\n got %v\nfull recompute %v", key, afterFanout[key], got)
		}
	}

	// A later sync with no new raw row starts its window run and nothing else.
	later := rig.seedSync(t, ctx, orgID, "work-items", target)
	rig.setRunStart(t, ctx, later, now.Add(time.Hour))
	if err := service.Fanout(ctx, later); err != nil {
		t.Fatal(err)
	}
	if got := touchedRunDays(rig.runsOf(t, ctx, orgID, later)); !reflect.DeepEqual(got, []string{targetKey}) {
		t.Fatalf("a fan-out with no new raw row started runs for %v, want only %s", got, targetKey)
	}
}

// One hundred touched days: a fan-out takes the 31 newest, the other 69 stay
// pending, and each later fan-out of the organization takes 31 more, also a
// fan-out of a sync that wrote no work item.
func TestPostSyncFanoutTakesTheNewestTouchedDaysAndCarriesTheRestOver(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	var items []touchedItem
	var days []string
	for offset := 1; offset <= 100; offset++ {
		day := target.AddDate(0, 0, -20-offset)
		days = append(days, day.Format("2006-01-02"))
		items = append(items, touchedItem{repo: uuid.Nil, id: fmt.Sprintf("linear:OPS-%d", offset), provider: "linear", day: day, synced: now})
	}
	sort.Strings(days) // oldest first
	// A row of the target day is of the window run: it takes none of the 31.
	items = append(items, touchedItem{repo: uuid.Nil, id: "linear:OPS-target", provider: "linear", day: target, synced: now})
	insertTouchedItems(t, ctx, rig.conn, orgID, items...)
	service := rig.service(t, rig.touched, nil)
	targetKey := target.Format("2006-01-02")

	first := rig.seedSync(t, ctx, orgID, "work-items", target)
	if err := service.Fanout(ctx, first); err != nil {
		t.Fatal(err)
	}
	wantFirst := append(append([]string{}, days[69:]...), targetKey)
	if got := touchedRunDays(rig.runsOf(t, ctx, orgID, first)); !reflect.DeepEqual(got, wantFirst) {
		t.Fatalf("first fan-out started %d runs %v, want the 31 newest touched days and the target day", len(got), got)
	}
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days[:69]) {
		t.Fatalf("pending after the first fan-out = %d days, want the 69 oldest", len(got))
	}

	// The second sync has no work-items unit, so its fan-out records nothing:
	// a raw row written during it belongs to the record of the sync that
	// wrote it, and its day gets no run here.
	notRecorded := target.AddDate(0, 0, -300)
	insertTouchedItems(t, ctx, rig.conn, orgID,
		touchedItem{repo: uuid.Nil, id: "linear:OPS-late", provider: "linear", day: notRecorded, synced: now.Add(time.Hour + time.Minute)})
	second := rig.seedSync(t, ctx, orgID, "commits", target)
	rig.setRunStart(t, ctx, second, now.Add(time.Hour))
	if err := service.Fanout(ctx, second); err != nil {
		t.Fatal(err)
	}
	wantSecond := append(append([]string{}, days[38:69]...), targetKey)
	if got := touchedRunDays(rig.runsOf(t, ctx, orgID, second)); !reflect.DeepEqual(got, wantSecond) {
		t.Fatalf("second fan-out started %d runs %v, want the next 31 days and the target day", len(got), got)
	}
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, days[:38]) {
		t.Fatalf("pending after the second fan-out = %d days, want the 38 oldest", len(got))
	}
}

// Every failure of the fan-out leaves the touched day pending or its run
// started: no failure point loses a day.
func TestPostSyncFanoutTouchedDaySurvivesEveryFailurePoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	touched := target.AddDate(0, 0, -40)
	touchedKey, targetKey := touched.Format("2006-01-02"), target.Format("2006-01-02")
	healthy := rig.service(t, rig.touched, nil)
	seed := func(t *testing.T) (string, syncdispatchruntime.PostSyncArgs) {
		orgID := uuid.NewString()
		insertTouchedItems(t, ctx, rig.conn, orgID,
			touchedItem{repo: uuid.Nil, id: "linear:OPS-1", provider: "linear", day: touched, synced: time.Now().UTC()})
		return orgID, rig.seedSync(t, ctx, orgID, "work-items", target)
	}
	both := []string{touchedKey, targetKey}

	t.Run("the record fails", func(t *testing.T) {
		orgID, args := seed(t)
		failing := rig.service(t, touchedFaultStore{TouchedDaysStore: rig.touched, failRecord: true}, nil)
		if err := failing.Fanout(ctx, args); err == nil {
			t.Fatal("a fan-out whose record failed returned no error: the day would be read as not touched")
		}
		if got := rig.runsOf(t, ctx, orgID, args); len(got) != 0 {
			t.Fatalf("runs after the failed record = %v, want none", got)
		}
		if err := healthy.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, args)); !reflect.DeepEqual(got, both) {
			t.Fatalf("runs after the next delivery = %v, want %v", got, both)
		}
	})

	t.Run("the transaction fails after the run start", func(t *testing.T) {
		orgID, args := seed(t)
		failing := rig.service(t, rig.touched, errors.New("the last writer failed"))
		if err := failing.Fanout(ctx, args); err == nil {
			t.Fatal("want the error of the last writer")
		}
		if got := rig.runsOf(t, ctx, orgID, args); len(got) != 0 {
			t.Fatalf("runs after the rollback = %v, want none", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{touchedKey}) {
			t.Fatalf("pending after the rollback = %v, want %s", got, touchedKey)
		}
		if err := healthy.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, args)); !reflect.DeepEqual(got, both) {
			t.Fatalf("runs after the next delivery = %v, want %v", got, both)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the next delivery = %v, want none", got)
		}
	})

	t.Run("the mark fails after the commit", func(t *testing.T) {
		orgID, args := seed(t)
		failing := rig.service(t, touchedFaultStore{TouchedDaysStore: rig.touched, failMark: true}, nil)
		if err := failing.Fanout(ctx, args); err != nil {
			t.Fatalf("a failed mark is not a failed fan-out (the runs are committed): %v", err)
		}
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, args)); !reflect.DeepEqual(got, both) {
			t.Fatalf("runs = %v, want %v", got, both)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{touchedKey}) {
			t.Fatalf("pending after the failed mark = %v, want %s", got, touchedKey)
		}
		next := rig.seedSync(t, ctx, orgID, "work-items", target)
		rig.setRunStart(t, ctx, next, time.Now().UTC().Add(time.Hour))
		if err := healthy.Fanout(ctx, next); err != nil {
			t.Fatal(err)
		}
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, next)); !reflect.DeepEqual(got, both) {
			t.Fatalf("runs of the next sync = %v, want one more run of %v", got, both)
		}
		if got := rig.pendingDays(t, ctx, orgID); len(got) != 0 {
			t.Fatalf("pending after the next sync = %v, want none", got)
		}
	})

	// A second delivery of one fan-out reads another repository list for a
	// day whose run it started already. The run stays as it is, the delivery
	// succeeds, and the day stays pending for the next sync.
	t.Run("a second delivery reads another repository list", func(t *testing.T) {
		orgID, args := seed(t)
		failing := rig.service(t, touchedFaultStore{TouchedDaysStore: rig.touched, failMark: true}, nil)
		if err := failing.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		repoB := uuid.New()
		insertTouchedItems(t, ctx, rig.conn, orgID,
			touchedItem{repo: repoB, id: "gitlab:acme/web#9", provider: "gitlab", day: touched, synced: time.Now().UTC()})
		if err := healthy.Fanout(ctx, args); err != nil {
			t.Fatalf("the second delivery failed: %v", err)
		}
		if got := rig.runsOf(t, ctx, orgID, args)[touchedKey].repos; got != uuid.Nil.String() {
			t.Fatalf("repositories of the started run = %q, want the first list unchanged", got)
		}
		if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{touchedKey}) {
			t.Fatalf("pending after the second delivery = %v, want %s", got, touchedKey)
		}
		next := rig.seedSync(t, ctx, orgID, "work-items", target)
		rig.setRunStart(t, ctx, next, time.Now().UTC().Add(time.Hour))
		if err := healthy.Fanout(ctx, next); err != nil {
			t.Fatal(err)
		}
		want := []string{uuid.Nil.String(), repoB.String()}
		sort.Strings(want)
		if got := rig.runsOf(t, ctx, orgID, next)[touchedKey].repos; got != strings.Join(want, ",") {
			t.Fatalf("repositories of the run of the next sync = %q, want %v", got, want)
		}
	})

	// The read starts at the start of the sync run minus the clock margin: a
	// row written exactly there is of the run, a row one millisecond earlier
	// is not.
	t.Run("the clock margin", func(t *testing.T) {
		orgID := uuid.NewString()
		start := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Millisecond)
		inside, outside := target.AddDate(0, 0, -50), target.AddDate(0, 0, -60)
		insertTouchedItems(t, ctx, rig.conn, orgID,
			touchedItem{repo: uuid.Nil, id: "linear:OPS-1", provider: "linear", day: inside, synced: start.Add(-5 * time.Minute)},
			touchedItem{repo: uuid.Nil, id: "linear:OPS-2", provider: "linear", day: outside, synced: start.Add(-5*time.Minute - time.Millisecond)})
		args := rig.seedSync(t, ctx, orgID, "work-items", target)
		rig.setRunStart(t, ctx, args, start)
		if err := healthy.Fanout(ctx, args); err != nil {
			t.Fatal(err)
		}
		want := []string{inside.Format("2006-01-02"), targetKey}
		if got := touchedRunDays(rig.runsOf(t, ctx, orgID, args)); !reflect.DeepEqual(got, want) {
			t.Fatalf("runs = %v, want %v", got, want)
		}
	})
}

// touchedCountObserver collects the touched-day counters of one fan-out.
type touchedCountObserver struct {
	counts map[jobruntime.PostSyncTouchedDaysEvent]uint64
}

func (observer *touchedCountObserver) ObservePostSyncTouchedDays(event jobruntime.PostSyncTouchedDaysEvent, count uint64) error {
	if observer.counts == nil {
		observer.counts = map[jobruntime.PostSyncTouchedDaysEvent]uint64{}
	}
	observer.counts[event] += count
	return nil
}

// A run accepts a bounded repository list. A day with exactly that many
// touched repositories gets a run of them. A day with one more has no run
// that the daily job accepts (a run of every repository is refused above the
// same cap), so the fan-out starts none, leaves the day pending and reports
// it: marking it dispatched would lose the day without a word.
func TestPostSyncFanoutLeavesADayOverTheRepositoryLimitPendingAndReportsIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	rig := newTouchedRig(t, ctx)
	orgID := uuid.NewString()
	target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
	atLimit, overLimit := target.AddDate(0, 0, -40), target.AddDate(0, 0, -41)
	now := time.Now().UTC()
	var items []touchedItem
	for index := 0; index < daily.MaxRepositoriesPerRun+1; index++ {
		repo := uuid.New()
		if index < daily.MaxRepositoriesPerRun {
			items = append(items, touchedItem{repo: repo, id: fmt.Sprintf("gh:acme/at#%d", index), provider: "github", day: atLimit, synced: now})
		}
		items = append(items, touchedItem{repo: repo, id: fmt.Sprintf("gh:acme/over#%d", index), provider: "github", day: overLimit, synced: now})
	}
	insertTouchedItems(t, ctx, rig.conn, orgID, items...)
	args := rig.seedSync(t, ctx, orgID, "work-items", target)
	observer := &touchedCountObserver{}
	service := rig.service(t, rig.touched, nil)
	service.SetTouchedDaysObserver(observer)
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	runs := rig.runsOf(t, ctx, orgID, args)
	atKey, overKey := atLimit.Format("2006-01-02"), overLimit.Format("2006-01-02")
	if got := touchedRunDays(runs); !reflect.DeepEqual(got, []string{atKey, target.Format("2006-01-02")}) {
		t.Fatalf("days with a run = %v, want the day at the limit and the target day only", got)
	}
	if listed := len(strings.Split(runs[atKey].repos, ",")); runs[atKey].fullOrg || listed != daily.MaxRepositoriesPerRun {
		t.Fatalf("the day at the limit: run of every repository = %v with %d listed repositories; want a run of its %d repositories",
			runs[atKey].fullOrg, listed, daily.MaxRepositoriesPerRun)
	}
	if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, []string{overKey}) {
		t.Fatalf("pending days after the fan-out = %v, want only the day over the limit %s", got, overKey)
	}
	if got := observer.counts[jobruntime.PostSyncTouchedDaysOverRepositoryLimit]; got != 1 {
		t.Fatalf("over_repository_limit counter = %d, want 1 (counts %v)", got, observer.counts)
	}
}

// touchedLimitWriter lowers the repository limit of the touched-day writer to
// limit: the smallest seam to put days over the limit with a few repositories.
type touchedLimitWriter struct {
	dailyPostSyncWriter
	limit int
}

func (writer touchedLimitWriter) RepositoryLimit() int { return writer.limit }

// overLimitFanout runs one fan-out with a repository limit of 1 and returns
// its Error lines of phase over_repository_limit and its counter.
func overLimitFanout(
	t *testing.T, ctx context.Context, rig *touchedRig, orgID string, args syncdispatchruntime.PostSyncArgs,
) (errorLines []string, counted uint64) {
	t.Helper()
	var logs bytes.Buffer
	writer := dailyPostSyncWriter{store: rig.store, publisher: nilPartitionPublisher{}}
	service, err := syncdispatchruntime.NewNativePostSyncService(
		rig.pool, writer, touchedStubRemaining{}, touchedStubWorkGraph{},
		touchedStubPublish{}, touchedStubPublish{}, synclog.New(slog.New(slog.NewJSONHandler(&logs, nil))),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetTouchedDays(rig.touched, touchedLimitWriter{dailyPostSyncWriter: writer, limit: 1}); err != nil {
		t.Fatal(err)
	}
	observer := &touchedCountObserver{}
	service.SetTouchedDaysObserver(observer)
	if err := service.Fanout(ctx, args); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if strings.Contains(line, `"phase":"over_repository_limit"`) {
			errorLines = append(errorLines, line)
		}
	}
	return errorLines, observer.counts[jobruntime.PostSyncTouchedDaysOverRepositoryLimit]
}

// A day over the repository limit starts no run and stays pending, but it must
// not hold one of the 31 slots: the days behind it still get their runs in the
// same fan-out, and the report is one Error line with the count of such days.
func TestPostSyncFanoutOverLimitDaysNeverHoldTheSlotsOfStartableDays(t *testing.T) {
	for _, test := range []struct {
		name         string
		over, normal int
	}{
		{"31 over-limit days newer than one normal day", 31, 1},
		{"40 over-limit days newer than 31 normal days", 40, 31},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
			defer cancel()
			rig := newTouchedRig(t, ctx)
			orgID := uuid.NewString()
			target := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC)
			now := time.Now().UTC()
			var items []touchedItem
			var overDays, normalDays []string
			for index := 0; index < test.over; index++ {
				day := target.AddDate(0, 0, -(40 + index))
				overDays = append(overDays, day.Format("2006-01-02"))
				for repoIndex := 0; repoIndex < 2; repoIndex++ {
					items = append(items, touchedItem{repo: uuid.New(), id: fmt.Sprintf("gh:acme/over#%d-%d", index, repoIndex), provider: "github", day: day, synced: now})
				}
			}
			for index := 0; index < test.normal; index++ {
				day := target.AddDate(0, 0, -(100 + index))
				normalDays = append(normalDays, day.Format("2006-01-02"))
				items = append(items, touchedItem{repo: uuid.New(), id: fmt.Sprintf("gh:acme/normal#%d", index), provider: "github", day: day, synced: now})
			}
			insertTouchedItems(t, ctx, rig.conn, orgID, items...)
			args := rig.seedSync(t, ctx, orgID, "work-items", target)
			errorLines, counted := overLimitFanout(t, ctx, rig, orgID, args)

			runs := rig.runsOf(t, ctx, orgID, args)
			for _, day := range normalDays {
				if _, ok := runs[day]; !ok {
					t.Fatalf("the normal day %s got no run in the first fan-out (days with a run: %v)", day, touchedRunDays(runs))
				}
			}
			for _, day := range overLimitOnly(overDays, runs) {
				t.Fatalf("the over-limit day %s got a run", day)
			}
			sort.Strings(overDays)
			if got := rig.pendingDays(t, ctx, orgID); !reflect.DeepEqual(got, overDays) {
				t.Fatalf("pending after the fan-out = %v, want the %d over-limit days", got, test.over)
			}
			if len(errorLines) != 1 {
				t.Fatalf("over_repository_limit Error lines = %d, want exactly 1", len(errorLines))
			}
			if counted != uint64(test.over) {
				t.Fatalf("over_repository_limit counter = %d, want %d", counted, test.over)
			}
		})
	}
}

func overLimitOnly(overDays []string, runs map[string]touchedRun) []string {
	var started []string
	for _, day := range overDays {
		if _, ok := runs[day]; ok {
			started = append(started, day)
		}
	}
	return started
}
