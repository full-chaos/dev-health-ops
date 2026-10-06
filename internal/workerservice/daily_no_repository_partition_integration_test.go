//go:build integration

package workerservice

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// CHAOS-8821. A work item with no repository (jira, linear; any ingest
// provider that is neither github nor gitlab) is stored under the nil
// repository id. The daily job computes only the repositories its discoverer
// returns, so before this change no daily family ever read those items.
//
// This test drives the REAL job path with the REAL family registry
// (dailyNativeFamilyRegistrations), a real PostgresStore, a real
// ClickHouseRepositoryDiscoverer and the real zero-row source checker:
// start run -> Dispatcher.Work (discovery + materialize) -> PartitionHandler.Work.

type nilPartitionFamilyRecorder struct {
	mu       sync.Mutex
	computed map[string]int
	other    map[string]jobruntime.DailyMetricsNativeFamilyOutcome
}

func (recorder *nilPartitionFamilyRecorder) ObserveDailyMetricsNativeFamily(
	family string, outcome jobruntime.DailyMetricsNativeFamilyOutcome, _ int, _ time.Duration,
) error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if outcome == jobruntime.DailyMetricsNativeFamilyOutcomeComputed {
		recorder.computed[family]++
		return nil
	}
	recorder.other[family] = outcome
	return nil
}

type nilPartitionPublisher struct{}

func (nilPartitionPublisher) PublishDispatchTx(context.Context, pgx.Tx, daily.Run, string) error {
	return nil
}
func (nilPartitionPublisher) PublishPartition(context.Context, daily.Run, daily.Partition) error {
	return nil
}
func (nilPartitionPublisher) PublishFinalizeTx(context.Context, pgx.Tx, daily.Run) error { return nil }

func nilPartitionExecution[T any](orgID, domainType, id string) jobruntime.EnvelopeArgs[T] {
	return jobruntime.EnvelopeArgs[T]{OrganizationID: &orgID, Domain: jobcontract.DomainLink{Type: domainType, ID: id}}
}

func openNilPartitionClickHouse(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	chschema.Apply(ctx, t, instance)
	dsn, err := containers.ClickHouseHTTPDSN(ctx, instance)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	password, _ := parsed.User.Password()
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
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func seedNilPartitionOrg(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time, withNilItems bool) {
	t.Helper()
	completed := day.Add(10 * time.Hour)
	insert := func(repo uuid.UUID, id, provider string) {
		t.Helper()
		batch, err := conn.PrepareBatch(ctx, `INSERT INTO work_items (
			repo_id, work_item_id, provider, type, status, created_at, completed_at, org_id, last_synced)`)
		if err != nil {
			t.Fatal(err)
		}
		if err := batch.Append(repo, id, provider, "issue", "done", day.AddDate(0, 0, -2), &completed, orgID, day); err != nil {
			t.Fatal(err)
		}
		if err := batch.Send(); err != nil {
			t.Fatal(err)
		}
	}
	repoID := uuid.New()
	insert(repoID, "gh:acme/api#1", "github")
	if withNilItems {
		insert(uuid.Nil, "linear:OPS-1", "linear")
		insert(uuid.Nil, "jira:OPS-2", "jira")
	}
	if err := conn.Exec(ctx, `INSERT INTO repos (id, org_id, repo, provider, created_at, last_synced) VALUES (?, ?, 'acme/api', 'github', ?, ?)`,
		repoID, orgID, day, day); err != nil {
		t.Fatal(err)
	}
}

func completedByProvider(t *testing.T, ctx context.Context, conn driver.Conn, orgID string, day time.Time) map[string]uint64 {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT provider, sum(items_completed) FROM work_item_metrics_daily FINAL WHERE org_id = ? AND day = ? GROUP BY provider`, orgID, day)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]uint64{}
	for rows.Next() {
		var provider string
		var count uint64
		if err := rows.Scan(&provider, &count); err != nil {
			t.Fatal(err)
		}
		result[provider] = count
	}
	return result
}

type nilPartitionRig struct {
	pool      *pgxpool.Pool
	store     *daily.PostgresStore
	conn      driver.Conn
	handler   *daily.PartitionHandler
	dispatch  *daily.Dispatcher
	recorder  *nilPartitionFamilyRecorder
	registers map[string]struct{}
}

func newNilPartitionRig(t *testing.T, ctx context.Context) *nilPartitionRig {
	t.Helper()
	t.Chdir("../..")
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	conn := openNilPartitionClickHouse(t, ctx)

	store, err := daily.NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	discoverer, err := daily.NewClickHouseRepositoryDiscoverer(conn)
	if err != nil {
		t.Fatal(err)
	}
	dispatch, err := daily.NewDispatcher(store, nilPartitionPublisher{}, discoverer)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := daily.NewPartitionHandler(store, nilPartitionPublisher{})
	if err != nil {
		t.Fatal(err)
	}
	checker, err := daily.NewClickHouseSourceDataChecker(pool, conn)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetSourceDataChecker(checker)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	native, postBridge, _, refusals := dailyNativeFamilyRegistrations(store, conn, nil, logger)
	if len(refusals) != 0 {
		t.Fatalf("family refusals: %v", refusals)
	}
	if err := handler.SetNativeFamilies(native); err != nil {
		t.Fatal(err)
	}
	if err := handler.SetPostBridgeNativeFamilies(postBridge); err != nil {
		t.Fatal(err)
	}
	recorder := &nilPartitionFamilyRecorder{
		computed: map[string]int{}, other: map[string]jobruntime.DailyMetricsNativeFamilyOutcome{},
	}
	handler.SetNativeFamilyObserver(recorder)
	registers := map[string]struct{}{}
	for name := range native {
		registers[name] = struct{}{}
	}
	for name := range postBridge {
		registers[name] = struct{}{}
	}
	return &nilPartitionRig{pool: pool, store: store, conn: conn, handler: handler, dispatch: dispatch, recorder: recorder, registers: registers}
}

// startRun stages one run in its own transaction, the way the scheduler
// (StartScheduledFanoutRunTx) and the post-sync writer (StartRunTx without
// repository ids) do.
func (rig *nilPartitionRig) startRun(t *testing.T, ctx context.Context, start func(pgx.Tx) (daily.Run, error)) daily.Run {
	t.Helper()
	tx, err := rig.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	run, err := start(tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return run
}

func (rig *nilPartitionRig) dispatchAndRun(t *testing.T, ctx context.Context, run daily.Run) []daily.Partition {
	t.Helper()
	orgID := run.OrganizationID
	dispatchExecution := &jobruntime.Execution[jobruntime.DailyMetricsDispatchArgs]{
		OrganizationID: &orgID,
		Envelope:       jobcontract.Envelope{OrganizationID: &orgID, Domain: jobcontract.DomainLink{Type: "daily_metrics_run", ID: run.ID}},
		Args: jobruntime.DailyMetricsDispatchArgs{EnvelopeArgs: func() jobruntime.EnvelopeArgs[jobcontract.DailyMetricsDispatchPayload] {
			args := nilPartitionExecution[jobcontract.DailyMetricsDispatchPayload](orgID, "daily_metrics_run", run.ID)
			args.Payload = jobcontract.DailyMetricsDispatchPayload{RunID: run.ID}
			return args
		}()},
	}
	if err := rig.dispatch.Work(ctx, dispatchExecution); err != nil {
		t.Fatalf("dispatcher: %v", err)
	}
	partitions, err := rig.store.DispatchablePartitions(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, partition := range partitions {
		execution := &jobruntime.Execution[jobruntime.DailyMetricsPartitionArgs]{
			OrganizationID: &orgID,
			Envelope:       jobcontract.Envelope{OrganizationID: &orgID, Domain: jobcontract.DomainLink{Type: "daily_metrics_partition", ID: partition.ID}},
			Args: jobruntime.DailyMetricsPartitionArgs{EnvelopeArgs: func() jobruntime.EnvelopeArgs[jobcontract.DailyMetricsPartitionPayload] {
				args := nilPartitionExecution[jobcontract.DailyMetricsPartitionPayload](orgID, "daily_metrics_partition", partition.ID)
				args.Payload = jobcontract.DailyMetricsPartitionPayload{PartitionID: partition.ID}
				return args
			}()},
		}
		if err := rig.handler.Work(ctx, execution); err != nil {
			t.Fatalf("partition %s of %v: %v", partition.ID, partition.RepoIDs, err)
		}
	}
	return partitions
}

func (rig *nilPartitionRig) partitionStatuses(t *testing.T, ctx context.Context, runID string) map[string]int {
	t.Helper()
	rows, err := rig.pool.Query(ctx, `SELECT status, count(*) FROM daily_metrics_partitions WHERE run_id = $1::uuid GROUP BY status`, runID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	result := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			t.Fatal(err)
		}
		result[status] = count
	}
	return result
}

func holdsNilRepository(partitions []daily.Partition) bool {
	for _, partition := range partitions {
		for _, id := range partition.RepoIDs {
			if string(id) == uuid.Nil.String() {
				return true
			}
		}
	}
	return false
}

func (rig *nilPartitionRig) assertEveryRegisteredFamilyComputedWithoutFailure(t *testing.T) {
	t.Helper()
	rig.recorder.mu.Lock()
	defer rig.recorder.mu.Unlock()
	if len(rig.recorder.other) != 0 {
		t.Fatalf("families with an outcome other than computed: %v", rig.recorder.other)
	}
	got := make([]string, 0, len(rig.recorder.computed))
	for name := range rig.recorder.computed {
		got = append(got, name)
	}
	want := make([]string, 0, len(rig.registers))
	for name := range rig.registers {
		want = append(want, name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("families executed = %v, registered = %v", got, want)
	}
	if len(want) == 0 {
		t.Fatal("no family registered: the assertion would pass on an empty set")
	}
}

// Both paths that start the daily job end in the same Dispatcher.Work
// discovery: the daily schedule (internal/scheduler/fixed/producers.go,
// StartScheduledFanoutRunTx, generation prefix "fixed-schedule:") and the
// post-sync fan-out (internal/workerservice/sync_dispatch.go, StartRunTx with
// no repository ids, generation "post-sync:"). One test per path: each fails if
// the nil partition is dropped on the way.
func TestDailyJobRunsEveryFamilyOnTheNoRepositoryPartitionBothStartPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)

	type startFn func(rig *nilPartitionRig, orgID string) func(pgx.Tx) (daily.Run, error)
	paths := map[string]startFn{
		"daily_schedule": func(rig *nilPartitionRig, orgID string) func(pgx.Tx) (daily.Run, error) {
			return func(tx pgx.Tx) (daily.Run, error) {
				return rig.store.StartScheduledFanoutRunTx(ctx, tx, daily.ScheduledFanoutRequest{
					OrganizationID: orgID, TargetDay: day,
					Generation: daily.ScheduledFanoutGenerationPrefix + "2026-08-04T01:00:00Z",
				}, nilPartitionPublisher{})
			}
		},
		"post_sync_fanout": func(rig *nilPartitionRig, orgID string) func(pgx.Tx) (daily.Run, error) {
			return func(tx pgx.Tx) (daily.Run, error) {
				return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
					OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
				}, nilPartitionPublisher{})
			}
		},
	}
	for name, start := range paths {
		t.Run(name, func(t *testing.T) {
			rig := newNilPartitionRig(t, ctx)
			orgID := uuid.NewString()
			seedNilPartitionOrg(t, ctx, rig.conn, orgID, day, true)
			run := rig.startRun(t, ctx, start(rig, orgID))
			partitions := rig.dispatchAndRun(t, ctx, run)
			if !holdsNilRepository(partitions) {
				t.Fatalf("no partition of the run holds the nil repository id: %v", partitions)
			}
			if statuses := rig.partitionStatuses(t, ctx, run.ID); len(statuses) != 1 || statuses["succeeded"] != len(partitions) {
				t.Fatalf("partition statuses = %v, want all succeeded", statuses)
			}
			rig.assertEveryRegisteredFamilyComputedWithoutFailure(t)
			got := completedByProvider(t, ctx, rig.conn, orgID, day)
			for _, provider := range []string{"github", "linear", "jira"} {
				if got[provider] != 1 {
					t.Fatalf("items completed by provider = %v, want 1 for %s", got, provider)
				}
			}
		})
	}
}

// Acceptance ii: every registered family executed on a partition that holds
// ONLY the nil repository id (the shape of the operator recompute). The work
// item families write; every other family writes nothing, does not fail, and
// the zero-row source check does not hold the partition failed.
func TestDailyJobEveryFamilyOnAPartitionOfOnlyTheNilRepository(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	rig := newNilPartitionRig(t, ctx)
	orgID := uuid.NewString()
	seedNilPartitionOrg(t, ctx, rig.conn, orgID, day, true)
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day,
			Generation:    daily.ManualDailyGenerationPrefix + uuid.NewString()[:8],
			RepositoryIDs: []daily.RepositoryID{daily.RepositoryID(uuid.Nil.String())},
		}, nilPartitionPublisher{})
	})
	partitions := rig.dispatchAndRun(t, ctx, run)
	if len(partitions) != 1 || len(partitions[0].RepoIDs) != 1 || string(partitions[0].RepoIDs[0]) != uuid.Nil.String() {
		t.Fatalf("partitions = %v, want exactly the nil repository", partitions)
	}
	if statuses := rig.partitionStatuses(t, ctx, run.ID); statuses["succeeded"] != 1 {
		t.Fatalf("partition statuses = %v, want succeeded (the zero-row source check must not fail it)", statuses)
	}
	rig.assertEveryRegisteredFamilyComputedWithoutFailure(t)

	got := completedByProvider(t, ctx, rig.conn, orgID, day)
	if got["linear"] != 1 || got["jira"] != 1 || got["github"] != 0 {
		t.Fatalf("items completed by provider = %v, want linear 1, jira 1, github 0 (github is not on this partition)", got)
	}

	// Every other family writes nothing under the nil repository id.
	workItemFamilies := map[string]bool{
		"work_item": true, "work_item_estimate": true, "work_item_attribution": true, "work_item_state": true,
	}
	raw, err := readFamiliesJSON()
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, family := range raw.Families {
		if family.Port != "go" || family.Phase == "finalize" || workItemFamilies[family.Name] {
			continue
		}
		for _, table := range family.Writes {
			var hasRepoColumn uint64
			if err := rig.conn.QueryRow(ctx,
				`SELECT count() FROM system.columns WHERE database = currentDatabase() AND table = ? AND name = 'repo_id'`, table).Scan(&hasRepoColumn); err != nil {
				t.Fatal(err)
			}
			if hasRepoColumn == 0 {
				continue
			}
			var rows uint64
			if err := rig.conn.QueryRow(ctx,
				`SELECT count() FROM `+table+` WHERE toString(repo_id) = '00000000-0000-0000-0000-000000000000'`).Scan(&rows); err != nil {
				t.Fatalf("count %s: %v", table, err)
			}
			if rows != 0 {
				t.Fatalf("family %s wrote %d rows to %s under the nil repository id, want 0", family.Name, rows, table)
			}
			checked++
		}
	}
	if checked == 0 {
		t.Fatal("no table was checked: the zero-write assertion would pass on an empty set")
	}
}

// Acceptance iii: an organization with NO nil-repository work item gets no
// extra partition member.
func TestDailyJobGitOnlyOrganizationGetsNoNilRepositoryPartition(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	day := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	rig := newNilPartitionRig(t, ctx)
	orgID := uuid.NewString()
	seedNilPartitionOrg(t, ctx, rig.conn, orgID, day, false)
	run := rig.startRun(t, ctx, func(tx pgx.Tx) (daily.Run, error) {
		return rig.store.StartRunTx(ctx, tx, daily.StartRunRequest{
			OrganizationID: orgID, TargetDay: day, Generation: "post-sync:" + uuid.NewString(),
		}, nilPartitionPublisher{})
	})
	partitions := rig.dispatchAndRun(t, ctx, run)
	if holdsNilRepository(partitions) {
		t.Fatalf("a git-only organization got the nil repository: %v", partitions)
	}
	members := 0
	for _, partition := range partitions {
		members += len(partition.RepoIDs)
	}
	if len(partitions) != 1 || members != 1 {
		t.Fatalf("partitions = %v, want one partition with the one repository", partitions)
	}
}

type familiesJSON struct {
	Families []struct {
		Name   string   `json:"name"`
		Writes []string `json:"writes"`
		Port   string   `json:"port"`
		Phase  string   `json:"phase"`
	} `json:"families"`
}

func readFamiliesJSON() (familiesJSON, error) {
	var result familiesJSON
	raw, err := os.ReadFile("internal/jobs/metrics/daily/families.json")
	if err != nil {
		return result, err
	}
	return result, json.Unmarshal(raw, &result)
}
