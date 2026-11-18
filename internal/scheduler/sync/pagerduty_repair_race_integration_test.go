//go:build integration

package sync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const pagerDutyRaceAttempts = 300

type raceTally struct {
	mu         sync.Mutex
	pairs      int
	byName     map[string]int
	attemptHit int
	lockWaits  int
	waitedOn   map[string]int
}

func (tally *raceTally) record(errs []error, waitedOn map[string]bool) error {
	tally.mu.Lock()
	defer tally.mu.Unlock()
	tally.pairs++
	if len(waitedOn) > 0 {
		tally.lockWaits++
	}
	for statement := range waitedOn {
		tally.waitedOn[statement]++
	}
	hit := false
	for _, err := range errs {
		if err == nil {
			continue
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			tally.byName[pgErr.ConstraintName]++
			hit = true
			continue
		}
		return err
	}
	if hit {
		tally.attemptHit++
	}
	return nil
}

func (tally *raceTally) String() string {
	names := make([]string, 0, len(tally.byName))
	for name := range tally.byName {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", name, tally.byName[name]))
	}
	waits := make([]string, 0, len(tally.waitedOn))
	for statement, count := range tally.waitedOn {
		waits = append(waits, fmt.Sprintf("%d x %q", count, statement))
	}
	sort.Strings(waits)
	return fmt.Sprintf("pairs=%d pairs_with_23505=%d pairs_with_lock_wait=%d by_constraint=[%s] lock_waiter_statements=[%s]", tally.pairs, tally.attemptHit, tally.lockWaits, strings.Join(parts, " "), strings.Join(waits, "; "))
}

// racePair opens one transaction per goroutine, waits until both are open,
// then releases both at once. A side pool polls pg_stat_activity for a
// heavyweight-lock wait while the pair runs.
func racePair(t *testing.T, poller *pgxpool.Pool, run func(i int) error) ([]error, map[string]bool) {
	t.Helper()
	start := make(chan struct{})
	errs := make([]error, 2)
	var ready, group sync.WaitGroup
	ready.Add(2)
	group.Add(2)
	for i := range 2 {
		go func() {
			defer group.Done()
			ready.Done()
			<-start
			errs[i] = run(i)
		}()
	}
	var waitedMu sync.Mutex
	waitedOn := map[string]bool{}
	done := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		for {
			select {
			case <-done:
				return
			default:
			}
			rows, err := poller.Query(context.Background(), `SELECT left(btrim(regexp_replace(query,'\s+',' ','g')),70) FROM pg_stat_activity WHERE wait_event_type='Lock' AND datname=current_database()`)
			if err != nil {
				continue
			}
			for rows.Next() {
				var statement string
				if rows.Scan(&statement) == nil {
					waitedMu.Lock()
					waitedOn[statement] = true
					waitedMu.Unlock()
				}
			}
			rows.Close()
		}
	}()
	ready.Wait()
	close(start)
	group.Wait()
	close(done)
	<-pollDone
	return errs, waitedOn
}

func newPollerPool(t *testing.T, fixture materializerFixture) *pgxpool.Pool {
	t.Helper()
	poller, err := pgxpool.New(context.Background(), fixture.pool.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(poller.Close)
	return poller
}

func resetPagerDutyInventory(t *testing.T, fixture materializerFixture) {
	t.Helper()
	for _, statement := range []string{`DELETE FROM sync_run_units`, `DELETE FROM integration_datasets`, `DELETE FROM integration_sources`} {
		if _, err := fixture.pool.Exec(context.Background(), statement); err != nil {
			t.Fatal(err)
		}
	}
}

func fixtureIntegrationID(t *testing.T, fixture materializerFixture) string {
	t.Helper()
	var integrationID string
	if err := fixture.pool.QueryRow(context.Background(), `SELECT integration_id::text FROM sync_configurations WHERE id=$1::uuid`, fixture.occurrence.ConfigID).Scan(&integrationID); err != nil {
		t.Fatal(err)
	}
	return integrationID
}

// TestPagerDutyRepairConcurrentMaterializeConverges races two full
// Materialize calls (distinct occurrences, same PagerDuty config) from a
// barrier, 300 times. The repair inserts derive their ids, so without the
// coordinator FOR UPDATE locks held past the domain commit a pair raises
// 23505 on a primary key about a third of the time (CHAOS-4204). This test
// is probabilistic; TestPagerDutyRepairPrepareLocksConfigurationRows is the
// deterministic guard.
func TestPagerDutyRepairConcurrentMaterializeConverges(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["operational"]`)
	materializer, err := NewNativeMaterializer(fixture.pool)
	if err != nil {
		t.Fatal(err)
	}
	materializer.WithCredentialFingerprint(pagerDutyFixtureDecryptor(t))
	poller := newPollerPool(t, fixture)
	tally := &raceTally{byName: map[string]int{}, waitedOn: map[string]int{}}
	for attempt := range pagerDutyRaceAttempts {
		resetPagerDutyInventory(t, fixture)
		errs, waitedOn := racePair(t, poller, func(i int) error {
			occurrence := fixture.occurrence
			occurrence.ID = fmt.Sprintf("occurrence:v1:pd-race-%d-%d", attempt, i)
			tx, err := fixture.pool.Begin(context.Background())
			if err != nil {
				return err
			}
			if _, err := materializer.Materialize(context.Background(), tx, occurrence); err != nil {
				_ = tx.Rollback(context.Background())
				return err
			}
			return tx.Commit(context.Background())
		})
		if err := tally.record(errs, waitedOn); err != nil {
			t.Fatalf("attempt %d: unexpected error: %v", attempt, err)
		}
	}
	t.Logf("PROBE production-path Materialize: %s", tally)
	if len(tally.byName) != 0 {
		t.Fatalf("concurrent PagerDuty repair raised 23505: %s", tally)
	}
	var datasets int
	if err := fixture.pool.QueryRow(context.Background(), `SELECT count(*) FROM integration_datasets WHERE is_enabled`).Scan(&datasets); err != nil {
		t.Fatal(err)
	}
	if datasets != len(pagerDutyOperationalDatasets) {
		t.Fatalf("enabled datasets=%d want %d", datasets, len(pagerDutyOperationalDatasets))
	}
}

// TestPagerDutyRepairPrepareLocksConfigurationRows: while one transaction
// holds preparePagerDutyRepair's sync_configurations FOR UPDATE, a second
// repair of the same integration cannot pass it. The repair inserts in
// applyDomainPlanMutations rely on this lock to never race (CHAOS-4204).
func TestPagerDutyRepairPrepareLocksConfigurationRows(t *testing.T) {
	fixture := startMaterializerPostgres(t)
	configurePagerDutyFixture(t, fixture, `["operational"]`)
	integrationID := fixtureIntegrationID(t, fixture)
	const credentialID = "00000000-0000-4000-8000-000000001006"
	credential := credentialID
	ctx := context.Background()
	holder, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if repair, _, reason, err := preparePagerDutyRepair(ctx, holder, fixture.occurrence.OrgID, integrationID, &credential, time.Now()); err != nil || reason != "" || repair == nil {
		t.Fatalf("holder prepare: repair=%v reason=%q err=%v", repair, reason, err)
	}
	contender, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = contender.Rollback(ctx) }()
	if _, err := contender.Exec(ctx, `SET LOCAL lock_timeout = '500ms'`); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = preparePagerDutyRepair(ctx, contender, fixture.occurrence.OrgID, integrationID, &credential, time.Now())
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "55P03" {
		t.Fatalf("contender prepare err=%v, want lock_not_available (55P03)", err)
	}
	t.Logf("PROBE prepare lock: contender blocked: %s", pgErr.Message)
}
