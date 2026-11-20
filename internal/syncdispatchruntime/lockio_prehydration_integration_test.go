//go:build integration

package syncdispatchruntime

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncbudget"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchruntime/synclog"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// acp lock-1 ruling 14250 (rule 1: no outbound I/O while the bucket lock is held), cells of item 3. A real Dispatch
// pass over one PagerDuty run whose credential is a client-credentials descriptor: the production estimator
// (InProcessBudgetEstimator) hydrates it with a token exchange through a counting RoundTripper, which asks Postgres,
// at each exchange, whether the run's dispatch bucket advisory lock is held.
//
//	(i)  cold cache, pre-hydrated: one exchange, before the lock; 0 under the lock; the unit is estimated as before.
//	(ii) the token expires between the pre-step and the lock (the record's clock jumps right after the exchange): 0
//	     exchanges under the lock, the skip counter is 1, and the unit takes main's hydration-failure path: the
//	     estimator's budget_estimate_unit_failed line and no estimate, the same outcome as a refused exchange (control).
//
// Mutants: drop the pre-hydration (preHydratePass hands the lock an empty record) -> (i) red; drop the no-network
// guard (hydratePagerDutyOnce calls out under the lock) -> (ii) red.

const (
	lockIORun         = "00000000-0000-4000-8000-00000000a101"
	lockIOOutbox      = "00000000-0000-4000-8000-00000000a102"
	lockIOIntegration = "00000000-0000-4000-8000-00000000a103"
	lockIOSource      = "00000000-0000-4000-8000-00000000a104"
	lockIOCredential  = "00000000-0000-4000-8000-00000000a105"
	lockIOUnit        = "00000000-0000-4000-8000-00000000a106"
	lockIOFeature     = "00000000-0000-4000-8000-00000000a107"
	lockIODiscovery   = "00000000-0000-4000-8000-00000000a108"
	lockIOSettingsKey = "lockio-prehydration-settings-key"
)

// jumpClock is the pass record's clock: the wall clock plus an offset a test moves.
type jumpClock struct {
	mu     sync.Mutex
	offset time.Duration
}

func (clock *jumpClock) now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return time.Now().Add(clock.offset)
}

func (clock *jumpClock) jump(by time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.offset += by
}

// lockCountingTransport answers the PagerDuty token exchange and counts every attempt, and the attempts made while
// the bucket's advisory lock is granted to any session.
type lockCountingTransport struct {
	pool      *pgxpool.Pool
	key       int64
	status    int
	afterSend func()

	mu        sync.Mutex
	total     int
	underLock int
	lockErr   error
}

// EgressOffline: the double opens no connection, so the provider egress guard passes it through.
func (*lockCountingTransport) EgressOffline() {}

func (transport *lockCountingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	var held int
	err := transport.pool.QueryRow(context.Background(), `
SELECT count(*) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND objsubid = 1 AND classid::bigint = $1 AND objid::bigint = $2`,
		int64(uint32(uint64(transport.key)>>32)), int64(uint32(uint64(transport.key)))).Scan(&held)
	transport.mu.Lock()
	transport.total++
	if held > 0 {
		transport.underLock++
	}
	if err != nil && transport.lockErr == nil {
		transport.lockErr = err
	}
	transport.mu.Unlock()
	if transport.afterSend != nil {
		transport.afterSend()
	}
	return &http.Response{
		StatusCode: transport.status, Header: http.Header{"Content-Type": {"application/json"}},
		Body:    io.NopCloser(strings.NewReader(`{"access_token": "cc-token", "expires_in": 3600}`)),
		Request: request,
	}, nil
}

func (transport *lockCountingTransport) counts() (total, underLock int, err error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return transport.total, transport.underLock, transport.lockErr
}

// seedLockIORun seeds one dispatchable PagerDuty run: its client-credentials credential (encrypted with cipher), the
// integration and source, the dispatch route and outbox row, a finished reference discovery, the canonical-incident
// entitlement and one planned incidents unit.
func seedLockIORun(t *testing.T, ctx context.Context, pool *pgxpool.Pool, cipher providerfoundation.FernetDecryptor) {
	t.Helper()
	if got := pgseed.SyncTransportRoute(ctx, t, pool, "dispatch_sync_run", "river", dispatchRouteGeneration, false, "celery"); got != dispatchRouteGeneration {
		t.Fatalf("dispatch_sync_run route generation = %d, want %d", got, dispatchRouteGeneration)
	}
	encrypted, err := cipher.Encrypt([]byte(`{"auth_mode": "client_credentials", "client_id": "i", "client_secret": "s", "subdomain": "acme", "region": "us"}`))
	if err != nil {
		t.Fatal(err)
	}
	pgseed.Integration(ctx, t, pool, lockIOIntegration, discoveryTestOrg, "pagerduty")
	pgseed.IntegrationSource(ctx, t, pool, lockIOSource, discoveryTestOrg, lockIOIntegration, "pagerduty", "acme")
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, created_at, updated_at)
VALUES ($1::uuid, $2, 'pagerduty', 'lockio', true, $3, now(), now())`, []any{lockIOCredential, discoveryTestOrg, encrypted.Reveal()}},
		{`UPDATE integrations SET credential_id = $1::uuid WHERE id = $2::uuid`, []any{lockIOCredential, lockIOIntegration}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	pgseed.EnsureSyncRun(ctx, t, pool, pgseed.SyncRun{ID: lockIORun, OrgID: discoveryTestOrg, IntegrationID: lockIOIntegration})
	pgseed.SyncDispatchOutbox(ctx, t, pool, lockIOOutbox, lockIORun, discoveryTestOrg, "dispatch_sync_run", "dispatched", "river", dispatchRouteGeneration)
	pgseed.SetFeatureFlag(ctx, t, pool, lockIOFeature, "canonical_incident_ingestion", "community", true)
	pgseed.OrgOverride(ctx, t, pool, discoveryTestOrg, lockIOFeature, true)
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ($1,$2,$3,$4,1,now(),now(),now())`, []any{lockIODiscovery, lockIORun, discoveryTestOrg, discoveryStatusSuccess}},
		{`INSERT INTO sync_run_units (id,org_id,sync_run_id,provider,dataset_key,source_id,status,updated_at,integration_id,cost_class,mode,attempts,created_at)
VALUES ($1,$2,$3,'pagerduty','incidents',$4,'planned',now(),$5,'rest_core','incremental',0,now())`, []any{lockIOUnit, discoveryTestOrg, lockIORun, lockIOSource, lockIOIntegration}},
	} {
		if _, err := pool.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

type lockIOOutcome struct {
	total, underLock int
	skips            map[string]int64
	unitFailed       bool
	dryRunEstimated  bool
	unitStatus       string
	lines            []string
}

// skipCounts reads syncbudget.LockedHydrationSkipCounter by reason.
func skipCounts(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var resource metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &resource); err != nil {
		t.Fatal(err)
	}
	counted := map[string]int64{}
	for _, scope := range resource.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != syncbudget.LockedHydrationSkipCounter {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				reason, _ := point.Attributes.Value("reason")
				counted[reason.AsString()] += point.Value
			}
		}
	}
	return counted
}

// runLockIOPass seeds the run on a fresh database and runs one real Dispatch pass with the production estimator.
func runLockIOPass(t *testing.T, status int, expireAfterExchange bool) lockIOOutcome {
	t.Helper()
	var outcome lockIOOutcome
	withSmallDispatchPool(t, 8, func(ctx context.Context, pool *pgxpool.Pool) {
		cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(lockIOSettingsKey), "")
		if err != nil {
			t.Fatal(err)
		}
		seedLockIORun(t, ctx, pool, cipher)
		reader := installBusyMeter(t)
		logs := &lockedLogBuffer{}
		logger := synclog.New(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
		clock := &jumpClock{}
		transport := &lockCountingTransport{pool: pool, key: BucketAdvisoryLockKeyForTest(discoveryTestOrg, "pagerduty", "rest_core"), status: status}
		if expireAfterExchange {
			// The token (3600 s) expires between the pre-step and the lock: the record's clock moves two hours on
			// as soon as the pre-step's exchange returns.
			transport.afterSend = func() { clock.jump(2 * time.Hour) }
		}
		estimator, err := newInProcessBudgetEstimator(pool, BudgetEstimatorDependencies{
			Decryptor: cipher, Getenv: func(string) string { return "" }, Logger: logger,
			PagerDutyDoer: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		})
		if err != nil {
			t.Fatal(err)
		}
		estimator.now = clock.now
		registry := &fakeJobRegistry{descriptors: map[string]jobruntime.Descriptor{
			jobcontract.KindSyncProviderUnit: providerUnitDescriptor("river"),
		}}
		producer, err := joboutbox.NewProducer(pool, registry)
		if err != nil {
			t.Fatal(err)
		}
		service, err := NewNativeDispatchSyncRunService(pool, logger, estimator, producer, registry)
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Dispatch(ctx, DispatchSyncRunArgs{TransportArgs: TransportArgs{
			Version: ContractVersionV1, OrgID: discoveryTestOrg, RunID: lockIORun,
			DispatchOutbox: lockIOOutbox, RouteGeneration: dispatchRouteGeneration,
		}}); err != nil {
			t.Fatalf("Dispatch: %v", err)
		}
		total, underLock, lockErr := transport.counts()
		if lockErr != nil {
			t.Fatalf("pg_locks read: %v", lockErr)
		}
		outcome.total, outcome.underLock = total, underLock
		outcome.skips = skipCounts(t, reader)
		outcome.lines = logs.lines()
		for _, line := range outcome.lines {
			if !strings.Contains(line, "unit_id="+lockIOUnit) {
				continue
			}
			if strings.Contains(line, "msg="+"dispatch_sync_run.budget_estimate_unit_failed") {
				outcome.unitFailed = true
			}
			if strings.Contains(line, "msg=dispatch_sync_run.budget_guard_dry_run ") {
				outcome.dryRunEstimated = true
			}
		}
		if err := pool.QueryRow(ctx, `SELECT status FROM sync_run_units WHERE id = $1::uuid`, lockIOUnit).Scan(&outcome.unitStatus); err != nil {
			t.Fatal(err)
		}
	})
	return outcome
}

func TestLockIOPrehydratedEstimateMakesNoCallUnderTheBucketLock(t *testing.T) {
	outcome := runLockIOPass(t, http.StatusOK, false)
	t.Logf("(i) exchanges %d, under the lock %d, skips %v, unit failed %v, estimated %v, unit %s",
		outcome.total, outcome.underLock, outcome.skips, outcome.unitFailed, outcome.dryRunEstimated, outcome.unitStatus)
	if outcome.underLock != 0 {
		t.Errorf("(i) %d token exchanges while the bucket lock was held, want 0", outcome.underLock)
	}
	if outcome.total != 1 {
		t.Errorf("(i) %d token exchanges in the pass, want 1 (the pre-step, once per credential)", outcome.total)
	}
	if outcome.skips[syncbudget.LockedHydrationSkipMiss]+outcome.skips[syncbudget.LockedHydrationSkipExpired] != 0 {
		t.Errorf("(i) skip counter %v, want 0", outcome.skips)
	}
	if outcome.unitFailed || !outcome.dryRunEstimated {
		t.Errorf("(i) unit failed %v, estimated %v: want the estimate as before (no failure, a dry-run observation)\n%s",
			outcome.unitFailed, outcome.dryRunEstimated, strings.Join(outcome.lines, "\n"))
	}
}

func TestLockIOTokenExpiredBeforeTheLockTakesMainsFailurePathWithNoCall(t *testing.T) {
	expired := runLockIOPass(t, http.StatusOK, true)
	t.Logf("(ii) exchanges %d, under the lock %d, skips %v, unit failed %v, estimated %v, unit %s",
		expired.total, expired.underLock, expired.skips, expired.unitFailed, expired.dryRunEstimated, expired.unitStatus)
	if expired.underLock != 0 {
		t.Errorf("(ii) %d token exchanges while the bucket lock was held, want 0", expired.underLock)
	}
	if expired.total != 1 {
		t.Errorf("(ii) %d token exchanges in the pass, want 1 (the pre-step)", expired.total)
	}
	if expired.skips[syncbudget.LockedHydrationSkipExpired] != 1 || expired.skips[syncbudget.LockedHydrationSkipMiss] != 0 {
		t.Errorf("(ii) skip counter %v, want expired=1", expired.skips)
	}
	warned := false
	for _, line := range expired.lines {
		if strings.Contains(line, "msg="+"dispatch_sync_run.budget_hydration_skipped_under_lock") && strings.Contains(line, "reason=expired") {
			warned = true
		}
	}
	if !warned {
		t.Errorf("(ii) no %s WARN with reason=expired", "dispatch_sync_run.budget_hydration_skipped_under_lock")
	}
	if !expired.unitFailed || expired.dryRunEstimated {
		t.Errorf("(ii) unit failed %v, estimated %v: want main's hydration-failure path (the unit's estimate failed, no estimate)",
			expired.unitFailed, expired.dryRunEstimated)
	}
	// Control: main's hydration failure itself (the exchange is refused): the same unit outcome.
	refused := runLockIOPass(t, http.StatusUnauthorized, false)
	t.Logf("control (refused exchange) exchanges %d, under the lock %d, skips %v, unit failed %v, estimated %v, unit %s",
		refused.total, refused.underLock, refused.skips, refused.unitFailed, refused.dryRunEstimated, refused.unitStatus)
	if refused.underLock != 0 || !refused.unitFailed || refused.dryRunEstimated {
		t.Fatalf("control: under the lock %d, unit failed %v, estimated %v; want 0, true, false", refused.underLock, refused.unitFailed, refused.dryRunEstimated)
	}
	if expired.unitStatus != refused.unitStatus {
		t.Errorf("(ii) unit status %q, want %q (the refused exchange's: main's failure path)", expired.unitStatus, refused.unitStatus)
	}
}
