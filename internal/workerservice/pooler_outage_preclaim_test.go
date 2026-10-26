package workerservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
	"github.com/jackc/pgx/v5/pgxpool"
)

// CHAOS-11608. During a ~2 min Postgres outage every worker group exited on
// preclaim attempt 1 with execution_liveness=error: the queue telemetry read
// and the domain_transaction probe flatten their driver error to a fixed
// sentence, so a refused dial reached dependencyCheckFailed with no cause to
// classify. These tests drive the production check functions against a pool
// whose endpoint refuses, never a synthetic dial check.

const poolerOutageQueue = "sync"

// poolerOutageDependencies builds the production checks over a lazy pool that
// dials addr on first use.
func poolerOutageDependencies(t *testing.T, addr string, logger *slog.Logger) *workerDependencies {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://worker:pw@"+addr+"/db?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	database := &postgresWorkerDatabase{
		pools:       &postgres.RuntimePools{Domain: pool, QueueControl: pool},
		domainRole:  "devhealth_domain",
		queueRole:   "devhealth_queue",
		riverSchema: "river",
	}
	sampler, err := database.NewQueueTelemetrySampler(riverstore.QueueTelemetryConfig{
		Schema:   "river",
		ClientID: "worker-11608",
		Queues:   []riverstore.QueueTelemetryQueue{{Name: poolerOutageQueue, MaxWorkers: 1}},
		Jobs: []riverstore.QueueTelemetryJob{
			{Queue: poolerOutageQueue, Kind: "sync.provider_unit", SupportedVersions: []int{1}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &workerDependencies{
		database: database, queueTelemetry: sampler, queueTelemetryRequired: true, logger: logger,
	}
}

// registerPoolerOutageChecks registers domain_transaction and
// execution_liveness exactly as production does. up, when set, reports that the
// endpoint is back: a fake server cannot serve a real BEGIN, so the recovered
// phase answers nil.
func registerPoolerOutageChecks(t *testing.T, registry *health.Registry, dependencies *workerDependencies, up func() bool) {
	t.Helper()
	claim := newClaimLiveness(time.Now(), []string{poolerOutageQueue})
	gate := func(check health.CheckFunc) health.CheckFunc {
		return func(ctx context.Context) error {
			if up != nil && up() {
				return nil
			}
			return check(ctx)
		}
	}
	if err := registry.RegisterRequired("domain_transaction", gate(dependencies.domainTransactionReady)); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("execution_liveness", gate(dependencies.claimLivenessReady(claim))); err != nil {
		t.Fatal(err)
	}
}

type poolerOutageEndpoint struct {
	addr string
	// reached reports that a dial reached the endpoint, so a refusal was
	// measured rather than a failure before any dial.
	reached func() bool
}

func poolerOutageEndpoints(t *testing.T) map[string]poolerOutageEndpoint {
	t.Helper()
	pooler := fakepg.NewPoolerRefusing(t)
	return map[string]poolerOutageEndpoint{
		"pooler_08P01": {
			addr:    fmt.Sprintf("%s:%d", pooler.Host, pooler.Port),
			reached: func() bool { return pooler.Connections() > 0 },
		},
		"econnrefused": {addr: closedPortAddr(t), reached: func() bool { return true }},
	}
}

func TestProductionChecksClassifyARefusedPoolerAsUnreachable(t *testing.T) {
	t.Parallel()
	for name, endpoint := range poolerOutageEndpoints(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			registry := health.NewRegistry(10 * time.Second)
			registerPoolerOutageChecks(t, registry, poolerOutageDependencies(t, endpoint.addr, nil), nil)
			readiness := registry.CheckRequired(context.Background())
			if !endpoint.reached() {
				t.Fatal("no dial reached the endpoint: the checks failed before measuring a refusal")
			}
			if readiness.Ready || len(readiness.Checks) != 2 {
				t.Fatalf("readiness = %+v, want two failed checks", readiness)
			}
			for _, status := range readiness.Checks {
				if !status.Failed || !status.Transient || status.Cause != health.CauseUnreachable || status.TimedOut {
					t.Errorf("%s: Failed=%v Transient=%v Cause=%q TimedOut=%v, want a failed, transient %q check",
						status.Name, status.Failed, status.Transient, status.Cause, status.TimedOut, health.CauseUnreachable)
				}
			}
		})
	}
}

func TestPreclaimSurvivesA120sPoolerOutageWithProductionChecks(t *testing.T) {
	t.Parallel()
	for name, endpoint := range poolerOutageEndpoints(t) {
		addr := endpoint.addr
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			clock := &preclaimFakeClock{}
			start := clock.now()
			registry := health.NewRegistry(10 * time.Second)
			var attempts atomic.Int32
			registerPoolerOutageChecks(t, registry, poolerOutageDependencies(t, addr, logger), func() bool {
				attempts.Add(1)
				return clock.now().Sub(start) >= 120*time.Second
			})
			component := preclaimReadinessComponent{
				registry: registry, logger: logger, budget: 300 * time.Second,
				now: clock.now, sleep: clock.sleep,
			}
			if err := component.Start(context.Background()); err != nil {
				t.Fatalf("Start() = %v, want nil once the endpoint returns inside the budget\n%s", err, logs.String())
			}
			// Two checks gate once per attempt each.
			if got := attempts.Load() / 2; got != 8 {
				t.Fatalf("attempts = %d, want 8", got)
			}
			want := []time.Duration{2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second, 30 * time.Second}
			if got := clock.recordedWaits(); !slices.Equal(got, want) {
				t.Fatalf("waits = %v, want %v", got, want)
			}
			text := logs.String()
			if strings.Contains(text, "preclaim readiness refused") {
				t.Fatalf("a recovered start must not log a refusal:\n%s", text)
			}
			if !strings.Contains(text, `"failed_causes":"domain_transaction=unreachable,execution_liveness=unreachable"`) {
				t.Fatalf("want retry lines naming both checks unreachable:\n%s", text)
			}
			if strings.Contains(text, addr) || strings.Contains(text, "server_login_retry") {
				t.Fatalf("log carries the dial address or driver text:\n%s", text)
			}
		})
	}
}

// A wrong password is a real answer, not an outage: preclaim must still exit
// on attempt 1.
func TestProductionChecksKeepAWrongPasswordNonTransient(t *testing.T) {
	t.Parallel()
	server := fakepg.New(t)
	addr := fmt.Sprintf("%s:%d", server.Host, server.Port)
	registry := health.NewRegistry(10 * time.Second)
	registerPoolerOutageChecks(t, registry, poolerOutageDependencies(t, addr, nil), nil)
	readiness := registry.CheckRequired(context.Background())
	if server.Connections() == 0 {
		t.Fatal("no connection reached the server: the checks never tried to log in")
	}
	for _, status := range readiness.Checks {
		if !status.Failed || status.Transient || status.Cause != "error" {
			t.Errorf("%s: Failed=%v Transient=%v Cause=%q, want a failed, non-transient \"error\" check",
				status.Name, status.Failed, status.Transient, status.Cause)
		}
	}
	clock := &preclaimFakeClock{}
	component := preclaimReadinessComponent{registry: registry, budget: 300 * time.Second, now: clock.now, sleep: clock.sleep}
	if err := component.Start(context.Background()); !errors.Is(err, errWorkerDependencyUnavailable) {
		t.Fatalf("Start() = %v, want errWorkerDependencyUnavailable", err)
	}
	if waits := clock.recordedWaits(); len(waits) != 0 {
		t.Fatalf("waits = %v, want an exit on attempt 1", waits)
	}
}

// A canceled check stopped for its caller's reason; it must never read as an
// unreachable endpoint.
func TestProductionChecksKeepACancelNonTransient(t *testing.T) {
	t.Parallel()
	for name, endpoint := range poolerOutageEndpoints(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dependencies := poolerOutageDependencies(t, endpoint.addr, nil)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			claim := newClaimLiveness(time.Now(), []string{poolerOutageQueue})
			for check, run := range map[string]health.CheckFunc{
				"domain_transaction": dependencies.domainTransactionReady,
				"execution_liveness": dependencies.claimLivenessReady(claim),
			} {
				err := run(ctx)
				if err == nil {
					t.Fatalf("%s(canceled) = nil, want a failure", check)
				}
				if health.IsTransient(err) || isTransientDialError(err) {
					t.Errorf("%s(canceled) = %v, classified transient; a cancel must stay non-transient", check, err)
				}
			}
		})
	}
}
