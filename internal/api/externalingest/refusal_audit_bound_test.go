package externalingest_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/ingestpg"
)

const overFailureLimit = "over_failure_limit"

// peekBelowStore reports every window as empty to Peek and counts for real in Increment: the state of a burst whose
// requests all passed the non-consuming failure check before any of them recorded its failure.
type peekBelowStore struct{ httpapi.CounterStore }

func (peekBelowStore) Peek(context.Context, httpapi.Hit) (int64, error) { return 0, nil }

// A refusal whose failure hit is not admitted writes no row, answers exactly as before, and is counted under
// over_failure_limit.
func TestRefusalAuditIsDroppedWhenTheFailureHitIsNotAdmitted(t *testing.T) {
	for _, site := range refusalSites() {
		t.Run(site.name, func(t *testing.T) {
			rig := newAuditRig(t, nil)
			rig.deps.Counters = peekBelowStore{httpapi.NewMemoryCounters(time.Now)}
			rig.server.SetToken(site.token())
			rt := auditRoutes[1]
			baseline := rig.do(t, context.Background(), rt, "Bearer "+auditToken)
			for i := 2; i <= 30; i++ {
				rig.do(t, context.Background(), rt, "Bearer "+auditToken)
			}
			if rows := len(rig.server.AuditRows()); rows != 30 {
				t.Fatalf("%d rows after 30 admitted refusals, want 30", rows)
			}
			before := auditFailureCounts(t)
			answer := rig.do(t, context.Background(), rt, "Bearer "+auditToken)
			if answer.Code != baseline.Code || answer.Body.String() != baseline.Body.String() {
				t.Errorf("answer changed: %d %q, was %d %q", answer.Code, answer.Body.String(), baseline.Code, baseline.Body.String())
			}
			if rows := len(rig.server.AuditRows()); rows != 30 {
				t.Errorf("%d rows after the 31st refusal, want 30", rows)
			}
			after := auditFailureCounts(t)
			if after[overFailureLimit]-before[overFailureLimit] != 1 {
				t.Errorf("counter %v -> %v, want +1 under %s", before, after, overFailureLimit)
			}
		})
	}
}

// releaseGate blocks each request after its token lookup until n requests have reached that point, so all n have
// passed the non-consuming failure check before any records its failure. A gate that never fills is a failure.
type releaseGate struct {
	mu       sync.Mutex
	want     int
	arrived  int
	release  chan struct{}
	timedOut bool
}

type releaseGateKey struct{}

func (g *releaseGate) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "FROM external_ingest_tokens") {
		return context.WithValue(ctx, releaseGateKey{}, true)
	}
	return ctx
}

func (g *releaseGate) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(releaseGateKey{}) == nil {
		return
	}
	g.mu.Lock()
	g.arrived++
	if g.arrived == g.want {
		close(g.release)
	}
	release := g.release
	g.mu.Unlock()
	select {
	case <-release:
	case <-time.After(30 * time.Second):
		g.mu.Lock()
		g.timedOut = true
		g.mu.Unlock()
	}
}

// 31 concurrent refusals of a known token from one address, all past the failure check before any is recorded,
// write exactly 30 rows: the rows per address are bounded by the failure limit, also under concurrency. Every
// request still gets its refusal, and the one not recorded is counted.
func TestConcurrentKnownTokenRefusalsWriteAtMostThirtyRows(t *testing.T) {
	const burst = 31
	for _, site := range refusalSites() {
		for _, rt := range auditRoutes {
			if site.reason == "feature_not_enabled" && rt.pattern == auditRoutes[0].pattern {
				continue
			}
			t.Run(site.name+"/"+rt.method+" "+rt.pattern, func(t *testing.T) {
				auditMeterReader()
				t.Setenv("TRUSTED_PROXIES", "")
				gate := &releaseGate{want: burst, release: make(chan struct{})}
				server, pool := ingestpg.StartWith(t, func(config *pgxpool.Config) {
					config.MaxConns = burst
					config.ConnConfig.Tracer = gate
				})
				server.SetToken(site.token())
				rig := &auditRig{server: server, pool: pool, deps: externalingest.Deps{Pool: pool, Counters: httpapi.NewMemoryCounters(time.Now)}}
				before := auditFailureCounts(t)
				statuses := make(chan int, burst)
				var wg sync.WaitGroup
				for range burst {
					wg.Add(1)
					go func() {
						defer wg.Done()
						statuses <- rig.do(t, context.Background(), rt, "Bearer "+auditToken).Code
					}()
				}
				wg.Wait()
				close(statuses)
				gate.mu.Lock()
				timedOut, arrived := gate.timedOut, gate.arrived
				gate.mu.Unlock()
				if timedOut || arrived != burst {
					t.Fatalf("the release gate did not fill (%d of %d arrived): the burst was not concurrent", arrived, burst)
				}
				for status := range statuses {
					if status != site.status {
						t.Errorf("a request answered %d, want %d", status, site.status)
					}
				}
				if rows := len(server.AuditRows()); rows != 30 {
					t.Errorf("%d audit rows, want exactly 30", rows)
				}
				after := auditFailureCounts(t)
				if after[overFailureLimit]-before[overFailureLimit] != 1 {
					t.Errorf("counter %v -> %v, want +1 under %s", before, after, overFailureLimit)
				}
			})
		}
	}
}
