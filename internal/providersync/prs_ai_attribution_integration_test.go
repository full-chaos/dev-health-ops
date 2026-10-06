//go:build integration

package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/aianalytics"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

type prAttributionStore struct {
	ctx    context.Context
	conn   driver.Conn
	client aianalytics.QueryClient
}

// startPRAttributionStore applies the REAL migration chain, so ai_attribution
// has its production engine and key and ai_attribution_resolved is the
// production view the readers use.
func startPRAttributionStore(t *testing.T) prAttributionStore {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeContext)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: instance.URI})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return prAttributionStore{ctx: ctx, conn: conn, client: client}
}

func (store prAttributionStore) reset(t *testing.T) {
	t.Helper()
	if err := store.conn.Exec(store.ctx, "TRUNCATE TABLE ai_attribution"); err != nil {
		t.Fatal(err)
	}
}

func (store prAttributionStore) count(t *testing.T, query string) uint64 {
	t.Helper()
	var n uint64
	if err := store.conn.QueryRow(store.ctx, query).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// prAttributionScenario is one provider's pair of real producers and the sink
// each route uses: the prs unit writes through the PR-social sink, the
// work-items unit through the provider's work-item ai_attribution adapter.
type prAttributionScenario struct {
	provider        string
	subjects        uint64
	prsEffect       func(t *testing.T) EffectBatch
	workItemRows    func(t *testing.T) []githubAIAttributionRow
	prsClaim        Claim
	workItemsClaim  Claim
	prsSink         func(driver.Conn) EffectSink
	prsReadback     func(driver.Conn) EffectReadback
	workItemAdapter func(driver.Conn) GitHubWorkItemEffectAdapter
}

func prAttributionScenarios() []prAttributionScenario {
	return []prAttributionScenario{{
		provider: "github", subjects: 2,
		prsEffect: func(t *testing.T) EffectBatch {
			batch, _ := prAttributionPRsEffect(t)
			return batch.Effects[1]
		},
		workItemRows:   githubWorkItemsRouteAttributionRows,
		prsClaim:       prAttributionClaim("github", "prs"),
		workItemsClaim: prAttributionClaim("github", "work-items"),
		prsSink: func(conn driver.Conn) EffectSink {
			return GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: prAttributionLease()}
		},
		prsReadback: func(conn driver.Conn) EffectReadback {
			return GitHubPullRequestSocialClickHouseEffects{Conn: conn, Lease: prAttributionLease()}
		},
		workItemAdapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
			return GitHubAIAttributionClickHouseAdapter{Conn: conn}
		},
	}, {
		provider: "gitlab", subjects: 2,
		prsEffect: func(t *testing.T) EffectBatch {
			batch, _ := glAttributionPRsBatch(t)
			return batch.Effects[2]
		},
		workItemRows:   glAttributionWorkItemsRows,
		prsClaim:       prAttributionClaim("gitlab", "prs"),
		workItemsClaim: prAttributionClaim("gitlab", "work-items"),
		prsSink: func(conn driver.Conn) EffectSink {
			return GitLabPullRequestSocialClickHouseEffects{Conn: conn, Lease: prAttributionLease()}
		},
		prsReadback: func(conn driver.Conn) EffectReadback {
			return GitLabPullRequestSocialClickHouseEffects{Conn: conn, Lease: prAttributionLease()}
		},
		workItemAdapter: func(conn driver.Conn) GitHubWorkItemEffectAdapter {
			return GitLabAIAttributionClickHouseAdapter{Conn: conn}
		},
	}}
}

func prAttributionLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// writeWorkItemsRoute writes the rows the work-items route produced for the
// same pull requests, stamped at ingestedAt, through that route's adapter.
func (scenario prAttributionScenario) writeWorkItemsRoute(
	t *testing.T, store prAttributionStore, ingestedAt time.Time,
) {
	t.Helper()
	rows := scenario.workItemRows(t)
	for index := range rows {
		rows[index].IngestedAt = ingestedAt
	}
	effect, err := effectBatchFromValues("ai_attribution", EffectReadbackRequired, rows)
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: scenario.workItemsClaim.OrgID, Provider: scenario.provider,
		Dataset: "work-items", Generation: scenario.workItemsClaim.GenerationKey(),
		Destination: "ai_attribution", ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if err := scenario.workItemAdapter(store.conn).WriteGitHubWorkItemEffect(store.ctx, identity, effect); err != nil {
		t.Fatal(err)
	}
}

// writePRsRoute writes the prs unit's ai_attribution effect through the sink
// the worker constructs, and requires its readback to answer Exact.
func (scenario prAttributionScenario) writePRsRoute(t *testing.T, store prAttributionStore) {
	t.Helper()
	effect := scenario.prsEffect(t)
	if err := scenario.prsSink(store.conn).WriteEffect(store.ctx, scenario.prsClaim, effect); err != nil {
		t.Fatal(err)
	}
	inspection, err := scenario.prsReadback(store.conn).InspectEffect(store.ctx, scenario.prsClaim, effect)
	if err != nil || inspection != EffectExact {
		t.Fatalf("readback after write: %v err=%v, want exact", inspection, err)
	}
}

// assertReaderSees proves the production reader path answers: the resolved
// view holds one row per pull request and the aiAttributionOverview resolver
// (internal/queryapi/aianalytics) reports the same population.
func (scenario prAttributionScenario) assertReaderSees(t *testing.T, store prAttributionStore, wantRows uint64) {
	t.Helper()
	resolved := store.count(t, "SELECT count() FROM ai_attribution_resolved")
	distinct := store.count(t, "SELECT uniqExact(repo_id, subject_id) FROM ai_attribution_resolved")
	if resolved != scenario.subjects || distinct != scenario.subjects {
		t.Fatalf("resolved rows=%d distinct pull requests=%d, want exactly %d (one per pull request)",
			resolved, distinct, scenario.subjects)
	}
	physical := store.count(t, "SELECT count() FROM ai_attribution FINAL")
	if physical != wantRows {
		t.Fatalf("ai_attribution FINAL rows=%d want %d: a second writer must not add keys", physical, wantRows)
	}
	overview, err := aianalytics.AttributionOverview(
		store.ctx, store.client, prAttributionOrgID,
		model.AIDateRangeInput{
			StartDate: graphqldate.New(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)),
			EndDate:   graphqldate.New(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)),
		}, nil, 50, 0,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !overview.DataAvailable || uint64(overview.TotalAttributed) != scenario.subjects ||
		uint64(len(overview.Rows)) != scenario.subjects {
		t.Fatalf("aiAttributionOverview total=%d rows=%d available=%v, want %d pull requests",
			overview.TotalAttributed, len(overview.Rows), overview.DataAvailable, scenario.subjects)
	}
	for _, row := range overview.Rows {
		if row.Provider != scenario.provider {
			t.Fatalf("overview row provider=%s want %s", row.Provider, scenario.provider)
		}
	}
}

func TestPRsSyncWritesPullRequestAttributionThroughTheProductionReader(t *testing.T) {
	store := startPRAttributionStore(t)
	for _, scenario := range prAttributionScenarios() {
		scenario := scenario
		base := prAttributionNormalizedAt()

		var prsOnlyKeys uint64
		t.Run(scenario.provider+"/prs on, work items off", func(t *testing.T) {
			store.reset(t)
			scenario.writePRsRoute(t, store)
			prsOnlyKeys = store.count(t, "SELECT count() FROM ai_attribution FINAL")
			if prsOnlyKeys == 0 {
				t.Fatal("the prs unit wrote no ai_attribution rows")
			}
			scenario.assertReaderSees(t, store, prsOnlyKeys)
		})

		// Both datasets on: the work-items route runs against the same pull
		// requests and writes no pull-request attribution, so the prs unit is
		// the only writer of every key. A prs unit that crashed between its
		// ai_attribution write and its ledger commit recovers by readback
		// after the work-items unit ran: that readback must still be exact
		// (an ambiguous answer is a terminal unit failure).
		t.Run(scenario.provider+"/both on, work items after prs", func(t *testing.T) {
			store.reset(t)
			scenario.writePRsRoute(t, store)
			scenario.writeWorkItemsRoute(t, store, base.Add(time.Hour))
			scenario.assertPRsReadbackExact(t, store)
			scenario.writePRsRoute(t, store)
			scenario.assertReaderSees(t, store, prsOnlyKeys)
		})

		t.Run(scenario.provider+"/both on, work items before prs", func(t *testing.T) {
			store.reset(t)
			scenario.writeWorkItemsRoute(t, store, base.Add(-time.Hour))
			scenario.writePRsRoute(t, store)
			scenario.assertPRsReadbackExact(t, store)
			scenario.assertReaderSees(t, store, prsOnlyKeys)
		})

		t.Run(scenario.provider+"/work items on, prs off", func(t *testing.T) {
			store.reset(t)
			scenario.writeWorkItemsRoute(t, store, base)
			if rows := store.count(t, "SELECT count() FROM ai_attribution"); rows != 0 {
				t.Fatalf("the work-items route wrote %d pull-request attribution rows, want none", rows)
			}
		})
	}
}

// assertPRsReadbackExact asks the prs unit's readback about its own effect, as
// crash recovery does, and requires the exact answer.
func (scenario prAttributionScenario) assertPRsReadbackExact(t *testing.T, store prAttributionStore) {
	t.Helper()
	got, err := scenario.prsReadback(store.conn).InspectEffect(store.ctx, scenario.prsClaim, scenario.prsEffect(t))
	if err != nil || got != EffectExact {
		t.Fatalf("prs readback after the work-items unit ran: %v err=%v, want exact", got, err)
	}
}

// TestPRsSyncAttributionReadbackAnswersAbsentExactAndConflict pins the three
// readback answers the effect committer recovers by, and that the lease fences
// the readback: a row not yet written is absent, the written row is exact, and
// a newer different version of the same key is a conflict (never silently
// exact).
func TestPRsSyncAttributionReadbackAnswersAbsentExactAndConflict(t *testing.T) {
	store := startPRAttributionStore(t)
	for _, scenario := range prAttributionScenarios() {
		scenario := scenario
		t.Run(scenario.provider, func(t *testing.T) {
			store.reset(t)
			effect := scenario.prsEffect(t)
			inspect := func() EffectInspection {
				t.Helper()
				got, err := scenario.prsReadback(store.conn).InspectEffect(store.ctx, scenario.prsClaim, effect)
				if err != nil {
					t.Fatal(err)
				}
				return got
			}
			if got := inspect(); got != EffectAbsent {
				t.Fatalf("before the write: %v want absent", got)
			}
			scenario.writePRsRoute(t, store)
			if got := inspect(); got != EffectExact {
				t.Fatalf("after the write: %v want exact", got)
			}
			rows, err := decodeEffectRows[githubAIAttributionRow](effect)
			if err != nil {
				t.Fatal(err)
			}
			for index := range rows {
				rows[index].Confidence = 0.5
				rows[index].IngestedAt = rows[index].IngestedAt.Add(time.Hour)
			}
			newer, err := effectBatchFromValues("ai_attribution", EffectReadbackRequired, rows)
			if err != nil {
				t.Fatal(err)
			}
			if err := scenario.prsSink(store.conn).WriteEffect(store.ctx, scenario.prsClaim, newer); err != nil {
				t.Fatal(err)
			}
			if got := inspect(); got != EffectConflict {
				t.Fatalf("after a newer different version: %v want conflict", got)
			}
			lost := GitHubPullRequestSocialClickHouseEffects{
				Conn: store.conn, Provider: scenario.provider,
				Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return providerfoundation.ErrLeaseLost }),
			}
			if _, err := lost.InspectEffect(store.ctx, scenario.prsClaim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
				t.Fatalf("readback with a lost lease: err=%v", err)
			}
		})
	}
}
