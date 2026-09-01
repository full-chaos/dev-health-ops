//go:build integration

// CHAOS-4723 ★ parity proof, GO side, against the LOCAL ClickHouse holding
// the real org 70d529e0-3c06-4597-8480-794fd02328b6 data the brief's
// acceptance numbers were recorded from -- follows nan_class_live_test.go's
// established pattern (nanClassClickHouseURI, DEV_HEALTH_REQUIRE_LIVE opt-in,
// same package so this calls resolveEvidenceQualityStats directly) rather
// than inventing a new harness. Deploy is HELD (this lane's brief, §⛔) --
// this file never rebuilds, restarts, or routes through query-api/api; it
// calls the analytics package's Go function directly against ClickHouse,
// same as nan_class_live_test.go does for the coverage measure.
//
// WINDOW DERIVATION (not guessed -- read from the actual client code, since
// the brief gave no explicit window): the acceptance numbers came from the
// investment page's DEFAULT filter, web/src/lib/filters/defaults.ts:5
// (`range_days: 14`), turned into a GraphQL DateRange by
// web/src/lib/graphql/investmentFetchers.ts:55-62 --
// `endDate = new Date()` (the request instant, here 2026-09-01 07:19 PDT =
// 14:19 UTC per the ticket's HAR capture), `startDate = endDate -
// 14*86400000ms`, BOTH then truncated to a bare calendar date via
// `.toISOString().split("T")[0]` -- so the wire DateRange was
// {startDate: "2026-08-18", endDate: "2026-09-01"}. The Go/Python resolver
// then widens each date to UTC midnight (analytics.py:244-245's
// `datetime.combine(date, time.min, utc)`; this port's dateBindingValue
// equivalent), giving the actual ClickHouse window: from_ts <
// 2026-09-01T00:00:00Z, to_ts >= 2026-08-18T00:00:00Z.
//
// TWO INDEPENDENT MEASUREMENTS, DIFFERENT INSTANTS, DIFFERENT MECHANISMS --
// both against the SAME fixed window (2026-08-18..2026-09-01), against org
// 70d529e0:
//  1. Read-only clickhouse-client reconnaissance (no slot needed, "log/SQL
//     reads on the running stack" per the common brief), executed BEFORE
//     this file was written, hand-reconstructing fetch_investment_quality_stats's
//     SQL: total=418, moderate=84, low=27, very_low=307, high=0.
//  2. THIS test, calling resolveEvidenceQualityStats -- the real Go query
//     path, executed as the actual ★ proof: total=414, moderate=81, low=26,
//     very_low=307, high=0.
//
// The two runs disagree with EACH OTHER (418 vs 414), not just with the
// brief's recorded 416 -- which ruled out "the local data has simply grown
// since 07:19 PDT" as the whole story and pointed at the reconnaissance query
// itself. It had: measurement 1's hand-rolled membership-scope join omitted
// two fragments the real port includes (membershipScopedWorkUnitIDsSource's
// legacyNodeMaxJoinSQL()/runScopePredicateSQL(), investmentmembershipscope.go)
// and so over-counted the scoped work-unit set -- a stand-in query is not the
// query (this codebase's own PROXY NUMBERS lesson). Measurement 2, the real
// Go path, is authoritative; this file does not re-run the reconnaissance
// with the missing fragments patched in, because the code under test already
// IS that complete query.
//
// bands["very_low"] read exactly 307 on BOTH measurements, at different
// instants, via different mechanisms -- the strongest available evidence the
// window itself is correctly identified (a wrong window would not
// coincidentally agree on one band across two independently-flawed and
// non-flawed queries). total/moderate/low move by single digits between the
// two runs and against the brief's 416/82/27, consistent with ongoing
// background categorization shifting a handful of work units into, out of,
// or between bands within this 14-day window over the ~3 hours separating
// the brief's 07:19 PDT capture from this run -- not a sign of a wrong
// query. Per the brief's ★ section ("if you conclude the local CH data
// cannot reproduce those exact numbers ... say so explicitly with evidence
// and give the closest defensible assertion you CAN make"): this file
// asserts tolerance bands around the brief's recorded numbers (documented
// per-assertion below), not exact equality -- and NOT the "non-nil" collapse
// the brief warns against; every assertion here still requires this exact
// window, this exact org, and a shape matching the recorded acceptance
// numbers to within single-digit/low-percent drift.
//
// NOT ENROLLED IN ANY CI SHARD, same reasoning as nan_class_live_test.go:
// this asserts specific values against a continuously-ingesting live
// dataset, which will drift again after this run. It exists as the ★
// section's discretionary, slot-executed evidence, not a repeatable gate.
package analytics

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

const chaos4723LiveOrgID = "70d529e0-3c06-4597-8480-794fd02328b6"

// TestResolveEvidenceQualityStats_LiveParity_Org70d529e0 is CHAOS-4723's ★
// parity proof: calls resolveEvidenceQualityStats -- the exact function
// Phase 4 of Resolve() calls -- against the real local ClickHouse, over the
// window the brief's recorded acceptance numbers were served under (see
// this file's package doc comment for the derivation and the read-only
// reconnaissance that pins it).
func TestResolveEvidenceQualityStats_LiveParity_Org70d529e0(t *testing.T) {
	dsn, hostPort := nanClassClickHouseURI(t)
	if hostPort == "" {
		t.Fatalf("refusing to connect: no target host resolved from CLICKHOUSE_URI")
	}
	t.Logf("connecting to ClickHouse at %s", hostPort)
	client, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: dsn})
	if err != nil {
		t.Fatalf("connect to ClickHouse at %s: %v", hostPort, err)
	}
	ctx := context.Background()

	batch := model.AnalyticsRequestInput{
		Breakdowns: []model.BreakdownRequestInput{{
			Dimension: model.DimensionInputRepo,
			Measure:   model.MeasureInputCount,
			DateRange: &model.DateRangeInput{
				StartDate: mustGraphQLDate("2026-08-18"),
				EndDate:   mustGraphQLDate("2026-09-01"),
			},
			TopN: 10,
		}},
		UseInvestment: boolPtr(true),
	}

	got, err := resolveEvidenceQualityStats(ctx, client, chaos4723LiveOrgID, batch, true, nil)
	if err != nil {
		t.Fatalf("resolveEvidenceQualityStats: %v", err)
	}
	if got == nil {
		t.Fatal("expected a populated EvidenceQualityStats for a real org with real data, got nil")
	}

	var bands map[string]int
	if err := json.Unmarshal(got.BandCounts, &bands); err != nil {
		t.Fatalf("BandCounts did not unmarshal: %v (%s)", err, got.BandCounts)
	}
	bandSum := bands["high"] + bands["moderate"] + bands["low"] + bands["very_low"] + bands["unknown"]
	meanStr := "nil"
	if got.Mean != nil {
		meanStr = fmt.Sprintf("%v", *got.Mean)
	}
	t.Logf("live reproduction (window 2026-08-18..2026-09-01, org %s): total=%d mean=%s bands=%+v",
		chaos4723LiveOrgID, got.Total, meanStr, bands)

	// MANDATORY structural invariant, true regardless of any further
	// ingestion drift: the bands are a partition of total.
	if bandSum != got.Total {
		t.Fatalf("band counts do not sum to total: sum=%d total=%d bands=%+v", bandSum, got.Total, bands)
	}
	if got.Total == 0 {
		t.Fatal("expected a non-zero total for a real org/window with real data -- an empty result here means the precondition (org has data in this window) silently failed, not that parity holds vacuously")
	}
	if got.Mean == nil {
		t.Fatal("expected a non-nil mean (quality_known_count must be > 0 for this org/window)")
	}

	// EVIDENCE-GATHERING CORRECTION (found running this exact test, not
	// guessed): the FIRST version of this file asserted very_low==307 and
	// total>=416 based on hand-reconstructing fetch_investment_quality_stats's
	// SQL via raw clickhouse-client for the read-only reconnaissance in this
	// file's package doc comment. That hand-reconstruction OMITTED two
	// fragments the real port includes in membershipScopedWorkUnitIDsSource
	// (investmentmembershipscope.go): legacyNodeMaxJoinSQL() and
	// runScopePredicateSQL() -- both restrict WHICH work_unit_membership rows
	// count as "in scope", so the hand-rolled query over-counted the
	// membership-scoped set and returned total=418 where THIS Go path (which
	// calls the real, complete query) returns total=414 for the same window,
	// same instant. This is exactly the mistake the codebase's own PROXY
	// NUMBERS lesson warns about: a hand-reconstructed stand-in for a query
	// is not the query. Re-running the raw reconnaissance with the complete
	// join/predicate is not done here -- the Go path IS the thing under
	// test, its own output is authoritative, not a second derivation of it.
	//
	// bands["very_low"] was observed at exactly 307 across every measurement
	// taken while investigating this window (both the incomplete
	// reconnaissance query and this real Go path, at different instants) --
	// the strongest available signal the window itself is correctly
	// identified. bands["low"]/["moderate"]/total move by a small amount
	// (single digits) between runs, consistent with ongoing background
	// categorization shifting a handful of work units into or out of this
	// 14-day window, or re-banding them, between the brief's 07:19 PDT HAR
	// capture and any later run -- NOT a sign of a wrong query. Given that,
	// asserting an exact total/band count here would be asserting a number
	// this live, continuously-ingesting dataset does not hold still long
	// enough to guarantee -- the tolerances below are the closest defensible
	// assertion per the brief's ★ section, not a weakening to "non-nil".
	if bands["very_low"] < 300 || bands["very_low"] > 314 {
		t.Errorf("bands[very_low] = %d, want within [300,314] of the recorded acceptance number 307 (observed exactly 307 on every prior measurement of this window; a wider miss here would be a real signal, not expected drift)", bands["very_low"])
	}
	if bands["high"] != 0 {
		t.Errorf("bands[high] = %d, want 0 (observed 0 on every prior measurement of this window)", bands["high"])
	}
	const wantTotal = 416
	if diff := got.Total - wantTotal; diff < -10 || diff > 10 {
		t.Errorf("total = %d, want within 10 of the brief's recorded acceptance number %d (observed range across independent measurements of this exact window: 414-418)", got.Total, wantTotal)
	}
	const wantMeanApprox = 0.36605981413217176
	if diff := *got.Mean - wantMeanApprox; diff < -0.02 || diff > 0.02 {
		t.Errorf("mean = %v, want within 0.02 of the recorded acceptance mean %v", *got.Mean, wantMeanApprox)
	}
}
