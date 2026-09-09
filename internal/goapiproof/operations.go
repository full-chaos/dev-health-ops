package goapiproof

import (
	"fmt"
	"sort"
)

// Window is the request window every windowed operation is asked for.
//
// It is a PARAMETER, not a constant, and it is folded into every
// receipt's request_identity, because a proof run is evidence for one
// request -- "the same operation over a different window" is a different
// request and must not silently reuse another run's verdict. The defaults
// below reproduce the window the 2026-09-07 live measurement used
// (lane-goapi-enable's go_api_request_payloads.py), so a re-run of that
// measurement is reproducible rather than "whatever today happens to be".
//
// Deliberately NOT derived from time.Now(): a rolling window makes two
// runs incomparable and makes a mismatch impossible to reproduce. The
// cost is that the defaults age; an operator moves them with the flags.
type Window struct {
	SinceUTC  string // RFC3339, e.g. "2026-06-01T00:00:00Z"
	UntilUTC  string // RFC3339
	SinceDate string // "YYYY-MM-DD"
	UntilDate string // "YYYY-MM-DD"
	WeekStart string // "YYYY-MM-DD"
}

// DefaultWindow reproduces the 2026-09-07 measurement's window.
func DefaultWindow() Window {
	return Window{
		SinceUTC:  "2026-06-01T00:00:00Z",
		UntilUTC:  "2026-09-01T00:00:00Z",
		SinceDate: "2026-06-01",
		UntilDate: "2026-09-01",
		WeekStart: "2026-09-01",
	}
}

// Validate refuses an incomplete window rather than sending an operation
// a zero-valued date and reading the resulting validation rejection as a
// parity finding.
func (w Window) Validate() error {
	for name, value := range map[string]string{
		"since-utc": w.SinceUTC, "until-utc": w.UntilUTC,
		"since-date": w.SinceDate, "until-date": w.UntilDate, "week-start": w.WeekStart,
	} {
		if value == "" {
			return fmt.Errorf("goapiproof: window field %s is empty", name)
		}
	}
	return nil
}

// OperationSpec is one registered operation's committed request shape and
// its declared parity configuration.
type OperationSpec struct {
	// Variables builds the GraphQL `variables` object for this operation.
	//
	// Ported from lane-goapi-enable's go_api_request_payloads.py, which
	// carries the lesson these shapes encode: an input object's required
	// fields are NOT interchangeable with the operation's top-level
	// variables. An earlier mechanical build put `orgId` inside
	// `CapacityForecastInput` (which has no such field); every one of
	// those requests failed validation, fell back to Python, and looked
	// exactly like a routing failure. Check the SDL, never infer.
	Variables func(orgID string, window Window) map[string]any

	// ResponseRoot is the GraphQL field the operation's REGISTERED
	// DOCUMENT selects at the top of `data` -- which is not always the
	// operation name. flowMatrix, investmentBreakdown and investmentFull
	// all select `analytics`, because query_route.go's operation keys are
	// its own Mux/PostgresSwitch keys, chosen to disambiguate several
	// registered documents that share a root field.
	//
	// It exists so every declared parity path can be checked against the
	// subtree it will actually be compared against. Assigning declarations
	// by operation NAME produced paths that matched nothing -- which fails
	// the run correctly, but for a reason nobody would have understood
	// from the failure.
	ResponseRoot string

	// Parity is this operation's declared comparator configuration.
	Parity Options
}

// volatileReason documents one excluded field. Kept as a named constant
// so the reason travels with every operation that cites it rather than
// being retyped (and drifting) per entry.
const volatileForecastIdentity = "freshly generated per request: an identical request produced Go forecastId=33fb9f32... / Python 78296c67... with computedAt ~350ms apart (CHAOS-5425, 2026-09-07 live measurement). Excluded per CHAOS-4381; scoped to the forecast operations only, and NOT a licence to exclude any other field."

// operationSpecs is the committed per-operation table.
//
// It is a table, not a default plus overrides: an operation missing from
// here is a REFUSAL (see SpecFor), never a request built from a guessed
// shape. A guessed shape fails validation, falls back to Python, and
// produces a receipt that looks like a routing defect.
//
// Three declaration kinds live in each entry's Parity, and all three obey
// the same rule -- a declaration that matches nothing FAILS the run:
//
//   - VolatileFields: values regenerated per request (forecastId,
//     computedAt). Populated below from the 2026-09-07 live measurement.
//   - FloatTierB: leaves whose value derives from a ClickHouse FLOATING-POINT
//     aggregate, per CHAOS-5451. Populated below from lane-goapi-parity's
//     read of the actual SQL, with the source line for each.
//   - BaselineDefects: differences where PYTHON is wrong and Go is right
//     (CHAOS-5447/5448/5449), each citing its ticket, evidence path and the
//     field subtree it covers.
//
// Which operation a declaration belongs to is decided by the operation's
// REGISTERED DOCUMENT, not by its name: several operations select a
// differently-named root field (investmentBreakdown and investmentFull both
// select `analytics`; flowMatrix does too), and evidenceQualityStats is in
// investmentBreakdown's document but NOT investmentFull's. Assigning by
// name would have produced entries that match nothing -- which fails the
// run, correctly, but for a reason nobody would have understood.
var operationSpecs = map[string]OperationSpec{
	"capacityForecast": {
		ResponseRoot: "capacityForecast",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{VolatileFields: map[string]string{
			"data.capacityForecast.forecastId": volatileForecastIdentity,
			"data.capacityForecast.computedAt": volatileForecastIdentity,
		}},
	},
	// capacityForecasts (the LIST) declares no Tier-B and no volatile
	// fields: its resolver reads stored columns back rather than
	// recomputing, so forecastId/computedAt are not per-request values and
	// throughputMean/throughputStddev are not engine aggregates
	// (lane-goapi-parity, CHAOS-5451 read of the resolver). An earlier
	// draft copied the SINGULAR operation's volatile pair onto it; those
	// entries would have matched nothing -- the list nests under
	// edges.node -- and failed every run as stale.
	"capacityForecasts": {
		ResponseRoot: "capacityForecasts",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5450",
			Reason: "Python's LIST path stringifies the driver's NAIVE datetime (resolvers/capacity.py:24) and emits a space-separated timestamp with no offset, while its own SINGULAR path (capacity.py:50) stringifies a tz-aware one and does emit an offset -- Python is internally inconsistent with itself and stays frozen. Note the state this describes: as of this entry NEITHER plane emits the RFC 3339 form the schema documents (schema.graphql:704-705 says isoformat; :430/:2239 type the field String!, so nothing enforced it), and Go's own capacity resolvers are being fixed to emit it under R55 -- so this covers a real divergence today and continues to after that fix, but it must not be read as a claim that Go is already RFC 3339. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5450/analysis.txt",
			Paths:  []string{"data.capacityForecasts.edges.node.computedAt"},
		}}},
	},
	"cognitiveLoad": {
		ResponseRoot: "cognitiveLoad",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"teamId": nil, "repoId": nil,
			}}
		},
	},
	"complexityTimeseries": {
		ResponseRoot: "complexityTimeseries",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"granularity": "DAY", "scope": "REPO",
				"repoIds": nil, "teamIds": nil, "limit": 500,
			}}
		},
	},
	"featureFlags": {
		ResponseRoot: "featureFlags",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{
				"orgId": orgID, "provider": nil, "project": nil,
				"includeArchived": false, "limit": 100,
			}
		},
	},
	"flowMatrix": {
		ResponseRoot: "analytics",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "batch": map[string]any{
				"flowMatrix": map[string]any{
					"dimension": "WORK_TYPE", "measure": "COUNT",
					"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
					"maxNodes":  50, "maxEdges": 200,
				},
			}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5448",
			Reason: "Python omits FINAL on work_item_cycle_times (templates.py:304/:397) where Go has it (flowmatrix.go:732/:802), so Python counts superseded ReplacingMergeTree row versions and its answer converges onto Go's only after a background merge. Go is correct. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5448/repro.txt",
			Paths:  []string{"data.analytics.flowMatrix.nodes.value", "data.analytics.flowMatrix.edges.value"},
		}}},
	},
	// hotspots.riskScore is deliberately NOT Tier B, and the reasoning is
	// worth keeping: it is a STORED Float64 column in file_hotspot_daily
	// (migration 007:48) that both planes read via argMax, so no float
	// arithmetic happens and CHAOS-5451's engine nondeterminism does not
	// apply. The measured divergence is ~1e-2 relative -- seven orders of
	// magnitude above a 1e-9 tolerance -- so Tier B would not have excused
	// it anyway; it is a real defect, not last-bit noise
	// (lane-goapi-parity, correcting the field list it was handed).
	"hotspots": {
		ResponseRoot: "hotspots",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"repoIds": nil, "teamIds": nil, "limit": 50,
			}}
		},
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5447",
			Reason: "Python argMaxes on computed_at ALONE where Go keys on (day, computed_at), so it can select a different physical row for the same file. Two distinct halves, only one of which is a tie: (a) INVERSION -- an older day recomputed later carries a newer computed_at and wins outright, no tie involved; (b) TIE -- on an identical computed_at Python falls back to ClickHouse's internal row order, measured at 31,072 of 100,000 seeded files resolving to the OLDER day against 0 of 100,000 for Go. Live shape: 17 of the 48 files present on both sides disagreed on churn in BOTH directions, and 22 of those 48 had identical churn/blame/cyclomatic but a different riskScore -- both-directions is the tell that this is row SELECTION, not arithmetic. Go is correct. Covers the whole rows subtree because a different row differs in every field, not only riskScore. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5447/repro.txt",
			Paths:  []string{"data.hotspots.rows"},
		}}},
	},
	// Both select the `analytics` root field, but NOT the same subtree:
	// evidenceQualityStats is in investmentBreakdown's registered document
	// and absent from investmentFull's, so declaring it on both would leave
	// a permanently-stale entry on one of them.
	"investmentBreakdown": {
		ResponseRoot: "analytics",
		Variables:    investmentVariables,
		Parity: Options{FloatTierB: map[string]string{
			"data.analytics.evidenceQualityStats.mean":   "avgIf(evidence_quality) -- ClickHouse float aggregate, order-nondeterministic (investmentquality.go:250, CHAOS-5451)",
			"data.analytics.evidenceQualityStats.stddev": "stddevPopIf(evidence_quality) -- ClickHouse float aggregate; 20 identical runs over 5M rows gave 9 distinct values (investmentquality.go:251, CHAOS-5451)",
			"data.analytics.breakdowns.items.value":      "on the investment path MeasureCount compiles to SUM(subcategory_kv.2), a FLOAT sum (validate.go:245-246). Derived from CHAOS-5451's rule rather than an observed divergence, and this table's payload always sets useInvestment=true; on the non-investment path the same measure is an exact integer sum",
		}},
	},
	// sankey.coverage.teamCoverage/.repoCoverage are deliberately NOT
	// declared, and the reason is path-dependent rather than permanent: on
	// the committed WORK_TYPE payload they are count()/countIf() integers
	// (sankeycoverage.go:128-131) and Tier A is correct. If a future
	// registered document adds a REPO dimension, useRepoAllocation flips on
	// (investment.go:501) and those same two leaves become sum()/sumIf()
	// float sums (sankeycoverage.go:192-195) -- Tier B at that point, not
	// before (lane-goapi-parity, CHAOS-5451).
	"investmentFull": {
		ResponseRoot: "analytics",
		Variables:    investmentVariables,
		Parity: Options{FloatTierB: map[string]string{
			"data.analytics.breakdowns.items.value": "on the investment path MeasureCount compiles to SUM(subcategory_kv.2), a FLOAT sum (validate.go:245-246). Rule-derived, not observed (CHAOS-5451)",
			"data.analytics.sankey.nodes.value":     "CompileSankey calls the same dbExpression as breakdowns, so on the investment path this is the same SUM(subcategory_kv.2) float sum; subcategory_kv ARRAY JOINs a Map(String, Float64) column (investment.go:514, migration 017:12). Rule-derived, not observed (CHAOS-5451)",
			"data.analytics.sankey.edges.value":     "same float sum as sankey.nodes.value -- one dbExpression, one compiled measure (validate.go:245-246, investment.go:514). Rule-derived, not observed (CHAOS-5451)",
		}},
	},
	"operatingReview": {
		ResponseRoot: "operatingReview",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{
				"weekStart": w.WeekStart, "teamId": nil,
			}}
		},
		Parity: Options{FloatTierB: map[string]string{
			"data.operatingReview.sections.metrics.value":            "avg()/sum() over Float64 (operatingreview.go:413-418,483-485,562-568,641,695,736,780,822,880) -- ClickHouse float aggregate, order-nondeterministic (CHAOS-5451). This is why two identical requests seconds apart on the SAME plane disagreed",
			"data.operatingReview.sections.metrics.delta.value":      "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.priorValue": "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.absolute":   "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
			"data.operatingReview.sections.metrics.delta.percent":    "derived from the same float aggregates as sections.metrics.value (CHAOS-5451)",
		}},
	},
	"reviewEdges": {
		ResponseRoot: "reviewEdges",
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"repoIds": nil, "limit": 500,
			}}
		},
	},
	// The overlay `threshold` fields, estimateCoverage.ratio and
	// rollingWindows.meanWeeklyThroughput are deliberately absent: the
	// thresholds are hardcoded constants (kernel.go:70), and the other two
	// are integer sums divided once in Go. All three are exact, and a
	// tolerance on an exact field excuses a real defect
	// (lane-goapi-parity, CHAOS-5451).
	"throughputForecast": {
		ResponseRoot: "throughputForecast",
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{
			VolatileFields: map[string]string{
				"data.throughputForecast.forecastId": volatileForecastIdentity,
				"data.throughputForecast.computedAt": volatileForecastIdentity,
			},
			FloatTierB: map[string]string{
				"data.throughputForecast.reviewBottleneck.value": "avg(pr_first_review_p50_hours) -- ClickHouse float aggregate (throughputforecast/clickhouse.go:393, CHAOS-5451)",
				"data.throughputForecast.reviewBottleneck.score": "derived from reviewBottleneck.value (CHAOS-5451)",
				"data.throughputForecast.wipCongestion.value":    "avg(wip_count_end_of_day) -- ClickHouse float aggregate (clickhouse.go:169, CHAOS-5451)",
				"data.throughputForecast.wipCongestion.score":    "derived from wipCongestion.value (CHAOS-5451)",
				"data.throughputForecast.incidentLoad.value":     "sum(incidents_count)/weeks -- ClickHouse float aggregate (clickhouse.go:490, CHAOS-5451)",
				"data.throughputForecast.incidentLoad.score":     "derived from incidentLoad.value (CHAOS-5451)",
				"data.throughputForecast.primaryRisk.value":      "a copy of whichever overlay is primary, so it inherits that overlay's float aggregate (CHAOS-5451)",
				"data.throughputForecast.primaryRisk.score":      "a copy of whichever overlay is primary (CHAOS-5451)",
				"data.throughputForecast.staleWip.p50AgeHours":   "avg(wip_age_p50_hours) -- ClickHouse float aggregate (clickhouse.go:237, CHAOS-5451)",
				"data.throughputForecast.staleWip.p90AgeHours":   "avg(wip_age_p90_hours) -- ClickHouse float aggregate (clickhouse.go:238, CHAOS-5451)",
			},
		},
	},
	"workGraphArtifacts": {Variables: workGraphVariables, ResponseRoot: "workGraphArtifacts"},
	"workGraphEdges": {
		ResponseRoot: "workGraphEdges",
		Variables:    workGraphVariables,
		Parity: Options{BaselineDefects: []BaselineDefect{{
			Ticket: "CHAOS-5449",
			Reason: "Python's un-deduped read returned 1000 rows carrying only 738 distinct edgeIds; Go's argMax dedup returned 1000 distinct edges and is a strict superset. Go is correct. Evidence: /var/lib/oci-cache/lane-scratch/lane-goapi-parity/5449/analysis.txt",
			Paths:  []string{"data.workGraphEdges.edges"},
		}}},
	},
	"workGraphFlow": {Variables: workGraphVariables, ResponseRoot: "workGraphFlow"},
}

// investmentVariables is shared by investmentBreakdown and investmentFull.
//
// `useInvestment` is a field of AnalyticsRequestInput, NOT of
// BreakdownRequestInput (whose fields are exactly dimension, measure,
// dateRange, topN). Nesting it one level deeper failed validation and fell
// back to Python -- the same wrong-object mistake as putting `orgId`
// inside an input that has no such field.
func investmentVariables(orgID string, w Window) map[string]any {
	return map[string]any{"orgId": orgID, "batch": map[string]any{
		"breakdowns": []any{map[string]any{
			"dimension": "WORK_TYPE", "measure": "COUNT",
			"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
			"topN":      10,
		}},
		"useInvestment": true,
	}}
}

func workGraphVariables(orgID string, _ Window) map[string]any {
	return map[string]any{"orgId": orgID, "filters": map[string]any{}}
}

// SpecFor returns the committed spec for an operation, or an error naming
// the operation. A missing spec is a REFUSAL with a named reason, never a
// skip: an operation the running binary registers but this table does not
// cover is exactly the gap a silent skip would hide.
func SpecFor(operation string) (OperationSpec, error) {
	spec, ok := operationSpecs[operation]
	if !ok {
		return OperationSpec{}, fmt.Errorf("goapiproof: no committed request payload for operation %q -- add one to operations.go rather than skipping it", operation)
	}
	return spec, nil
}

// KnownOperations lists every operation this table covers, sorted.
func KnownOperations() []string {
	names := make([]string, 0, len(operationSpecs))
	for name := range operationSpecs {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// AssertCoverage checks this table against the operation set the RUNNING
// query-api reports at GET /registry -- the only authority on what is
// actually registered. Both directions are errors:
//
//   - registered but uncovered: prove would silently measure fewer than
//     all operations, which is the "a measurement that did not happen"
//     failure D15/R4 exists to stop.
//   - covered but unregistered: a stale entry here, which reads as
//     coverage while proving nothing.
func AssertCoverage(registered []string) error {
	registeredSet := make(map[string]bool, len(registered))
	for _, name := range registered {
		registeredSet[name] = true
	}

	var uncovered, stale []string
	for _, name := range registered {
		if _, ok := operationSpecs[name]; !ok {
			uncovered = append(uncovered, name)
		}
	}
	for name := range operationSpecs {
		if !registeredSet[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(uncovered)
	sort.Strings(stale)

	switch {
	case len(uncovered) > 0 && len(stale) > 0:
		return fmt.Errorf("goapiproof: request-payload table disagrees with the running registry: uncovered=%v stale=%v", uncovered, stale)
	case len(uncovered) > 0:
		return fmt.Errorf("goapiproof: the running query-api registers operations this table cannot build a request for: %v", uncovered)
	case len(stale) > 0:
		return fmt.Errorf("goapiproof: this table covers operations the running query-api does not register: %v", stale)
	}
	return nil
}
