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
//   - FloatTierB: merged floating-point aggregates, per CHAOS-5451's
//     measured ClickHouse thread-order nondeterminism. EMPTY today ON
//     PURPOSE: lane-goapi-parity owns the field list with source lines and
//     an evidence path, and inventing entries ahead of that evidence would
//     relax fields nobody measured -- while an invented entry that matched
//     nothing would fail every run. Add them here, each with its written
//     reason, when that list arrives.
//   - BaselineDefects: differences where PYTHON is wrong and Go is right
//     (CHAOS-5448, CHAOS-5450). EMPTY today for the same reason: the exact
//     field paths come from lane-goapi-parity. Until they land, those
//     differences are recorded as ordinary mismatches -- which is the
//     honest state, not a gap being papered over.
var operationSpecs = map[string]OperationSpec{
	"capacityForecast": {
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{VolatileFields: map[string]string{
			"data.capacityForecast.forecastId": volatileForecastIdentity,
			"data.capacityForecast.computedAt": volatileForecastIdentity,
		}},
	},
	"capacityForecasts": {
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "filters": map[string]any{}}
		},
		Parity: Options{VolatileFields: map[string]string{
			"data.capacityForecasts.forecastId": volatileForecastIdentity,
			"data.capacityForecasts.computedAt": volatileForecastIdentity,
		}},
	},
	"cognitiveLoad": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"teamId": nil, "repoId": nil,
			}}
		},
	},
	"complexityTimeseries": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"granularity": "DAY", "scope": "REPO",
				"repoIds": nil, "teamIds": nil, "limit": 500,
			}}
		},
	},
	"featureFlags": {
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{
				"orgId": orgID, "provider": nil, "project": nil,
				"includeArchived": false, "limit": 100,
			}
		},
	},
	"flowMatrix": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "batch": map[string]any{
				"flowMatrix": map[string]any{
					"dimension": "WORK_TYPE", "measure": "COUNT",
					"dateRange": map[string]any{"startDate": w.SinceDate, "endDate": w.UntilDate},
					"maxNodes":  50, "maxEdges": 200,
				},
			}}
		},
	},
	"hotspots": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceUtc": w.SinceUTC, "untilUtc": w.UntilUTC,
				"repoIds": nil, "teamIds": nil, "limit": 50,
			}}
		},
	},
	"investmentBreakdown": {Variables: investmentVariables},
	"investmentFull":      {Variables: investmentVariables},
	"operatingReview": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{
				"weekStart": w.WeekStart, "teamId": nil,
			}}
		},
	},
	"reviewEdges": {
		Variables: func(orgID string, w Window) map[string]any {
			return map[string]any{"input": map[string]any{
				"orgId": orgID, "sinceDate": w.SinceDate, "untilDate": w.UntilDate,
				"repoIds": nil, "limit": 500,
			}}
		},
	},
	"throughputForecast": {
		Variables: func(orgID string, _ Window) map[string]any {
			return map[string]any{"orgId": orgID, "input": map[string]any{}}
		},
		Parity: Options{VolatileFields: map[string]string{
			"data.throughputForecast.forecastId": volatileForecastIdentity,
			"data.throughputForecast.computedAt": volatileForecastIdentity,
		}},
	},
	"workGraphArtifacts": {Variables: workGraphVariables},
	"workGraphEdges":     {Variables: workGraphVariables},
	"workGraphFlow":      {Variables: workGraphVariables},
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
