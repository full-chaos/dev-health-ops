// Package finite is the ONE write/serialization boundary for non-finite and
// otherwise-undefined metric values (CHAOS-4806, ruling R73, chris verbatim
// 18:5xZ: "Use correctness over parity there, so null makes sense but
// shouldn't prevent computation or wreck a window of metrics."):
//
//   - an undefined value (a NaN or +-Inf produced or received, or a value a
//     caller already knows is undefined -- a zero/empty denominator) becomes
//     NULL for THAT FIELD ONLY, tagged with a bounded reason code and counted
//     by family+field+reason (never a log line alone);
//   - the rest of the row, the family, the partition and the window keep
//     computing and writing regardless -- nothing here ever refuses a write;
//   - nothing here ever lets a NaN/+-Inf token reach the wire, ClickHouse or
//     JSON, and 0.0 never stands in for undefined.
//
// Python parity is explicitly NOT a goal here: where the Python plane emits
// bare NaN/Infinity json.dumps tokens (allow_nan=True), or substitutes 0.0
// for an empty average, that is a baseline_defect (R60) -- pin it as such in
// differential fixtures rather than mirroring it.
//
// This package is deliberately free of ClickHouse/GraphQL/otel dependencies
// so every writer and serializer across both the worker and query-api
// binaries can import it without pulling in the other's stack; each binary
// registers MetricsSource() with its own health.MetricsSource registry
// (see cmd/dev-health-worker/dependencies.go) to expose the counter.
package finite

import (
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
)

// Reason is a bounded, closed vocabulary of WHY a value was treated as
// undefined -- never a free-form string, so telemetry cardinality stays
// fixed regardless of how many call sites adopt this package.
type Reason string

const (
	// ReasonNaN: the value itself is NaN (math.IsNaN).
	ReasonNaN Reason = "nan"
	// ReasonPositiveInf: the value itself is +Inf.
	ReasonPositiveInf Reason = "positive_infinity"
	// ReasonNegativeInf: the value itself is -Inf.
	ReasonNegativeInf Reason = "negative_infinity"
	// ReasonUndefinedInput: the caller already knows the value is undefined
	// before it would even compute to NaN -- a zero/empty denominator, or an
	// average/percentile taken over zero present rows -- and is nulling it
	// directly rather than routing a computed NaN through Check.
	ReasonUndefinedInput Reason = "undefined_input"
)

// Check classifies value: ("", true) when it is safe to write as-is, or
// (reason, false) when it is a NaN or an infinity. Every other function in
// this package is built on this one predicate.
func Check(value float64) (Reason, bool) {
	switch {
	case math.IsNaN(value):
		return ReasonNaN, false
	case math.IsInf(value, 1):
		return ReasonPositiveInf, false
	case math.IsInf(value, -1):
		return ReasonNegativeInf, false
	default:
		return "", true
	}
}

// Finite reports whether value is safe to write as-is. It performs no
// telemetry -- use NullIfNonFinite/JSONLiteral at an actual write or
// serialization boundary instead, so a boundary trip is always counted.
func Finite(value float64) (float64, bool) {
	if _, ok := Check(value); ok {
		return value, true
	}
	return 0, false
}

// NullIfNonFinite is the ClickHouse-row-writer half of the boundary: it
// returns a pointer to value when value is finite (write it to the
// Nullable(Float64) column), or nil when it is not (write ClickHouse NULL
// for that ONE field) -- recording the trip against family/field so the
// row, its family, its partition and its window keep writing regardless.
func NullIfNonFinite(family, field string, value float64) *float64 {
	reason, ok := Check(value)
	if ok {
		v := value
		return &v
	}
	observe(family, field, reason)
	return nil
}

// Undefined records a boundary trip for a value the caller already knows is
// undefined (a zero/empty denominator, an average over zero present rows,
// etc. -- R73's "zero/empty denominator" clause) and returns nil, so the
// caller can write ClickHouse NULL / a nullable field's zero value without
// first synthesizing a NaN just to run it through Check. Always uses
// ReasonUndefinedInput.
func Undefined(family, field string) *float64 {
	observe(family, field, ReasonUndefinedInput)
	return nil
}

// JSONLiteral is the JSON-serialization half of the boundary: it calls
// render(value) and returns that string when value is finite, or the bare
// token "null" -- valid JSON, unlike Python's own json.dumps(allow_nan=True)
// "NaN"/"Infinity"/"-Infinity" tokens, a baseline_defect (R60) never
// mirrored here -- recording the trip against family/field either way.
// render is only ever called with a finite value.
func JSONLiteral(family, field string, value float64, render func(float64) string) string {
	reason, ok := Check(value)
	if !ok {
		observe(family, field, reason)
		return "null"
	}
	return render(value)
}

// DropNonFinite returns values with every NaN/+-Inf entry removed, recording
// one boundary trip per dropped entry against family/field -- for a
// percentile, mean, or any other order-statistic/aggregate helper that must
// never let a single bad input poison, or propagate a NaN through, the
// whole computation. The row/family/partition/window still compute over
// whatever remains, per R73 -- an all-non-finite (or empty) input drops to
// an empty slice, which callers must handle as "no data" (e.g. via
// Undefined), never as a reason to refuse the rest of the write.
func DropNonFinite(family, field string, values []float64) []float64 {
	clean := make([]float64, 0, len(values))
	for _, v := range values {
		if reason, ok := Check(v); ok {
			clean = append(clean, v)
		} else {
			observe(family, field, reason)
		}
	}
	return clean
}

// --- telemetry -------------------------------------------------------------

type counterKey struct {
	family string
	field  string
	reason Reason
}

var (
	countsMu sync.Mutex
	counts   = map[counterKey]uint64{}
)

func observe(family, field string, reason Reason) {
	countsMu.Lock()
	counts[counterKey{family, field, reason}]++
	countsMu.Unlock()
}

// Count returns the current boundary-trip count for (family, field,
// reason) -- exported so tests in other packages can assert the counter
// actually fired, without a hidden reset between test cases.
func Count(family, field string, reason Reason) uint64 {
	countsMu.Lock()
	defer countsMu.Unlock()
	return counts[counterKey{family, field, reason}]
}

// Metrics is the health.MetricsSource half of this package
// (WritePrometheus(io.Writer) error), mirroring
// internal/jobs/metrics/daily/benchmarking.RowsWrittenMetrics's shape so
// every binary registers it the same way.
type Metrics struct{}

var metricsSingleton = &Metrics{}

// MetricsSource returns the process-wide singleton; construct it via this
// function, never directly.
func MetricsSource() *Metrics { return metricsSingleton }

// WritePrometheus implements health.MetricsSource.
func (m *Metrics) WritePrometheus(output io.Writer) error {
	if m == nil {
		return nil
	}
	if _, err := io.WriteString(output,
		"# HELP dev_health_metrics_finite_boundary_nonfinite_total Undefined metric values (NaN, +-Inf, or a caller-known zero/empty denominator) converted to NULL at the write/serialization boundary instead of reaching the wire, by family, field and reason (CHAOS-4806, ruling R73).\n"+
			"# TYPE dev_health_metrics_finite_boundary_nonfinite_total counter\n"); err != nil {
		return err
	}
	countsMu.Lock()
	keys := make([]counterKey, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	snapshot := make(map[counterKey]uint64, len(counts))
	for k, v := range counts {
		snapshot[k] = v
	}
	countsMu.Unlock()

	// Deterministic order so /metrics output (and any test scraping it) is
	// stable across runs.
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].family != keys[j].family {
			return keys[i].family < keys[j].family
		}
		if keys[i].field != keys[j].field {
			return keys[i].field < keys[j].field
		}
		return keys[i].reason < keys[j].reason
	})
	for _, k := range keys {
		if _, err := fmt.Fprintf(output,
			"dev_health_metrics_finite_boundary_nonfinite_total{family=%q,field=%q,reason=%q} %d\n",
			k.family, k.field, k.reason, snapshot[k]); err != nil {
			return err
		}
	}
	return nil
}
