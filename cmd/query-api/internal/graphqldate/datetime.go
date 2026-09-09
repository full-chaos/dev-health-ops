package graphqldate

import (
	"fmt"
	"time"
)

// RFC3339UTC renders an instant as RFC 3339, "T"-separated, always carrying
// an explicit "+00:00" offset.
//
// SCOPE, stated precisely because an earlier draft of this comment
// overclaimed and a review caught it: this is the canonical wire form for
// the FORECAST timestamp fields R55 ruled on — CapacityForecast.computedAt
// (singular and list) and ThroughputForecast.computedAt. It is NOT a
// universal rule for every String-typed timestamp field in the schema.
//
// The known, DELIBERATE exception is featureflags.isoformatUTC
// (cmd/query-api/internal/featureflags/featureflags.go:99-126), which
// renders FeatureFlagItem.createdAt/archivedAt WITHOUT any offset. That is
// correct there and must not be "unified" into this helper: the Python
// producer receives a NAIVE datetime from clickhouse_connect for those
// columns, so its isoformat() genuinely emits no suffix, and a live
// ClickHouse precedent test asserts exactly that. Routing those fields
// through RFC3339UTC would ADD an offset Python never emits and break a
// proven parity contract. R55 ruled on the forecast fields only; changing
// the feature-flag wire format is a separate public-API decision that
// nobody has made.
//
// WHY THIS EXISTS (CHAOS-5450, team-lead ruling R55). Three sibling fields
// used to render the same kind of value three different ways, and the
// 2026-09-07 Go/Python parity run surfaced it:
//
//	capacityForecasts (list)     "2026-09-07 04:00:18.650000"        Python: no offset at all
//	capacityForecast (singular)  "2026-09-07 04:00:18.650000+00:00"  space separator
//	throughputForecast           "2026-09-07T04:00:18.934060+00:00"  "T" separator
//
// The divergence was never enforced away because these fields are typed
// `String!` in the shipped schema (contracts/graphql/v1/schema.graphql:430
// and :2239), not `DateTime` -- so nothing at the API boundary constrained
// the format and all three shapes were equally "valid". The documented
// intent is a DateTime (.github/docs-legacy/product/capacity-planning.md),
// and the `DateTime` scalar is documented as isoformat
// (schema.graphql:704-705), which none of the three space-separated forms
// satisfied.
//
// R55 picks the "T"-separated offset form as canonical because it was
// already what throughputForecast emitted on BOTH planes and what the web
// client's own fixture assumes (web/src/components/capacity/
// ForecastCard.test.tsx). Python is frozen and keeps its own shapes; the
// difference is recorded as the CHAOS-5450 baseline defect in the proof
// comparator rather than chased.
//
// FRACTIONAL-SECOND RULE, deliberately hand-rolled rather than handed to a
// Go layout: the fraction is SIX digits when the microsecond is non-zero
// and ENTIRELY ABSENT when it is zero. That is Python's `datetime.isoformat()`
// rule, and no Go layout expresses it -- ".999999" trims trailing zeros and
// would render a microsecond of 123000 as ".123", while ".000000" would emit
// a bogus ".000000" on a whole second. Sub-microsecond precision is
// truncated, matching Python's microsecond resolution.
//
// A non-UTC input is CONVERTED, never relabelled, so the "+00:00" suffix is
// always true of the value it is attached to.
//
// FOLLOW-UP, not done here: the underlying fix is to type these fields
// `DateTime` in the schema and let the scalar carry the contract, at which
// point this helper becomes the scalar's marshaler rather than a per-resolver
// call. That is a schema change with a web-side blast radius and is filed
// separately.
// WireFormatLabel names the format RFC3339UTC produces, for the `served`
// telemetry on every resolver that uses it.
//
// Without it a silent revert of the formatter — the exact regression the
// tests here pin — is invisible in production logs: the resolver keeps
// serving, keeps logging `served`, and only the string shape changes. One
// low-cardinality constant field makes the wire format greppable per
// request. Change it in the same edit as the format itself, never
// separately, or the label starts lying.
const WireFormatLabel = "rfc3339_utc"

func RFC3339UTC(moment time.Time) string {
	utc := moment.UTC()
	rendered := utc.Format("2006-01-02T15:04:05")
	if microseconds := utc.Nanosecond() / 1000; microseconds != 0 {
		rendered += fmt.Sprintf(".%06d", microseconds)
	}
	return rendered + "+00:00"
}
