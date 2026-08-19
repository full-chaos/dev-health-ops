package providersync

import (
	"log/slog"
	"time"
)

// futureWatermarkWriteSkewTolerance mirrors Python's
// _FUTURE_WRITE_SKEW_TOLERANCE_SECONDS (src/dev_health_ops/sync/watermarks.py):
// clocks routinely drift by seconds, so a clamp inside this tolerance is
// silent and anything beyond it is warned about.
const futureWatermarkWriteSkewTolerance = 60 * time.Second

// normalizeWatermarkWrite is THE write-boundary normalizer for every Go
// watermark write. It is the exact mirror of Python's
// `_normalize_watermark_write` (CHAOS-3412 clause C10(c)).
//
// A watermark is a COVERAGE CLAIM -- it asserts "everything up to here has
// been read" -- so it has TWO ceilings, and BOTH are required:
//
//  1. now. A watermark can never sit in the future whatever a provider
//     reports. Several Go routes prefer a provider-derived watermark over the
//     claim's window end (github_prs_route.go, gitlab_incidents_route.go,
//     jira_incidents_route.go, launchdarkly_route.go, native_rest.go), so a
//     single skewed source record would otherwise write a future watermark
//     past every planner-side clamp -- and because the stored value is then
//     monotonically defended, nothing could ever lower it again.
//
//  2. coverageUpperBound -- the unit's window END (Claim.BeforeAt). The unit
//     only fetched up to its window end, so a stamp beyond that claims data it
//     never read. Such a value is usually in the PAST, so the now ceiling does
//     NOT catch it: the next run starts after the overclaimed point and every
//     record in between is silently skipped forever. Python measured real gaps
//     of ~5h that a 60s overlap masked.
//
// Enforcing this at each call site is what CHAOS-3412 got wrong twice on the
// Python side. It lives HERE, at the one write API, so a route cannot opt out,
// and watermark_write_boundary_test.go DERIVES the writer set from this
// package's source rather than trusting a hand-maintained list.
func normalizeWatermarkWrite(
	incoming time.Time,
	coverageUpperBound *time.Time,
	now time.Time,
	orgID, sourceID, datasetKey string,
) time.Time {
	logger := slog.Default()
	value := incoming.UTC()
	ceiling := now.UTC()
	bound := "now"
	if coverageUpperBound != nil {
		upper := coverageUpperBound.UTC()
		if upper.Before(ceiling) {
			ceiling, bound = upper, "window_end"
		}
	}
	if !value.After(ceiling) {
		return value
	}
	overshoot := value.Sub(ceiling)
	if overshoot > futureWatermarkWriteSkewTolerance && logger != nil {
		logger.Warn(
			"sync.watermark.write_clamped",
			slog.String("org_id", orgID),
			slog.String("source_id", sourceID),
			slog.String("dataset_key", datasetKey),
			slog.String("requested_last_synced_at", value.Format(time.RFC3339Nano)),
			slog.String("clamped_to", ceiling.Format(time.RFC3339Nano)),
			slog.String("ceiling", bound),
			slog.Float64("overshoot_seconds", overshoot.Seconds()),
			slog.String("reason",
				"a watermark claims every record up to it has been read. "+
					"Clamping to the highest point actually covered. A window_end "+
					"clamp means a provider reported coverage beyond what the unit "+
					"fetched; a now clamp usually means a source record carried a "+
					"skewed timestamp."),
		)
	}
	return ceiling
}
