package sync

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

const (
	SyncModeIncremental = "incremental"
	SyncModeFullResync  = "full_resync"
	SyncModeBackfill    = "backfill"

	// defaultInitialSyncDepthDays mirrors Python's
	// _DEFAULT_INITIAL_SYNC_DEPTH_DAYS (src/dev_health_ops/sync/planner.py).
	// Reconciled under CHAOS-3427: both sides are 30, and the resolution
	// order (dataset override > integration config > default, then the tier
	// backfill_days cap, then a floor of 1) matches resolve_initial_sync_depth
	// exactly. The heavy ratchet below caps the WINDOW, never this depth.
	defaultInitialSyncDepthDays = 30
	canonicalWorkItemsDataset   = "work-items"
	familyDatasetFlagPrefix     = "family_dataset_"

	// heavyCostClass is the registry cost class the incremental window ratchet
	// scopes to (CHAOS-3412 clause C2). Resolved from datasetSpecification, not
	// from a hardcoded key list, so a newly-registered HEAVY dataset inherits
	// the cap with no planner change.
	heavyCostClass = "heavy"

	// defaultIncrementalHeavyMaxWindowDays mirrors Python's
	// _DEFAULT_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS (CHAOS-3412 clause C1).
	defaultIncrementalHeavyMaxWindowDays = 7
	incrementalHeavyMaxWindowDaysEnv     = "SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS"

	// futureWatermarkRecoveryWindow mirrors Python's
	// _FUTURE_WATERMARK_RECOVERY_SECONDS (CHAOS-3412 clause C10(a)).
	futureWatermarkRecoveryWindow = time.Hour
)

// warnedCapClamps mirrors Python's _WARNED_CAP_CLAMPS: the clamp helper runs
// once per (source x heavy dataset), so an unguarded warning would bury the
// log under hundreds of identical lines on a large org. Both inputs are
// restart-loaded settings, so one line per distinct (cap, overlap) per process
// is the complete signal.
var warnedCapClamps sync.Map

// warnedEnvIntRejections dedupes warnUnparseableEnvInt on the same (key, raw)
// pair. incrementalHeavyMaxWindowDays runs once per (source x heavy dataset),
// so an unguarded warning would bury the log exactly as an unguarded cap-clamp
// warning would. The raw value is part of the key so a genuinely changed
// setting still warns.
var warnedEnvIntRejections sync.Map

// warnUnparseableEnvInt makes a REJECTED integer setting loud instead of
// silently falling back. It exists because Go's strconv and Python's int()
// accept different grammars, and every Go/Python settings divergence found so
// far surfaced as behaviour drift rather than as an error: Python accepts
// underscore digit separators (int("3_0") == 30) and non-ASCII decimal digits
// (int("٣٠") == 30), Go's Atoi rejects both. Porting those acceptances is an
// explicit non-goal (see boundedEnvInt), so a value only the Python worker
// understands has to announce itself here, naming the raw text and the value
// actually in force.
//
// Warn, never fail: refusing to plan on a bad settings string would recreate
// the do-nothing failure mode the CHAOS-3412 ratchet exists to kill.
func warnUnparseableEnvInt(key, raw string, fallback int) {
	type rejectionKey struct{ key, raw string }
	if _, warned := warnedEnvIntRejections.LoadOrStore(rejectionKey{key: key, raw: raw}, true); warned {
		return
	}
	slog.Default().Warn(
		"sync.settings.env_int_rejected",
		slog.String("setting", key),
		slog.String("raw_value", raw),
		slog.Int("value_in_force", fallback),
		slog.String("reason",
			"value is not a Go-parseable integer and was IGNORED. Python's int() "+
				"accepts underscore digit separators and non-ASCII decimal digits; "+
				"this worker deliberately does not. Write plain ASCII digits with an "+
				"optional leading sign (surrounding whitespace is fine)."),
	)
}

var (
	ErrInvalidPlan       = errors.New("invalid scheduled sync plan")
	ErrBackfillScheduled = errors.New("scheduled sync backfill is not supported")
)

// PlanSource is the secret-free source state consumed by the pure planner.
type PlanSource struct {
	ID         string
	ExternalID string
	Provider   string
	FullName   string
}

// PlanDataset is the secret-free dataset state consumed by the pure planner.
type PlanDataset struct {
	Key              string
	InitialDepthDays *int
}

// WatermarkKey is the canonical incremental-watermark identity.
type WatermarkKey struct {
	SourceID string
	Dataset  string
}

// PlannerInput contains only already-authorized, enabled rows. Loading and
// locking those rows remains the PostgreSQL materializer's responsibility.
type PlannerInput struct {
	OrgID                string
	IntegrationID        string
	Mode                 string
	Now                  time.Time
	Before               *time.Time
	IntegrationDepthDays *int
	TierBackfillDaysCap  *int
	WatermarkOverlap     time.Duration
	Sources              []PlanSource
	Datasets             []PlanDataset
	Watermarks           map[WatermarkKey]time.Time
}

// PlannedUnit is the complete secret-free unit row prior to persistence.
type PlannedUnit struct {
	OrgID          string          `json:"org_id"`
	IntegrationID  string          `json:"integration_id"`
	SourceID       string          `json:"source_id"`
	Provider       string          `json:"provider"`
	Dataset        string          `json:"dataset_key"`
	CostClass      string          `json:"cost_class"`
	Mode           string          `json:"mode"`
	WindowStart    *time.Time      `json:"window_start"`
	WindowEnd      *time.Time      `json:"window_end"`
	ProcessorFlags map[string]bool `json:"processor_flags"`
}

type datasetSpec struct {
	CostClass      string
	Incremental    bool
	ProcessorFlags map[string]bool
	LegacyTargets  []string
}

var supportedProviderDatasets = map[string]map[string]struct{}{
	"github":       setOf("repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"gitlab":       setOf("repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "deployments", "incidents", "security", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments", "feature-flags"),
	"jira":         setOf("incidents", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"linear":       setOf("work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"),
	"launchdarkly": setOf("feature-flags"),
	"pagerduty":    setOf("services", "business-services", "escalation-policies", "schedules", "on-calls", "users", "teams", "incidents", "incident-alerts", "incident-log-entries", "incident-notes"),
}

var lightDatasets = setOf(
	"repo-metadata", "incidents", "work-item-labels", "work-item-projects",
	"services", "business-services", "escalation-policies", "schedules", "users", "teams",
)
var heavyDatasets = setOf("commit-stats", "files", "blame", "tests")
var noWatermarkDatasets = setOf(
	"repo-metadata", "services", "business-services", "escalation-policies",
	"schedules", "on-calls", "users", "teams",
)

var processorFlagsByDataset = map[string]map[string]bool{
	"commits":      {"sync_git": true, "sync_commits": true},
	"commit-stats": {"sync_git": true, "sync_commit_stats": true},
	"files":        {"sync_git": true, "sync_files": true},
	"blame":        {"blame_only": true, "sync_blame": true},
	"prs":          {"sync_prs": true},
	"pr-reviews":   {"sync_prs": true},
	"pr-comments":  {"sync_prs": true},
	"cicd":         {"sync_cicd": true},
	"tests":        {"sync_tests": true},
	"deployments":  {"sync_deployments": true},
	"incidents":    {"sync_incidents": true},
	"security":     {"sync_security": true},
}

var legacyTargetsByDataset = map[string][]string{
	"repo-metadata": {"git"}, "commits": {"git"}, "commit-stats": {"git"}, "files": {"git"},
	"blame": {"blame"}, "prs": {"prs"}, "pr-reviews": {"prs"}, "pr-comments": {"prs"},
	"cicd": {"cicd"}, "tests": {"tests"}, "deployments": {"deployments"},
	"incidents": {"incidents"}, "security": {"security"},
	"work-items": {"work-items"}, "work-item-labels": {"work-items"},
	"work-item-projects": {"work-items"}, "work-item-history": {"work-items"},
	"work-item-comments": {"work-items"}, "feature-flags": {"feature-flags"},
	"services": {"operational"}, "business-services": {"operational"},
	"escalation-policies": {"operational"}, "schedules": {"operational"},
	"on-calls": {"operational"}, "users": {"operational"}, "teams": {"operational"},
	"incident-alerts": {"operational"}, "incident-log-entries": {"operational"},
	"incident-notes": {"operational"},
}

// BuildScheduledPlan is the pure scheduled planner. It deliberately rejects
// backfill: scheduled occurrences may be incremental or full-resync only.
func BuildScheduledPlan(input PlannerInput) ([]PlannedUnit, error) {
	if input.Mode == SyncModeBackfill {
		return nil, ErrBackfillScheduled
	}
	if input.Mode != SyncModeIncremental && input.Mode != SyncModeFullResync {
		return nil, fmt.Errorf("%w: unsupported sync run mode %q", ErrInvalidPlan, input.Mode)
	}
	if input.OrgID == "" || input.IntegrationID == "" || input.Now.IsZero() {
		return nil, ErrInvalidPlan
	}
	now := input.Now.UTC()
	before := now
	if input.Before != nil {
		before = input.Before.UTC()
	}
	units := make([]PlannedUnit, 0, len(input.Sources)*len(input.Datasets))
	for _, source := range input.Sources {
		provider := strings.ToLower(source.Provider)
		prsEnabled := false
		familyDatasets := workitemcontract.FamilyDatasets()
		family := make([]PlanDataset, 0, len(familyDatasets))
		for _, dataset := range input.Datasets {
			spec, ok := datasetSpecification(provider, dataset.Key)
			if !ok {
				continue
			}
			if slices.Contains(spec.LegacyTargets, "prs") {
				prsEnabled = true
			}
			if slices.Contains(familyDatasets, dataset.Key) {
				family = append(family, dataset)
				continue
			}
			// CHAOS-4054: plan only identities the execution registry says are
			// independently plannable. An alias identity (github pr-comments,
			// gitlab tests, ...) folds into its canonical writer and is never
			// minted as its own unit; a pair that is not RouteReady is not
			// shipped. This reads the registry alone -- there is no route
			// enablement env plane to consult. Family datasets are excluded
			// from this check above: their admission is governed by the
			// atomic-family collapse below.
			descriptor, known := providersync.Descriptor(provider, dataset.Key)
			if !known || !descriptor.RouteReady || !descriptor.Plannable {
				continue
			}
			start, end, ok := resolveWindow(input, source, dataset, spec, now, before)
			if !ok {
				// Already synced past the requested end (or a corrupt,
				// unrecoverable window): plan NO unit for this dataset rather
				// than a zero-width one that would finalize SUCCESS having
				// fetched nothing. See normalizeStampingWindow.
				continue
			}
			units = append(units, newPlannedUnit(input, source, dataset.Key, spec, start, end))
		}
		if len(family) == 0 {
			continue
		}
		// Individual family ALIASES are deliberately unchecked above -- their
		// admission is the atomic-family collapse's business. The CANONICAL
		// claim this collapse emits ("work-items") is an ordinary plannable
		// identity, so gate it here, after collapse, the same way every
		// non-family dataset already is above.
		canonicalDescriptor, known := providersync.Descriptor(
			provider, canonicalWorkItemsDataset,
		)
		if !known || !canonicalDescriptor.RouteReady || !canonicalDescriptor.Plannable {
			continue
		}
		canonical, ok := datasetSpecification(provider, canonicalWorkItemsDataset)
		if !ok {
			continue
		}
		// Each family dataset owns its own watermark identity, so resolve each
		// one independently and merge -- earliest start (a nil start means "no
		// lower bound" and wins), latest end. CHAOS-3412: a family dataset
		// already synced past the requested end resolves to ZERO windows and is
		// DROPPED before the merge; if none contribute, the whole composite is
		// dropped. Mirrors _build_work_item_family_units' `contributing` filter,
		// which exists because the pre-fix index-aligned merge raised on a
		// mismatched window count and took down a merely partially-caught-up
		// plan.
		var earliest *time.Time
		var latest time.Time
		contributing := make([]PlanDataset, 0, len(family))
		openStart := false
		for _, dataset := range family {
			spec, ok := datasetSpecification(provider, dataset.Key)
			if !ok {
				continue
			}
			start, end, ok := resolveWindow(input, source, dataset, spec, now, before)
			if !ok {
				continue
			}
			contributing = append(contributing, dataset)
			if latest.IsZero() || end.After(latest) {
				latest = end
			}
			if start == nil {
				openStart = true
				continue
			}
			if earliest == nil || start.Before(*earliest) {
				value := *start
				earliest = &value
			}
		}
		if len(contributing) == 0 {
			continue
		}
		if openStart {
			earliest = nil
		}
		flags := cloneFlags(canonical.ProcessorFlags)
		// CHAOS-3606: the native work-item route has one indivisible all-five
		// writer. A sibling with an empty/caught-up window still has no
		// independent owner while this canonical unit executes, so its literal
		// flag records route ownership rather than contribution to the merged
		// window above.
		//
		// CHAOS-4054: unconditional for every atomic family, matching
		// _build_work_item_family_units and the now-unconditional strict
		// admission in providerunit.validateProviderFamilyExecutionClaim.
		// Stamping only the contributing aliases for gitlab/jira/linear would
		// mint a canonical claim the executor must reject as incomplete — a
		// self-inflicted route-fault loop the moment one sibling is caught up.
		for _, dataset := range familyDatasets {
			flags[familyDatasetFlag(dataset)] = true
		}
		if provider == "github" {
			// CHAOS-646: thread the PRS-as-work-items signal onto the composite
			// so _work_item_kwargs sets include_pull_requests correctly.
			flags["sync_prs"] = prsEnabled
		}
		unit := newPlannedUnit(input, source, canonicalWorkItemsDataset, canonical, earliest, latest)
		unit.ProcessorFlags = flags
		units = append(units, unit)
	}
	return units, nil
}

// resolveWindow is the single window pipeline every planned unit goes through:
// resolve the start, apply the HEAVY incremental ratchet, then enforce the
// watermark-stamping postcondition. ok=false means "plan no unit".
func resolveWindow(
	input PlannerInput, source PlanSource, dataset PlanDataset, spec datasetSpec,
	now, before time.Time,
) (*time.Time, time.Time, bool) {
	start := resolveWindowStart(input, source, dataset, spec, now)
	end := applyHeavyWindowRatchet(input.Mode, spec.CostClass, start, before, input.WatermarkOverlap)
	return normalizeStampingWindow(start, end, now, input.WatermarkOverlap)
}

func resolveWindowStart(input PlannerInput, source PlanSource, dataset PlanDataset, spec datasetSpec, now time.Time) *time.Time {
	if input.Mode == SyncModeIncremental {
		if !spec.Incremental {
			return nil
		}
		if watermark, ok := input.Watermarks[WatermarkKey{SourceID: source.ExternalID, Dataset: dataset.Key}]; ok {
			value := watermark.UTC().Add(-max(input.WatermarkOverlap, 0))
			return &value
		}
	}
	depth := resolveInitialSyncDepth(dataset.InitialDepthDays, input.IntegrationDepthDays, input.TierBackfillDaysCap)
	value := now.AddDate(0, 0, -depth)
	return &value
}

// incrementalHeavyMaxWindowDays is the configured HEAVY incremental window cap
// in days (CHAOS-3412 clause C1): SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS when
// it parses to a POSITIVE integer, otherwise the 7-day default. Zero, negative
// and non-integer values fall back to the default rather than erroring --
// refusing to plan would recreate the do-nothing failure mode the ratchet
// exists to kill.
//
// TrimSpace matches Python's int(), which strips surrounding whitespace. The
// two acceptances NOT ported -- underscore digit separators and non-ASCII
// decimal digits -- are the accepted grammar restriction documented on
// boundedEnvInt; a value only Python would accept warns via
// warnUnparseableEnvInt rather than falling back in silence.
//
// A non-POSITIVE value is not routed through that warning: Python falls back
// on it identically (planner.py:735-743), so there is no divergence to
// announce, and the CHAOS-3412 clause C1 contract already states the
// fall-back as intended behaviour.
func incrementalHeavyMaxWindowDays() int {
	raw, ok := os.LookupEnv(incrementalHeavyMaxWindowDaysEnv)
	if !ok {
		return defaultIncrementalHeavyMaxWindowDays
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		warnUnparseableEnvInt(incrementalHeavyMaxWindowDaysEnv, raw, defaultIncrementalHeavyMaxWindowDays)
		return defaultIncrementalHeavyMaxWindowDays
	}
	if value <= 0 {
		return defaultIncrementalHeavyMaxWindowDays
	}
	return value
}

// effectiveHeavyMaxWindow is the cap clamped to STRICTLY exceed the watermark
// overlap (CHAOS-3412 clause C8), mirroring Python's
// _effective_heavy_max_window_days.
//
// The incremental read starts at `watermark - overlap`, so a capped window
// spans [W-overlap, W-overlap+cap]. When overlap >= cap that end lands at or
// before W itself, and the monotonic watermark write silently DISCARDS it:
// every later tick re-plans the identical slice, re-fetches it, and reports
// SUCCESS while the watermark never moves -- the same permanent stall wearing
// a different hat. floor(overlap_days)+1 is strictly greater than the overlap
// for any real overlap value, so every successful capped run advances by a
// positive amount.
//
// The clamp is LOUD but never fatal, deliberately: failing closed here would
// reproduce the do-nothing failure mode. A visibly clamped window is wider and
// more expensive than the operator asked for, but it makes progress, and the
// warning names both values so the misconfiguration can be corrected.
func effectiveHeavyMaxWindow(overlap time.Duration) time.Duration {
	capDays := incrementalHeavyMaxWindowDays()
	capWindow := time.Duration(capDays) * 24 * time.Hour
	if overlap <= 0 {
		return capWindow
	}
	overlapSeconds := int64(overlap / time.Second)
	minCapDays := overlapSeconds/int64(24*time.Hour/time.Second) + 1
	if minCapDays <= int64(capDays) {
		return capWindow
	}
	minCapWindow := time.Duration(minCapDays) * 24 * time.Hour
	type clampKey struct {
		capDays        int
		overlapSeconds int64
	}
	if _, warned := warnedCapClamps.LoadOrStore(
		clampKey{capDays: capDays, overlapSeconds: overlapSeconds}, true,
	); !warned {
		slog.Default().Warn(
			"sync.planner.heavy_window_cap_clamped_below_watermark_overlap",
			slog.Int("configured_cap_days", capDays),
			slog.Int64("watermark_overlap_seconds", overlapSeconds),
			slog.Int64("effective_cap_days", minCapDays),
			slog.String("reason",
				"SYNC_WATERMARK_OVERLAP >= SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS "+
					"would stall the HEAVY incremental ratchet: every capped window "+
					"would end at or before its own watermark and the monotonic "+
					"watermark write would be discarded. Widening the cap so each "+
					"run makes progress. Lower SYNC_WATERMARK_OVERLAP or raise "+
					"SYNC_INCREMENTAL_HEAVY_MAX_WINDOW_DAYS to remove this clamp."),
		)
	}
	return minCapWindow
}

// applyHeavyWindowRatchet caps a HEAVY dataset's INCREMENTAL window span
// (CHAOS-3412 clauses C2-C6), mirroring the ratchet block in Python's
// _resolve_windows.
//
//   - C2 SCOPE: HEAVY cost class only, resolved from the dataset registry.
//   - C3 APPLICATION: both incremental sub-cases -- cold start (no watermark)
//     AND behind-watermark. A long-idle org ratchets like a cold-start one.
//   - C4 DIRECTION: a MIN, never an assignment. window_start never moves, and
//     a watermark already inside the cap keeps its natural end; a tighter
//     requested `before` still wins.
//   - C5 OPEN START: a nil start (WatermarkBehavior.NONE) is never capped and
//     the cap must not synthesize one.
//   - C6 UNIT SHAPE: this caps ONE window; it never splits the unit into
//     several. Continuation is the next scheduled tick.
//
// Unit cost is linear in window span, so a HEAVY dataset cold-starting on a
// wide initial depth planned one window that could not fit the sync budget:
// the unit was deferred, no watermark was stamped, and the next tick recomputed
// the identical unfittable span forever. On success the watermark stamps at the
// window END (clause C7), so successive ticks ratchet forward.
func applyHeavyWindowRatchet(
	mode string, costClass string, start *time.Time, end time.Time, overlap time.Duration,
) time.Time {
	if mode != SyncModeIncremental || costClass != heavyCostClass || start == nil {
		return end
	}
	capped := start.UTC().Add(effectiveHeavyMaxWindow(overlap))
	if capped.Before(end) {
		return capped
	}
	return end
}

// normalizeStampingWindow is the shared postcondition for every mode whose
// SUCCESS path stamps a watermark -- INCREMENTAL and FULL_RESYNC -- mirroring
// Python's _watermark_stamping_window (CHAOS-3412 clauses C10(a) and C11).
// BACKFILL is deliberately outside it and this planner rejects backfill
// outright.
//
//  1. The end is clamped to `now`. A future `before` would otherwise persist a
//     FUTURE watermark and the next run would start in the future, silently
//     skipping everything up to it.
//
//  2. A resolved start AHEAD of now is corrupt state, not a window: it can only
//     come from a skewed provider watermark or from pre-fix planner code that
//     persisted a future window end. Left alone, rule 3 plans ZERO units, the
//     run finalizes FAILED, and with no unit there is nothing left to re-stamp
//     the watermark -- a permanent stall. Clamp back to a bounded recovery
//     window and warn loudly, naming the stored value, the clamp and the
//     residual gap.
//
//  3. An empty or inverted window (end <= start) plans ZERO units. Persisting
//     since_at >= before_at is worse than useless: the budget's window-span
//     helper floors a negative span to 1, so an inverted unit is admitted at
//     the CHEAPEST possible cost, fetches nothing, and finalizes SUCCESS -- a
//     false coverage claim.
//
// Returns ok=false for "plan no unit for this dataset". A plan that resolves
// zero units in total finalizes FAILED by pre-existing semantics; that is the
// decided-correct outcome (loud beats a false-coverage SUCCESS) and Go must
// not "helpfully" convert it into success.
func normalizeStampingWindow(
	start *time.Time, end time.Time, now time.Time, overlap time.Duration,
) (*time.Time, time.Time, bool) {
	now = now.UTC()
	end = end.UTC()
	if end.After(now) {
		end = now
	}
	if start == nil {
		return nil, end, true
	}
	resolved := start.UTC()
	if resolved.After(now) {
		recovery := futureWatermarkRecoveryWindow
		if overlap > recovery {
			recovery = overlap
		}
		healed := now.Add(-recovery)
		slog.Default().Warn(
			"sync.planner.future_watermark_clamped",
			slog.String("resolved_window_start", resolved.Format(time.RFC3339Nano)),
			slog.String("now", now.Format(time.RFC3339Nano)),
			slog.Int64("watermark_overlap_seconds", int64(overlap/time.Second)),
			slog.String("healed_window_start", healed.Format(time.RFC3339Nano)),
			slog.String("reason",
				"resolved window start is ahead of now, which means the stored "+
					"watermark is corrupt. Planning a bounded recovery window so a "+
					"unit runs and re-stamps a sane watermark. Records between the "+
					"true last-synced point and this recovery window are NOT "+
					"re-fetched -- run a bounded backfill if that span matters."),
		)
		resolved = healed
	}
	if !end.After(resolved) {
		return nil, time.Time{}, false
	}
	healedStart := resolved
	return &healedStart, end, true
}

func resolveInitialSyncDepth(dataset, integration, tierCap *int) int {
	depth := defaultInitialSyncDepthDays
	if integration != nil {
		depth = *integration
	}
	if dataset != nil {
		depth = *dataset
	}
	if tierCap != nil && *tierCap < depth {
		depth = *tierCap
	}
	if depth < 1 {
		return 1
	}
	return depth
}

func newPlannedUnit(input PlannerInput, source PlanSource, dataset string, spec datasetSpec, start *time.Time, end time.Time) PlannedUnit {
	end = end.UTC()
	return PlannedUnit{
		OrgID: input.OrgID, IntegrationID: input.IntegrationID, SourceID: source.ID,
		Provider: strings.ToLower(source.Provider), Dataset: dataset, CostClass: spec.CostClass,
		Mode: input.Mode, WindowStart: start, WindowEnd: &end, ProcessorFlags: cloneFlags(spec.ProcessorFlags),
	}
}

func datasetSpecification(provider, dataset string) (datasetSpec, bool) {
	supported, ok := supportedProviderDatasets[strings.ToLower(provider)]
	if !ok {
		return datasetSpec{}, false
	}
	if _, ok := supported[dataset]; !ok {
		return datasetSpec{}, false
	}
	cost := "medium"
	if _, ok := lightDatasets[dataset]; ok {
		cost = "light"
	} else if _, ok := heavyDatasets[dataset]; ok {
		cost = "heavy"
	}
	flags := cloneFlags(processorFlagsByDataset[dataset])
	targets := slices.Clone(legacyTargetsByDataset[dataset])
	if provider == "pagerduty" && dataset == "incidents" {
		targets = []string{"operational"}
	}
	if provider == "jira" && dataset == "incidents" {
		cost = "medium"
		flags = map[string]bool{}
		targets = []string{"operational"}
	}
	_, noWatermark := noWatermarkDatasets[dataset]
	return datasetSpec{CostClass: cost, Incremental: !noWatermark, ProcessorFlags: flags, LegacyTargets: targets}, true
}

func familyDatasetFlag(dataset string) string {
	return familyDatasetFlagPrefix + strings.ReplaceAll(dataset, "-", "_")
}

func cloneFlags(flags map[string]bool) map[string]bool {
	result := make(map[string]bool, len(flags))
	for key, value := range flags {
		result[key] = value
	}
	return result
}

func setOf(values ...string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
