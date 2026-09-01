package sync

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// CHAOS-4602 design page §4: BuildBackfillPlan is the pure backfill sibling
// of BuildScheduledPlan, which explicitly rejects backfill
// (ErrBackfillScheduled). It ports src/dev_health_ops/sync/planner.py's
// _build_planned_units + _resolve_windows' BACKFILL branch
// (_backfill_windows/_chunk_to_window) and both family folds
// (_build_work_item_family_units/_build_fold_family_units) verbatim for the
// one difference backfill introduces: a dataset's window resolution can fan
// out into MANY chunked windows (one PlannedUnit each) instead of the single
// watermark-derived window every other mode plans.
//
// Python's _build_work_item_family_units also carries a Jira-specific
// defense-in-depth check, _is_non_project_jira_source (CHAOS-4582), that
// refuses to plan a work-items unit for a legacy non-project Jira source
// row. Ported here as PlanSource.NonProjectJiraSource, resolved by the
// materializer (loadPlanSources) and checked in
// buildBackfillWorkItemFamilyUnits below: pre-existing bad-shape rows
// predate this ticket's own source discovery (org 70d529e0 alone carries
// legacy disabled SUP/OPS/JIRA rows) and the planner reads them regardless
// of who wrote them -- this is NOT a gap this PR's own discovery closes by
// construction.
var ErrMismatchedFamilyWindowCounts = errors.New(
	"work-item-family datasets resolved to mismatched window counts",
)

const (
	// defaultBackfillChunkDays mirrors _backfill_windows' hardcoded default
	// (planner.py:1854) for every dataset except the Linear work-item family.
	defaultBackfillChunkDays = 7

	// defaultLinearBackfillMaxWindowDays/linearBackfillMaxWindowDaysEnv mirror
	// _DEFAULT_LINEAR_BACKFILL_MAX_WINDOW_DAYS / LINEAR_BACKFILL_MAX_WINDOW_DAYS
	// (planner.py:1296-1331): Linear's org-wide work-item crawl trades
	// per-window fixed overhead against Linear's own rate limit, so it gets a
	// wider (14-day default) chunk than every other provider's 7.
	defaultLinearBackfillMaxWindowDays = 14
	linearBackfillMaxWindowDaysEnv     = "LINEAR_BACKFILL_MAX_WINDOW_DAYS"
)

// BuildBackfillPlan is the pure backfill planner. It rejects every mode but
// backfill (the mirror image of BuildScheduledPlan rejecting backfill) and
// requires input.Since/input.Before -- both concrete, since < before -- the
// same precondition _backfill_windows enforces.
func BuildBackfillPlan(input PlannerInput) ([]PlannedUnit, error) {
	if input.Mode != SyncModeBackfill {
		return nil, fmt.Errorf(
			"%w: BuildBackfillPlan only supports backfill mode, got %q",
			ErrInvalidPlan, input.Mode,
		)
	}
	if input.OrgID == "" || input.IntegrationID == "" || input.Now.IsZero() {
		return nil, ErrInvalidPlan
	}
	if input.Since == nil || input.Before == nil {
		return nil, fmt.Errorf(
			"%w: backfill sync planning requires since and before", ErrInvalidPlan,
		)
	}
	since := input.Since.UTC()
	before := input.Before.UTC()
	if !since.Before(before) {
		return nil, fmt.Errorf("%w: backfill since must be before before", ErrInvalidPlan)
	}

	var units []PlannedUnit
	familyDatasets := workitemcontract.FamilyDatasets()
	for _, source := range input.Sources {
		provider := strings.ToLower(source.Provider)
		prsEnabled := false
		family := make([]PlanDataset, 0, len(familyDatasets))
		prSocial := make([]PlanDataset, 0, len(prSocialFamilyDatasets))
		testOps := make([]PlanDataset, 0, len(testOpsFamilyDatasets))
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
			if slices.Contains(prSocialFamilyDatasets, dataset.Key) {
				prSocial = append(prSocial, dataset)
				continue
			}
			if slices.Contains(testOpsFamilyDatasets, dataset.Key) {
				testOps = append(testOps, dataset)
				continue
			}
			descriptor, known := providersync.Descriptor(provider, dataset.Key)
			// CHAOS-4731: same visibility as the scheduled planner's gate.
			switch {
			case !known:
				globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomeUnknownPair)
			case !descriptor.RouteReady || !descriptor.Plannable:
				globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomeRouteNotReady)
			case !descriptor.ExecutedProofSatisfied(input.ExecutedProof):
				globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomeExecutedProofUnsatisfied)
			}
			if !known || !descriptor.RouteReady || !descriptor.Plannable ||
				!descriptor.ExecutedProofSatisfied(input.ExecutedProof) {
				continue
			}
			windows, err := resolveBackfillWindows(since, before, provider, dataset.Key)
			if err != nil {
				return nil, err
			}
			if len(windows) > 0 {
				globalPlanGateTelemetry.observe(provider, dataset.Key, planGateOutcomePlanned)
			}
			for _, window := range windows {
				start := window.Since
				units = append(units, newPlannedUnit(input, source, dataset.Key, spec, &start, window.Before))
			}
		}

		familyUnits, err := buildBackfillWorkItemFamilyUnits(
			input, source, provider, since, before, family, familyDatasets, prsEnabled,
		)
		if err != nil {
			return nil, err
		}
		units = append(units, familyUnits...)

		prSocialUnits, err := buildBackfillFoldFamilyUnits(
			input, source, provider, since, before, prSocial, canonicalPRSocialDataset,
		)
		if err != nil {
			return nil, err
		}
		units = append(units, prSocialUnits...)

		testOpsUnits, err := buildBackfillFoldFamilyUnits(
			input, source, provider, since, before, testOps, canonicalTestOpsDataset,
		)
		if err != nil {
			return nil, err
		}
		units = append(units, testOpsUnits...)
	}
	return units, nil
}

// resolveBackfillWindows mirrors _backfill_windows: chunk [since, before]
// into chunkDays-day windows (ChunkDateRange, already ported and unit
// tested against Python's chunk_date_range golden vectors), then map each
// UTC-calendar-date chunk boundary back onto the EXACT requested instant at
// either end (chunkToWindow) rather than the truncated calendar date.
func resolveBackfillWindows(since, before time.Time, provider, dataset string) ([]DateWindow, error) {
	chunkDays := backfillChunkDays(provider, dataset)
	chunks, err := ChunkDateRange(since, before, chunkDays)
	if err != nil {
		// Codex review (gate round 9, P2): ChunkDateRange's errors are
		// deterministic given the same config/request (chunkDays comes
		// from LINEAR_BACKFILL_MAX_WINDOW_DAYS or a provider default,
		// since/before from the request itself) -- retrying can never
		// produce a different outcome, exactly the class ErrInvalidPlan
		// exists for. Without this wrap, occurrence_reconciler.go's generic
		// error path deferred it with retry-with-backoff instead of
		// quarantining immediately, so the API reported "pending" through
		// the whole bounded await before eventually failing.
		return nil, fmt.Errorf("%w: %w", ErrInvalidPlan, err)
	}
	windows := make([]DateWindow, 0, len(chunks))
	for _, chunk := range chunks {
		start, end := chunkToWindow(chunk.Since, chunk.Before, since, before)
		windows = append(windows, DateWindow{Since: start, Before: end})
	}
	return windows, nil
}

// chunkToWindow mirrors _chunk_to_window (planner.py:1864-1880): a chunk
// boundary that lands on the SAME UTC calendar date as the requested
// since/before keeps the exact requested instant untruncated; an interior
// chunk boundary becomes that UTC calendar day's start (a chunk's Since is
// already UTC midnight from ChunkDateRange) or end
// (23:59:59.999999 -- Python's datetime.combine(date, time.max) is
// microsecond-precision, matched here to the microsecond rather than
// Go's native nanosecond resolution).
func chunkToWindow(chunkSince, chunkBefore, requestedSince, requestedBefore time.Time) (time.Time, time.Time) {
	start := chunkSince
	if chunkSince.Equal(truncateToUTCDate(requestedSince)) {
		start = requestedSince
	}
	end := chunkBefore.Add(24*time.Hour - time.Microsecond)
	if chunkBefore.Equal(truncateToUTCDate(requestedBefore)) {
		end = requestedBefore
	}
	return start, end
}

// backfillChunkDays mirrors _backfill_windows' chunk_days selection: 7 days
// for every provider/dataset except the Linear work-item family, which
// widens to linearBackfillMaxWindowDays (default 14, env-overridable).
func backfillChunkDays(provider, dataset string) int {
	if strings.EqualFold(provider, "linear") && workitemcontract.IsFamilyDataset(dataset) {
		return linearBackfillMaxWindowDays()
	}
	return defaultBackfillChunkDays
}

// linearBackfillMaxWindowDays mirrors _linear_backfill_max_window_days.
// Shares warnUnparseableEnvInt's dedupe-by-(key,raw) with
// incrementalHeavyMaxWindowDays so a bad setting warns loudly exactly once,
// not once per (source x dataset).
func linearBackfillMaxWindowDays() int {
	raw, ok := os.LookupEnv(linearBackfillMaxWindowDaysEnv)
	if !ok {
		return defaultLinearBackfillMaxWindowDays
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		warnUnparseableEnvInt(linearBackfillMaxWindowDaysEnv, raw, defaultLinearBackfillMaxWindowDays)
		return defaultLinearBackfillMaxWindowDays
	}
	if value <= 0 {
		return defaultLinearBackfillMaxWindowDays
	}
	return value
}

// backfillDatasetWindows pairs one family member with its own independently
// resolved chunk windows, prior to the index-aligned merge below.
type backfillDatasetWindows struct {
	dataset PlanDataset
	windows []DateWindow
}

func resolveBackfillFamilyWindows(
	since, before time.Time, provider string, datasets []PlanDataset,
) ([]backfillDatasetWindows, error) {
	resolved := make([]backfillDatasetWindows, 0, len(datasets))
	for _, dataset := range datasets {
		windows, err := resolveBackfillWindows(since, before, provider, dataset.Key)
		if err != nil {
			return nil, err
		}
		resolved = append(resolved, backfillDatasetWindows{dataset: dataset, windows: windows})
	}
	return resolved, nil
}

// mergeBackfillFamilyWindows mirrors _merge_family_windows: drop any member
// that resolved to zero windows, then index-align the survivors' window
// lists (earliest start, latest end per index). Every backfill window is a
// concrete (non-nil) instant -- unlike the scheduled planner's optional open
// start -- so this merge never needs BuildScheduledPlan's nil-wins-as-earliest
// handling. chunk_days is keyed on (provider, family membership) only, never
// on the individual dataset_key, so every contributing member is expected to
// resolve to the SAME chunk count; a mismatch is a real invariant violation,
// not a partial-catch-up case, and is surfaced as an error rather than
// silently dropped (Python raises ValueError for the same case).
func mergeBackfillFamilyWindows(
	resolved []backfillDatasetWindows,
) (merged []DateWindow, contributing []backfillDatasetWindows, err error) {
	contributing = make([]backfillDatasetWindows, 0, len(resolved))
	for _, dw := range resolved {
		if len(dw.windows) > 0 {
			contributing = append(contributing, dw)
		}
	}
	if len(contributing) == 0 {
		return nil, contributing, nil
	}
	count := len(contributing[0].windows)
	for _, dw := range contributing[1:] {
		if len(dw.windows) != count {
			return nil, nil, ErrMismatchedFamilyWindowCounts
		}
	}
	merged = make([]DateWindow, count)
	for i := 0; i < count; i++ {
		earliest := contributing[0].windows[i].Since
		latest := contributing[0].windows[i].Before
		for _, dw := range contributing[1:] {
			window := dw.windows[i]
			if window.Since.Before(earliest) {
				earliest = window.Since
			}
			if window.Before.After(latest) {
				latest = window.Before
			}
		}
		merged[i] = DateWindow{Since: earliest, Before: latest}
	}
	return merged, contributing, nil
}

// buildBackfillWorkItemFamilyUnits mirrors _build_work_item_family_units'
// backfill path: the atomic work-item family collapses onto ONE composite
// "work-items" unit PER merged chunk window, unconditionally stamping every
// family member's completion flag (CHAOS-3606/CHAOS-4054 -- the native
// route's all-five writer, not a per-member contribution signal).
func buildBackfillWorkItemFamilyUnits(
	input PlannerInput, source PlanSource, provider string, since, before time.Time,
	family []PlanDataset, familyDatasets []string, prsEnabled bool,
) ([]PlannedUnit, error) {
	if len(family) == 0 {
		return nil, nil
	}
	if provider == "jira" && source.NonProjectJiraSource {
		// CHAOS-4582 defense-in-depth, ported verbatim (same guard as
		// buildWorkItemFamilyUnit's scheduled-mode sibling): a pre-existing
		// non-project Jira source row 400s against Jira on every attempt.
		slog.Default().Error("sync.plan.jira_source_not_a_project",
			slog.String("org_id", input.OrgID),
			slog.String("source_id", source.ID),
			slog.String("external_id", source.ExternalID),
			slog.String("error_category", "jira_source_not_a_project"))
		return nil, nil
	}
	canonicalDescriptor, known := providersync.Descriptor(provider, canonicalWorkItemsDataset)
	// CHAOS-4731: same visibility as the scheduled planner's family gate.
	switch {
	case !known:
		globalPlanGateTelemetry.observe(provider, canonicalWorkItemsDataset, planGateOutcomeUnknownPair)
	case !canonicalDescriptor.RouteReady || !canonicalDescriptor.Plannable:
		globalPlanGateTelemetry.observe(provider, canonicalWorkItemsDataset, planGateOutcomeRouteNotReady)
	case !canonicalDescriptor.ExecutedProofSatisfied(input.ExecutedProof):
		globalPlanGateTelemetry.observe(provider, canonicalWorkItemsDataset, planGateOutcomeExecutedProofUnsatisfied)
	}
	if !known || !canonicalDescriptor.RouteReady || !canonicalDescriptor.Plannable ||
		!canonicalDescriptor.ExecutedProofSatisfied(input.ExecutedProof) {
		return nil, nil
	}
	canonical, ok := datasetSpecification(provider, canonicalWorkItemsDataset)
	if !ok {
		return nil, nil
	}
	resolved, err := resolveBackfillFamilyWindows(since, before, provider, family)
	if err != nil {
		return nil, err
	}
	merged, _, err := mergeBackfillFamilyWindows(resolved)
	if err != nil {
		return nil, err
	}
	if len(merged) == 0 {
		return nil, nil
	}
	flags := cloneFlags(canonical.ProcessorFlags)
	for _, dataset := range familyDatasets {
		flags[familyDatasetFlag(dataset)] = true
	}
	if provider == "github" {
		flags["sync_prs"] = prsEnabled
	}
	units := make([]PlannedUnit, 0, len(merged))
	for _, window := range merged {
		start := window.Since
		unit := newPlannedUnit(input, source, canonicalWorkItemsDataset, canonical, &start, window.Before)
		unit.ProcessorFlags = cloneFlags(flags)
		units = append(units, unit)
	}
	globalPlanGateTelemetry.observe(provider, canonicalWorkItemsDataset, planGateOutcomePlanned)
	return units, nil
}

// buildBackfillFoldFamilyUnits mirrors _build_fold_family_units' backfill
// path for the non-atomic PR-social/TestOps alias families: only the
// members that actually contributed a window (non-empty after the merge's
// drop step) receive a family_dataset_<key> flag and fold in their own
// processor flags; a member that resolved to zero windows is left
// completely untouched, exactly as it is left untouched in every other
// mode.
func buildBackfillFoldFamilyUnits(
	input PlannerInput, source PlanSource, provider string, since, before time.Time,
	members []PlanDataset, canonicalDataset string,
) ([]PlannedUnit, error) {
	if len(members) == 0 {
		return nil, nil
	}
	canonicalDescriptor, known := providersync.Descriptor(provider, canonicalDataset)
	// CHAOS-4731: same visibility as the scheduled planner's fold gate.
	switch {
	case !known:
		globalPlanGateTelemetry.observe(provider, canonicalDataset, planGateOutcomeUnknownPair)
	case !canonicalDescriptor.RouteReady || !canonicalDescriptor.Plannable:
		globalPlanGateTelemetry.observe(provider, canonicalDataset, planGateOutcomeRouteNotReady)
	case !canonicalDescriptor.ExecutedProofSatisfied(input.ExecutedProof):
		globalPlanGateTelemetry.observe(provider, canonicalDataset, planGateOutcomeExecutedProofUnsatisfied)
	}
	if !known || !canonicalDescriptor.RouteReady || !canonicalDescriptor.Plannable ||
		!canonicalDescriptor.ExecutedProofSatisfied(input.ExecutedProof) {
		return nil, nil
	}
	canonicalSpec, ok := datasetSpecification(provider, canonicalDataset)
	if !ok {
		return nil, nil
	}
	resolved, err := resolveBackfillFamilyWindows(since, before, provider, members)
	if err != nil {
		return nil, err
	}
	merged, contributing, err := mergeBackfillFamilyWindows(resolved)
	if err != nil {
		return nil, err
	}
	if len(merged) == 0 {
		return nil, nil
	}
	contributingKeys := make(map[string]struct{}, len(contributing))
	for _, dw := range contributing {
		contributingKeys[dw.dataset.Key] = struct{}{}
	}
	flags := cloneFlags(canonicalSpec.ProcessorFlags)
	for _, dataset := range members {
		if _, ok := contributingKeys[dataset.Key]; !ok {
			continue
		}
		flags[familyDatasetFlag(dataset.Key)] = true
		memberSpec, ok := datasetSpecification(provider, dataset.Key)
		if !ok {
			continue
		}
		for flagName, flagValue := range memberSpec.ProcessorFlags {
			flags[flagName] = flagValue
		}
	}
	units := make([]PlannedUnit, 0, len(merged))
	for _, window := range merged {
		start := window.Since
		unit := newPlannedUnit(input, source, canonicalDataset, canonicalSpec, &start, window.Before)
		unit.ProcessorFlags = cloneFlags(flags)
		units = append(units, unit)
	}
	globalPlanGateTelemetry.observe(provider, canonicalDataset, planGateOutcomePlanned)
	return units, nil
}
