package sync

import (
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// WatermarkRow is one sync_watermarks row as the watermark precedence
// reads it. LastSyncedAt is nil for a row whose last_synced_at is NULL.
type WatermarkRow struct {
	SourceID     string
	DatasetKey   string
	RepoID       string
	Target       string
	LastSyncedAt *time.Time
}

type watermarkPair struct{ key, value string }

// WatermarkIndex is sync/watermark_lag.py's WatermarkIndex, the in-memory
// form of watermarks.get_watermark's three tiers:
//
//  1. canonical (source_id, dataset_key);
//  2. legacy target column (repo_id, target == dataset_key);
//  3. reverse-legacy fallback to the raw legacy row (repo_id, target ==
//     legacy target, dataset_key == legacy target), legacy targets in sorted
//     order.
//
// The first row wins within a tier, and a row whose last_synced_at is NULL
// still occupies its tier (it resolves to nil and hides the lower tiers).
// The scheduled planner's watermark load and the api's run-units freshness
// both resolve through it.
type WatermarkIndex struct {
	canonical, legacyTarget, rawLegacy map[watermarkPair]*time.Time
}

// NewWatermarkIndex indexes rows once, in the order given.
func NewWatermarkIndex(rows []WatermarkRow) *WatermarkIndex {
	index := &WatermarkIndex{
		canonical:    map[watermarkPair]*time.Time{},
		legacyTarget: map[watermarkPair]*time.Time{},
		rawLegacy:    map[watermarkPair]*time.Time{},
	}
	setDefault := func(tier map[watermarkPair]*time.Time, key watermarkPair, at *time.Time) {
		if _, taken := tier[key]; !taken {
			tier[key] = at
		}
	}
	for _, row := range rows {
		setDefault(index.canonical, watermarkPair{row.SourceID, row.DatasetKey}, row.LastSyncedAt)
		setDefault(index.legacyTarget, watermarkPair{row.RepoID, row.Target}, row.LastSyncedAt)
		if row.Target == row.DatasetKey {
			setDefault(index.rawLegacy, watermarkPair{row.RepoID, row.Target}, row.LastSyncedAt)
		}
	}
	return index
}

// Resolve is WatermarkIndex.resolve: the watermark for one (source key,
// dataset), nil when no tier holds one.
func (index *WatermarkIndex) Resolve(sourceKey, dataset string) *time.Time {
	key := watermarkPair{sourceKey, dataset}
	if at, ok := index.canonical[key]; ok {
		return at
	}
	if at, ok := index.legacyTarget[key]; ok {
		return at
	}
	for _, legacy := range sortedLegacyTargets(dataset) {
		if at, ok := index.rawLegacy[watermarkPair{sourceKey, legacy}]; ok {
			return at
		}
	}
	return nil
}

// WatermarkLookupValues is WatermarkIndex.relevant_lookup_values: every
// dataset_key or target value that could satisfy a tier for one of the
// datasets, sorted.
func WatermarkLookupValues(datasets []string) []string {
	seen := map[string]bool{}
	for _, dataset := range datasets {
		seen[dataset] = true
		for _, legacy := range incrementalLegacyTargets(dataset) {
			seen[legacy] = true
		}
	}
	values := make([]string, 0, len(seen))
	for value := range seen {
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// incrementalLegacyTargets is watermarks.py's
// _DATASET_KEY_TO_LEGACY_TARGETS: a dataset's legacy targets, only for a
// dataset with an INCREMENTAL watermark (repo-metadata and the other
// no-watermark datasets never resolve through the raw legacy row). The
// planner reads a watermark only for incremental datasets, so the filter
// changes nothing it plans.
func incrementalLegacyTargets(dataset string) []string {
	if providersync.DatasetWatermark(dataset) != providersync.WatermarkIncremental {
		return nil
	}
	return legacyTargetsByDataset[dataset]
}

func sortedLegacyTargets(dataset string) []string {
	targets := append([]string(nil), incrementalLegacyTargets(dataset)...)
	sort.Strings(targets)
	return targets
}

// HeavyRatchet is sync/watermark_lag.py's heavy_max_window_days and
// heavy_net_advance_seconds for this process's environment: the HEAVY
// incremental window cap in days the planner actually uses (widened past
// SYNC_WATERMARK_OVERLAP, never below one day), and the net watermark
// advance of one capped tick in seconds (cap minus overlap, at least one).
func HeavyRatchet() (capDays int, netAdvanceSeconds int64) {
	overlapSeconds := int64(boundedEnvInt("SYNC_WATERMARK_OVERLAP", 0, 0))
	window := effectiveHeavyMaxWindow(time.Duration(overlapSeconds) * time.Second)
	capDays = int(window / (24 * time.Hour))
	if capDays < 1 {
		capDays = defaultIncrementalHeavyMaxWindowDays
	}
	netAdvanceSeconds = int64(capDays)*24*60*60 - overlapSeconds
	if netAdvanceSeconds < 1 {
		netAdvanceSeconds = 1
	}
	return capDays, netAdvanceSeconds
}

// WatermarkLag is sync/watermark_lag.py's WatermarkLag for one dataset.
type WatermarkLag struct {
	WatermarkAt   *time.Time
	LagSeconds    *int64
	CatchingUp    bool
	TicksBehind   *int64
	WindowCapDays int
}

// ComputeWatermarkLag is compute_watermark_lag with the cap and net advance
// resolved once by the caller: lag is now minus the watermark in whole
// seconds (truncated, clamped at zero); catching up only for a "heavy" cost
// class trailing by strictly more than the cap; ticks behind the ceiling of
// lag over the net advance, only when catching up.
func ComputeWatermarkLag(costClass string, watermarkAt *time.Time, now time.Time, capDays int, netAdvanceSeconds int64) WatermarkLag {
	if capDays < 1 {
		capDays = defaultIncrementalHeavyMaxWindowDays
	}
	out := WatermarkLag{WindowCapDays: capDays}
	if watermarkAt == nil {
		return out
	}
	watermark := watermarkAt.UTC()
	out.WatermarkAt = &watermark
	now = now.UTC()
	// int(timedelta.total_seconds()): whole seconds plus the microsecond
	// difference, truncated toward zero. Computed from Unix seconds so a
	// span beyond time.Duration's range is still exact.
	seconds := now.Unix() - watermark.Unix()
	micros := int64(now.Nanosecond()/1000) - int64(watermark.Nanosecond()/1000)
	if seconds > 0 && micros < 0 {
		seconds--
	} else if seconds < 0 && micros > 0 {
		seconds++
	}
	if seconds < 0 {
		seconds = 0
	}
	out.LagSeconds = &seconds
	out.CatchingUp = costClass == heavyCostClass && seconds > int64(capDays)*24*60*60
	if out.CatchingUp {
		advance := netAdvanceSeconds
		if advance < 1 {
			advance = 1
		}
		ticks := (seconds + advance - 1) / advance
		out.TicksBehind = &ticks
	}
	return out
}
