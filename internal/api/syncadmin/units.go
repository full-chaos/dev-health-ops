package syncadmin

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synccoverage"
)

// Rollup bounds of SyncRunService.build_unit_rollups.
const (
	slowestUnitLimit = 5
	failedUnitIDCap  = 100
)

// decodedUnit is a runUnit with its JSON columns read as json.loads does.
type decodedUnit struct {
	runUnit
	result pyjson.Value
	// resultDict is result when it is a dict, else nil.
	resultDict *pyjson.Object
}

// getRunUnits is integrations.py's get_sync_run_units: the limit query
// (int | None) validated first, then get_run (404 when absent), then every
// unit of the run for the rollups, the retry, exhausted and budget-blocked
// counts and the dataset freshness, and the first limit units (a Python
// slice) as SyncRunUnitResponse.
func (h *handlers) getRunUnits(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	var limit *big.Int
	if raw := pybody.LastQuery(r.URL.Query(), "limit"); raw != nil {
		if value, ok := errs.QueryInt("limit", raw, 0, nil, nil); ok {
			limit = value
		}
	}
	if len(errs) > 0 {
		writeQueryErrors(w, errs)
		return
	}
	id, err := pythonparity.ParseUUID(r.PathValue("run_id"))
	if err != nil {
		policy.WriteDetail(w, http.StatusNotFound, "Sync run not found", nil)
		return
	}
	ctx := r.Context()
	org := orgID(r)
	run, err := h.store.syncRunByID(ctx, org, id)
	if err != nil {
		h.fail(w, r, "get_sync_run", err)
		return
	}
	if run == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Sync run not found", nil)
		return
	}
	rows, err := h.store.runUnits(ctx, org, id)
	if err != nil {
		h.fail(w, r, "list_units", err)
		return
	}
	units := make([]decodedUnit, len(rows))
	for index, row := range rows {
		units[index].runUnit = row
		if units[index].result, err = decodeStored(row.Result); err != nil {
			h.fail(w, r, "decode_unit_result", err)
			return
		}
		units[index].resultDict, _ = units[index].result.(*pyjson.Object)
	}

	summary := unitRollups(units)
	summary.Set("unit_count", pyjson.IntOf(int64(len(units))))
	var nextRetry *time.Time
	exhausted, budgetBlocked := int64(0), int64(0)
	for _, unit := range units {
		if unit.Status == "retrying" && unit.AvailableAt != nil && (nextRetry == nil || unit.AvailableAt.Before(*nextRetry)) {
			nextRetry = unit.AvailableAt
		}
		if unit.resultDict != nil {
			if flag, _ := unit.resultDict.Get("retry_exhausted"); flag == true {
				exhausted++
			} else if category, _ := unit.resultDict.Get("error_category"); category == "worker_lost_retry_exhausted" {
				exhausted++
			}
			if category, _ := unit.resultDict.Get("error_category"); unit.Status == "retrying" && category == "budget_deferred" {
				budgetBlocked++
			}
		}
	}
	summary.Set("next_retry_at", pyTimeOrNone(nextRetry))
	summary.Set("retry_exhausted_unit_count", pyjson.IntOf(exhausted))
	summary.Set("budget_blocked_unit_count", pyjson.IntOf(budgetBlocked))

	freshness, catchingUp, err := h.datasetFreshness(r, org, units)
	if err != nil {
		h.fail(w, r, "dataset_freshness", err)
		return
	}
	summary.Set("dataset_freshness", freshness)
	summary.Set("catching_up_dataset_count", pyjson.IntOf(catchingUp))
	summary.Set("dataset_freshness_scope", "run")

	listed := make([]pyjson.Value, 0, len(units))
	for _, unit := range units[:pySliceEnd(limit, len(units))] {
		response, err := unitResponse(unit)
		if err != nil {
			h.fail(w, r, "render_unit", err)
			return
		}
		listed = append(listed, response)
	}
	summary.Set("units", listed)
	policy.WriteModel(w, http.StatusOK, summarizeInModelOrder(summary), nil)
}

// pySliceEnd is the stop of seq[:limit] for a sequence of length n: nil is
// n, a negative limit counts from the end, and both clamp to [0, n].
func pySliceEnd(limit *big.Int, n int) int {
	if limit == nil {
		return n
	}
	length := big.NewInt(int64(n))
	stop := new(big.Int).Set(limit)
	if stop.Sign() < 0 {
		stop.Add(stop, length)
		if stop.Sign() < 0 {
			return 0
		}
	}
	if stop.Cmp(length) > 0 {
		return n
	}
	return int(stop.Int64())
}

// countMap is a {key: count} dict in insertion order.
type countMap struct {
	keys   []string
	counts map[string]int64
}

func (c *countMap) add(key string) {
	if c.counts == nil {
		c.counts = map[string]int64{}
	}
	if _, seen := c.counts[key]; !seen {
		c.keys = append(c.keys, key)
	}
	c.counts[key]++
}

func (c *countMap) object() *pyjson.Object {
	out := pyjson.NewObject()
	for _, key := range c.keys {
		out.Set(key, pyjson.IntOf(c.counts[key]))
	}
	return out
}

// nestedCountMap is a {key: {status: count}} dict in insertion order.
type nestedCountMap struct {
	keys  []string
	inner map[string]*countMap
}

func (c *nestedCountMap) add(key, status string) {
	if c.inner == nil {
		c.inner = map[string]*countMap{}
	}
	if _, seen := c.inner[key]; !seen {
		c.keys = append(c.keys, key)
		c.inner[key] = &countMap{}
	}
	c.inner[key].add(status)
}

func (c *nestedCountMap) object() *pyjson.Object {
	out := pyjson.NewObject()
	for _, key := range c.keys {
		out.Set(key, c.inner[key].object())
	}
	return out
}

// unitRollups is SyncRunService.build_unit_rollups.
func unitRollups(units []decodedUnit) *pyjson.Object {
	var byStatus, byCostClass, errorCategories countMap
	var bySource, byDataset nestedCountMap
	failedIDs := []pyjson.Value{}
	failedCount := int64(0)
	type timed struct {
		seconds int64
		id      string
	}
	var timedUnits []timed
	failedSources, failedDatasets := map[string]bool{}, map[string]bool{}
	for _, unit := range units {
		status, source, dataset, id := unit.Status, unit.SourceID.String(), unit.DatasetKey, unit.ID.String()
		byStatus.add(status)
		bySource.add(source, status)
		byDataset.add(dataset, status)
		byCostClass.add(unit.CostClass)
		if status == "failed" {
			failedCount++
			if len(failedIDs) < failedUnitIDCap {
				failedIDs = append(failedIDs, id)
			}
			failedSources[source] = true
			failedDatasets[dataset] = true
			category := pyjson.Value("unknown")
			if unit.resultDict != nil {
				if value, ok := unit.resultDict.Get("error_category"); ok {
					category = value
				}
			}
			errorCategories.add(pyjson.Str(category))
		}
		if unit.DurationSeconds != nil {
			timedUnits = append(timedUnits, timed{*unit.DurationSeconds, id})
		}
	}
	// list.sort(key=..., reverse=True) is stable: equal durations keep
	// their order.
	sort.SliceStable(timedUnits, func(left, right int) bool { return timedUnits[left].seconds > timedUnits[right].seconds })
	slowest := []pyjson.Value{}
	for index, unit := range timedUnits {
		if index == slowestUnitLimit {
			break
		}
		slowest = append(slowest, unit.id)
	}
	var partial pyjson.Value
	if byStatus.counts["success"] > 0 && byStatus.counts["failed"] > 0 {
		summary := pyjson.NewObject()
		summary.Set("failed_sources", sortedSet(failedSources))
		summary.Set("failed_datasets", sortedSet(failedDatasets))
		summary.Set("error_categories", errorCategories.object())
		partial = summary
	}
	out := pyjson.NewObject()
	out.Set("by_status", byStatus.object())
	out.Set("by_source", bySource.object())
	out.Set("by_dataset", byDataset.object())
	out.Set("by_cost_class", byCostClass.object())
	out.Set("slowest_unit_ids", slowest)
	out.Set("failed_unit_ids", failedIDs)
	out.Set("failed_unit_count", pyjson.IntOf(failedCount))
	out.Set("partial_failure_summary", partial)
	return out
}

func sortedSet(values map[string]bool) []pyjson.Value {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]pyjson.Value, len(keys))
	for index, key := range keys {
		out[index] = key
	}
	return out
}

// freshnessPair is one (source uuid, dataset) of build_dataset_freshness.
type freshnessPair struct {
	sourceID, dataset, watermarkKey, sourceName, costClass string
}

// datasetFreshness is SyncRunService.build_dataset_freshness: one entry per
// (source, effective dataset) of a unit with a source row, INCREMENTAL
// datasets only, first unit wins; the watermark resolved through the
// planner's precedence, the lag judged against the planner's HEAVY cap;
// sorted by source label, source id, dataset. It also returns how many
// entries are catching up.
func (h *handlers) datasetFreshness(r *http.Request, org string, units []decodedUnit) ([]pyjson.Value, int64, error) {
	var pairs []freshnessPair
	seen := map[[2]string]bool{}
	for _, unit := range units {
		if !unit.HasSource {
			continue
		}
		var flags json.RawMessage
		if unit.ProcessorFlags != nil {
			flags = json.RawMessage(*unit.ProcessorFlags)
		}
		for _, dataset := range synccoverage.EffectiveDatasetKeys(unit.DatasetKey, flags) {
			if providersync.DatasetWatermark(dataset) != providersync.WatermarkIncremental {
				continue
			}
			key := [2]string{unit.SourceID.String(), dataset}
			if seen[key] {
				continue
			}
			seen[key] = true
			name := ""
			if unit.SourceFullName != nil && *unit.SourceFullName != "" {
				name = *unit.SourceFullName
			} else if unit.SourceName != nil {
				name = *unit.SourceName
			}
			external := ""
			if unit.SourceExternalID != nil {
				external = *unit.SourceExternalID
			}
			pairs = append(pairs, freshnessPair{
				sourceID: key[0], dataset: dataset, watermarkKey: external, sourceName: name,
				costClass: providersync.DatasetCostClass(unit.Provider, dataset, unit.CostClass),
			})
		}
	}
	if len(pairs) == 0 {
		return []pyjson.Value{}, 0, nil
	}
	sourceKeySet, datasetSet := map[string]bool{}, map[string]bool{}
	for _, pair := range pairs {
		sourceKeySet[pair.watermarkKey] = true
		datasetSet[pair.dataset] = true
	}
	rows, err := h.store.watermarkRows(r.Context(), org, keysOf(sourceKeySet), schedsync.WatermarkLookupValues(keysOf(datasetSet)))
	if err != nil {
		return nil, 0, err
	}
	index := schedsync.NewWatermarkIndex(rows)
	capDays, netAdvance := schedsync.HeavyRatchet()
	now := h.clock()
	sort.SliceStable(pairs, func(left, right int) bool {
		a, b := pairs[left], pairs[right]
		if a.sourceName != b.sourceName {
			return a.sourceName < b.sourceName
		}
		if a.sourceID != b.sourceID {
			return a.sourceID < b.sourceID
		}
		return a.dataset < b.dataset
	})
	entries := make([]pyjson.Value, 0, len(pairs))
	catchingUp := int64(0)
	for _, pair := range pairs {
		lag := schedsync.ComputeWatermarkLag(pair.costClass, index.Resolve(pair.watermarkKey, pair.dataset), now, capDays, netAdvance)
		entry := pyjson.NewObject()
		entry.Set("source_id", pair.sourceID)
		entry.Set("source_name", pair.sourceName)
		entry.Set("dataset_key", pair.dataset)
		entry.Set("cost_class", pair.costClass)
		entry.Set("watermark_at", pyTimeOrNone(lag.WatermarkAt))
		entry.Set("lag_seconds", intOrNone(lag.LagSeconds))
		entry.Set("catching_up", lag.CatchingUp)
		entry.Set("ticks_behind", intOrNone(lag.TicksBehind))
		entry.Set("window_cap_days", pyjson.IntOf(int64(lag.WindowCapDays)))
		entries = append(entries, entry)
		if lag.CatchingUp {
			catchingUp++
		}
	}
	return entries, catchingUp, nil
}

func keysOf(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func intOrNone(value *int64) pyjson.Value {
	if value == nil {
		return nil
	}
	return pyjson.IntOf(*value)
}

// unitResponse is _unit_to_response: the unit's columns, and the result
// dict's retry fields validated as SyncRunUnitResponse declares them. A
// value the model refuses is an error (the Python api's unhandled 500).
func unitResponse(unit decodedUnit) (pyjson.Value, error) {
	get := func(key string) pyjson.Value {
		if unit.resultDict == nil {
			return nil
		}
		value, _ := unit.resultDict.Get(key)
		return value
	}
	out := pyjson.NewObject()
	out.Set("id", unit.ID.String())
	out.Set("org_id", unit.OrgID)
	out.Set("sync_run_id", unit.SyncRunID.String())
	out.Set("integration_id", unit.IntegrationID.String())
	out.Set("source_id", unit.SourceID.String())
	if unit.HasSource {
		out.Set("source_name", stringOrNone(unit.SourceName))
		out.Set("source_full_name", stringOrNone(unit.SourceFullName))
	} else {
		out.Set("source_name", nil)
		out.Set("source_full_name", nil)
	}
	out.Set("provider", unit.Provider)
	out.Set("dataset_key", unit.DatasetKey)
	out.Set("cost_class", unit.CostClass)
	out.Set("mode", unit.Mode)
	out.Set("since_at", pyTimeOrNone(unit.SinceAt))
	out.Set("before_at", pyTimeOrNone(unit.BeforeAt))
	out.Set("status", unit.Status)
	out.Set("attempts", pyjson.IntOf(unit.Attempts))
	out.Set("available_at", pyTimeOrNone(unit.AvailableAt))
	out.Set("rate_limit_deferrals", pyjson.IntOf(unit.RateLimitDeferrals))
	out.Set("budget_deferrals", pyjson.IntOf(unit.BudgetDeferrals))
	out.Set("duration_seconds", intOrNone(unit.DurationSeconds))
	out.Set("error", stringOrNone(unit.Error))
	var err error
	set := func(key string, value pyjson.Value, validate func(pyjson.Value) (pyjson.Value, error)) {
		if err != nil {
			return
		}
		var validated pyjson.Value
		if validated, err = validate(value); err != nil {
			err = fmt.Errorf("%s: %w", key, err)
			return
		}
		out.Set(key, validated)
	}
	set("error_category", get("error_category"), optionalStr)
	out.Set("last_heartbeat_at", pyTimeOrNone(unit.LastHeartbeatAt))
	out.Set("result", unit.result)
	set("retry_count", get("retry_count"), optionalInt)
	set("retry_reason", get("retry_reason"), optionalStr)
	set("last_lease_expired_at", get("last_lease_expired_at"), optionalDatetime)
	nextRetry := get("next_retry_at")
	if nextRetry == nil && unit.Status == "retrying" && unit.AvailableAt != nil {
		out.Set("next_retry_at", pyTime(*unit.AvailableAt))
	} else {
		set("next_retry_at", nextRetry, optionalDatetime)
	}
	set("retry_exhausted", get("retry_exhausted"), optionalBool)
	set("retry_surfaces", get("retry_surfaces"), optionalStrList)
	set("linear_page_count", get("linear_page_count"), optionalInt)
	set("linear_batch_count", get("linear_batch_count"), optionalInt)
	out.Set("created_at", pyTime(unit.CreatedAt))
	out.Set("updated_at", pyTime(unit.UpdatedAt))
	if err != nil {
		return nil, fmt.Errorf("%w: SyncRunUnitResponse %v", errUnrenderable, err)
	}
	return out, nil
}

func optionalStr(value pyjson.Value) (pyjson.Value, error) {
	switch value.(type) {
	case nil, string:
		return value, nil
	}
	return nil, fmt.Errorf("string_type, got %s", pyjson.Repr(value))
}

func optionalInt(value pyjson.Value) (pyjson.Value, error) {
	if value == nil {
		return nil, nil
	}
	number, kind, _ := pybody.PydanticInt(value)
	if kind != "" {
		return nil, fmt.Errorf("%s, got %s", kind, pyjson.Repr(value))
	}
	return pyjson.Int{Int: number}, nil
}

func optionalBool(value pyjson.Value) (pyjson.Value, error) {
	if value == nil {
		return nil, nil
	}
	parsed, kind, _ := pybody.PydanticBool(value)
	if kind != "" {
		return nil, fmt.Errorf("%s, got %s", kind, pyjson.Repr(value))
	}
	return parsed, nil
}

func optionalDatetime(value pyjson.Value) (pyjson.Value, error) {
	if value == nil {
		return nil, nil
	}
	parsed, failure := pybody.PydanticDatetime(value)
	if failure != nil {
		return nil, fmt.Errorf("%s, got %s", failure.Type, pyjson.Repr(value))
	}
	return pytime.Pydantic(parsed), nil
}

func optionalStrList(value pyjson.Value) (pyjson.Value, error) {
	if value == nil {
		return nil, nil
	}
	items, ok := value.([]pyjson.Value)
	if !ok {
		return nil, fmt.Errorf("list_type, got %s", pyjson.Repr(value))
	}
	for _, item := range items {
		if _, isString := item.(string); !isString {
			return nil, fmt.Errorf("string_type, got %s", pyjson.Repr(item))
		}
	}
	return items, nil
}

// summaryFieldOrder is SyncRunUnitSummary's field order.
var summaryFieldOrder = []string{
	"by_status", "by_source", "by_dataset", "by_cost_class", "slowest_unit_ids", "failed_unit_ids",
	"failed_unit_count", "unit_count", "partial_failure_summary", "next_retry_at", "retry_exhausted_unit_count",
	"budget_blocked_unit_count", "dataset_freshness", "catching_up_dataset_count", "dataset_freshness_scope", "units",
}

func summarizeInModelOrder(values *pyjson.Object) *pyjson.Object {
	out := pyjson.NewObject()
	for _, key := range summaryFieldOrder {
		value, _ := values.Get(key)
		out.Set(key, value)
	}
	return out
}
