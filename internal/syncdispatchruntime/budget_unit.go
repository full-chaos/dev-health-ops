package syncdispatchruntime

import (
	"encoding/json"
	"os"
	"strconv"
	"time"
)

// Episode-tracking error categories BudgetGuard reads/writes into
// sync_run_units.result.error_category -- ports budget_guard.py's
// module-level category constants verbatim (lines 34-137 of that file).
const (
	// rateLimitCooldownDeferredCategory / rateLimitCooldownExhaustedCategory
	// are this shared-cooldown gate's OWN categories, distinct from
	// 'budget_deferred' (this file) and 'rate_limit' (the in-worker 429
	// path, sync_units.py) so an operator can tell the three apart
	// (docs/providers/rate-limit-policy.md).
	rateLimitCooldownDeferredCategory  = "rate_limit_cooldown_deferred"
	rateLimitCooldownExhaustedCategory = "rate_limit_cooldown_exhausted"

	// budgetDeferredCategory / budgetDeferralExhaustedCategory are the
	// budget-episode's own categories (CHAOS-3412).
	budgetDeferredCategory          = "budget_deferred"
	budgetDeferralExhaustedCategory = "budget_deferral_exhausted"

	// deferralExhaustedCategory is the aggregate-clock's category
	// (CHAOS-3412 review round 2, F2) -- it names NO single episode.
	deferralExhaustedCategory = "deferral_exhausted"

	// rateLimitEpisodeErrorCategory is the in-worker 429 path's own
	// category (sync_units.py), duplicated here for the same
	// reverse-import-cycle reason Python duplicates it.
	rateLimitEpisodeErrorCategory = "rate_limit"
)

// rateLimitEpisodeErrorCategories / budgetEpisodeErrorCategories are the
// "episode-validated evidence" sets terminalizeUnit's chokepoint checks a
// verdict's asserted episode against -- ports
// _RATE_LIMIT_EPISODE_ERROR_CATEGORIES / _BUDGET_EPISODE_ERROR_CATEGORIES
// verbatim.
var (
	rateLimitEpisodeErrorCategories = map[string]bool{
		rateLimitEpisodeErrorCategory:     true,
		rateLimitCooldownDeferredCategory: true,
	}
	budgetEpisodeErrorCategories = map[string]bool{
		budgetDeferredCategory: true,
	}
)

// episodeKindByErrorCategory ports _EPISODE_KIND_BY_ERROR_CATEGORY verbatim
// -- human-readable names for the aggregate-exhaustion error text.
var episodeKindByErrorCategory = map[string]string{
	budgetDeferredCategory:            "sync budget admission",
	rateLimitCooldownDeferredCategory: "provider rate-limit cooldown",
	rateLimitEpisodeErrorCategory:     "provider rate limit (in-worker 429)",
}

// budgetUnit is the sync_run_units projection BudgetGuard's native core
// needs -- every column native_dispatch_sync_run.go's budget machinery
// reads or writes (CHAOS-4175 map, item 16). result is decoded once at
// load time; only its error_category key is ever read.
type budgetUnit struct {
	id            string
	orgID         string
	syncRunID     string
	integrationID string
	sourceID      string
	provider      string
	datasetKey    string
	costClass     string
	sinceAt       *time.Time
	beforeAt      *time.Time

	status         string
	availableAt    *time.Time
	updatedAt      time.Time
	leaseOwner     *string
	leaseExpiresAt *time.Time
	lastHeartbeat  *time.Time

	rateLimitDeferrals   int
	rateLimitFirstSeenAt *time.Time

	budgetDeferrals       int
	budgetFirstDeferredAt *time.Time

	firstBlockedAt *time.Time

	result map[string]any
	// processorFlags carries validate_provider_family_claim's own
	// family_dataset_* bitset (SyncRunUnit.processor_flags), needed only by
	// Dispatch()'s own provider-family admission check right before
	// enqueueing a claimed unit -- every other reader of budgetUnit ignores
	// this field.
	processorFlags map[string]bool
}

// lastErrorCategory ports _unit_last_error_category verbatim: the unit's
// own last-recorded cause, or "" if result carries none. Every exhaustion
// predicate below treats "" the same way Python treats None -- "no
// evidence", never a match for any episode set.
func (unit budgetUnit) lastErrorCategory() string {
	if unit.result == nil {
		return ""
	}
	category, _ := unit.result["error_category"].(string)
	return category
}

// lastEpisodeKind ports _last_episode_kind verbatim.
func (unit budgetUnit) lastEpisodeKind() string {
	category := unit.lastErrorCategory()
	if kind, ok := episodeKindByErrorCategory[category]; ok {
		return kind
	}
	if category == "" {
		return "unknown (None)"
	}
	return "unknown ('" + category + "')"
}

// windowSpanDays ports window_span_days (budget_types.py) verbatim: the
// unit's own since_at/before_at window, floored at 1 day, defaulting to 1
// when either bound is absent.
func (unit budgetUnit) windowSpanDays() int {
	if unit.sinceAt == nil || unit.beforeAt == nil {
		return 1
	}
	days := int(unit.beforeAt.Sub(*unit.sinceAt).Hours() / 24)
	if days < 1 {
		return 1
	}
	return days
}

func decodeUnitResult(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	return decoded
}

// decodeProcessorFlags decodes sync_run_units.processor_flags (a JSON
// object of family_dataset_* flag -> bool) into the map[string]bool shape
// providerfamilycontract.ValidateClaim expects. A malformed or absent
// value decodes to an empty, non-nil map -- ValidateClaim's own logic
// already treats "flag absent" as false via a plain map lookup, so this
// only needs to guarantee a non-nil map to range over, not distinguish
// "malformed JSON" from "no flags set".
func decodeProcessorFlags(raw []byte) map[string]bool {
	flags := map[string]bool{}
	if len(raw) == 0 {
		return flags
	}
	_ = json.Unmarshal(raw, &flags)
	return flags
}

// staleDispatchCutoff ports budget_guard.py's _stale_dispatch_cutoff,
// reusing dispatch_guard.go's staleDispatchSeconds (same env var,
// SYNC_UNIT_DISPATCH_STALE_SECONDS, same 900s default). Python actually
// has TWO slightly different readers of this one env var -- guard.py's
// _stale_dispatch_seconds_guard floors at max(1, value), budget_guard.py's
// inline _env_int(...) call floors at max(0, value) -- a divergence that
// only bites at the pathological SYNC_UNIT_DISPATCH_STALE_SECONDS=0 (a
// zero-second stale window is nonsensical either way). One canonical
// definition here, matching guard.py's floor, is a deliberate
// simplification of an unintentional two-copies drift in Python, not a
// behavior change any real deployment would ever observe.
func staleDispatchCutoff(now time.Time) time.Time {
	return now.Add(-staleDispatchSeconds())
}

// budgetEnvInt ports budget_guard.py's OWN _env_int verbatim -- NOT
// dispatch_guard.go's envPositiveInt (shared with guard.py's _env_int,
// which floors differently: max(1, value) there vs max(0, value) here).
// Missing or unparseable -> default; a validly-parsed value floors at 0,
// never substitutes default just because it parsed to something small.
// Every call site in budget_guard.py that needs a stricter floor wraps this
// with its OWN outer max(1, ...) -- ported below at each such site, not
// folded into this shared reader.
func budgetEnvInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	if value < 0 {
		return 0
	}
	return value
}

// budgetMaxDeferrals ports _budget_max_deferrals verbatim: max(1, _env_int(...)).
func budgetMaxDeferrals() int {
	value := budgetEnvInt("SYNC_BUDGET_MAX_DEFERRALS", 10)
	if value < 1 {
		return 1
	}
	return value
}

// budgetDeferralWallClockSeconds ports _budget_deferral_wall_clock_seconds
// verbatim (default 6 hours).
func budgetDeferralWallClockSeconds() time.Duration {
	value := budgetEnvInt("SYNC_BUDGET_DEFERRAL_WALL_CLOCK_SECONDS", 6*60*60)
	if value < 1 {
		value = 1
	}
	return time.Duration(value) * time.Second
}

// deferralTotalWallClockSeconds ports _deferral_total_wall_clock_seconds
// verbatim (default 24 hours).
func deferralTotalWallClockSeconds() time.Duration {
	value := budgetEnvInt("SYNC_DEFERRAL_TOTAL_WALL_CLOCK_SECONDS", 24*60*60)
	if value < 1 {
		value = 1
	}
	return time.Duration(value) * time.Second
}
