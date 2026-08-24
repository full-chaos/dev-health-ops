package syncdispatchruntime

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// ambiguousRouteFamilyAttribution mirrors sync_units.py's
// _AMBIGUOUS_ROUTE_FAMILY_ATTRIBUTION verbatim, duplicated here for the
// same reverse-import-cycle reason Python duplicates it across
// budget_guard.py and sync_units.py.
const ambiguousRouteFamilyAttribution = "ambiguous_dimension"

// cooldownKey ports the (org_id, provider, integration_id, route_family)
// -- or, for the ambiguous-attribution fallback, (org_id, provider,
// integration_id, dimension) -- tuple key _active_cooldowns groups
// observations by. A plain Go struct is comparable/hashable, so it can be
// used as a map key directly, matching Python's tuple key exactly.
type cooldownKey struct {
	orgID             string
	provider          string
	integrationID     string
	familyOrDimension string
}

// rateLimitObservation is the provider_rate_limit_observations projection
// _active_cooldowns/_cooldown_expiry need. No Go ORM exists for this table
// yet (grep confirms only migration/retention/authorization code
// references it); queried directly here.
type rateLimitObservation struct {
	id                     string
	orgID                  string
	provider               string
	integrationID          string
	routeFamily            *string
	routeFamilyAttribution *string
	dimension              *string
	retryAfterSeconds      *float64
	resetAt                *time.Time
	observedAt             time.Time
}

// cooldownExpiry ports _cooldown_expiry verbatim: the moment an
// observation's cooldown lifts, coalesce(reset_at, observed_at +
// retry_after_seconds), falling back to a conservative fixed window when
// the signal carried neither.
func cooldownExpiry(observation rateLimitObservation) time.Time {
	if observation.resetAt != nil {
		return *observation.resetAt
	}
	if observation.retryAfterSeconds != nil {
		delay := *observation.retryAfterSeconds
		if delay < 0 {
			delay = 0
		}
		return observation.observedAt.Add(time.Duration(delay * float64(time.Second)))
	}
	return observation.observedAt.Add(rateLimitDefaultCountdownSeconds * time.Second)
}

// cooldownLookbackSeconds ports _cooldown_lookback_seconds verbatim.
func cooldownLookbackSeconds() time.Duration {
	jitterMax := budgetEnvInt("SYNC_BUDGET_DEFERRAL_JITTER_SECONDS", 5)
	skewMargin := budgetEnvInt("SYNC_RATE_LIMIT_COOLDOWN_LOOKBACK_SKEW_SECONDS", 300)
	defaultSeconds := rateLimitMaxTotalWaitSecondsBudget + jitterMax + skewMargin
	return time.Duration(budgetEnvInt("SYNC_RATE_LIMIT_COOLDOWN_LOOKBACK_SECONDS", defaultSeconds)) * time.Second
}

// activeCooldowns ports _active_cooldowns verbatim, including its fail-open
// discipline: ANY error reading the observation store -- the query itself,
// or a single malformed row (a non-finite retry_after_seconds would panic a
// naive port, matching Python's OverflowError) -- must never block
// dispatch. A broken read logs a warning and returns two empty maps, exactly
// as if no cooldown existed, rather than propagating the error like every
// other function in this family does. This is the one deliberate exception
// to that rule, carried over from Python's own explicit design.
func activeCooldowns(
	ctx context.Context, tx pgx.Tx, logger *slog.Logger, syncRunID string, candidates []budgetUnit, now time.Time,
) (familyCooldowns, dimensionCooldowns map[cooldownKey]time.Time) {
	if logger == nil {
		logger = slog.Default()
	}
	familyCooldowns = map[cooldownKey]time.Time{}
	dimensionCooldowns = map[cooldownKey]time.Time{}

	orgIDs := map[string]bool{}
	providers := map[string]bool{}
	integrationIDs := map[string]bool{}
	for _, unit := range candidates {
		orgIDs[unit.orgID] = true
		providers[unit.provider] = true
		integrationIDs[unit.integrationID] = true
	}
	if len(orgIDs) == 0 || len(providers) == 0 || len(integrationIDs) == 0 {
		return familyCooldowns, dimensionCooldowns
	}

	lookbackCutoff := now.Add(-cooldownLookbackSeconds())
	rows, err := tx.Query(ctx, `
SELECT id::text, org_id, provider, integration_id::text, route_family, route_family_attribution,
       dimension, retry_after_seconds, reset_at, observed_at
FROM public.provider_rate_limit_observations
WHERE org_id = ANY($1) AND provider = ANY($2) AND integration_id = ANY($3::uuid[])
  AND observed_at >= $4`,
		mapKeysToSlice(orgIDs), mapKeysToSlice(providers), mapKeysToSlice(integrationIDs), lookbackCutoff)
	if err != nil {
		logger.WarnContext(ctx, "dispatch_sync_run.cooldown_observation_read_failed",
			slog.String("sync_run_id", syncRunID), slog.String("error", err.Error()))
		return familyCooldowns, dimensionCooldowns
	}
	defer rows.Close()

	for rows.Next() {
		var observation rateLimitObservation
		if err := rows.Scan(&observation.id, &observation.orgID, &observation.provider, &observation.integrationID,
			&observation.routeFamily, &observation.routeFamilyAttribution, &observation.dimension,
			&observation.retryAfterSeconds, &observation.resetAt, &observation.observedAt); err != nil {
			logger.WarnContext(ctx, "dispatch_sync_run.cooldown_observation_row_malformed",
				slog.String("sync_run_id", syncRunID), slog.String("error", err.Error()))
			continue
		}
		expiry := cooldownExpiry(observation)
		if !expiry.After(now) {
			continue
		}
		prefix := cooldownKey{orgID: observation.orgID, provider: observation.provider, integrationID: observation.integrationID}
		if observation.routeFamilyAttribution != nil && *observation.routeFamilyAttribution == ambiguousRouteFamilyAttribution {
			if observation.dimension == nil {
				continue
			}
			key := prefix
			key.familyOrDimension = *observation.dimension
			if existing, ok := dimensionCooldowns[key]; !ok || expiry.After(existing) {
				dimensionCooldowns[key] = expiry
			}
			continue
		}
		if observation.routeFamily == nil {
			continue
		}
		key := prefix
		key.familyOrDimension = *observation.routeFamily
		if existing, ok := familyCooldowns[key]; !ok || expiry.After(existing) {
			familyCooldowns[key] = expiry
		}
	}
	if err := rows.Err(); err != nil {
		// No Python precedent for a failure THIS late: Python's .all() fetches
		// every row eagerly, so its fail-open try/except only ever wraps the
		// initial query -- there is no "stream broke after some rows were
		// already read" case to mirror. Keeping whatever this pass already
		// scanned, rather than discarding it for two empty maps, is the
		// closer match to fail-open's actual intent ("a broken read must
		// never block dispatch," not "a broken read must erase correct
		// information this pass already has").
		logger.WarnContext(ctx, "dispatch_sync_run.cooldown_observation_read_failed",
			slog.String("sync_run_id", syncRunID), slog.String("error", err.Error()))
	}
	return familyCooldowns, dimensionCooldowns
}

// matchingCooldownExpiry ports _matching_cooldown_expiry verbatim:
// whole-unit deferral on ANY estimate match, waiting for the LAST one to
// clear (max expiry) when more than one matches.
func matchingCooldownExpiry(
	estimates []budgetEstimate, orgID, provider, integrationID string,
	familyCooldowns, dimensionCooldowns map[cooldownKey]time.Time,
) (time.Time, bool) {
	var latest time.Time
	found := false
	for _, estimate := range estimates {
		familyKey := cooldownKey{orgID: orgID, provider: provider, integrationID: integrationID, familyOrDimension: estimate.RouteFamily}
		if expiry, ok := familyCooldowns[familyKey]; ok {
			if !found || expiry.After(latest) {
				latest = expiry
			}
			found = true
		}
		dimensionKey := cooldownKey{orgID: orgID, provider: provider, integrationID: integrationID, familyOrDimension: estimate.Bucket.Dimension}
		if expiry, ok := dimensionCooldowns[dimensionKey]; ok {
			if !found || expiry.After(latest) {
				latest = expiry
			}
			found = true
		}
	}
	return latest, found
}

func mapKeysToSlice(set map[string]bool) []string {
	values := make([]string, 0, len(set))
	for value := range set {
		values = append(values, value)
	}
	return values
}
