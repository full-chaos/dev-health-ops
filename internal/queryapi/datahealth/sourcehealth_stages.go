package datahealth

// sourceHealthStages is the closed set of stage codes the source-health field
// may serve. A stored value outside it is served as SourceHealthStageOther, so
// no worker text can reach the field through the stage. The set is every
// category the sync writers persist into a run or unit result (the codes of the
// providersync, syncdispatchruntime, syncreconciler, scheduler/sync and
// jobs/providerunit packages, and the job runtime's own categories);
// TestSourceHealthStagesCoverEveryWriter fails when a writer adds a code that
// is not here.
var sourceHealthStages = map[string]bool{
	"all_artifacts_unreadable":         true,
	"auth":                             true,
	"budget":                           true,
	"budget_deferral_exhausted":        true,
	"budget_deferred":                  true,
	"cancelled":                        true,
	"deferral_exhausted":               true,
	"dispatch_denied":                  true,
	"duplicate_natural_key":            true,
	"effect_recovery_ambiguous":        true,
	"feature_disabled":                 true,
	"github_files_inventory_failed":    true,
	"github_tests_artifact_oversized":  true,
	"idempotency":                      true,
	"invalid_provider_family_claim":    true,
	"not_found":                        true,
	"pagerduty_sync_disabled":          true,
	"pagination_incomplete":            true,
	"panic":                            true,
	"permanent":                        true,
	"previous_release_snapshot":        true,
	"provider_budget_contention":       true,
	"provider_dataset_unavailable":     true,
	"provider_rate_limited":            true,
	"provider_unit_chunk_continuation": true,
	"provider_unit_retryable":          true,
	"rate_limit":                       true,
	"rate_limit_cooldown_deferred":     true,
	"rate_limit_cooldown_exhausted":    true,
	"reference_discovery_failed":       true,
	"repository_identity_ambiguous":    true,
	"response_too_large":               true,
	"result_too_large":                 true,
	"retryable":                        true,
	"route_reconciliation_required":    true,
	"route_unavailable":                true,
	"tenant_scope":                     true,
	"terminal_domain":                  true,
	"terminal_river_delivery":          true,
	"time_limit":                       true,
	"timeout":                          true,
	"validation":                       true,
	"worker_lost":                      true,
	"worker_lost_retry_exhausted":      true,
}
