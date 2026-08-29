package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxMetricJobs               = 512
	maxMetricQueues             = 32
	maxMetricDomains            = 128
	maxMetricSyncLeases         = 128
	maxMetricStreams            = 64
	maxMetricBudgets            = 128
	maxMetricConcurrencyBudgets = 128
	poolDomain                  = "domain"
	poolQueueControl            = "queue_control"
	poolResultAcquired          = "acquired"
	poolResultTimeout           = "timeout"
	poolResultCancelled         = "cancelled"
	poolResultError             = "error"
)

// SyncLeaseResult is the bounded result vocabulary for expired sync-lease
// recovery. A failed compare-and-swap is not a recovery result and must not be
// observed.
type SyncLeaseResult string

const (
	SyncLeaseResultRetrying SyncLeaseResult = "retrying"
	SyncLeaseResultFailed   SyncLeaseResult = "failed"
)

// ReportRunLeaseResult is the bounded durable outcome of one expired report
// execution lease. Retrying means a new holder was fenced in; failed means the
// persisted reclaim ceiling terminalized the ReportRun.
type ReportRunLeaseResult string

const (
	ReportRunLeaseResultRetrying ReportRunLeaseResult = "retrying"
	ReportRunLeaseResultFailed   ReportRunLeaseResult = "failed"
)

// IdempotencyRenewalRetiredReason names why a job claim's lease renewal gave
// up. The set is closed and mirrors the two non-transient arms of the renewal
// loop, because they mean opposite things operationally: fenced is a correct
// handover the fleet should barely notice, while transient_exhausted means the
// database was unreachable for longer than a whole lease and a handler was
// stopped mid-flight. Collapsing them into one number would hide a database
// incident behind ordinary takeover traffic.
type IdempotencyRenewalRetiredReason string

const (
	// IdempotencyRenewalFenced is the renewal UPDATE matching zero rows: the
	// run is provably no longer ours.
	IdempotencyRenewalFenced IdempotencyRenewalRetiredReason = "fenced"
	// IdempotencyRenewalTransientExhausted is consecutive renewal failures
	// outlasting the lease itself.
	IdempotencyRenewalTransientExhausted IdempotencyRenewalRetiredReason = "transient_exhausted"
)

// DailyMetricsLeaseStage names which daily-metrics claim reported a lease it
// could not take. The set is closed: partition and finalize are the only two
// daily-metrics claims that hold a bounded lease a dead claimant can orphan.
type DailyMetricsLeaseStage string

const (
	DailyMetricsLeaseStagePartition DailyMetricsLeaseStage = "partition"
	DailyMetricsLeaseStageFinalize  DailyMetricsLeaseStage = "finalize"
)

// DailyMetricsLeaseResult is the bounded durable outcome of one daily-metrics
// claim that met an existing lease. Snoozed means the lease was still live and
// the claimant parked until it expires instead of reporting "nothing to do";
// reclaimed means an expired lease was durably taken over; release_lost means a
// claimant could no longer stand its own row down. Each one is a durable fact
// about a lease that a stalled run depends on, so each is counted.
type DailyMetricsLeaseResult string

const (
	DailyMetricsLeaseResultSnoozed   DailyMetricsLeaseResult = "snoozed"
	DailyMetricsLeaseResultReclaimed DailyMetricsLeaseResult = "reclaimed"
	// DailyMetricsLeaseResultReleaseLost counts a claimant that tried to hand
	// back a lease it no longer held. Release is fenced on a live lease, so a
	// claimant that outlives its own lease cannot stand its row down; that used
	// to vanish into a discarded error, and it is the leading indicator of the
	// orphaned leases behind CHAOS-3991.
	DailyMetricsLeaseResultReleaseLost DailyMetricsLeaseResult = "release_lost"
)

type dailyMetricsLeaseLabels struct {
	Stage  DailyMetricsLeaseStage
	Result DailyMetricsLeaseResult
}

// DailyMetricsRunTrigger identifies which of the two entry points created a
// daily-metrics run: the nightly all-org fixed schedule, or a post-sync
// re-drive for one completed sync (CHAOS-4263). The set is closed: those are
// the only two callers of daily.PostgresStore's Start*RunTx methods.
type DailyMetricsRunTrigger string

const (
	DailyMetricsRunTriggerScheduledFanout DailyMetricsRunTrigger = "scheduled_fanout"
	DailyMetricsRunTriggerPostSync        DailyMetricsRunTrigger = "post_sync"
)

// DailyMetricsDiscoveryOutcome is the bounded result of resolving live
// ClickHouse repository identity for one daily-metrics run (CHAOS-4263).
// NoRepositories must be counted, not just logged: before this, a run that
// discovered zero repositories terminalized as an ordinary success, and a
// dashboard reading zero on this series could not tell "no stale orgs today"
// apart from "the discovery step never runs".
type DailyMetricsDiscoveryOutcome string

const (
	DailyMetricsDiscoveryOutcomeMaterialized   DailyMetricsDiscoveryOutcome = "materialized"
	DailyMetricsDiscoveryOutcomeNoRepositories DailyMetricsDiscoveryOutcome = "no_repositories"
	// DailyMetricsDiscoveryOutcomeRepositoryCapExceeded records a discovery
	// that resolved more repositories than the run is allowed to partition
	// (CHAOS-4263, codex adversarial review round 3) -- a fail-loud outcome,
	// never a silent truncation to the cap.
	DailyMetricsDiscoveryOutcomeRepositoryCapExceeded DailyMetricsDiscoveryOutcome = "repository_cap_exceeded"
)

type dailyMetricsDiscoveryLabels struct {
	Trigger DailyMetricsRunTrigger
	Outcome DailyMetricsDiscoveryOutcome
}

// PostSyncFanoutOutcome is the bounded result of NativePostSyncService.Fanout
// deciding whether one completed sync's post_sync job re-drove daily metrics
// (CHAOS-4263, codex adversarial-review round 2). "no_repositories" here
// names the same "nothing published" outcome as DailyMetricsDiscoveryOutcome
// for dashboard continuity, even though at THIS layer it means "this sync had
// no daily-relevant capability" or "no successful sync_run_unit at all" --
// live ClickHouse repository discovery itself happens later, inside the
// daily_dispatch job this outcome would have published, never inside Fanout.
type PostSyncFanoutOutcome string

const (
	PostSyncFanoutOutcomePublished      PostSyncFanoutOutcome = "published"
	PostSyncFanoutOutcomeNoRepositories PostSyncFanoutOutcome = "no_repositories"
	PostSyncFanoutOutcomeError          PostSyncFanoutOutcome = "error"
)

func postSyncFanoutOutcomes() []PostSyncFanoutOutcome {
	return []PostSyncFanoutOutcome{
		PostSyncFanoutOutcomePublished, PostSyncFanoutOutcomeNoRepositories, PostSyncFanoutOutcomeError,
	}
}

// TeamRepoOwnershipDerivationOutcome is the bounded result of one
// sync.team_repo_ownership_derivation worker run (CHAOS-4365 item 1b).
// "no_signal" names an org whose team_project_ownership/linkage rows exist
// but genuinely produced nothing (every candidate conflicted, or matched no
// repo-bearing item) -- the designed-empty case, not a failure.
// "inputs_not_ready" (team-lead ruling, codex finding #4, 2026-08-28) is a
// DIFFERENT zero-row case: this org has zero team_project_ownership rows,
// or zero linkage rows of any kind (work_items, work_item_dependencies,
// work_graph_issue_pr) -- the first-sync gap this producer's deliberately
// unsequenced Fanout publish (no prerequisite completion key, same as
// team-autoimport, see native_post_sync.go's publishTeamRepoOwnershipDerivation)
// can hit when its sibling producers (team-autoimport's async Python
// bridge, the workgraph builder) have not landed their own writes for this
// org yet. It converges on the next qualifying sync (idempotent,
// re-triggered by every sync with git||hasWorkItems) -- observable here so
// a persistently high inputs_not_ready rate for one org is diagnosable,
// rather than sequenced against those other producers' own completion.
// "error" is a genuine ClickHouse read/write failure the worker's own
// retry/backoff will re-attempt.
// "route_missing" (team-lead ruling, 2026-08-28: "non-fatal != silent") is a
// DISTINCT failure surfaced from the PUBLISH side, not the worker side: the
// Fanout's sibling writer (native_post_sync.go's
// publishTeamRepoOwnershipDerivation) swallows a deterministic outbox
// rejection (worker_job_routes has no row for this kind, or the row is
// paused -- exactly the CHAOS-4365 item 1b deploy-ordering gap: migration
// 0115 must land before/with the worker image, or every publish attempt
// hits this) so the rest of the fanout's handoffs are never taken down with
// it (CHAOS-3946) -- but a swallowed rejection must never also be a SILENT
// one. This outcome, plus an ERROR-level slog line
// (PostSyncTeamRepoOwnershipDerivationFailedMessage), makes a persistently
// missing/paused route visible instead of indistinguishable from a
// healthy no-op.
// "rows_retracted" (team-lead ruling, 2026-08-28, codex R3 finding: "removed
// ownership remains authorized indefinitely") is observed SEPARATELY from
// the primary outcome above whenever a run closes out one or more prior
// inferred (team, repo) claims absent from its new derivation -- a run can
// both retract stale claims and write fresh ones (or error afterward), and
// neither fact should shadow the other in telemetry.
type TeamRepoOwnershipDerivationOutcome string

const (
	TeamRepoOwnershipDerivationOutcomeRowsWritten    TeamRepoOwnershipDerivationOutcome = "rows_written"
	TeamRepoOwnershipDerivationOutcomeNoSignal       TeamRepoOwnershipDerivationOutcome = "no_signal"
	TeamRepoOwnershipDerivationOutcomeInputsNotReady TeamRepoOwnershipDerivationOutcome = "inputs_not_ready"
	TeamRepoOwnershipDerivationOutcomeError          TeamRepoOwnershipDerivationOutcome = "error"
	TeamRepoOwnershipDerivationOutcomeRouteMissing   TeamRepoOwnershipDerivationOutcome = "route_missing"
	TeamRepoOwnershipDerivationOutcomeRowsRetracted  TeamRepoOwnershipDerivationOutcome = "rows_retracted"
)

func teamRepoOwnershipDerivationOutcomes() []TeamRepoOwnershipDerivationOutcome {
	return []TeamRepoOwnershipDerivationOutcome{
		TeamRepoOwnershipDerivationOutcomeRowsWritten,
		TeamRepoOwnershipDerivationOutcomeNoSignal,
		TeamRepoOwnershipDerivationOutcomeInputsNotReady,
		TeamRepoOwnershipDerivationOutcomeError,
		TeamRepoOwnershipDerivationOutcomeRouteMissing,
		TeamRepoOwnershipDerivationOutcomeRowsRetracted,
	}
}

// TeamRepoOwnershipResolutionArm identifies which identity resolved a
// derived team_repo_ownership row (CHAOS-4458 part (b)): the pre-existing
// direct project_id join (every provider), or the linear_team_key join added
// because a Linear-only org's team_project_ownership rows are keyed
// "{org_id}:linear:{team_key}" while work_items.project_id for a Linear item
// carries the raw Linear Project UUID -- a disjoint id space by design
// (providersync/team_repo_ownership_derivation.go's TeamRepoOwnershipWorkItem
// doc comment has the full trace). Mirrors providersync's own
// TeamRepoOwnershipResolutionArm* string constants as a distinct type here
// since jobruntime must not import providersync.
type TeamRepoOwnershipResolutionArm string

const (
	TeamRepoOwnershipResolutionArmProjectID     TeamRepoOwnershipResolutionArm = "project_id"
	TeamRepoOwnershipResolutionArmLinearTeamKey TeamRepoOwnershipResolutionArm = "linear_team_key"
)

func teamRepoOwnershipResolutionArms() []TeamRepoOwnershipResolutionArm {
	return []TeamRepoOwnershipResolutionArm{
		TeamRepoOwnershipResolutionArmProjectID,
		TeamRepoOwnershipResolutionArmLinearTeamKey,
	}
}

// TeamCatalogEntryPoint names which of the two Python team-autoimport call
// sites CHAOS-4431's native/bridge dispatch decided for (they are NOT
// equivalent -- see teamCatalogAutoimportBridge's doc comment,
// cmd/dev-health-worker/team_catalog_clients.go): reference discovery
// mirrors run_team_autoimport_strict (selection-blind), post_sync mirrors
// run_team_autoimport (selection-gated).
type TeamCatalogEntryPoint string

const (
	TeamCatalogEntryPointReferenceDiscovery TeamCatalogEntryPoint = "reference_discovery"
	TeamCatalogEntryPointPostSync           TeamCatalogEntryPoint = "post_sync"
)

func teamCatalogEntryPoints() []TeamCatalogEntryPoint {
	return []TeamCatalogEntryPoint{TeamCatalogEntryPointReferenceDiscovery, TeamCatalogEntryPointPostSync}
}

// TeamCatalogOutcome is the bounded dispatch result for one (provider,
// entry point) call: "native" ran a registered Go collector and never
// touched the Python bridge; "bridge" fell through to the existing Python
// path unchanged (provider has no native collector, or provider resolution
// failed); "skipped" is the post_sync-only case of a native provider whose
// org has every CHAOS-4323 selection off -- nothing to import either way, so
// neither the collector nor the bridge ran; "native_failed_nonfatal" is the
// post_sync-only case of a native collector call that returned an error --
// mirroring Python's non-strict run_team_autoimport, which catches every
// populator exception and returns a zero summary rather than failing the
// job (the strict reference-discovery seam never uses this value; it
// propagates the error instead, same as run_team_autoimport_strict).
type TeamCatalogOutcome string

const (
	TeamCatalogOutcomeNative               TeamCatalogOutcome = "native"
	TeamCatalogOutcomeBridge               TeamCatalogOutcome = "bridge"
	TeamCatalogOutcomeSkipped              TeamCatalogOutcome = "skipped"
	TeamCatalogOutcomeNativeFailedNonfatal TeamCatalogOutcome = "native_failed_nonfatal"
	// TeamCatalogOutcomeRosterPreservationFailed (team-lead ruling,
	// 2026-08-28) names a successful collection call that nonetheless
	// reported TeamCatalogResult.RosterPreservationFailed: the existing-
	// manual_members pre-read (CHAOS-4446's PreserveExistingTeamManualMembers)
	// failed and the collector chose to continue rather than hard-fail the
	// whole write. No collector does this today -- Linear's always
	// hard-fails instead, surfacing as native_failed_nonfatal/a propagated
	// error -- but the outcome exists so a future collector's choice to
	// soft-fail that one step is never silently indistinguishable from a
	// clean run.
	TeamCatalogOutcomeRosterPreservationFailed TeamCatalogOutcome = "roster_preservation_failed"
	// TeamCatalogOutcomeCollectorSkipped (codex review finding, CHAOS-4432,
	// 2026-08-28) is DISTINCT from TeamCatalogOutcomeSkipped: that value
	// means every CHAOS-4323 selection was off and neither the collector
	// nor the bridge ever ran. This value means the collector itself WAS
	// called, resolved credentials, attempted its walk, and then chose to
	// report a clean, successful zero result (TeamCatalogResult.Skipped)
	// rather than an error -- e.g. GitLab's non-strict walk-failure Python
	// parity (a root/subgroups/per-group-projects/native-projects fetch
	// failed). Conflating the two into one "skipped" outcome would make
	// dev_health_team_catalog_dispatch_total unable to distinguish "this
	// org has nothing configured, working as intended" from "this
	// provider's fetch is failing and needs attention" -- exactly the
	// signal an operator/alert needs.
	TeamCatalogOutcomeCollectorSkipped TeamCatalogOutcome = "collector_skipped"
)

func teamCatalogOutcomes() []TeamCatalogOutcome {
	return []TeamCatalogOutcome{
		TeamCatalogOutcomeNative, TeamCatalogOutcomeBridge, TeamCatalogOutcomeSkipped,
		TeamCatalogOutcomeNativeFailedNonfatal, TeamCatalogOutcomeRosterPreservationFailed,
		TeamCatalogOutcomeCollectorSkipped,
	}
}

// TeamCatalogTable is the bounded destination-table set a native team-catalog
// collector writes to (CHAOS-4431): the five reference-catalog tables plus
// the `members` table the Linear collector writes alongside `teams`.
type TeamCatalogTable string

const (
	TeamCatalogTableTeams                TeamCatalogTable = "teams"
	TeamCatalogTableMembers              TeamCatalogTable = "members"
	TeamCatalogTableTeamMemberships      TeamCatalogTable = "team_memberships"
	TeamCatalogTableProjects             TeamCatalogTable = "projects"
	TeamCatalogTableTeamProjectOwnership TeamCatalogTable = "team_project_ownership"
	// TeamCatalogTableTeamRepoOwnership is GitHub's destination table --
	// DISTINCT from TeamCatalogTableTeamProjectOwnership (team-lead ruling,
	// 2026-08-28): TeamCatalogResult.OwnershipWritten and .RepoOwnershipWritten
	// are separate fields specifically so GitHub's rows are never reported
	// under Linear/GitLab's team_project_ownership label.
	TeamCatalogTableTeamRepoOwnership TeamCatalogTable = "team_repo_ownership"
	// TeamCatalogTableSprints is Linear's (CHAOS-4431 codex review P1) cycle-
	// to-sprint reference discovery -- unconditional reference data, never
	// gated on a CHAOS-4323 selection, reported separately from the other
	// five tables so a dashboard can see sprints kept flowing even on a run
	// where every writable category was off.
	TeamCatalogTableSprints TeamCatalogTable = "sprints"
	// TeamCatalogTableTeamsSkippedPolicy counts teams a call deliberately
	// left untouched because their CHAOS-2622 sync_policy was not the
	// auto-apply default (team-lead ruling, 2026-08-28, codex review
	// findings #3/#6's fail-safe guard, pending the CHAOS-4444-class full
	// drift-aware projector). Reported as a row count, not folded into
	// TeamCatalogTableTeams's written count, so "skipped for policy" is never
	// indistinguishable from "genuinely zero teams this run".
	TeamCatalogTableTeamsSkippedPolicy TeamCatalogTable = "teams_skipped_policy"
	// TeamCatalogTableMembershipsSkippedManualConflict counts
	// team_memberships rows a call deliberately left unwritten because of an
	// active manual membership or manual_attribution_fallbacks conflict
	// (team-lead ruling, 2026-08-28, codex review finding #6's fail-safe
	// guard, pending the CHAOS-4444-class full drift-aware projector).
	TeamCatalogTableMembershipsSkippedManualConflict TeamCatalogTable = "team_memberships_skipped_manual_conflict"
	// TeamCatalogTableTeamsStagedForReview counts distinct teams a call
	// staged (or refreshed) at least one PENDING team_drift_changes row for
	// (CHAOS-4444, the full clickhouse_team_drift_projector.py parity on
	// top of TeamCatalogTableTeamsSkippedPolicy's plain-skip interim
	// guard).
	TeamCatalogTableTeamsStagedForReview TeamCatalogTable = "teams_staged_for_review"
	// TeamCatalogTableMembershipsStagedForReview counts team_memberships
	// rows a call staged (or refreshed) a PENDING identity-drift
	// team_drift_changes row for (CHAOS-4444, the row-level counterpart to
	// TeamCatalogTableTeamsStagedForReview, mirroring
	// TeamCatalogTableMembershipsSkippedManualConflict's granularity).
	TeamCatalogTableMembershipsStagedForReview TeamCatalogTable = "team_memberships_staged_for_review"
	// TeamCatalogTableDriftChangesSuperseded counts team_drift_changes rows
	// (team-level and identity-level combined) a call transitioned to
	// STATUS_SUPERSEDED because a previously-pending diff was replaced by a
	// newer, different diff this run (CHAOS-4444).
	TeamCatalogTableDriftChangesSuperseded TeamCatalogTable = "team_drift_changes_superseded"
	// TeamCatalogTableProjectsWithoutKey (CHAOS-4530, "a team key is not a
	// project key") counts `projects` rows this call wrote with a nil
	// project_key -- a qualified subset of TeamCatalogTableProjects's count,
	// same pattern as TeamCatalogTableTeamsSkippedPolicy. Before CHAOS-4530
	// this was invisible: the Linear collector stamped every project-catalog
	// row's project_key with the owning TEAM's key, so project_key was never
	// actually empty and nothing measured the gap. Now that a real Linear
	// project's ownership/catalog rows deliberately carry no per-project key
	// (Linear has no such concept yet), this makes the resulting "reachable
	// by project_key" gap visible per run instead of requiring a live
	// ClickHouse readback to notice it, and gives a signal for when a future
	// genuine per-project key source should close it.
	TeamCatalogTableProjectsWithoutKey TeamCatalogTable = "projects_without_key"
)

func teamCatalogTables() []TeamCatalogTable {
	return []TeamCatalogTable{
		TeamCatalogTableTeams, TeamCatalogTableMembers, TeamCatalogTableTeamMemberships,
		TeamCatalogTableProjects, TeamCatalogTableTeamProjectOwnership, TeamCatalogTableTeamRepoOwnership,
		TeamCatalogTableSprints, TeamCatalogTableTeamsSkippedPolicy, TeamCatalogTableMembershipsSkippedManualConflict,
		TeamCatalogTableTeamsStagedForReview, TeamCatalogTableMembershipsStagedForReview, TeamCatalogTableDriftChangesSuperseded,
		TeamCatalogTableProjectsWithoutKey,
	}
}

// teamCatalogProvider clamps to the same closed, known provider set
// zeroUnitFinalizationProvider already uses, for the same cardinality
// discipline -- an unrecognized value folds into "unknown" rather than
// growing the label set without limit.
func teamCatalogProvider(provider string) string {
	return zeroUnitFinalizationProvider(provider)
}

type teamCatalogDispatchLabels struct {
	Provider   string
	EntryPoint TeamCatalogEntryPoint
	Outcome    TeamCatalogOutcome
}

type teamCatalogRowsLabels struct {
	Provider string
	Table    TeamCatalogTable
}

// DailyMetricsNativeFamilyOutcome is the bounded durable outcome of one
// native family compute attempt inside a metrics.daily partition (CHAOS-4276).
// Computed means the executor wrote rows (possibly zero, a legitimate quiet
// day -- see rowsWritten, not this label, for that) and the compatibility
// bridge skipped it; Refused means the executor failed and the compatibility
// bridge computed it instead (the fail-open policy -- see
// PartitionHandler.computeNativeFamilies).
type DailyMetricsNativeFamilyOutcome string

const (
	DailyMetricsNativeFamilyOutcomeComputed DailyMetricsNativeFamilyOutcome = "computed"
	DailyMetricsNativeFamilyOutcomeRefused  DailyMetricsNativeFamilyOutcome = "refused"
)

func dailyMetricsNativeFamilyOutcomes() []DailyMetricsNativeFamilyOutcome {
	return []DailyMetricsNativeFamilyOutcome{
		DailyMetricsNativeFamilyOutcomeComputed, DailyMetricsNativeFamilyOutcomeRefused,
	}
}

type dailyMetricsNativeFamilyOutcomeLabels struct {
	Family  string
	Outcome DailyMetricsNativeFamilyOutcome
}

// DailyMetricsCompatRetryDecision is the bounded durable outcome of one
// ambiguous metrics.daily compatibility-bridge execution (CHAOS-4319). The
// Go side only ever observes "persisted_failed" -- the point PartitionHandler
// gives up retrying a ledger row stuck at "ambiguous" and durably records a
// failed_permanent partition instead of letting River discard the job with
// no trace. "retry_authorized" is the Python bridge's mirror label for the
// companion decision (a classified runner failure that safely re-authorizes
// another attempt); it shares this metric name and label set on the Python
// side (dev_health_metric_compat_retry_total) but is never emitted here,
// since Go never observes that a retry was authorized -- only that one
// eventually wasn't and had to be persisted as terminal.
//
// CHAOS-4543: the four "released_*" decisions below are the non-terminal
// sibling of "persisted_failed" -- PartitionHandler.Work's
// releasePartitionWithReason branches (progress_stalled, capacity_exhausted,
// resource_exhausted, process_signaled) release the partition back to the
// ordinarily re-dispatchable 'failed' status WITH a bounded failure_reason
// attached, rather than terminalizing it. Before CHAOS-4543,
// resource_exhausted and process_signaled had no branch at all here (see
// PartitionHandler.Work) and were invisible on this counter exactly as they
// were invisible in daily_metrics_partitions.failure_reason; progress_stalled
// and capacity_exhausted had a durable failure_reason but never incremented
// this counter either. All four now emit here, one bounded decision value
// per reason, matching the SQL-visible failure_reason column's own
// vocabulary -- an operator can go from a rate/volume signal on this counter
// straight to the affected partitions via `SELECT * FROM
// daily_metrics_partitions WHERE failure_reason = '<reason>'` (the ordinal
// and repo_ids columns already carry the rest of the detail CHAOS-4543's own
// diagnostic needed).
type DailyMetricsCompatRetryDecision string

const (
	DailyMetricsCompatRetryDecisionPersistedFailed           DailyMetricsCompatRetryDecision = "persisted_failed"
	DailyMetricsCompatRetryDecisionReleasedProgressStalled   DailyMetricsCompatRetryDecision = "released_progress_stalled"
	DailyMetricsCompatRetryDecisionReleasedCapacityExhausted DailyMetricsCompatRetryDecision = "released_capacity_exhausted"
	DailyMetricsCompatRetryDecisionReleasedResourceExhausted DailyMetricsCompatRetryDecision = "released_resource_exhausted"
	DailyMetricsCompatRetryDecisionReleasedProcessSignaled   DailyMetricsCompatRetryDecision = "released_process_signaled"
	// DailyMetricsCompatRetryDecisionReleasedResourceExhaustedDeterministic
	// (CHAOS-4543) is the subset of "released_resource_exhausted" the Go
	// side ALSO knows is a known-deterministic guard (never the RSS
	// watchdog) -- see ErrCompatibilityResourceExhaustedDeterministic's doc
	// comment. Counted separately so the rate of wasted-attempt-budget
	// discards (a deterministic guard that will keep refusing until an
	// operator changes something) is distinguishable from ordinary
	// transient resource pressure on this same shared counter.
	DailyMetricsCompatRetryDecisionReleasedResourceExhaustedDeterministic DailyMetricsCompatRetryDecision = "released_resource_exhausted_deterministic"
)

func dailyMetricsCompatRetryDecisions() []DailyMetricsCompatRetryDecision {
	return []DailyMetricsCompatRetryDecision{
		DailyMetricsCompatRetryDecisionPersistedFailed,
		DailyMetricsCompatRetryDecisionReleasedProgressStalled,
		DailyMetricsCompatRetryDecisionReleasedCapacityExhausted,
		DailyMetricsCompatRetryDecisionReleasedResourceExhausted,
		DailyMetricsCompatRetryDecisionReleasedProcessSignaled,
		DailyMetricsCompatRetryDecisionReleasedResourceExhaustedDeterministic,
	}
}

// dailyMetricsNativeFamilies is the closed, compile-time set of
// metrics.daily families a native Go executor MAY exist for (CHAOS-4276),
// mirrored the same way dailyMetricsZeroRowsWithSourceFamilies is: a fixed
// list, not a runtime-registered MetricDimensions field. A runtime
// dimension would need the collector built AFTER every native executor's
// construction outcome is known, but a construction failure (chris's ruling:
// fail-open -- the family just stays on the compatibility bridge) must not
// block worker startup or leave the collector unable to report the refusal
// it is trying to observe. Every series below is emitted at zero from
// process start, exactly like the zero-rows-with-source series, so a
// construction refusal is visible on the SAME counter a healthy deploy
// would move, not absent from it.
var dailyMetricsNativeFamilies = []string{"team_wellbeing", "repo_user_commit"}

// dailyMetricsZeroRowsWithSourceFamilies is the closed set of metrics.daily
// families CHAOS-4263 scoped this check to (chris's ruling 2026-08-25): the
// four the RCA found stale despite fresh source data. The other 19 families in
// families.json are out of this ticket.
var dailyMetricsZeroRowsWithSourceFamilies = []string{"cicd", "deploy", "incident", "testops_risk"}

// remainingMetricsOpenDayZeroRowFamilies is the closed set of remaining-
// metrics families CHAOS-4384 scoped this check to. Only dora's own
// StartRunTx cross-trigger coverage check (postgres.go) calls this today --
// the same "only dora needs it" scoping doraScopeDayAndBackfill already
// documents -- but the list stays open to extension the same way the
// CHAOS-4263 daily-family list above is.
var remainingMetricsOpenDayZeroRowFamilies = []string{"dora"}

// remainingMetricsManualBackfillFamilies is the closed set of remaining-
// metrics families the CHAOS-4254 `metrics remaining start` operator command
// accepts -- the same day-scoped set
// internal/jobs/metrics/remaining.ManualBackfillDayScopedFamilies pins, kept
// independently here (rather than imported) to avoid a jobruntime ->
// remaining import: remaining already imports jobruntime for its lease and
// coverage observers, and a reverse import would cycle.
var remainingMetricsManualBackfillFamilies = []string{"complexity", "dora", "release_impact"}

// RemainingMetricsManualBackfillOutcome is the bounded durable outcome of one
// CHAOS-4254 operator-triggered manual backfill request for a historical
// (organization, family, day) no automatic trigger ever dispatched.
type RemainingMetricsManualBackfillOutcome string

const (
	// RemainingMetricsManualBackfillOutcomeStarted means a new run/partition
	// was inserted and (when a live publisher was wired) dispatched.
	RemainingMetricsManualBackfillOutcomeStarted RemainingMetricsManualBackfillOutcome = "started"
	// RemainingMetricsManualBackfillOutcomeAlreadyRan means this exact
	// (org, family, generation, day) request had already been started by a
	// prior call -- StartRunTx's ON CONFLICT DO NOTHING idempotency, not an
	// error.
	RemainingMetricsManualBackfillOutcomeAlreadyRan RemainingMetricsManualBackfillOutcome = "already_ran"
	// RemainingMetricsManualBackfillOutcomeAlreadyCovered means the command
	// refused: a succeeded partition already provides unambiguous non-zero
	// or ambiguous multi-day coverage for this day, and inserting another
	// would risk duplicating rows in an append-only table with no dedup on
	// replay (CHAOS-4242).
	RemainingMetricsManualBackfillOutcomeAlreadyCovered RemainingMetricsManualBackfillOutcome = "already_covered"
	// RemainingMetricsManualBackfillOutcomeInProgress means the command
	// refused: an automatic run for this day is still pending/running under
	// a different generation, and its eventual completion is invisible to
	// the succeeded-only coverage check (codex review round 2, P1).
	RemainingMetricsManualBackfillOutcomeInProgress RemainingMetricsManualBackfillOutcome = "in_progress"
	// RemainingMetricsManualBackfillOutcomeExhausted means every generation
	// the command tried (the base one plus its bounded numbered retries)
	// already existed in a terminal, not-usefully-covering state -- no new
	// work was dispatched (codex review round 3, P2).
	RemainingMetricsManualBackfillOutcomeExhausted RemainingMetricsManualBackfillOutcome = "exhausted"
)

func remainingMetricsManualBackfillOutcomes() []RemainingMetricsManualBackfillOutcome {
	return []RemainingMetricsManualBackfillOutcome{
		RemainingMetricsManualBackfillOutcomeStarted,
		RemainingMetricsManualBackfillOutcomeAlreadyRan,
		RemainingMetricsManualBackfillOutcomeAlreadyCovered,
		RemainingMetricsManualBackfillOutcomeInProgress,
		RemainingMetricsManualBackfillOutcomeExhausted,
	}
}

type remainingMetricsManualBackfillLabels struct {
	Family  string
	Outcome RemainingMetricsManualBackfillOutcome
}

// dailyMetricsRedriveReasons is the closed set of bounded reasons an
// operator-invoked stranded-partition redrive (CHAOS-4358) can report.
// "failed_permanent_reset" counts partitions whose terminal failed_permanent
// state (CHAOS-4319) the operator explicitly overrode back to the ordinary
// re-dispatchable 'failed'; "dispatch_redriven" counts partitions that became
// reachable because a fresh metrics.daily_dispatch job was enqueued for their
// run outside the outbox's normal per-run dedupe (the CHAOS-4358 gap: once
// River discards every daily_partition job for a run, nothing else ever
// re-publishes that dispatch). Both are counted in partitions, not runs, so
// the series answers "how much stranded work moved", not "how many API
// calls happened".
var dailyMetricsRedriveReasons = []string{"failed_permanent_reset", "dispatch_redriven"}

// dailyMetricsFinalizeSweepOutcomes is the closed set of bounded outcomes a
// CHAOS-4389 stranded-finalize sweep can report. "detected" counts runs
// found status='running' with every partition succeeded but finalization
// never reaching a terminal state; "finalized" counts runs a fresh
// metrics.daily_finalize job was actually enqueued for (a subset of
// detected -- a run whose finalize lease is still live is left alone this
// pass and only becomes eligible on a later one).
var dailyMetricsFinalizeSweepOutcomes = []string{"detected", "finalized"}

// dailyMetricsFinalizeLedgerRepairOutcomes is the closed set of bounded
// outcomes CHAOS-4409's finalize-ledger bulk repair can report per execution
// row it considered. "repaired" counts a daily/finalize
// metric_compatibility_executions row (stuck 'ambiguous' or stuck-
// 'executing') this pass moved to 'retry_authorized', unblocking
// `daily-finalize --run`/`--all-complete` for the run it belongs to;
// "skipped_claim_active" counts a row left untouched because its original
// claim still read as live at repair time -- exactly what a single /repair
// call against it would refuse with 409 today. See
// worker_metrics.py's _bulk_redrive_ambiguous_executions, whose
// operation='partition'-only filter (closed before CHAOS-4409) is what let a
// stuck finalize row answer ambiguous_refused forever.
var dailyMetricsFinalizeLedgerRepairOutcomes = []string{"repaired", "skipped_claim_active"}

// dailyMetricsFinalizeRedriveOutcomes is the closed set of bounded outcomes
// a CHAOS-4405 historical finalize-redrive pass can report. "redriven"
// counts days a fresh metrics.daily_finalize job was actually enqueued for a
// run that was already eligible (never attempted, or failed/expired-lease);
// "redriven_reset_from_succeeded" counts days that additionally required
// resetting an already-'succeeded' run back to a claimable state (team-lead's
// approval condition (5), so a dashboard can tell "an ordinary redrive" apart
// from "we mutated a terminal row" at a glance); "skipped_ineligible" counts
// days in the requested range with no eligible run (none exists for that day,
// or its partitions are not 100% succeeded). A dry run (RedriveFinalizeForRange's
// dryRun=true) never calls this observer for the three outcomes above -- it
// makes no durable state change, so it must never move a counter meant to
// describe real work.
//
// "redriven_failed" (team-lead escalation on conditions (2)/(3),
// 2026-08-28) is different: it is observed by
// PostgresStore.transitionFinalize (postgres.go), not
// RedriveFinalizeForRange, the moment a redriven finalize's own claim
// resolves as a failure and this run's finalize-redrive event closes
// 'closed_failed' -- an indefinite time after the original redrive call
// returned. Counted in runs, not calendar days (transitionFinalize has no
// day-range concept), unlike the other three outcomes here.
//
// "redriven_orphaned" (team-lead escalation, same day, closing the
// remaining residual gap) is observed by
// PostgresStore.ReconcileOrphanedFinalizeRedriveRuns (redrive.go), called
// from `daily-finalize --all-complete` right before FindStrandedFinalizeRuns
// itself. It closes an 'open' event whose redriven River job was
// discarded/cancelled, or never reached River at all, before ClaimFinalize
// was ever called for it -- the one shape neither "redriven_failed" (a claim
// that DID resolve) nor CompleteFinalize/ReleaseFinalize (which only ever
// fire for a claim that happens) can ever observe, since no claim happens
// here at all. Counted in runs, like "redriven_failed".
var dailyMetricsFinalizeRedriveOutcomes = []string{
	"redriven", "redriven_reset_from_succeeded", "skipped_ineligible", "redriven_failed", "redriven_orphaned",
}

// dailyMetricsPartitionRecomputeFamilies mirrors
// daily.SupportedPartitionRecomputeFamilies (kept as a duplicated closed
// list rather than an import: internal/jobruntime cannot import
// internal/jobs/metrics/daily, the same layering reason
// remainingMetricsManualBackfillFamilies duplicates
// remaining.ManualBackfillDayScopedFamilies above).
var dailyMetricsPartitionRecomputeFamilies = []string{"repo_user_commit"}

// dailyMetricsPartitionRecomputeOutcomes is the closed set of bounded
// outcomes a CHAOS-4459 historical partition-recompute pass can report.
// "redriven" counts calendar days a fresh metrics.daily_partition job was
// actually enqueued for a run that was reset from 'succeeded';
// "skipped_ineligible" counts days in the requested range with no eligible
// run. A dry run never calls this observer -- it makes no durable state
// change, so it must never move a counter meant to describe real work.
var dailyMetricsPartitionRecomputeOutcomes = []string{"redriven", "skipped_ineligible"}

type dailyMetricsPartitionRecomputeLabels struct {
	Family  string
	Outcome string
}

var durationBuckets = []float64{
	0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
}

// repoCountBuckets (CHAOS-4329) matches Python's
// DEV_HEALTH_TEAM_METRICS_DAILY_REPO_COUNT buckets exactly
// (metrics/prometheus.py) -- small-integer repo fan-out counts, not
// durations.
var repoCountBuckets = []float64{1, 2, 3, 5, 8, 13, 21, 34, 55}

// StreamLabels are pre-registered so stream telemetry cannot create an
// unbounded consumer-group label set.
type StreamLabels struct {
	Stream        string
	ConsumerGroup string
}

// BudgetLabels are pre-registered provider/cost-class dimensions. They never
// contain an organization, repository, or credential identity.
type BudgetLabels struct {
	Provider  string
	CostClass string
}

// ConcurrencyBudgetLabels identify one registry-owned durable budget without
// including organization identity. Tenant identity is a lease-key concern,
// never a metric label.
type ConcurrencyBudgetLabels struct {
	Kind  string
	Scope string
}

// SyncLeaseLabels are pre-registered provider/dataset-family dimensions for
// expired sync-lease recovery. They never contain tenant or repository data.
type SyncLeaseLabels struct {
	Provider      string
	DatasetFamily string
}

// MetricDimensions is the complete low-cardinality vocabulary accepted by a
// MetricsCollector. Jobs normally come from registry-selected queues;
// sync-lease, stream, and budget pairs come from static deployment
// configuration.
type MetricDimensions struct {
	Jobs               []JobLabels
	DomainTypes        []string
	SyncLeases         []SyncLeaseLabels
	Streams            []StreamLabels
	Budgets            []BudgetLabels
	ConcurrencyBudgets []ConcurrencyBudgetLabels
}

type queueLabels struct {
	Queue string
}

type jobResultLabels struct {
	Job    JobLabels
	Result Result
}

type attemptLabels struct {
	Kind     string
	Result   Result
	Category ErrorCategory
}

type cancellationLabels struct {
	Kind   string
	Reason ErrorCategory
}

type deterministicFailureLabels struct {
	Kind   string
	Reason Reason
}

type syncLeaseResultLabels struct {
	Lease  SyncLeaseLabels
	Result SyncLeaseResult
}

type concurrencyBudgetResultLabels struct {
	Budget ConcurrencyBudgetLabels
	Result string
}

type poolAcquireLabels struct {
	Pool   string
	Result string
}

type histogram struct {
	bounds  []float64
	buckets []uint64
	count   uint64
	sum     float64
}

func newHistogram() *histogram {
	return newHistogramWithBounds(durationBuckets)
}

// newHistogramWithBounds builds a histogram over a caller-supplied bucket
// boundary set instead of the package-default duration buckets (CHAOS-4329:
// teamMetricsDailyRepoCount is a small-integer repo-fan-out count, not a
// duration, so the 0.01s..3600s duration buckets would put every real
// observation in the same bucket).
func newHistogramWithBounds(bounds []float64) *histogram {
	return &histogram{bounds: bounds, buckets: make([]uint64, len(bounds)+1)}
}

func (histogram *histogram) observe(value float64) {
	index := len(histogram.bounds)
	for candidate, upperBound := range histogram.bounds {
		if value <= upperBound {
			index = candidate
			break
		}
	}
	histogram.buckets[index]++
	histogram.count++
	histogram.sum += value
}

// MetricsCollector is a dependency-free Prometheus collector and implements
// Observer. All mutable state is protected by one mutex so related running and
// attempt updates are observed consistently during exposition.
type MetricsCollector struct {
	mu sync.RWMutex

	allowedJobs               map[JobLabels]struct{}
	allowedQueues             map[queueLabels]struct{}
	allowedKinds              map[string]struct{}
	allowedDomains            map[string]struct{}
	allowedSyncLeases         map[SyncLeaseLabels]struct{}
	allowedStreams            map[StreamLabels]struct{}
	allowedBudgets            map[BudgetLabels]struct{}
	allowedConcurrencyBudgets map[ConcurrencyBudgetLabels]struct{}

	runtimeInfo *RuntimeInfo

	jobsAvailable                        map[JobLabels]int64
	jobOldestAge                         map[queueLabels]float64
	jobsRunning                          map[JobLabels]int64
	executionSaturation                  map[queueLabels]float64
	jobWait                              map[JobLabels]*histogram
	jobDuration                          map[jobResultLabels]*histogram
	jobAttempts                          map[attemptLabels]uint64
	jobPanics                            map[string]uint64
	cancellations                        map[cancellationLabels]uint64
	deterministicFailures                map[deterministicFailureLabels]uint64
	domainMismatch                       map[string]uint64
	syncLeaseExpired                     map[syncLeaseResultLabels]uint64
	reportRunLeaseExpired                map[ReportRunLeaseResult]uint64
	idempotencyRenewalRetired            map[IdempotencyRenewalRetiredReason]uint64
	dailyMetricsLease                    map[dailyMetricsLeaseLabels]uint64
	dailyMetricsDiscovery                map[dailyMetricsDiscoveryLabels]uint64
	dailyMetricsFamilyZeroRowsWithSource map[string]uint64
	// dailyMetricsRedrive (CHAOS-4358) counts operator-invoked stranded-
	// partition redrive activity by bounded reason. See
	// dailyMetricsRedriveReasons for the vocabulary.
	dailyMetricsRedrive map[string]uint64
	// dailyMetricsFinalizeSweep (CHAOS-4389) counts stranded-finalize sweep
	// activity by bounded outcome. See dailyMetricsFinalizeSweepOutcomes for
	// the vocabulary.
	dailyMetricsFinalizeSweep map[string]uint64
	// dailyMetricsFinalizeLedgerRepair (CHAOS-4409) counts finalize-ledger
	// bulk-repair activity by bounded outcome. See
	// dailyMetricsFinalizeLedgerRepairOutcomes for the vocabulary. No
	// production caller wires this today -- see
	// ObserveDailyMetricsFinalizeLedgerRepair's own doc comment -- it exists
	// so a future long-running caller (or a test asserting the contract) has
	// a real series to observe, matching
	// ObserveRemainingMetricsManualBackfill's precedent for the identical
	// one-shot-CLI-cannot-be-scraped constraint.
	dailyMetricsFinalizeLedgerRepair map[string]uint64
	// dailyMetricsFinalizeRedrive (CHAOS-4405) counts operator-invoked
	// historical finalize-redrive activity by bounded outcome, per calendar
	// day considered. See dailyMetricsFinalizeRedriveOutcomes for the
	// vocabulary.
	dailyMetricsFinalizeRedrive map[string]uint64
	// dailyMetricsPartitionRecompute (CHAOS-4459) counts operator-invoked
	// historical partition-recompute activity by family and bounded
	// outcome, per calendar day considered. See
	// dailyMetricsPartitionRecomputeOutcomes for the vocabulary.
	dailyMetricsPartitionRecompute map[dailyMetricsPartitionRecomputeLabels]uint64
	// Native family compute (CHAOS-4276, the daily bridge's per-partition
	// counterpart to the DORA/capacity counters above). Duration is tracked
	// per family since native families are cut over independently and one
	// family's compute cost tells nothing about another's.
	dailyMetricsNativeFamilyOutcome     map[dailyMetricsNativeFamilyOutcomeLabels]uint64
	dailyMetricsNativeFamilyRowsWritten map[string]uint64
	dailyMetricsNativeFamilyDuration    map[string]*histogram
	// Ambiguous-refused terminal persistence (CHAOS-4319). See
	// DailyMetricsCompatRetryDecision's doc comment for why Go only ever
	// records the "persisted_failed" half of this decision axis.
	dailyMetricsCompatRetry map[DailyMetricsCompatRetryDecision]uint64
	// teamMetricsDailyRepoCount (CHAOS-4329): distinct repo_id rows written
	// per (team_id, day) in one team_metrics_daily write. Unlabeled --
	// deliberately no team_id label, same cardinality discipline as every
	// other per-tenant-identity signal in this file.
	teamMetricsDailyRepoCount *histogram
	postSyncFanout            map[PostSyncFanoutOutcome]uint64
	// teamRepoOwnershipDerivation (CHAOS-4365 item 1b): per-outcome counter for
	// sync.team_repo_ownership_derivation's worker; teamRepoOwnershipDerivationRowCount
	// is the paired rows-written histogram, observed only on the
	// rows_written outcome -- same pairing convention as
	// teamMetricsDailyRepoCount above.
	teamRepoOwnershipDerivation         map[TeamRepoOwnershipDerivationOutcome]uint64
	teamRepoOwnershipDerivationRowCount *histogram
	// teamRepoOwnershipResolutionArm (CHAOS-4458 part (b)): per-arm counter
	// of rows produced by each identity resolution -- see
	// TeamRepoOwnershipResolutionArm's doc comment.
	teamRepoOwnershipResolutionArm map[TeamRepoOwnershipResolutionArm]uint64
	// teamCatalogDispatch/teamCatalogRowsWritten (CHAOS-4431): per-call
	// dispatch outcome for the native/bridge team-catalog split, and rows a
	// native collector actually wrote per destination table.
	teamCatalogDispatch    map[teamCatalogDispatchLabels]uint64
	teamCatalogRowsWritten map[teamCatalogRowsLabels]uint64
	workGraphReleaseLost   uint64
	remainingReleaseLost   uint64
	// remainingOpenDayZeroRow (CHAOS-4384), keyed by family (only "dora"
	// today -- remainingMetricsOpenDayZeroRowFamilies). Counts a dora
	// cross-trigger coverage check finding a 0-row succeeded partition for a
	// day that has not closed yet, and therefore refusing to treat it as
	// terminal coverage.
	remainingOpenDayZeroRow map[string]uint64
	// remainingManualBackfill (CHAOS-4254), keyed by family and outcome. See
	// RemainingMetricsManualBackfillOutcome for the outcome vocabulary.
	remainingManualBackfill map[remainingMetricsManualBackfillLabels]uint64
	zeroUnitFinalizations   map[zeroUnitFinalizationLabels]uint64

	// Coverage-cache invalidation pair (CHAOS-4226), keyed by clamped
	// provider. Both maps gain the key on the first emit so the consumed
	// series is present (at 0) whenever the emitted series is.
	coverageCacheInvalidationsEmitted  map[string]uint64
	coverageCacheInvalidationsConsumed map[string]uint64

	// External ingest, work_item -> project reassignment (CHAOS-4194).
	//
	// These two are a PAIR and only mean something together. The sunk counter
	// alone cannot distinguish "no provider reassigned anything today" from
	// "every event was refused at the registry", which are the same flat line
	// and opposite operational situations. Before this, a producer shipping
	// against an unregistered kind saw a clean successful sync and no rows,
	// with nothing outward to look at.
	externalProjectMembershipsSunk map[string]uint64
	externalRecordRefusals         map[externalRefusalLabels]uint64

	// Native DORA compute (CHAOS-3092 R1). The HTTP compatibility bridge could
	// only ever report a status code, so a partition that computed nothing and
	// a partition that computed everything were indistinguishable from the
	// outside. Going native is the moment the work itself becomes observable.
	doraPartitions    uint64
	doraDays          uint64
	doraRowsWritten   uint64
	doraSkippedRows   uint64
	doraEmptyOutcomes uint64
	// doraRefusals counts native DORA construction refusals BY REASON.
	//
	// It exists because absence is not a signal: worker_dora_native_partitions
	// _total sitting at zero means "unproven", not "healthy", and an alert
	// cannot distinguish a refused executor from a quiet day. This is the
	// positive statement an alert can bind to.
	doraRefusals map[string]uint64

	// Report-domain dedup guard (CHAOS-4140). The weekly-report engine
	// (internal/jobs/report) reads several append-only daily rollup tables
	// through a latest-generation subquery (dedupFromSource) because a
	// legitimate re-drive -- a DORA partition retry, a cicd/deploy/incident
	// post-sync re-trigger -- writes a fresh computed_at generation without
	// deleting the prior one. Both maps are keyed by "<table>|<reason>" and
	// gain the key together on first emit, so observedRows and skippedRows
	// for the same key are always both present -- skipped==0 is itself an
	// informative reading (this query's key range currently carries no
	// duplicate generation), not an absent series.
	reportDedupObservedRows map[string]uint64
	reportDedupSkippedRows  map[string]uint64

	// budgetEstimateFailures counts dispatch_sync_run's BudgetGuard falling
	// open on a bridge estimate fetch, by reason (CHAOS-4175). Standing
	// order: new fail-open logic must carry a counter, so a fetch that
	// silently admits units with zero budget checked cannot go unnoticed --
	// zero here means "the estimate bridge stayed healthy," not "nothing to
	// measure."
	budgetEstimateFailures map[string]uint64

	// Native capacity compute (CUT-20 R2). capacitySkippedScopes is the one
	// that matters most: Python skips a scope with no history or no positive
	// item target by logging and continuing, so without a counter a run that
	// forecast nothing is indistinguishable from a run that had nothing to
	// forecast.
	capacityPartitions    uint64
	capacityScopes        uint64
	capacityRowsWritten   uint64
	capacitySkippedScopes uint64
	capacityRefusals      map[string]uint64

	// Compatibility-bridge partitions (CHAOS-4243). Before this, the bridge
	// could only ever report a status code, so a partition that wrote real
	// data and a partition that silently wrote nothing were indistinguishable
	// from the outside -- the same rationale that native dora/capacity
	// compute already gets its own rows-written/empty counters for above.
	// Keyed by family; rowsWritten only counts completions that reported a
	// non-nil count (not every family's evidence carries one).
	compatibilityRowsWritten       map[string]uint64
	compatibilityZeroRowPartitions map[string]uint64

	streamLag                 map[StreamLabels]int64
	streamPending             map[StreamLabels]int64
	streamOldestPending       map[StreamLabels]float64
	budgetWait                map[BudgetLabels]*histogram
	concurrencyBudgetCapacity map[ConcurrencyBudgetLabels]int64
	concurrencyBudgetLeased   map[ConcurrencyBudgetLabels]int64
	concurrencyBudgetWait     map[ConcurrencyBudgetLabels]*histogram
	concurrencyBudgetEvents   map[concurrencyBudgetResultLabels]uint64
	poolSaturation            map[string]float64
	poolAcquire               map[poolAcquireLabels]*histogram
}

var _ Observer = (*MetricsCollector)(nil)
var _ SyncLeaseObserver = (*MetricsCollector)(nil)
var _ ReportRunLeaseObserver = (*MetricsCollector)(nil)
var _ DailyMetricsLeaseObserver = (*MetricsCollector)(nil)
var _ DailyMetricsDiscoveryObserver = (*MetricsCollector)(nil)
var _ DailyMetricsZeroRowsObserver = (*MetricsCollector)(nil)
var _ DailyMetricsNativeFamilyObserver = (*MetricsCollector)(nil)
var _ DailyMetricsCompatRetryObserver = (*MetricsCollector)(nil)
var _ PostSyncFanoutObserver = (*MetricsCollector)(nil)
var _ TeamRepoOwnershipDerivationObserver = (*MetricsCollector)(nil)
var _ TeamCatalogObserver = (*MetricsCollector)(nil)
var _ WorkGraphLeaseObserver = (*MetricsCollector)(nil)
var _ RemainingMetricsLeaseObserver = (*MetricsCollector)(nil)
var _ RemainingMetricsOpenDayZeroRowObserver = (*MetricsCollector)(nil)
var _ RemainingMetricsManualBackfillObserver = (*MetricsCollector)(nil)
var _ ZeroUnitFinalizationObserver = (*MetricsCollector)(nil)
var _ CoverageCacheInvalidationObserver = (*MetricsCollector)(nil)
var _ TeamMetricsDailyRepoCountObserver = (*MetricsCollector)(nil)
var _ DailyMetricsRedriveObserver = (*MetricsCollector)(nil)
var _ DailyMetricsFinalizeSweepObserver = (*MetricsCollector)(nil)
var _ DailyMetricsFinalizeLedgerRepairObserver = (*MetricsCollector)(nil)
var _ DailyMetricsFinalizeRedriveObserver = (*MetricsCollector)(nil)
var _ DailyMetricsPartitionRecomputeObserver = (*MetricsCollector)(nil)

func NewMetricsCollector(dimensions MetricDimensions) (*MetricsCollector, error) {
	if len(dimensions.Jobs) > maxMetricJobs {
		return nil, errors.New("metric job dimensions exceed bounds")
	}
	if len(dimensions.DomainTypes) > maxMetricDomains ||
		len(dimensions.SyncLeases) > maxMetricSyncLeases || len(dimensions.Streams) > maxMetricStreams ||
		len(dimensions.Budgets) > maxMetricBudgets || len(dimensions.ConcurrencyBudgets) > maxMetricConcurrencyBudgets {
		return nil, errors.New("metric dimensions exceed cardinality bounds")
	}

	collector := &MetricsCollector{
		allowedJobs:                          make(map[JobLabels]struct{}, len(dimensions.Jobs)),
		allowedQueues:                        make(map[queueLabels]struct{}),
		allowedKinds:                         make(map[string]struct{}),
		allowedDomains:                       make(map[string]struct{}, len(dimensions.DomainTypes)),
		allowedSyncLeases:                    make(map[SyncLeaseLabels]struct{}, len(dimensions.SyncLeases)),
		allowedStreams:                       make(map[StreamLabels]struct{}, len(dimensions.Streams)),
		allowedBudgets:                       make(map[BudgetLabels]struct{}, len(dimensions.Budgets)),
		allowedConcurrencyBudgets:            make(map[ConcurrencyBudgetLabels]struct{}, len(dimensions.ConcurrencyBudgets)),
		dailyMetricsCompatRetry:              make(map[DailyMetricsCompatRetryDecision]uint64, len(dailyMetricsCompatRetryDecisions())),
		dailyMetricsNativeFamilyOutcome:      make(map[dailyMetricsNativeFamilyOutcomeLabels]uint64, len(dailyMetricsNativeFamilies)*len(dailyMetricsNativeFamilyOutcomes())),
		dailyMetricsNativeFamilyRowsWritten:  make(map[string]uint64, len(dailyMetricsNativeFamilies)),
		dailyMetricsNativeFamilyDuration:     make(map[string]*histogram, len(dailyMetricsNativeFamilies)),
		teamMetricsDailyRepoCount:            newHistogramWithBounds(repoCountBuckets),
		jobsAvailable:                        make(map[JobLabels]int64, len(dimensions.Jobs)),
		jobOldestAge:                         make(map[queueLabels]float64),
		jobsRunning:                          make(map[JobLabels]int64, len(dimensions.Jobs)),
		executionSaturation:                  make(map[queueLabels]float64),
		jobWait:                              make(map[JobLabels]*histogram, len(dimensions.Jobs)),
		jobDuration:                          make(map[jobResultLabels]*histogram),
		jobAttempts:                          make(map[attemptLabels]uint64),
		jobPanics:                            make(map[string]uint64),
		cancellations:                        make(map[cancellationLabels]uint64),
		deterministicFailures:                make(map[deterministicFailureLabels]uint64),
		domainMismatch:                       make(map[string]uint64, len(dimensions.DomainTypes)),
		syncLeaseExpired:                     make(map[syncLeaseResultLabels]uint64, len(dimensions.SyncLeases)*len(syncLeaseResults())),
		reportRunLeaseExpired:                make(map[ReportRunLeaseResult]uint64, len(reportRunLeaseResults())),
		idempotencyRenewalRetired:            make(map[IdempotencyRenewalRetiredReason]uint64, len(idempotencyRenewalRetiredReasons())),
		dailyMetricsLease:                    make(map[dailyMetricsLeaseLabels]uint64, len(dailyMetricsLeaseSeries())),
		dailyMetricsDiscovery:                make(map[dailyMetricsDiscoveryLabels]uint64, len(dailyMetricsDiscoverySeries())),
		dailyMetricsFamilyZeroRowsWithSource: make(map[string]uint64, len(dailyMetricsZeroRowsWithSourceFamilies)),
		remainingOpenDayZeroRow:              make(map[string]uint64, len(remainingMetricsOpenDayZeroRowFamilies)),
		remainingManualBackfill:              make(map[remainingMetricsManualBackfillLabels]uint64, len(remainingMetricsManualBackfillFamilies)*len(remainingMetricsManualBackfillOutcomes())),
		dailyMetricsRedrive:                  make(map[string]uint64, len(dailyMetricsRedriveReasons)),
		dailyMetricsFinalizeSweep:            make(map[string]uint64, len(dailyMetricsFinalizeSweepOutcomes)),
		dailyMetricsFinalizeLedgerRepair:     make(map[string]uint64, len(dailyMetricsFinalizeLedgerRepairOutcomes)),
		dailyMetricsFinalizeRedrive:          make(map[string]uint64, len(dailyMetricsFinalizeRedriveOutcomes)),
		dailyMetricsPartitionRecompute:       make(map[dailyMetricsPartitionRecomputeLabels]uint64, len(dailyMetricsPartitionRecomputeFamilies)*len(dailyMetricsPartitionRecomputeOutcomes)),
		postSyncFanout:                       make(map[PostSyncFanoutOutcome]uint64, len(postSyncFanoutOutcomes())),
		teamRepoOwnershipDerivation:          make(map[TeamRepoOwnershipDerivationOutcome]uint64, len(teamRepoOwnershipDerivationOutcomes())),
		teamRepoOwnershipDerivationRowCount:  newHistogramWithBounds(repoCountBuckets),
		teamRepoOwnershipResolutionArm:       make(map[TeamRepoOwnershipResolutionArm]uint64, len(teamRepoOwnershipResolutionArms())),
		teamCatalogDispatch:                  make(map[teamCatalogDispatchLabels]uint64),
		teamCatalogRowsWritten:               make(map[teamCatalogRowsLabels]uint64),
		zeroUnitFinalizations:                make(map[zeroUnitFinalizationLabels]uint64),
		coverageCacheInvalidationsEmitted:    make(map[string]uint64),
		coverageCacheInvalidationsConsumed:   make(map[string]uint64),
		externalProjectMembershipsSunk:       make(map[string]uint64),
		externalRecordRefusals:               make(map[externalRefusalLabels]uint64),
		streamLag:                            make(map[StreamLabels]int64, len(dimensions.Streams)),
		streamPending:                        make(map[StreamLabels]int64, len(dimensions.Streams)),
		streamOldestPending:                  make(map[StreamLabels]float64, len(dimensions.Streams)),
		budgetWait:                           make(map[BudgetLabels]*histogram, len(dimensions.Budgets)),
		concurrencyBudgetCapacity:            make(map[ConcurrencyBudgetLabels]int64, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetLeased:              make(map[ConcurrencyBudgetLabels]int64, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetWait:                make(map[ConcurrencyBudgetLabels]*histogram, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetEvents:              make(map[concurrencyBudgetResultLabels]uint64, len(dimensions.ConcurrencyBudgets)*2),
		poolSaturation:                       map[string]float64{poolDomain: 0, poolQueueControl: 0},
		poolAcquire:                          make(map[poolAcquireLabels]*histogram, 8),
	}
	for _, labels := range dimensions.Jobs {
		if err := validateJobLabels(labels); err != nil {
			return nil, err
		}
		if _, duplicate := collector.allowedJobs[labels]; duplicate {
			return nil, errors.New("duplicate metric job dimensions")
		}
		collector.allowedJobs[labels] = struct{}{}
		queue := queueLabels{Queue: labels.Queue}
		collector.allowedQueues[queue] = struct{}{}
		collector.allowedKinds[labels.Kind] = struct{}{}
		collector.executionSaturation[queue] = 0
		collector.jobsAvailable[labels] = 0
		collector.jobsRunning[labels] = 0
		collector.jobWait[labels] = newHistogram()
		collector.jobPanics[labels.Kind] = 0
	}
	for labels := range collector.allowedQueues {
		collector.jobOldestAge[labels] = 0
	}
	if len(collector.allowedQueues) > maxMetricQueues {
		return nil, errors.New("metric queue dimensions exceed bounds")
	}
	for _, domainType := range dimensions.DomainTypes {
		if !metricIdentifier(domainType, 64) {
			return nil, errors.New("invalid metric domain dimension")
		}
		if _, duplicate := collector.allowedDomains[domainType]; duplicate {
			return nil, errors.New("duplicate metric domain dimension")
		}
		collector.allowedDomains[domainType] = struct{}{}
		collector.domainMismatch[domainType] = 0
	}
	for _, labels := range dimensions.SyncLeases {
		if !metricIdentifier(labels.Provider, 64) || !metricIdentifier(labels.DatasetFamily, 96) {
			return nil, errors.New("invalid metric sync lease dimensions")
		}
		if _, duplicate := collector.allowedSyncLeases[labels]; duplicate {
			return nil, errors.New("duplicate metric sync lease dimensions")
		}
		collector.allowedSyncLeases[labels] = struct{}{}
		for _, result := range syncLeaseResults() {
			collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}] = 0
		}
	}
	for _, result := range reportRunLeaseResults() {
		collector.reportRunLeaseExpired[result] = 0
	}
	for _, reason := range idempotencyRenewalRetiredReasons() {
		collector.idempotencyRenewalRetired[reason] = 0
	}
	for _, labels := range dailyMetricsLeaseSeries() {
		collector.dailyMetricsLease[labels] = 0
	}
	for _, labels := range dailyMetricsDiscoverySeries() {
		collector.dailyMetricsDiscovery[labels] = 0
	}
	for _, family := range dailyMetricsZeroRowsWithSourceFamilies {
		collector.dailyMetricsFamilyZeroRowsWithSource[family] = 0
	}
	for _, family := range remainingMetricsOpenDayZeroRowFamilies {
		collector.remainingOpenDayZeroRow[family] = 0
	}
	for _, family := range remainingMetricsManualBackfillFamilies {
		for _, outcome := range remainingMetricsManualBackfillOutcomes() {
			collector.remainingManualBackfill[remainingMetricsManualBackfillLabels{Family: family, Outcome: outcome}] = 0
		}
	}
	for _, reason := range dailyMetricsRedriveReasons {
		collector.dailyMetricsRedrive[reason] = 0
	}
	for _, outcome := range dailyMetricsFinalizeSweepOutcomes {
		collector.dailyMetricsFinalizeSweep[outcome] = 0
	}
	for _, outcome := range dailyMetricsFinalizeLedgerRepairOutcomes {
		collector.dailyMetricsFinalizeLedgerRepair[outcome] = 0
	}
	for _, outcome := range dailyMetricsFinalizeRedriveOutcomes {
		collector.dailyMetricsFinalizeRedrive[outcome] = 0
	}
	for _, family := range dailyMetricsPartitionRecomputeFamilies {
		for _, outcome := range dailyMetricsPartitionRecomputeOutcomes {
			collector.dailyMetricsPartitionRecompute[dailyMetricsPartitionRecomputeLabels{Family: family, Outcome: outcome}] = 0
		}
	}
	for _, decision := range dailyMetricsCompatRetryDecisions() {
		collector.dailyMetricsCompatRetry[decision] = 0
	}
	for _, family := range dailyMetricsNativeFamilies {
		collector.dailyMetricsNativeFamilyRowsWritten[family] = 0
		collector.dailyMetricsNativeFamilyDuration[family] = newHistogram()
		for _, outcome := range dailyMetricsNativeFamilyOutcomes() {
			collector.dailyMetricsNativeFamilyOutcome[dailyMetricsNativeFamilyOutcomeLabels{Family: family, Outcome: outcome}] = 0
		}
	}
	for _, labels := range dimensions.Streams {
		if !metricIdentifier(labels.Stream, 96) || !metricIdentifier(labels.ConsumerGroup, 96) {
			return nil, errors.New("invalid metric stream dimensions")
		}
		if _, duplicate := collector.allowedStreams[labels]; duplicate {
			return nil, errors.New("duplicate metric stream dimensions")
		}
		collector.allowedStreams[labels] = struct{}{}
		collector.streamLag[labels] = 0
		collector.streamPending[labels] = 0
		collector.streamOldestPending[labels] = 0
	}
	for _, labels := range dimensions.Budgets {
		if !metricIdentifier(labels.Provider, 64) || !metricIdentifier(labels.CostClass, 64) {
			return nil, errors.New("invalid metric budget dimensions")
		}
		if _, duplicate := collector.allowedBudgets[labels]; duplicate {
			return nil, errors.New("duplicate metric budget dimensions")
		}
		collector.allowedBudgets[labels] = struct{}{}
		collector.budgetWait[labels] = newHistogram()
	}
	for _, labels := range dimensions.ConcurrencyBudgets {
		if !metricIdentifier(labels.Kind, 96) ||
			(labels.Scope != "fleet" && labels.Scope != "organization") {
			return nil, errors.New("invalid concurrency budget dimensions")
		}
		if _, duplicate := collector.allowedConcurrencyBudgets[labels]; duplicate {
			return nil, errors.New("duplicate concurrency budget dimensions")
		}
		collector.allowedConcurrencyBudgets[labels] = struct{}{}
		collector.concurrencyBudgetCapacity[labels] = 0
		collector.concurrencyBudgetLeased[labels] = 0
		collector.concurrencyBudgetWait[labels] = newHistogram()
		for _, result := range []string{"expired", "recovered"} {
			collector.concurrencyBudgetEvents[concurrencyBudgetResultLabels{Budget: labels, Result: result}] = 0
		}
	}
	for _, pool := range []string{poolDomain, poolQueueControl} {
		for _, result := range poolAcquireResults() {
			collector.poolAcquire[poolAcquireLabels{Pool: pool, Result: result}] = newHistogram()
		}
	}
	return collector, nil
}

// DimensionsForQueues derives job and domain dimensions from the validated
// runtime registry. Sync-lease, stream, and budget pairs remain explicit
// inputs here since they come from static deployment configuration rather
// than the job registry.
func DimensionsForQueues(registry *Registry, queues []string, streams []StreamLabels, budgets []BudgetLabels, syncLeases []SyncLeaseLabels, concurrencyBudgets ...[]ConcurrencyBudgetLabels) (MetricDimensions, error) {
	if registry == nil {
		return MetricDimensions{}, errors.New("runtime registry is required")
	}
	descriptors, err := registry.SelectedQueues(queues)
	if err != nil {
		return MetricDimensions{}, err
	}
	if len(descriptors) == 0 {
		return MetricDimensions{}, errors.New("runtime queues have no registered jobs")
	}
	dimensions := MetricDimensions{
		Streams:    append([]StreamLabels(nil), streams...),
		Budgets:    append([]BudgetLabels(nil), budgets...),
		SyncLeases: append([]SyncLeaseLabels(nil), syncLeases...),
	}
	if len(concurrencyBudgets) > 0 {
		dimensions.ConcurrencyBudgets = append([]ConcurrencyBudgetLabels(nil), concurrencyBudgets[0]...)
	}
	domains := make(map[string]struct{})
	for _, descriptor := range descriptors {
		dimensions.Jobs = append(dimensions.Jobs, JobLabels{
			Queue: descriptor.Queue,
			Kind:  descriptor.Kind,
		})
		domains[descriptor.DomainLink] = struct{}{}
	}
	for domainType := range domains {
		dimensions.DomainTypes = append(dimensions.DomainTypes, domainType)
	}
	sort.Strings(dimensions.DomainTypes)
	return dimensions, nil
}

func (collector *MetricsCollector) RuntimeRegistered(_ context.Context, info RuntimeInfo) {
	if !boundedIdentity(info.Version, 128) || !boundedIdentity(info.Commit, 128) {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	copy := info
	collector.runtimeInfo = &copy
}

func (collector *MetricsCollector) JobStarted(_ context.Context, labels JobLabels) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return
	}
	collector.jobsRunning[labels]++
}

func (collector *MetricsCollector) JobFinished(_ context.Context, labels JobLabels, result Result, category ErrorCategory, duration time.Duration) {
	if !validOutcome(result, category) || duration < 0 {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return
	}
	if collector.jobsRunning[labels] > 0 {
		collector.jobsRunning[labels]--
	}
	durationLabels := jobResultLabels{Job: labels, Result: result}
	metric := collector.jobDuration[durationLabels]
	if metric == nil {
		metric = newHistogram()
		collector.jobDuration[durationLabels] = metric
	}
	metric.observe(duration.Seconds())
	collector.jobAttempts[attemptLabels{Kind: labels.Kind, Result: result, Category: category}]++
}

func (collector *MetricsCollector) JobPanicked(_ context.Context, labels JobLabels) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; ok {
		collector.jobPanics[labels.Kind]++
	}
}

func (collector *MetricsCollector) JobCancelled(_ context.Context, labels JobLabels, reason ErrorCategory) {
	if !validErrorCategory(reason) || reason == CategoryNone {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; ok {
		collector.cancellations[cancellationLabels{Kind: labels.Kind, Reason: reason}]++
	}
}

// ObserveDeterministicFailure satisfies Observer. Reason's zero value
// (isZero) is never bucketed: an unset Reason means no call site attached
// one, not that a genuine empty-string reason occurred (Reason's exported
// constructor space is a closed compile-time catalog -- see errors.go).
func (collector *MetricsCollector) ObserveDeterministicFailure(_ context.Context, labels JobLabels, reason Reason) {
	if reason.isZero() {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; ok {
		collector.deterministicFailures[deterministicFailureLabels{Kind: labels.Kind, Reason: reason}]++
	}
}

func (collector *MetricsCollector) DomainMismatch(_ context.Context, domainType string) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedDomains[domainType]; ok {
		collector.domainMismatch[domainType]++
	}
}

// BudgetWait satisfies Observer. Generic worker middleware has no safe
// provider/cost-class dimensions, so it cannot populate the provider-labelled
// metric. Concrete budget implementations call ObserveProviderBudgetWait.
func (*MetricsCollector) BudgetWait(context.Context, JobLabels, time.Duration, string) {}

func (collector *MetricsCollector) SetJobsAvailable(labels JobLabels, count int64) error {
	if count < 0 {
		return errors.New("available job count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return errors.New("job metric dimensions are not registered")
	}
	collector.jobsAvailable[labels] = count
	return nil
}

func (collector *MetricsCollector) SetJobOldestAge(queue string, age time.Duration) error {
	if age < 0 {
		return errors.New("oldest job age cannot be negative")
	}
	labels := queueLabels{Queue: queue}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedQueues[labels]; !ok {
		return errors.New("queue metric dimensions are not registered")
	}
	collector.jobOldestAge[labels] = age.Seconds()
	return nil
}

func (collector *MetricsCollector) ObserveJobWait(labels JobLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("job wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.jobWait[labels]
	if !ok {
		return errors.New("job metric dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

// JobWait satisfies Observer. It is the generic middleware entry point the
// adapter calls on every execution; negative or unregistered observations are
// dropped rather than surfaced, exactly like the other Observer methods.
func (collector *MetricsCollector) JobWait(_ context.Context, labels JobLabels, wait time.Duration) {
	_ = collector.ObserveJobWait(labels, wait)
}

// ObserveSyncLeaseExpired records an expired-lease recovery that successfully
// changed durable state to RETRYING or FAILED. Callers must not record failed
// compare-and-swap attempts.
func (collector *MetricsCollector) ObserveSyncLeaseExpired(labels SyncLeaseLabels, result SyncLeaseResult) error {
	if !validSyncLeaseResult(result) {
		return errors.New("sync lease result is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedSyncLeases[labels]; !ok {
		return errors.New("sync lease metric dimensions are not registered")
	}
	collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}]++
	return nil
}

// ObserveReportRunLeaseExpired records only durable expired-lease outcomes.
// A worker that loses the row-lock race must not call this method.
func (collector *MetricsCollector) ObserveReportRunLeaseExpired(result ReportRunLeaseResult) error {
	if !validReportRunLeaseResult(result) {
		return errors.New("report run lease result is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.reportRunLeaseExpired[result]++
	return nil
}

// ObserveIdempotencyRenewalRetired records a job claim whose lease renewal
// stopped for good. Before this counter existed the retirement was invisible:
// the goroutine returned, the handler carried on believing it held a lease,
// and a healthy run and an orphaned one looked identical from outside the
// process. Callers must not record an ordinary stop -- a claim that finished
// its work and stopped renewal deliberately has not retired, and counting it
// would bury the real signal under normal completions.
func (collector *MetricsCollector) ObserveIdempotencyRenewalRetired(
	reason IdempotencyRenewalRetiredReason,
) error {
	if !validIdempotencyRenewalRetiredReason(reason) {
		return errors.New("idempotency renewal retired reason is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.idempotencyRenewalRetired[reason]++
	return nil
}

// ObserveDailyMetricsLease records a daily-metrics claim that met an existing
// lease and resolved it durably: either it parked for a live lease instead of
// reporting success, or it took over an expired one. Callers must not record a
// claim that simply found nothing to do -- that outcome is indistinguishable
// from a healthy no-op and would make the counter useless as a stall signal.
func (collector *MetricsCollector) ObserveDailyMetricsLease(
	stage DailyMetricsLeaseStage,
	result DailyMetricsLeaseResult,
) error {
	labels := dailyMetricsLeaseLabels{Stage: stage, Result: result}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.dailyMetricsLease[labels]; !ok {
		return errors.New("daily metrics lease dimensions are not registered")
	}
	collector.dailyMetricsLease[labels]++
	return nil
}

// ObserveDailyMetricsDiscovery records the outcome of resolving live
// ClickHouse repository identity for one daily-metrics run, whichever of the
// two triggers created it (CHAOS-4263).
func (collector *MetricsCollector) ObserveDailyMetricsDiscovery(
	trigger DailyMetricsRunTrigger,
	outcome DailyMetricsDiscoveryOutcome,
) error {
	labels := dailyMetricsDiscoveryLabels{Trigger: trigger, Outcome: outcome}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.dailyMetricsDiscovery[labels]; !ok {
		return errors.New("daily metrics discovery dimensions are not registered")
	}
	collector.dailyMetricsDiscovery[labels]++
	return nil
}

// ObserveDailyMetricsFamilyZeroRowsWithSource records that a metrics.daily
// family's source data existed for a partition's repositories and day, but
// its output table had zero rows for that scope (CHAOS-4263, chris's ruling
// 2026-08-25). The metric name intentionally breaks from this file's worker_
// prefix convention: it is a cross-repo/cross-PR contract with CHAOS-4266
// (the live-e2e gate), which consumes it by this exact name.
func (collector *MetricsCollector) ObserveDailyMetricsFamilyZeroRowsWithSource(family string) error {
	if !slices.Contains(dailyMetricsZeroRowsWithSourceFamilies, family) {
		return errors.New("daily metrics zero-rows-with-source family is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsFamilyZeroRowsWithSource[family]++
	return nil
}

// ObserveDailyMetricsRedrive records count partitions moved by one bounded
// reason during an operator-invoked stranded-partition redrive (CHAOS-4358).
// count must be >= 0: a redrive pass that touched nothing still calls this
// with 0 rather than skipping the call, so "no stranding today" and "the
// redrive path never ran" both read as a present, zero-valued series.
func (collector *MetricsCollector) ObserveDailyMetricsRedrive(reason string, count int) error {
	if !slices.Contains(dailyMetricsRedriveReasons, reason) {
		return errors.New("daily metrics redrive reason is not registered")
	}
	if count < 0 {
		return errors.New("daily metrics redrive count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsRedrive[reason] += uint64(count)
	return nil
}

// ObserveDailyMetricsFinalizeSweep records count runs for one bounded
// outcome during a CHAOS-4389 stranded-finalize sweep. count must be >= 0,
// matching ObserveDailyMetricsRedrive's discipline: a sweep pass that found
// nothing still calls this with 0 so the series stays present, not absent.
func (collector *MetricsCollector) ObserveDailyMetricsFinalizeSweep(outcome string, count int) error {
	if !slices.Contains(dailyMetricsFinalizeSweepOutcomes, outcome) {
		return errors.New("daily metrics finalize sweep outcome is not registered")
	}
	if count < 0 {
		return errors.New("daily metrics finalize sweep count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsFinalizeSweep[outcome] += uint64(count)
	return nil
}

// ObserveDailyMetricsFinalizeLedgerRepair records count finalize-ledger
// execution rows moved (or left alone) by one bounded outcome during a
// CHAOS-4409 finalize-ledger bulk repair. count must be >= 0, matching
// ObserveDailyMetricsFinalizeSweep's discipline: a repair pass that touched
// nothing still calls this with 0 so the series stays present, not absent.
//
// No production caller wires this today: the only place that invokes the
// bulk-repair endpoint for finalize rows is `dev-health-workerctl metrics
// daily-finalize`, a one-shot CLI process with no /metrics endpoint to
// scrape -- a counter incremented here before that process exits is never
// observed by anything. This mirrors dispatchMetricsRemaining's own
// documented reasoning for never wiring
// RemainingMetricsManualBackfillObserver from workerctl (see that function's
// doc comment): the method/series exists so a future long-running caller
// (or a test asserting the contract) has a real observer to call, not so
// the CLI can call it today. The CLI's own JSON result (`ledger_repair`)
// is the actual audit trail for a manual invocation.
func (collector *MetricsCollector) ObserveDailyMetricsFinalizeLedgerRepair(outcome string, count int) error {
	if !slices.Contains(dailyMetricsFinalizeLedgerRepairOutcomes, outcome) {
		return errors.New("daily metrics finalize ledger repair outcome is not registered")
	}
	if count < 0 {
		return errors.New("daily metrics finalize ledger repair count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsFinalizeLedgerRepair[outcome] += uint64(count)
	return nil
}

// ObserveDailyMetricsFinalizeRedrive records count calendar days for one
// bounded outcome during a CHAOS-4405 historical finalize-redrive pass.
// count must be >= 0, matching every other bounded-vocabulary observer in
// this package: a pass that touched nothing still calls this with 0 so the
// series stays present, not absent.
func (collector *MetricsCollector) ObserveDailyMetricsFinalizeRedrive(outcome string, count int) error {
	if !slices.Contains(dailyMetricsFinalizeRedriveOutcomes, outcome) {
		return errors.New("daily metrics finalize redrive outcome is not registered")
	}
	if count < 0 {
		return errors.New("daily metrics finalize redrive count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsFinalizeRedrive[outcome] += uint64(count)
	return nil
}

// ObserveDailyMetricsPartitionRecompute records count calendar days for one
// CHAOS-4459 operator-invoked historical partition-recompute pass, keyed by
// family and bounded outcome (dailyMetricsPartitionRecomputeOutcomes). See
// DailyMetricsPartitionRecomputeObserver's doc comment for the vocabulary.
func (collector *MetricsCollector) ObserveDailyMetricsPartitionRecompute(family, outcome string, count int) error {
	if !slices.Contains(dailyMetricsPartitionRecomputeFamilies, family) {
		return errors.New("daily metrics partition recompute family is not registered")
	}
	if !slices.Contains(dailyMetricsPartitionRecomputeOutcomes, outcome) {
		return errors.New("daily metrics partition recompute outcome is not registered")
	}
	if count < 0 {
		return errors.New("daily metrics partition recompute count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsPartitionRecompute[dailyMetricsPartitionRecomputeLabels{Family: family, Outcome: outcome}] += uint64(count)
	return nil
}

// ObserveDailyMetricsNativeFamily records one native family compute attempt
// inside a metrics.daily partition (CHAOS-4276). rowsWritten and duration are
// only meaningful for a Computed outcome -- a Refused attempt did not
// complete, so both are recorded as zero rather than whatever partial value
// the caller happened to measure before giving up (mirrors
// ObserveDORAPartition's rowsWritten/skippedRows discipline: a caller must
// not report work that did not durably happen).
func (collector *MetricsCollector) ObserveDailyMetricsNativeFamily(
	family string, outcome DailyMetricsNativeFamilyOutcome, rowsWritten int, duration time.Duration,
) error {
	if !slices.Contains(dailyMetricsNativeFamilyOutcomes(), outcome) {
		return errors.New("daily metrics native family outcome is not registered")
	}
	if !slices.Contains(dailyMetricsNativeFamilies, family) {
		return errors.New("daily metrics native family is not registered")
	}
	if rowsWritten < 0 || duration < 0 {
		return errors.New("daily metrics native family counts cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsNativeFamilyOutcome[dailyMetricsNativeFamilyOutcomeLabels{Family: family, Outcome: outcome}]++
	if outcome == DailyMetricsNativeFamilyOutcomeComputed {
		collector.dailyMetricsNativeFamilyRowsWritten[family] += uint64(rowsWritten)
		collector.dailyMetricsNativeFamilyDuration[family].observe(duration.Seconds())
	}
	return nil
}

// ObserveDailyMetricsCompatRetry records one ambiguous_refused metrics.daily
// compatibility-bridge execution's terminal disposition (CHAOS-4319). See
// DailyMetricsCompatRetryDecision for why Go only ever reports
// "persisted_failed".
func (collector *MetricsCollector) ObserveDailyMetricsCompatRetry(decision DailyMetricsCompatRetryDecision) error {
	if !slices.Contains(dailyMetricsCompatRetryDecisions(), decision) {
		return errors.New("daily metrics compat retry decision is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.dailyMetricsCompatRetry[decision]++
	return nil
}

// ObserveTeamMetricsDailyRepoCount records the distinct repo_id count for
// ONE team_id in one team_metrics_daily write (CHAOS-4329). Callers must
// call this once per team per write, AFTER the write durably lands (mirrors
// ObserveZeroUnitFinalization's post-commit rule), never once per row --
// TeamWellbeingExecutor's ComputeFamily computes this by grouping the exact
// rows it just wrote by team_id and counting each team's distinct repo_id,
// exactly like Python's record_team_metrics_daily_repo_rows.
func (collector *MetricsCollector) ObserveTeamMetricsDailyRepoCount(repoCount int) error {
	if repoCount <= 0 {
		return errors.New("team metrics daily repo count must be positive")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.teamMetricsDailyRepoCount.observe(float64(repoCount))
	return nil
}

// ObservePostSyncFanout records the outcome of one post_sync job's decision
// about whether to re-drive daily metrics for its organization (CHAOS-4263,
// codex adversarial-review round 2).
func (collector *MetricsCollector) ObservePostSyncFanout(outcome PostSyncFanoutOutcome) error {
	if !slices.Contains(postSyncFanoutOutcomes(), outcome) {
		return errors.New("post-sync fanout outcome is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.postSyncFanout[outcome]++
	return nil
}

// ObserveTeamRepoOwnershipDerivation records the outcome of one
// sync.team_repo_ownership_derivation worker run (CHAOS-4365 item 1b).
// rowCount is only observed into the paired histogram on the rows_written
// outcome -- a no_signal or error run has no meaningful row count to bucket.
func (collector *MetricsCollector) ObserveTeamRepoOwnershipDerivation(outcome TeamRepoOwnershipDerivationOutcome, rowCount int) error {
	if !slices.Contains(teamRepoOwnershipDerivationOutcomes(), outcome) {
		return errors.New("team-repo-ownership-derivation outcome is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.teamRepoOwnershipDerivation[outcome]++
	if outcome == TeamRepoOwnershipDerivationOutcomeRowsWritten {
		collector.teamRepoOwnershipDerivationRowCount.observe(float64(rowCount))
	}
	return nil
}

// ObserveTeamRepoOwnershipDerivationResolutionArm records, per
// sync.team_repo_ownership_derivation run, how many written rows came from
// each identity-resolution arm (CHAOS-4458 part (b)): the pre-existing
// direct project_id join, or the linear_team_key join added because Linear's
// team_project_ownership writer and its work-item normalizer disagree on
// what project_id means (team-attribution.md §0.2). count must be >= 0 and
// is called for every registered arm on every run (even 0) so both series
// stay present regardless of which arm (if either) actually produced rows.
func (collector *MetricsCollector) ObserveTeamRepoOwnershipDerivationResolutionArm(arm TeamRepoOwnershipResolutionArm, count int) error {
	if !slices.Contains(teamRepoOwnershipResolutionArms(), arm) {
		return errors.New("team-repo-ownership-derivation resolution arm is not registered")
	}
	if count < 0 {
		return errors.New("team-repo-ownership-derivation resolution arm count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.teamRepoOwnershipResolutionArm[arm] += uint64(count)
	return nil
}

// ObserveTeamCatalogDispatch records one dispatch decision for CHAOS-4431's
// native/bridge team-catalog split.
func (collector *MetricsCollector) ObserveTeamCatalogDispatch(provider string, entryPoint TeamCatalogEntryPoint, outcome TeamCatalogOutcome) error {
	if !slices.Contains(teamCatalogEntryPoints(), entryPoint) {
		return errors.New("team-catalog entry point is not registered")
	}
	if !slices.Contains(teamCatalogOutcomes(), outcome) {
		return errors.New("team-catalog outcome is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.teamCatalogDispatch[teamCatalogDispatchLabels{
		Provider: teamCatalogProvider(provider), EntryPoint: entryPoint, Outcome: outcome,
	}]++
	return nil
}

// ObserveTeamCatalogRowsWritten records rows a native team-catalog collector
// wrote to one destination table. Only called on the native outcome.
func (collector *MetricsCollector) ObserveTeamCatalogRowsWritten(provider string, table TeamCatalogTable, rowsWritten int) error {
	if !slices.Contains(teamCatalogTables(), table) {
		return errors.New("team-catalog table is not registered")
	}
	if rowsWritten < 0 {
		return errors.New("team-catalog rows written must not be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.teamCatalogRowsWritten[teamCatalogRowsLabels{
		Provider: teamCatalogProvider(provider), Table: table,
	}] += uint64(rowsWritten)
	return nil
}

// zeroUnitFinalizationLabels are (provider, reason) for
// ObserveZeroUnitFinalization. Provider is clamped to a closed, known set
// (zeroUnitFinalizationProvider); reason is NOT a closed enum -- unlike
// every other multi-dimension counter in this file, CHAOS-4159's whole
// premise is that new planner code paths keep adding new reason values over
// time, and the metric's job is to make an as-yet-unclassified one visible,
// not to reject it. maxZeroUnitFinalizationSeries bounds the resulting
// cardinality risk instead: once that many distinct (provider, reason) pairs
// have been observed, anything new collapses into a fixed overflow bucket
// rather than growing the label set without limit.
type zeroUnitFinalizationLabels struct {
	Provider string
	Reason   string
}

// zeroUnitFinalizationProviders mirrors the closed provider set
// providerfoundation/budget.go's cost-class switch and _run_provider's own
// "unknown" residual both use. Any other value is folded into "unknown" --
// the exact string Python's _run_provider itself falls back to when an
// Integration row has no usable provider.
var zeroUnitFinalizationProviders = map[string]struct{}{
	"github": {}, "gitlab": {}, "jira": {}, "linear": {},
	"launchdarkly": {}, "pagerduty": {},
}

const zeroUnitFinalizationUnknownProvider = "unknown"

const maxZeroUnitFinalizationSeries = 64

const zeroUnitFinalizationOverflowReason = "cardinality_capped"

func zeroUnitFinalizationProvider(provider string) string {
	if _, known := zeroUnitFinalizationProviders[provider]; known {
		return provider
	}
	return zeroUnitFinalizationUnknownProvider
}

// ObserveZeroUnitFinalization records one sync run that finalized with zero
// planned units, labeled by provider and by the cause finalize classified it
// under (CHAOS-4175, CHAOS-4159's Go counterpart). Callers must call this
// AFTER the finalizing transaction has durably committed: a counter bumped
// inside a transaction that later rolls back would overcount every retry of
// a finalization that eventually succeeds once (see
// zero_unit_telemetry.py's module docstring for the same rule in Python).
func (collector *MetricsCollector) ObserveZeroUnitFinalization(provider, reason string) error {
	if !metricIdentifier(reason, 128) {
		return errors.New("zero unit finalization reason is invalid")
	}
	provider = zeroUnitFinalizationProvider(provider)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	key := zeroUnitFinalizationLabels{Provider: provider, Reason: reason}
	if _, seen := collector.zeroUnitFinalizations[key]; !seen &&
		len(collector.zeroUnitFinalizations) >= maxZeroUnitFinalizationSeries {
		// A SINGLE, provider-independent overflow key -- not
		// {provider, cardinality_capped} -- so the map can never grow past
		// maxZeroUnitFinalizationSeries+1 regardless of how many distinct
		// providers each independently trigger overflow. A per-provider
		// overflow bucket would still be an unbounded-in-provider-count
		// series set, just with a higher, less obviously-wrong ceiling
		// (codex adversarial review, CHAOS-4175).
		key = zeroUnitFinalizationLabels{
			Provider: zeroUnitFinalizationUnknownProvider,
			Reason:   zeroUnitFinalizationOverflowReason,
		}
	}
	collector.zeroUnitFinalizations[key]++
	return nil
}

// ObserveCoverageCacheInvalidation records one finalize that decided to
// invalidate the home-dashboard coverage cache (emitted) and whether Valkey
// acknowledged the epoch bump (consumed). Same post-commit rule as
// ObserveZeroUnitFinalization: the caller must only report after the
// finalizing transaction durably committed. Provider is clamped to the same
// closed set as the zero-unit counter.
func (collector *MetricsCollector) ObserveCoverageCacheInvalidation(provider string, consumed bool) error {
	provider = zeroUnitFinalizationProvider(provider)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.coverageCacheInvalidationsEmitted[provider]++
	if consumed {
		collector.coverageCacheInvalidationsConsumed[provider]++
	} else {
		collector.coverageCacheInvalidationsConsumed[provider] += 0
	}
	return nil
}

// External-ingest label domains (CHAOS-4194). Both are CLOSED sets checked
// against, not free text, because both label values originate in a
// customer-pushed batch. `source_system` comes off the batch pointer and
// `reason` is a refusal code the ingest path chooses, but neither is worth
// trusting to stay bounded on the strength of "the caller only passes good
// values" -- an unbounded label here is a customer-controlled cardinality
// explosion in the exposition, not a cosmetic defect.
//
// The record KIND is deliberately not a label. It is the one value in a
// refusal that is genuinely attacker-controlled and unbounded -- the whole
// point of unsupported_kind_for_system is that the kind was not recognised --
// so labeling by it would let a malformed push mint a new series per record.
// The kind is already carried on the durable rejection row, which is where an
// operator should read it.
var externalTelemetrySystems = map[string]struct{}{
	"github": {}, "gitlab": {}, "jira": {}, "linear": {},
	"custom": {}, "pagerduty": {}, "atlassian": {},
}

// ExternalRefused* are the refusal codes normalizeExternalRecords emits.
const (
	ExternalRefusedUnsupportedKind       = "unsupported_kind_for_system"
	ExternalRefusedEntityFamilyMismatch  = "entity_family_mismatch"
	ExternalRefusedInvalidField          = "invalid_field"
	ExternalRefusedOutsideSourceInstance = "record_outside_source_instance"
	// ExternalRefusedUnresolvableProject and ExternalRefusedContradictoryEvent
	// are project_membership_transition.v1's own refusals (CHAOS-4194): a
	// from/to project id that cannot be a provider PROJECT entity, and two
	// records in one batch asserting the same event_id with different content.
	ExternalRefusedUnresolvableProject = "unresolvable_project_entity"
	ExternalRefusedContradictoryEvent  = "contradictory_event_id"
	// ExternalRefusedNamelessMembership is a membership event naming neither
	// the project joined nor the project left. Presence is keyed per
	// (subject, project), so such a row could not retire or create any
	// membership -- it would sit in the history looking like a removal that
	// silently did nothing.
	ExternalRefusedNamelessMembership = "membership_event_names_no_project"
)

var externalRefusalReasons = []string{
	ExternalRefusedUnsupportedKind,
	ExternalRefusedEntityFamilyMismatch,
	ExternalRefusedInvalidField,
	ExternalRefusedOutsideSourceInstance,
	ExternalRefusedUnresolvableProject,
	ExternalRefusedContradictoryEvent,
	ExternalRefusedNamelessMembership,
}

const externalTelemetryUnknownSystem = "unknown"

type externalRefusalLabels struct {
	SourceSystem string
	Reason       string
}

func externalTelemetrySystem(system string) string {
	if _, known := externalTelemetrySystems[system]; known {
		return system
	}
	return externalTelemetryUnknownSystem
}

// ObserveExternalProjectMembershipsSunk records project_membership_transition.v1
// rows that reached ClickHouse, by the provider that pushed them (CHAOS-4194).
//
// Callers must call this AFTER the sink write has returned successfully, for
// the same reason ObserveZeroUnitFinalization insists on a committed
// transaction: the external ingest handler RETRIES a transient sink failure,
// so counting at append time would count every attempt of a write that
// eventually succeeds once.
func (collector *MetricsCollector) ObserveExternalProjectMembershipsSunk(provider string, rows int) error {
	if rows < 0 {
		return errors.New("sunk project membership rows cannot be negative")
	}
	if rows == 0 {
		return nil
	}
	provider = externalTelemetrySystem(provider)
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.externalProjectMembershipsSunk == nil {
		collector.externalProjectMembershipsSunk = map[string]uint64{}
	}
	collector.externalProjectMembershipsSunk[provider] += uint64(rows)
	return nil
}

// ObserveExternalKindRefused records one external-ingest record the registry
// refused, by source system and refusal code (CHAOS-4194).
//
// An unknown reason is an ERROR rather than a fold into an "other" bucket. The
// reason set is this repo's own closed vocabulary, so an unrecognised one means
// a refusal path was added without extending the exposition -- exactly the
// silent-new-failure-mode this counter exists to prevent -- and swallowing it
// would hide the omission behind a plausible-looking series.
func (collector *MetricsCollector) ObserveExternalKindRefused(sourceSystem, reason string) error {
	if !slices.Contains(externalRefusalReasons, reason) {
		return fmt.Errorf("unknown external refusal reason %q", reason)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.externalRecordRefusals == nil {
		collector.externalRecordRefusals = map[externalRefusalLabels]uint64{}
	}
	collector.externalRecordRefusals[externalRefusalLabels{
		SourceSystem: externalTelemetrySystem(sourceSystem), Reason: reason,
	}]++
	return nil
}

// ObserveWorkGraphLeaseReleaseLost records a work-graph claimant that could not
// stand its own row down because its lease had already expired (CHAOS-4002).
// Unlike the daily-metrics lease, work-graph has one lease-fenced release path
// shared by all five kinds, so there is no stage dimension to record.
// Budget-estimate fail-open reasons (CHAOS-4175). Closed set: the ONLY
// remaining fail-open trigger, after the contract-rejection split, is a
// genuine bridge/estimate-outage failure (transport, 5xx, decode) --
// budgetEnforceRun's own classification already refuses a batch that
// exceeds the endpoint's documented size limit instead of falling open on
// it, so that case never reaches this counter at all.
const BudgetEstimateFailureBridgeUnavailable = "bridge_unavailable"

var budgetEstimateFailureReasons = []string{BudgetEstimateFailureBridgeUnavailable}

// ObserveBudgetEstimateFailure records dispatch_sync_run's BudgetGuard
// falling open on an estimate-bridge chunk failure, by reason -- the
// positive signal an alert can bind to, since admission proceeding with
// zero estimates checked is otherwise indistinguishable from a quiet pass.
func (collector *MetricsCollector) ObserveBudgetEstimateFailure(reason string) error {
	if !slices.Contains(budgetEstimateFailureReasons, reason) {
		return fmt.Errorf("unknown budget estimate failure reason %q", reason)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.budgetEstimateFailures == nil {
		collector.budgetEstimateFailures = map[string]uint64{}
	}
	collector.budgetEstimateFailures[reason]++
	return nil
}

func (collector *MetricsCollector) ObserveWorkGraphLeaseReleaseLost() error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.workGraphReleaseLost++
	return nil
}

// ObserveRemainingMetricsLeaseReleaseLost is ObserveWorkGraphLeaseReleaseLost's
// remaining-metrics equivalent (CHAOS-4002).
func (collector *MetricsCollector) ObserveRemainingMetricsLeaseReleaseLost() error {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.remainingReleaseLost++
	return nil
}

// ObserveRemainingMetricsOpenDayZeroRow records one dora cross-trigger
// coverage check (CHAOS-4384) that found a 0-row succeeded partition for a
// day that has not closed yet, and refused to treat it as terminal coverage.
func (collector *MetricsCollector) ObserveRemainingMetricsOpenDayZeroRow(family string) error {
	if !slices.Contains(remainingMetricsOpenDayZeroRowFamilies, family) {
		return errors.New("remaining metrics open-day zero-row family is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.remainingOpenDayZeroRow[family]++
	return nil
}

// ObserveRemainingMetricsManualBackfill records one CHAOS-4254 operator-
// triggered manual backfill request outcome. family and outcome are plain
// strings (not RemainingMetricsManualBackfillOutcome) so
// internal/jobs/metrics/remaining can implement this capability with a
// package-local interface and no dependency on jobruntime's exported types
// -- the same decoupling ObserveRemainingMetricsOpenDayZeroRow uses.
func (collector *MetricsCollector) ObserveRemainingMetricsManualBackfill(family, outcome string) error {
	if !slices.Contains(remainingMetricsManualBackfillFamilies, family) {
		return errors.New("remaining metrics manual backfill family is not registered")
	}
	typedOutcome := RemainingMetricsManualBackfillOutcome(outcome)
	if !slices.Contains(remainingMetricsManualBackfillOutcomes(), typedOutcome) {
		return errors.New("remaining metrics manual backfill outcome is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.remainingManualBackfill[remainingMetricsManualBackfillLabels{Family: family, Outcome: typedOutcome}]++
	return nil
}

// ObserveDORAPartition records one completed native DORA partition.
//
// skippedRows is the count of candidate rows the executor could not use --
// today, rows whose repo_id is not a parseable UUID. Python TOLERATES those
// (_has_valid_repo) and finalizes the partition anyway, and CHAOS-4130 ruled
// that the port must preserve that disposition rather than convert a tolerated
// partial into a retry or a failure. Telemetry is therefore the ONLY place the
// partiality becomes visible: without this counter a run that silently dropped
// most of its input is indistinguishable from a clean one, in both runtimes.
//
// doraEmptyOutcomes is tracked separately from doraPartitions because "ran and
// wrote nothing" is the shape a broken cutover takes. A rate that is normally
// near zero and jumps is a signal; a rows-written counter alone would just
// flatten, which reads the same as low traffic.
func (collector *MetricsCollector) ObserveDORAPartition(
	days int, rowsWritten int, skippedRows int,
) error {
	// Negative counts mean the caller miscounted. Clamping would fold a
	// reporting bug into a plausible-looking number; refusing keeps the
	// counters trustworthy, and the executor already ignores this error so a
	// telemetry fault cannot fail a partition whose work is durably written.
	if days < 0 || rowsWritten < 0 || skippedRows < 0 {
		return errors.New("dora partition counts cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.doraPartitions++
	collector.doraDays += uint64(days)
	collector.doraRowsWritten += uint64(rowsWritten)
	collector.doraSkippedRows += uint64(skippedRows)
	if rowsWritten == 0 {
		collector.doraEmptyOutcomes++
	}
	return nil
}

// DORA refusal reasons. A closed set, because an unbounded reason string
// becomes an unbounded label cardinality.
const (
	DORARefusedOrderingContractMismatch = "ordering_contract_mismatch"
	DORARefusedContractUnparseable      = "unparseable"
	DORARefusedUnknownSchema            = "unknown_schema"
	DORARefusedInspectFailed            = "inspect_failed"
)

var doraRefusalReasons = []string{
	DORARefusedOrderingContractMismatch,
	DORARefusedContractUnparseable,
	DORARefusedUnknownSchema,
	DORARefusedInspectFailed,
}

// ObserveDORARefused records that the native DORA executor refused to build.
//
// The dora kind is then not registered, while the other remaining kinds are.
// That is deliberate (CHAOS-3092 R1) -- a DORA-only fault must not take down
// six healthy siblings -- but it means the only outward sign would otherwise be
// a metric that never moves. This counter is the positive signal, and the
// reason label is what tells an operator whether to fix configuration, finish a
// migration, or look at ClickHouse.
func (collector *MetricsCollector) ObserveDORARefused(reason string) error {
	if !slices.Contains(doraRefusalReasons, reason) {
		return fmt.Errorf("unknown dora refusal reason %q", reason)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.doraRefusals == nil {
		collector.doraRefusals = map[string]uint64{}
	}
	collector.doraRefusals[reason]++
	return nil
}

// Report dedup-guard reasons (CHAOS-4140). A closed set of exactly one value
// today: the guard exists to protect a single shape (an append-only daily
// rollup carrying more than one compute generation for the same natural key
// after a partition retry). It is still a named, bounded reason -- not a bare
// counter -- so a second guard shape introduced later (e.g. a table merged
// mid-flight from two writers) adds a reason rather than overloading this
// one's meaning.
const ReportDedupReasonRetryGeneration = "retry_generation"

var reportDedupGuardReasons = []string{ReportDedupReasonRetryGeneration}

// ObserveReportDedupGuard records one dedup-guarded report chart read over an
// append-only daily rollup table (internal/jobs/report/dedup.go's
// appendOnlyDailyKeys). observedRows is the physical row count the guard's
// key range scanned; skippedRows is how many of those it discarded as a
// stale compute generation (a row sharing a natural key with another row
// carrying a later computed_at). CHAOS-4140 found dora_metrics_daily itself
// unguarded (a straight fall-through to the raw table, silently double- or
// triple-counting every retried partition into every DORA chart); this
// counter is the positive signal that the guard is both reachable and
// actually encountering the duplicate generations it exists to discard --
// skippedRows==0 across every table is exactly as informative as a nonzero
// reading, so it is recorded every call, not only when duplicates are found.
func (collector *MetricsCollector) ObserveReportDedupGuard(table, reason string, observedRows, skippedRows int) error {
	if observedRows < 0 || skippedRows < 0 {
		return errors.New("report dedup guard counts cannot be negative")
	}
	if skippedRows > observedRows {
		return fmt.Errorf(
			"report dedup guard skipped %d rows exceeds observed %d for table %q",
			skippedRows, observedRows, table,
		)
	}
	if table == "" {
		return errors.New("report dedup guard table must not be empty")
	}
	if !slices.Contains(reportDedupGuardReasons, reason) {
		return fmt.Errorf("unknown report dedup guard reason %q", reason)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.reportDedupObservedRows == nil {
		collector.reportDedupObservedRows = map[string]uint64{}
		collector.reportDedupSkippedRows = map[string]uint64{}
	}
	key := table + "|" + reason
	collector.reportDedupObservedRows[key] += uint64(observedRows)
	collector.reportDedupSkippedRows[key] += uint64(skippedRows)
	return nil
}

// Capacity refusal reasons. Closed set, bounded label cardinality.
const (
	// Both of these are REACHABLE, which was not true of the set they replace.
	// An earlier version declared a seed_missing reason that nothing ever
	// emitted -- the seed is enforced when the run row is created and again per
	// partition, neither of which is a refusal to BUILD -- so the exposition
	// carried a series that could never move. A counter that cannot fire is
	// worse than an absent one: it implies coverage that does not exist.
	CapacityRefusedSchemaIncompatible = "schema_incompatible"
	CapacityRefusedInspectFailed      = "inspect_failed"
)

var capacityRefusalReasons = []string{
	CapacityRefusedSchemaIncompatible,
	CapacityRefusedInspectFailed,
}

// ObserveCapacityPartition records one completed native capacity partition.
func (collector *MetricsCollector) ObserveCapacityPartition(
	scopes, rowsWritten, skipped int,
) error {
	if scopes < 0 || rowsWritten < 0 || skipped < 0 {
		return errors.New("capacity partition counts cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.capacityPartitions++
	collector.capacityScopes += uint64(scopes)
	collector.capacityRowsWritten += uint64(rowsWritten)
	collector.capacitySkippedScopes += uint64(skipped)
	return nil
}

// ObserveCapacityRefused records that the native capacity executor refused to
// build, by reason -- the positive signal an alert can bind to, since a flat
// partitions counter cannot be told apart from a quiet day.
func (collector *MetricsCollector) ObserveCapacityRefused(reason string) error {
	if !slices.Contains(capacityRefusalReasons, reason) {
		return fmt.Errorf("unknown capacity refusal reason %q", reason)
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.capacityRefusals == nil {
		collector.capacityRefusals = map[string]uint64{}
	}
	collector.capacityRefusals[reason]++
	return nil
}

// compatibilityBridgeFamilies is the closed set of remaining-metrics families
// still routed through the Python compatibility bridge (dora and capacity are
// native and observed separately above). Bounded label cardinality, same
// discipline as doraRefusalReasons/capacityRefusalReasons.
var compatibilityBridgeFamilies = []string{
	"complexity", "release_impact", "recommendations",
	"membership_backfill",
}

// ObserveCompatibilityPartition implements remaining.CompatibilityObserver.
// rowsWritten nil means the family's evidence carries no countable row
// signal; a non-nil zero is counted both in compatibilityRowsWritten (as 0,
// keeping the series present) and in compatibilityZeroRowPartitions, so a
// success that wrote nothing is visible without waiting on a log line.
func (collector *MetricsCollector) ObserveCompatibilityPartition(family string, rowsWritten *int) error {
	if !slices.Contains(compatibilityBridgeFamilies, family) {
		return fmt.Errorf("unknown compatibility bridge family %q", family)
	}
	if rowsWritten != nil && *rowsWritten < 0 {
		return errors.New("compatibility rows written cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.compatibilityRowsWritten == nil {
		collector.compatibilityRowsWritten = map[string]uint64{}
	}
	if collector.compatibilityZeroRowPartitions == nil {
		collector.compatibilityZeroRowPartitions = map[string]uint64{}
	}
	if rowsWritten == nil {
		return nil
	}
	collector.compatibilityRowsWritten[family] += uint64(*rowsWritten)
	if *rowsWritten == 0 {
		collector.compatibilityZeroRowPartitions[family]++
	}
	return nil
}

func (collector *MetricsCollector) SetExecutionSaturation(queue string, ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return errors.New("execution saturation must be between zero and one")
	}
	labels := queueLabels{Queue: queue}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedQueues[labels]; !ok {
		return errors.New("execution saturation queue is not registered")
	}
	collector.executionSaturation[labels] = ratio
	return nil
}

func (collector *MetricsCollector) SetStreamLag(labels StreamLabels, lag int64) error {
	if lag < 0 {
		return errors.New("stream lag cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamLag[labels] = lag
	return nil
}

func (collector *MetricsCollector) SetStreamPending(labels StreamLabels, pending int64) error {
	if pending < 0 {
		return errors.New("stream pending count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamPending[labels] = pending
	return nil
}

func (collector *MetricsCollector) SetStreamOldestPending(labels StreamLabels, age time.Duration) error {
	if age < 0 {
		return errors.New("oldest pending age cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamOldestPending[labels] = age.Seconds()
	return nil
}

func (collector *MetricsCollector) ObserveProviderBudgetWait(labels BudgetLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("budget wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.budgetWait[labels]
	if !ok {
		return errors.New("budget metric dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

func (collector *MetricsCollector) SetConcurrencyBudgetCapacity(labels ConcurrencyBudgetLabels, capacity int) error {
	if capacity < 1 {
		return errors.New("concurrency budget capacity must be positive")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedConcurrencyBudgets[labels]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetCapacity[labels] = int64(capacity)
	return nil
}

func (collector *MetricsCollector) SetConcurrencyBudgetLeased(labels ConcurrencyBudgetLabels, leased int) error {
	if leased < 0 {
		return errors.New("concurrency budget leased capacity cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedConcurrencyBudgets[labels]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetLeased[labels] = int64(leased)
	return nil
}

func (collector *MetricsCollector) ObserveConcurrencyBudgetWait(labels ConcurrencyBudgetLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("concurrency budget wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.concurrencyBudgetWait[labels]
	if !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

func (collector *MetricsCollector) ObserveConcurrencyBudgetExpiry(labels ConcurrencyBudgetLabels, result string) error {
	if result != "expired" && result != "recovered" {
		return errors.New("invalid concurrency budget lease event")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	key := concurrencyBudgetResultLabels{Budget: labels, Result: result}
	if _, ok := collector.concurrencyBudgetEvents[key]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetEvents[key]++
	return nil
}

func (collector *MetricsCollector) SetDatabasePoolSaturation(pool string, ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return errors.New("database pool saturation must be between zero and one")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.poolSaturation[pool]; !ok {
		return errors.New("database pool metric dimension is not registered")
	}
	collector.poolSaturation[pool] = ratio
	return nil
}

func (collector *MetricsCollector) ObserveDatabasePoolAcquire(pool, result string, duration time.Duration) error {
	if duration < 0 {
		return errors.New("database pool acquisition duration cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.poolAcquire[poolAcquireLabels{Pool: pool, Result: result}]
	if !ok {
		return errors.New("database pool metric dimensions are not registered")
	}
	metric.observe(duration.Seconds())
	return nil
}

// PrometheusText returns deterministic Prometheus text exposition. It never
// includes timestamps, job IDs, organizations, payloads, or error strings.
func (collector *MetricsCollector) PrometheusText() string {
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	var output strings.Builder
	collector.writeRuntime(&output)
	collector.writeJobs(&output)
	collector.writeSyncLeases(&output)
	collector.writeReportRunLeases(&output)
	collector.writeIdempotencyRenewalRetired(&output)
	collector.writeDailyMetricsLeases(&output)
	collector.writeDailyMetricsDiscovery(&output)
	collector.writeDailyMetricsFamilyZeroRowsWithSource(&output)
	collector.writeDailyMetricsRedrive(&output)
	collector.writeDailyMetricsFinalizeSweep(&output)
	collector.writeDailyMetricsFinalizeLedgerRepair(&output)
	collector.writeDailyMetricsFinalizeRedrive(&output)
	collector.writeDailyMetricsPartitionRecompute(&output)
	collector.writeDailyMetricsNativeFamily(&output)
	collector.writeDailyMetricsCompatRetry(&output)
	collector.writeTeamMetricsDailyRepoCount(&output)
	collector.writePostSyncFanout(&output)
	collector.writeTeamRepoOwnershipDerivation(&output)
	collector.writeTeamCatalogDispatch(&output)
	collector.writeTeamCatalogRowsWritten(&output)
	collector.writeWorkGraphLease(&output)
	collector.writeRemainingMetricsLease(&output)
	collector.writeReportDedupGuard(&output)
	collector.writeZeroUnitFinalizations(&output)
	collector.writeCoverageCacheInvalidations(&output)
	collector.writeExternalIngest(&output)
	collector.writeStreams(&output)
	collector.writeBudgets(&output)
	collector.writeConcurrencyBudgets(&output)
	collector.writePools(&output)
	return output.String()
}

// WritePrometheus writes one deterministic snapshot to output.
func (collector *MetricsCollector) WritePrometheus(output io.Writer) error {
	if output == nil {
		return errors.New("Prometheus output is required")
	}
	_, err := io.WriteString(output, collector.PrometheusText())
	return err
}

func (collector *MetricsCollector) writeRuntime(output *strings.Builder) {
	writeMetadata(output, "worker_runtime_info", "Build identity for this worker runtime.", "gauge")
	if collector.runtimeInfo != nil {
		writeFloatSample(output, "worker_runtime_info", []metricLabel{
			{"version", collector.runtimeInfo.Version},
			{"commit", collector.runtimeInfo.Commit},
		}, 1)
	}
}

func (collector *MetricsCollector) writeJobs(output *strings.Builder) {
	jobs := sortedJobs(collector.allowedJobs)
	writeMetadata(output, "worker_jobs_available", "Current jobs available to the worker by queue and kind.", "gauge")
	for _, labels := range jobs {
		writeIntSample(output, "worker_jobs_available", jobMetricLabels(labels), collector.jobsAvailable[labels])
	}

	queues := sortedQueues(collector.allowedQueues)
	writeMetadata(output, "worker_job_oldest_age_seconds", "Age of the oldest available job by queue.", "gauge")
	for _, labels := range queues {
		writeFloatSample(output, "worker_job_oldest_age_seconds", queueMetricLabels(labels), collector.jobOldestAge[labels])
	}

	writeMetadata(output, "worker_jobs_running", "Current jobs executing by queue and kind.", "gauge")
	for _, labels := range jobs {
		writeIntSample(output, "worker_jobs_running", jobMetricLabels(labels), collector.jobsRunning[labels])
	}

	writeMetadata(output, "worker_execution_saturation_ratio", "Fraction of THIS process's configured worker execution capacity currently in use. Per-process, not fleet-wide: average across replicas for fleet utilization.", "gauge")
	for _, labels := range queues {
		writeFloatSample(output, "worker_execution_saturation_ratio", queueMetricLabels(labels), collector.executionSaturation[labels])
	}

	writeMetadata(output, "worker_job_wait_seconds", "Time from job availability to execution start.", "histogram")
	for _, labels := range jobs {
		writeHistogram(output, "worker_job_wait_seconds", jobMetricLabels(labels), collector.jobWait[labels])
	}

	writeMetadata(output, "worker_job_duration_seconds", "Job execution duration by safe result.", "histogram")
	durationKeys := make([]jobResultLabels, 0, len(collector.jobDuration))
	for labels := range collector.jobDuration {
		durationKeys = append(durationKeys, labels)
	}
	sort.Slice(durationKeys, func(left, right int) bool {
		if compareJobs(durationKeys[left].Job, durationKeys[right].Job) != 0 {
			return compareJobs(durationKeys[left].Job, durationKeys[right].Job) < 0
		}
		return durationKeys[left].Result < durationKeys[right].Result
	})
	for _, labels := range durationKeys {
		metricLabels := append(jobMetricLabels(labels.Job), metricLabel{"result", string(labels.Result)})
		writeHistogram(output, "worker_job_duration_seconds", metricLabels, collector.jobDuration[labels])
	}

	writeMetadata(output, "worker_job_attempts_total", "Completed worker execution attempts by kind and safe outcome.", "counter")
	attemptKeys := make([]attemptLabels, 0, len(collector.jobAttempts))
	for labels := range collector.jobAttempts {
		attemptKeys = append(attemptKeys, labels)
	}
	sort.Slice(attemptKeys, func(left, right int) bool {
		if attemptKeys[left].Kind != attemptKeys[right].Kind {
			return attemptKeys[left].Kind < attemptKeys[right].Kind
		}
		if attemptKeys[left].Result != attemptKeys[right].Result {
			return attemptKeys[left].Result < attemptKeys[right].Result
		}
		return attemptKeys[left].Category < attemptKeys[right].Category
	})
	for _, labels := range attemptKeys {
		writeUintSample(output, "worker_job_attempts_total", []metricLabel{
			{"kind", labels.Kind}, {"result", string(labels.Result)}, {"error_category", string(labels.Category)},
		}, collector.jobAttempts[labels])
	}

	writeMetadata(output, "worker_job_panics_total", "Recovered worker panics by kind.", "counter")
	for _, kind := range sortedStrings(collector.allowedKinds) {
		writeUintSample(output, "worker_job_panics_total", []metricLabel{{"kind", kind}}, collector.jobPanics[kind])
	}

	writeMetadata(output, "worker_job_cancellations_total", "Worker cancellations by kind and bounded reason.", "counter")
	cancellationKeys := make([]cancellationLabels, 0, len(collector.cancellations))
	for labels := range collector.cancellations {
		cancellationKeys = append(cancellationKeys, labels)
	}
	sort.Slice(cancellationKeys, func(left, right int) bool {
		if cancellationKeys[left].Kind != cancellationKeys[right].Kind {
			return cancellationKeys[left].Kind < cancellationKeys[right].Kind
		}
		return cancellationKeys[left].Reason < cancellationKeys[right].Reason
	})
	for _, labels := range cancellationKeys {
		writeUintSample(output, "worker_job_cancellations_total", []metricLabel{
			{"kind", labels.Kind}, {"reason", string(labels.Reason)},
		}, collector.cancellations[labels])
	}

	writeMetadata(output, "worker_job_deterministic_failures_total", "Permanent failures by kind and bounded reason (CHAOS-4242).", "counter")
	deterministicKeys := make([]deterministicFailureLabels, 0, len(collector.deterministicFailures))
	for labels := range collector.deterministicFailures {
		deterministicKeys = append(deterministicKeys, labels)
	}
	sort.Slice(deterministicKeys, func(left, right int) bool {
		if deterministicKeys[left].Kind != deterministicKeys[right].Kind {
			return deterministicKeys[left].Kind < deterministicKeys[right].Kind
		}
		return deterministicKeys[left].Reason.String() < deterministicKeys[right].Reason.String()
	})
	for _, labels := range deterministicKeys {
		writeUintSample(output, "worker_job_deterministic_failures_total", []metricLabel{
			{"kind", labels.Kind}, {"reason", labels.Reason.String()},
		}, collector.deterministicFailures[labels])
	}

	writeMetadata(output, "worker_domain_state_mismatch_total", "Domain precondition mismatches by bounded domain type.", "counter")
	for _, domainType := range sortedStrings(collector.allowedDomains) {
		writeUintSample(output, "worker_domain_state_mismatch_total", []metricLabel{{"domain_type", domainType}}, collector.domainMismatch[domainType])
	}
}

func (collector *MetricsCollector) writeStreams(output *strings.Builder) {
	streams := sortedStreams(collector.allowedStreams)
	writeMetadata(output, "worker_stream_lag", "Current stream consumer lag.", "gauge")
	for _, labels := range streams {
		writeIntSample(output, "worker_stream_lag", streamMetricLabels(labels), collector.streamLag[labels])
	}
	writeMetadata(output, "worker_stream_pending", "Current pending stream entries.", "gauge")
	for _, labels := range streams {
		writeIntSample(output, "worker_stream_pending", streamMetricLabels(labels), collector.streamPending[labels])
	}
	writeMetadata(output, "worker_stream_oldest_pending_seconds", "Age of the oldest pending stream entry.", "gauge")
	for _, labels := range streams {
		writeFloatSample(output, "worker_stream_oldest_pending_seconds", streamMetricLabels(labels), collector.streamOldestPending[labels])
	}
}

func (collector *MetricsCollector) writeSyncLeases(output *strings.Builder) {
	writeMetadata(output, "worker_sync_lease_expired_total", "Expired sync leases recovered by bounded provider, dataset family, and durable result.", "counter")
	for _, labels := range sortedSyncLeases(collector.allowedSyncLeases) {
		for _, result := range syncLeaseResults() {
			writeUintSample(output, "worker_sync_lease_expired_total", []metricLabel{
				{"provider", labels.Provider}, {"dataset_family", labels.DatasetFamily}, {"result", string(result)},
			}, collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}])
		}
	}
}

func (collector *MetricsCollector) writeReportRunLeases(output *strings.Builder) {
	writeMetadata(output, "worker_report_run_lease_expired_total", "Expired report execution leases by bounded durable result.", "counter")
	for _, result := range reportRunLeaseResults() {
		writeUintSample(output, "worker_report_run_lease_expired_total", []metricLabel{
			{"result", string(result)},
		}, collector.reportRunLeaseExpired[result])
	}
}

func (collector *MetricsCollector) writeIdempotencyRenewalRetired(output *strings.Builder) {
	writeMetadata(output, "worker_idempotency_renewal_retired_total", "Job claim lease renewals that gave up, by bounded reason.", "counter")
	for _, reason := range idempotencyRenewalRetiredReasons() {
		writeUintSample(output, "worker_idempotency_renewal_retired_total", []metricLabel{
			{"reason", string(reason)},
		}, collector.idempotencyRenewalRetired[reason])
	}
}

func (collector *MetricsCollector) writeZeroUnitFinalizations(output *strings.Builder) {
	writeMetadata(output, "devhealth_sync_run_zero_unit_finalizations_total",
		"Sync runs finalized with zero planned units, by provider and by the cause finalize classified them under.",
		"counter")
	keys := make([]zeroUnitFinalizationLabels, 0, len(collector.zeroUnitFinalizations))
	for key := range collector.zeroUnitFinalizations {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].Provider != keys[right].Provider {
			return keys[left].Provider < keys[right].Provider
		}
		return keys[left].Reason < keys[right].Reason
	})
	for _, key := range keys {
		writeUintSample(output, "devhealth_sync_run_zero_unit_finalizations_total", []metricLabel{
			{"provider", key.Provider}, {"reason", key.Reason},
		}, collector.zeroUnitFinalizations[key])
	}
}

func (collector *MetricsCollector) writeTeamCatalogDispatch(output *strings.Builder) {
	writeMetadata(output, "dev_health_team_catalog_dispatch_total",
		"CHAOS-4431 team/member/project reference-catalog dispatch decisions, by provider, entry point (reference_discovery|post_sync), and outcome (native|bridge|skipped).",
		"counter")
	keys := make([]teamCatalogDispatchLabels, 0, len(collector.teamCatalogDispatch))
	for key := range collector.teamCatalogDispatch {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].Provider != keys[right].Provider {
			return keys[left].Provider < keys[right].Provider
		}
		if keys[left].EntryPoint != keys[right].EntryPoint {
			return keys[left].EntryPoint < keys[right].EntryPoint
		}
		return keys[left].Outcome < keys[right].Outcome
	})
	for _, key := range keys {
		writeUintSample(output, "dev_health_team_catalog_dispatch_total", []metricLabel{
			{"provider", key.Provider}, {"entry_point", string(key.EntryPoint)}, {"outcome", string(key.Outcome)},
		}, collector.teamCatalogDispatch[key])
	}
}

func (collector *MetricsCollector) writeTeamCatalogRowsWritten(output *strings.Builder) {
	writeMetadata(output, "dev_health_team_catalog_rows_written_total",
		"Rows a native CHAOS-4431 team-catalog collector wrote, by provider and destination table.",
		"counter")
	keys := make([]teamCatalogRowsLabels, 0, len(collector.teamCatalogRowsWritten))
	for key := range collector.teamCatalogRowsWritten {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].Provider != keys[right].Provider {
			return keys[left].Provider < keys[right].Provider
		}
		return keys[left].Table < keys[right].Table
	})
	for _, key := range keys {
		writeUintSample(output, "dev_health_team_catalog_rows_written_total", []metricLabel{
			{"provider", key.Provider}, {"table", string(key.Table)},
		}, collector.teamCatalogRowsWritten[key])
	}
}

func (collector *MetricsCollector) writeCoverageCacheInvalidations(output *strings.Builder) {
	providers := make([]string, 0, len(collector.coverageCacheInvalidationsEmitted))
	for provider := range collector.coverageCacheInvalidationsEmitted {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	writeMetadata(output, "devhealth_sync_coverage_cache_invalidations_emitted_total",
		"Sync-run finalizations that decided to invalidate the home-dashboard coverage cache, by provider.",
		"counter")
	for _, provider := range providers {
		writeUintSample(output, "devhealth_sync_coverage_cache_invalidations_emitted_total", []metricLabel{
			{"provider", provider},
		}, collector.coverageCacheInvalidationsEmitted[provider])
	}
	writeMetadata(output, "devhealth_sync_coverage_cache_invalidations_consumed_total",
		"Coverage-cache invalidations Valkey acknowledged (epoch bump applied), by provider; emitted minus consumed is the alertable gap.",
		"counter")
	for _, provider := range providers {
		writeUintSample(output, "devhealth_sync_coverage_cache_invalidations_consumed_total", []metricLabel{
			{"provider", provider},
		}, collector.coverageCacheInvalidationsConsumed[provider])
	}
}

func (collector *MetricsCollector) writeExternalIngest(output *strings.Builder) {
	writeMetadata(output, "worker_external_project_memberships_sunk_total",
		"project_membership_transition.v1 rows durably written to ClickHouse, by source system.", "counter")
	providers := make([]string, 0, len(collector.externalProjectMembershipsSunk))
	for provider := range collector.externalProjectMembershipsSunk {
		providers = append(providers, provider)
	}
	sort.Strings(providers)
	for _, provider := range providers {
		writeUintSample(output, "worker_external_project_memberships_sunk_total",
			[]metricLabel{{"provider", provider}}, collector.externalProjectMembershipsSunk[provider])
	}
	writeMetadata(output, "worker_external_record_refused_total",
		"External-ingest records refused by the kind registry, by source system and refusal reason.", "counter")
	keys := make([]externalRefusalLabels, 0, len(collector.externalRecordRefusals))
	for key := range collector.externalRecordRefusals {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		if keys[left].SourceSystem != keys[right].SourceSystem {
			return keys[left].SourceSystem < keys[right].SourceSystem
		}
		return keys[left].Reason < keys[right].Reason
	})
	for _, key := range keys {
		writeUintSample(output, "worker_external_record_refused_total", []metricLabel{
			{"source_system", key.SourceSystem}, {"reason", key.Reason},
		}, collector.externalRecordRefusals[key])
	}
}

func (collector *MetricsCollector) writeDailyMetricsLeases(output *strings.Builder) {
	writeMetadata(output, "worker_daily_metrics_lease_total", "Daily-metrics lease encounters by stage and bounded durable outcome.", "counter")
	for _, labels := range dailyMetricsLeaseSeries() {
		writeUintSample(output, "worker_daily_metrics_lease_total", []metricLabel{
			{"stage", string(labels.Stage)}, {"result", string(labels.Result)},
		}, collector.dailyMetricsLease[labels])
	}
}

func (collector *MetricsCollector) writeDailyMetricsDiscovery(output *strings.Builder) {
	writeMetadata(output, "worker_daily_metrics_discovery_total", "Repository-discovery outcomes for daily-metrics runs, by trigger and bounded outcome (CHAOS-4263).", "counter")
	for _, labels := range dailyMetricsDiscoverySeries() {
		writeUintSample(output, "worker_daily_metrics_discovery_total", []metricLabel{
			{"trigger", string(labels.Trigger)}, {"outcome", string(labels.Outcome)},
		}, collector.dailyMetricsDiscovery[labels])
	}
}

func (collector *MetricsCollector) writeDailyMetricsFamilyZeroRowsWithSource(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_families_zero_rows_with_source_total", "Metrics.daily families whose source data existed for a partition's repos+day but whose output table had zero rows for that scope (CHAOS-4263).", "counter")
	for _, family := range dailyMetricsZeroRowsWithSourceFamilies {
		writeUintSample(output, "dev_health_daily_metrics_families_zero_rows_with_source_total",
			[]metricLabel{{"family", family}}, collector.dailyMetricsFamilyZeroRowsWithSource[family])
	}
}

func (collector *MetricsCollector) writeDailyMetricsRedrive(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_redrive_partitions_total", "Daily-metrics partitions moved by an operator-invoked stranded-run redrive, by bounded reason (CHAOS-4358).", "counter")
	for _, reason := range dailyMetricsRedriveReasons {
		writeUintSample(output, "dev_health_daily_metrics_redrive_partitions_total",
			[]metricLabel{{"reason", reason}}, collector.dailyMetricsRedrive[reason])
	}
}

// writeDailyMetricsFinalizeSweep exposes the CHAOS-4389 stranded-finalize
// sweep counters as two distinct series (rather than one metric split by
// label) so "how many runs are stuck" and "how many did we actually move"
// each have their own unambiguous name for an alert to key off.
func (collector *MetricsCollector) writeDailyMetricsFinalizeSweep(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_stranded_finalize_runs_detected_total", "Daily-metrics runs found status='running' with every partition succeeded but finalization never reaching a terminal state, detected by a CHAOS-4389 stranded-finalize sweep.", "counter")
	writeUintSample(output, "dev_health_daily_metrics_stranded_finalize_runs_detected_total", nil, collector.dailyMetricsFinalizeSweep["detected"])
	writeMetadata(output, "dev_health_daily_metrics_runs_finalized_by_sweep_total", "Daily-metrics runs whose metrics.daily_finalize job was freshly (re-)enqueued by a CHAOS-4389 stranded-finalize sweep, outside the outbox's normal per-run dedupe.", "counter")
	writeUintSample(output, "dev_health_daily_metrics_runs_finalized_by_sweep_total", nil, collector.dailyMetricsFinalizeSweep["finalized"])
}

// writeDailyMetricsFinalizeLedgerRepair exposes the CHAOS-4409 finalize-
// ledger bulk-repair counters, by bounded outcome. See
// ObserveDailyMetricsFinalizeLedgerRepair's own doc comment for why no
// production caller increments this today.
func (collector *MetricsCollector) writeDailyMetricsFinalizeLedgerRepair(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_finalize_ledger_repair_rows_total", "Daily/finalize metric_compatibility_executions rows a CHAOS-4409 finalize-ledger bulk repair moved to retry_authorized (\"repaired\") or left alone because the original claim still read as live (\"skipped_claim_active\"), by bounded outcome.", "counter")
	for _, outcome := range dailyMetricsFinalizeLedgerRepairOutcomes {
		writeUintSample(output, "dev_health_daily_metrics_finalize_ledger_repair_rows_total",
			[]metricLabel{{"outcome", outcome}}, collector.dailyMetricsFinalizeLedgerRepair[outcome])
	}
}

// writeDailyMetricsFinalizeRedrive exposes the CHAOS-4405 historical
// finalize-redrive counter, by bounded outcome (redriven vs
// skipped_ineligible) -- one metric, label-split, since both outcomes
// describe the SAME unit of work (a calendar day considered), unlike
// writeDailyMetricsFinalizeSweep's two independently-alertable metrics
// above.
func (collector *MetricsCollector) writeDailyMetricsFinalizeRedrive(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_finalize_redrive_days_total", "Calendar days considered by an operator-invoked CHAOS-4405 historical finalize-redrive pass, by bounded outcome.", "counter")
	for _, outcome := range dailyMetricsFinalizeRedriveOutcomes {
		writeUintSample(output, "dev_health_daily_metrics_finalize_redrive_days_total",
			[]metricLabel{{"outcome", outcome}}, collector.dailyMetricsFinalizeRedrive[outcome])
	}
}

// writeDailyMetricsPartitionRecompute exposes the CHAOS-4459 historical
// partition-recompute counter, by family and bounded outcome (redriven vs
// skipped_ineligible) -- the partition-level counterpart to
// writeDailyMetricsFinalizeRedrive above.
func (collector *MetricsCollector) writeDailyMetricsPartitionRecompute(output *strings.Builder) {
	writeMetadata(output, "dev_health_daily_metrics_partition_recompute_days_total", "Calendar days considered by an operator-invoked CHAOS-4459 historical partition-recompute pass, by family and bounded outcome.", "counter")
	for _, family := range dailyMetricsPartitionRecomputeFamilies {
		for _, outcome := range dailyMetricsPartitionRecomputeOutcomes {
			writeUintSample(output, "dev_health_daily_metrics_partition_recompute_days_total",
				[]metricLabel{{"family", family}, {"outcome", outcome}},
				collector.dailyMetricsPartitionRecompute[dailyMetricsPartitionRecomputeLabels{Family: family, Outcome: outcome}])
		}
	}
}

// writeDailyMetricsNativeFamily exposes the daily bridge's per-family native
// compute counters (CHAOS-4276) -- the same three-metric shape as the DORA/
// capacity native counters above (partitions/rows/refused), except keyed by
// family name (this bridge computes many families per partition, not one
// per River kind) and with a per-family duration histogram, since native
// families are cut over independently and one family's compute cost tells
// nothing about another's.
func (collector *MetricsCollector) writeDailyMetricsNativeFamily(output *strings.Builder) {
	families := append([]string(nil), dailyMetricsNativeFamilies...)
	sort.Strings(families)

	writeMetadata(output, "worker_daily_metrics_native_family_outcome_total", "Native metrics.daily family compute attempts, by family and bounded outcome (CHAOS-4276).", "counter")
	for _, family := range families {
		for _, outcome := range dailyMetricsNativeFamilyOutcomes() {
			labels := dailyMetricsNativeFamilyOutcomeLabels{Family: family, Outcome: outcome}
			writeUintSample(output, "worker_daily_metrics_native_family_outcome_total",
				[]metricLabel{{"family", family}, {"outcome", string(outcome)}}, collector.dailyMetricsNativeFamilyOutcome[labels])
		}
	}

	writeMetadata(output, "worker_daily_metrics_native_family_rows_written_total", "Rows written by native metrics.daily family executors, by family.", "counter")
	for _, family := range families {
		writeUintSample(output, "worker_daily_metrics_native_family_rows_written_total",
			[]metricLabel{{"family", family}}, collector.dailyMetricsNativeFamilyRowsWritten[family])
	}

	writeMetadata(output, "worker_daily_metrics_native_family_duration_seconds", "Native metrics.daily family compute duration, by family. Only Computed attempts are observed.", "histogram")
	for _, family := range families {
		writeHistogram(output, "worker_daily_metrics_native_family_duration_seconds",
			[]metricLabel{{"family", family}}, collector.dailyMetricsNativeFamilyDuration[family])
	}
}

// writeDailyMetricsCompatRetry exposes the terminal ambiguous_refused
// disposition counter (CHAOS-4319) plus the CHAOS-4543 non-terminal
// released-with-reason decisions. The metric name and "decision" label are
// a deliberate cross-language contract with the Python bridge's
// dev_health_metric_compat_retry_total (worker_metrics.py) -- see
// DailyMetricsCompatRetryDecision for the full bounded vocabulary this side
// emits.
func (collector *MetricsCollector) writeDailyMetricsCompatRetry(output *strings.Builder) {
	writeMetadata(output, "dev_health_metric_compat_retry_total", "Terminal or released-with-reason disposition of a metrics.daily compatibility-bridge execution, by worker_kind and bounded decision (CHAOS-4319, CHAOS-4543).", "counter")
	for _, decision := range dailyMetricsCompatRetryDecisions() {
		writeUintSample(output, "dev_health_metric_compat_retry_total",
			[]metricLabel{{"worker_kind", "daily"}, {"decision", string(decision)}}, collector.dailyMetricsCompatRetry[decision])
	}
}

// writeTeamMetricsDailyRepoCount exposes team_metrics_daily's per-team repo
// fan-out (CHAOS-4329) under the SAME metric name Python's identical
// histogram uses (dev_health_team_metrics_daily_repo_count,
// metrics/prometheus.py) -- deliberately breaking this file's worker_
// prefix convention so one Grafana query covers rows written by either
// language, the same reasoning writeDailyMetricsFamilyZeroRowsWithSource
// already documents for its own cross-language metric name.
func (collector *MetricsCollector) writeTeamMetricsDailyRepoCount(output *strings.Builder) {
	writeMetadata(output, "dev_health_team_metrics_daily_repo_count", "Distinct repo_id rows written to team_metrics_daily per (team_id, day) in one write. A value of 1 is an ordinary single-repo team, not a defect (CHAOS-4329).", "histogram")
	writeHistogram(output, "dev_health_team_metrics_daily_repo_count", nil, collector.teamMetricsDailyRepoCount)
}
func (collector *MetricsCollector) writePostSyncFanout(output *strings.Builder) {
	writeMetadata(output, "dev_health_post_sync_fanout_total", "Post-sync fanout outcomes: whether a completed sync's post_sync job published a daily-metrics re-drive (CHAOS-4263).", "counter")
	for _, outcome := range postSyncFanoutOutcomes() {
		writeUintSample(output, "dev_health_post_sync_fanout_total",
			[]metricLabel{{"outcome", string(outcome)}}, collector.postSyncFanout[outcome])
	}
}

// writeTeamRepoOwnershipDerivation renders CHAOS-4365 item 1b's per-outcome
// counter and paired rows-written histogram. This call site was missing
// entirely until now: ObserveTeamRepoOwnershipDerivation incremented
// collector.teamRepoOwnershipDerivation and observed
// collector.teamRepoOwnershipDerivationRowCount, but PrometheusText() never
// rendered either -- the data was recorded in memory and never actually
// exported, silently defeating requirement #4 (codex adversarial review,
// 2026-08-28, found during the per-file fork audit rather than by codex
// itself).
func (collector *MetricsCollector) writeTeamRepoOwnershipDerivation(output *strings.Builder) {
	writeMetadata(output, "dev_health_team_repo_ownership_derivation_total", "sync.team_repo_ownership_derivation worker outcomes: rows_written, no_signal (designed-empty, not a failure), or error (CHAOS-4365 item 1b).", "counter")
	for _, outcome := range teamRepoOwnershipDerivationOutcomes() {
		writeUintSample(output, "dev_health_team_repo_ownership_derivation_total",
			[]metricLabel{{"outcome", string(outcome)}}, collector.teamRepoOwnershipDerivation[outcome])
	}
	writeMetadata(output, "dev_health_team_repo_ownership_derivation_row_count", "Rows written by one sync.team_repo_ownership_derivation worker run, observed only on the rows_written outcome.", "histogram")
	writeHistogram(output, "dev_health_team_repo_ownership_derivation_row_count", nil, collector.teamRepoOwnershipDerivationRowCount)
	writeMetadata(output, "dev_health_team_repo_ownership_derivation_resolution_arm_total", "Rows produced per identity-resolution arm across sync.team_repo_ownership_derivation runs: project_id (pre-existing direct join) vs linear_team_key (CHAOS-4458 part (b) -- Linear's team_project_ownership writer keys rows \"{org_id}:linear:{team_key}\" while work_items.project_id for a Linear item is the raw Linear Project UUID, a disjoint id space).", "counter")
	for _, arm := range teamRepoOwnershipResolutionArms() {
		writeUintSample(output, "dev_health_team_repo_ownership_derivation_resolution_arm_total",
			[]metricLabel{{"arm", string(arm)}}, collector.teamRepoOwnershipResolutionArm[arm])
	}
}

func (collector *MetricsCollector) writeWorkGraphLease(output *strings.Builder) {
	writeMetadata(output, "worker_workgraph_lease_release_lost_total", "Work-graph releases that found their own lease already expired.", "counter")
	writeUintSample(output, "worker_workgraph_lease_release_lost_total", nil, collector.workGraphReleaseLost)
}

func (collector *MetricsCollector) writeRemainingMetricsLease(output *strings.Builder) {
	writeMetadata(output, "worker_remaining_metrics_lease_release_lost_total", "Remaining-metrics releases that found their own lease already expired.", "counter")
	writeUintSample(output, "worker_remaining_metrics_lease_release_lost_total", nil, collector.remainingReleaseLost)

	writeMetadata(output, "dev_health_remaining_metrics_open_day_zero_row_total", "Remaining-metrics dora cross-trigger coverage checks that found a 0-row succeeded partition for a day still open, and refused to treat it as terminal (CHAOS-4384).", "counter")
	for _, family := range remainingMetricsOpenDayZeroRowFamilies {
		writeUintSample(output, "dev_health_remaining_metrics_open_day_zero_row_total",
			[]metricLabel{{"family", family}}, collector.remainingOpenDayZeroRow[family])
	}

	// Emitted for every (family, outcome) pair, including zeros -- same
	// alerting reasoning as worker_dora_native_refused_total below: a series
	// that only appears once an operator runs the CHAOS-4254 command cannot
	// be dashboarded ahead of the first use.
	writeMetadata(output, "dev_health_remaining_metrics_manual_backfill_total", "Operator-triggered CHAOS-4254 `metrics remaining start` requests, by family and outcome.", "counter")
	for _, family := range remainingMetricsManualBackfillFamilies {
		for _, outcome := range remainingMetricsManualBackfillOutcomes() {
			labels := remainingMetricsManualBackfillLabels{Family: family, Outcome: outcome}
			writeUintSample(output, "dev_health_remaining_metrics_manual_backfill_total",
				[]metricLabel{{"family", family}, {"outcome", string(outcome)}}, collector.remainingManualBackfill[labels])
		}
	}

	writeMetadata(output, "worker_dora_native_partitions_total", "Partitions computed by the native Go DORA executor.", "counter")
	writeUintSample(output, "worker_dora_native_partitions_total", nil, collector.doraPartitions)
	writeMetadata(output, "worker_dora_native_days_total", "Days covered by native DORA partitions.", "counter")
	writeUintSample(output, "worker_dora_native_days_total", nil, collector.doraDays)
	writeMetadata(output, "worker_dora_native_rows_written_total", "Metric rows written by the native Go DORA executor.", "counter")
	writeUintSample(output, "worker_dora_native_rows_written_total", nil, collector.doraRowsWritten)
	writeMetadata(output, "worker_dora_native_skipped_rows_total", "Candidate rows the native DORA executor tolerated and skipped, matching Python's disposition.", "counter")
	writeUintSample(output, "worker_dora_native_skipped_rows_total", nil, collector.doraSkippedRows)
	writeMetadata(output, "worker_dora_native_empty_partitions_total", "Native DORA partitions that completed having written no rows.", "counter")
	writeUintSample(output, "worker_dora_native_empty_partitions_total", nil, collector.doraEmptyOutcomes)

	// Emitted for EVERY reason, including zeros. A series that only appears
	// once it fires cannot be alerted on with a rate() rule until after the
	// first failure, which is the moment the alert was supposed to precede.
	writeMetadata(output, "worker_dora_native_refused_total", "Native DORA executor construction refusals, by reason.", "counter")
	for _, reason := range doraRefusalReasons {
		writeUintSample(output, "worker_dora_native_refused_total",
			[]metricLabel{{"reason", reason}}, collector.doraRefusals[reason])
	}

	writeMetadata(output, "worker_capacity_native_partitions_total", "Partitions computed by the native Go capacity executor.", "counter")
	writeUintSample(output, "worker_capacity_native_partitions_total", nil, collector.capacityPartitions)
	writeMetadata(output, "worker_capacity_native_scopes_total", "Team/work-scope pairs the native capacity executor considered.", "counter")
	writeUintSample(output, "worker_capacity_native_scopes_total", nil, collector.capacityScopes)
	writeMetadata(output, "worker_capacity_native_rows_written_total", "Forecast rows written by the native Go capacity executor.", "counter")
	writeUintSample(output, "worker_capacity_native_rows_written_total", nil, collector.capacityRowsWritten)
	writeMetadata(output, "worker_capacity_native_skipped_scopes_total", "Scopes the native capacity executor tolerated and skipped, matching Python's disposition.", "counter")
	writeUintSample(output, "worker_capacity_native_skipped_scopes_total", nil, collector.capacitySkippedScopes)
	writeMetadata(output, "worker_capacity_native_refused_total", "Native capacity executor construction refusals, by reason.", "counter")
	for _, reason := range capacityRefusalReasons {
		writeUintSample(output, "worker_capacity_native_refused_total",
			[]metricLabel{{"reason", reason}}, collector.capacityRefusals[reason])
	}

	writeMetadata(output, "worker_dispatch_budget_estimate_failures_total", "dispatch_sync_run BudgetGuard estimate-bridge fetches that fell open, by reason.", "counter")
	for _, reason := range budgetEstimateFailureReasons {
		writeUintSample(output, "worker_dispatch_budget_estimate_failures_total",
			[]metricLabel{{"reason", reason}}, collector.budgetEstimateFailures[reason])
	}

	// Compatibility-bridge rows written / zero-row completions, by family
	// (CHAOS-4243). Emitted for every family in the closed set, including
	// zeros, so the series exists before it ever needs to move.
	writeMetadata(output, "worker_remaining_bridge_rows_written_total", "Rows the Python compatibility bridge reported writing for one remaining-metrics family, where the family's evidence carries a count.", "counter")
	for _, family := range compatibilityBridgeFamilies {
		writeUintSample(output, "worker_remaining_bridge_rows_written_total",
			[]metricLabel{{"family", family}}, collector.compatibilityRowsWritten[family])
	}
	writeMetadata(output, "worker_remaining_bridge_zero_row_partitions_total", "Compatibility-bridge partitions that reported success while writing zero rows.", "counter")
	for _, family := range compatibilityBridgeFamilies {
		writeUintSample(output, "worker_remaining_bridge_zero_row_partitions_total",
			[]metricLabel{{"family", family}}, collector.compatibilityZeroRowPartitions[family])
	}
}

// writeReportDedupGuard exposes CHAOS-4140's report dedup-guard counters
// (ObserveReportDedupGuard). Unlike the DORA/capacity refusal reasons above,
// the (table, reason) key set is not statically known to this package --
// internal/jobs/report owns appendOnlyDailyKeys, not jobruntime -- so only
// keys actually observed are emitted, in sorted order for a deterministic
// snapshot.
func (collector *MetricsCollector) writeReportDedupGuard(output *strings.Builder) {
	if len(collector.reportDedupObservedRows) == 0 {
		return
	}
	keys := make([]string, 0, len(collector.reportDedupObservedRows))
	for key := range collector.reportDedupObservedRows {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	writeMetadata(output, "worker_report_dedup_guard_observed_rows_total", "Physical rows a report chart's dedup guard scanned for its query's key range, by source table and reason.", "counter")
	for _, key := range keys {
		table, reason, _ := strings.Cut(key, "|")
		writeUintSample(output, "worker_report_dedup_guard_observed_rows_total",
			[]metricLabel{{"table", table}, {"reason", reason}}, collector.reportDedupObservedRows[key])
	}
	writeMetadata(output, "worker_report_dedup_guard_skipped_rows_total", "Rows a report chart's dedup guard discarded as a stale compute generation (a retried partition's earlier computed_at), by source table and reason.", "counter")
	for _, key := range keys {
		table, reason, _ := strings.Cut(key, "|")
		writeUintSample(output, "worker_report_dedup_guard_skipped_rows_total",
			[]metricLabel{{"table", table}, {"reason", reason}}, collector.reportDedupSkippedRows[key])
	}
}

func (collector *MetricsCollector) writeBudgets(output *strings.Builder) {
	writeMetadata(output, "worker_budget_wait_seconds", "Time spent waiting for a provider cost budget.", "histogram")
	for _, labels := range sortedBudgets(collector.allowedBudgets) {
		writeHistogram(output, "worker_budget_wait_seconds", []metricLabel{
			{"provider", labels.Provider}, {"cost_class", labels.CostClass},
		}, collector.budgetWait[labels])
	}
}

func (collector *MetricsCollector) writeConcurrencyBudgets(output *strings.Builder) {
	labels := make([]ConcurrencyBudgetLabels, 0, len(collector.allowedConcurrencyBudgets))
	for value := range collector.allowedConcurrencyBudgets {
		labels = append(labels, value)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Kind == labels[j].Kind {
			return labels[i].Scope < labels[j].Scope
		}
		return labels[i].Kind < labels[j].Kind
	})
	writeMetadata(output, "worker_concurrency_budget_capacity", "Configured durable concurrency budget capacity.", "gauge")
	for _, labels := range labels {
		values := []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}
		writeIntSample(output, "worker_concurrency_budget_capacity", values, collector.concurrencyBudgetCapacity[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_leased", "Currently leased durable concurrency capacity.", "gauge")
	for _, labels := range labels {
		values := []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}
		writeIntSample(output, "worker_concurrency_budget_leased", values, collector.concurrencyBudgetLeased[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_wait_seconds", "Time spent waiting for durable concurrency capacity.", "histogram")
	for _, labels := range labels {
		writeHistogram(output, "worker_concurrency_budget_wait_seconds", []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}, collector.concurrencyBudgetWait[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_lease_events_total", "Expired and recovered durable concurrency leases.", "counter")
	for _, labels := range labels {
		for _, result := range []string{"expired", "recovered"} {
			writeUintSample(output, "worker_concurrency_budget_lease_events_total", []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}, {"result", result}}, collector.concurrencyBudgetEvents[concurrencyBudgetResultLabels{Budget: labels, Result: result}])
		}
	}
}

func (collector *MetricsCollector) writePools(output *strings.Builder) {
	writeMetadata(output, "worker_database_pool_saturation_ratio", "Fraction of configured database pool capacity currently acquired.", "gauge")
	for _, pool := range []string{poolDomain, poolQueueControl} {
		writeFloatSample(output, "worker_database_pool_saturation_ratio", []metricLabel{{"pool", pool}}, collector.poolSaturation[pool])
	}
	writeMetadata(output, "worker_database_pool_acquire_seconds", "Database pool acquisition latency by bounded result.", "histogram")
	for _, pool := range []string{poolDomain, poolQueueControl} {
		for _, result := range poolAcquireResults() {
			labels := poolAcquireLabels{Pool: pool, Result: result}
			writeHistogram(output, "worker_database_pool_acquire_seconds", []metricLabel{
				{"pool", pool}, {"result", result},
			}, collector.poolAcquire[labels])
		}
	}
}

type metricLabel struct {
	name  string
	value string
}

func writeMetadata(output *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func writeIntSample(output *strings.Builder, name string, labels []metricLabel, value int64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(strconv.FormatInt(value, 10))
	output.WriteByte('\n')
}

func writeUintSample(output *strings.Builder, name string, labels []metricLabel, value uint64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(strconv.FormatUint(value, 10))
	output.WriteByte('\n')
}

func writeFloatSample(output *strings.Builder, name string, labels []metricLabel, value float64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(formatMetricFloat(value))
	output.WriteByte('\n')
}

func writeHistogram(output *strings.Builder, name string, labels []metricLabel, metric *histogram) {
	cumulative := uint64(0)
	for index, bound := range metric.bounds {
		cumulative += metric.buckets[index]
		bucketLabels := append(append([]metricLabel(nil), labels...), metricLabel{"le", formatMetricFloat(bound)})
		writeUintSample(output, name+"_bucket", bucketLabels, cumulative)
	}
	cumulative += metric.buckets[len(metric.bounds)]
	infLabels := append(append([]metricLabel(nil), labels...), metricLabel{"le", "+Inf"})
	writeUintSample(output, name+"_bucket", infLabels, cumulative)
	writeFloatSample(output, name+"_sum", labels, metric.sum)
	writeUintSample(output, name+"_count", labels, metric.count)
}

func writeSamplePrefix(output *strings.Builder, name string, labels []metricLabel) {
	output.WriteString(name)
	if len(labels) > 0 {
		output.WriteByte('{')
		for index, label := range labels {
			if index > 0 {
				output.WriteByte(',')
			}
			output.WriteString(label.name)
			output.WriteString("=\"")
			output.WriteString(escapeMetricLabel(label.value))
			output.WriteByte('"')
		}
		output.WriteByte('}')
	}
	output.WriteByte(' ')
}

func escapeMetricLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func formatMetricFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func jobMetricLabels(labels JobLabels) []metricLabel {
	return []metricLabel{{"queue", labels.Queue}, {"kind", labels.Kind}}
}

func queueMetricLabels(labels queueLabels) []metricLabel {
	return []metricLabel{{"queue", labels.Queue}}
}

func streamMetricLabels(labels StreamLabels) []metricLabel {
	return []metricLabel{{"stream", labels.Stream}, {"consumer_group", labels.ConsumerGroup}}
}

func validateJobLabels(labels JobLabels) error {
	if !metricIdentifier(labels.Queue, 96) || !metricIdentifier(labels.Kind, 96) {
		return errors.New("invalid metric job dimensions")
	}
	return nil
}

func metricIdentifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '.' && character != '_' && character != '-' && character != ':' {
			return false
		}
	}
	return true
}

func validResult(result Result) bool {
	return result == ResultSuccess || result == ResultDuplicate || result == ResultRetry || result == ResultDiscard || result == ResultCancel
}

func validOutcome(result Result, category ErrorCategory) bool {
	if !validResult(result) || !validErrorCategory(category) {
		return false
	}
	if result == ResultSuccess || result == ResultDuplicate {
		return category == CategoryNone
	}
	return category != CategoryNone
}

func validErrorCategory(category ErrorCategory) bool {
	switch category {
	case CategoryNone, CategoryValidation, CategoryPanic, CategoryTimeout, CategoryCancelled,
		CategoryRetryable, CategoryPermanent, CategoryTerminalDomain, CategoryTenant,
		CategoryBudget, CategoryRateLimited, CategoryIdempotency:
		return true
	default:
		return false
	}
}

func validSyncLeaseResult(result SyncLeaseResult) bool {
	return result == SyncLeaseResultRetrying || result == SyncLeaseResultFailed
}

func syncLeaseResults() []SyncLeaseResult {
	return []SyncLeaseResult{SyncLeaseResultFailed, SyncLeaseResultRetrying}
}

func validReportRunLeaseResult(result ReportRunLeaseResult) bool {
	return result == ReportRunLeaseResultRetrying || result == ReportRunLeaseResultFailed
}

// dailyMetricsLeaseSeries is the closed cross product of stages and results.
// Every series is pre-seeded so a scrape distinguishes "no stalls" from "the
// worker never reached this code".
func dailyMetricsLeaseSeries() []dailyMetricsLeaseLabels {
	series := make([]dailyMetricsLeaseLabels, 0, 6)
	for _, stage := range []DailyMetricsLeaseStage{DailyMetricsLeaseStageFinalize, DailyMetricsLeaseStagePartition} {
		for _, result := range []DailyMetricsLeaseResult{
			DailyMetricsLeaseResultReclaimed, DailyMetricsLeaseResultReleaseLost, DailyMetricsLeaseResultSnoozed,
		} {
			series = append(series, dailyMetricsLeaseLabels{Stage: stage, Result: result})
		}
	}
	return series
}

// dailyMetricsDiscoverySeries is the closed cross product of triggers and
// outcomes. Every series is pre-seeded so a scrape distinguishes "materialized
// non-empty every time" from "discovery never runs for this trigger".
func dailyMetricsDiscoverySeries() []dailyMetricsDiscoveryLabels {
	series := make([]dailyMetricsDiscoveryLabels, 0, 6)
	for _, trigger := range []DailyMetricsRunTrigger{
		DailyMetricsRunTriggerScheduledFanout, DailyMetricsRunTriggerPostSync,
	} {
		for _, outcome := range []DailyMetricsDiscoveryOutcome{
			DailyMetricsDiscoveryOutcomeMaterialized, DailyMetricsDiscoveryOutcomeNoRepositories,
			DailyMetricsDiscoveryOutcomeRepositoryCapExceeded,
		} {
			series = append(series, dailyMetricsDiscoveryLabels{Trigger: trigger, Outcome: outcome})
		}
	}
	return series
}

func reportRunLeaseResults() []ReportRunLeaseResult {
	return []ReportRunLeaseResult{ReportRunLeaseResultFailed, ReportRunLeaseResultRetrying}
}

func validIdempotencyRenewalRetiredReason(reason IdempotencyRenewalRetiredReason) bool {
	return reason == IdempotencyRenewalFenced || reason == IdempotencyRenewalTransientExhausted
}

func idempotencyRenewalRetiredReasons() []IdempotencyRenewalRetiredReason {
	return []IdempotencyRenewalRetiredReason{
		IdempotencyRenewalFenced, IdempotencyRenewalTransientExhausted,
	}
}

func poolAcquireResults() []string {
	return []string{poolResultAcquired, poolResultCancelled, poolResultError, poolResultTimeout}
}

func sortedJobs(values map[JobLabels]struct{}) []JobLabels {
	result := make([]JobLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return compareJobs(result[left], result[right]) < 0 })
	return result
}

func compareJobs(left, right JobLabels) int {
	if left.Queue != right.Queue {
		return strings.Compare(left.Queue, right.Queue)
	}
	return strings.Compare(left.Kind, right.Kind)
}

func sortedQueues(values map[queueLabels]struct{}) []queueLabels {
	result := make([]queueLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Queue < result[right].Queue
	})
	return result
}

func sortedStreams(values map[StreamLabels]struct{}) []StreamLabels {
	result := make([]StreamLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Stream != result[right].Stream {
			return result[left].Stream < result[right].Stream
		}
		return result[left].ConsumerGroup < result[right].ConsumerGroup
	})
	return result
}

func sortedSyncLeases(values map[SyncLeaseLabels]struct{}) []SyncLeaseLabels {
	result := make([]SyncLeaseLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Provider != result[right].Provider {
			return result[left].Provider < result[right].Provider
		}
		return result[left].DatasetFamily < result[right].DatasetFamily
	})
	return result
}

func sortedBudgets(values map[BudgetLabels]struct{}) []BudgetLabels {
	result := make([]BudgetLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Provider != result[right].Provider {
			return result[left].Provider < result[right].Provider
		}
		return result[left].CostClass < result[right].CostClass
	})
	return result
}

func sortedStrings(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
