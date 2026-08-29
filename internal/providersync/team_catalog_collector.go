package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TeamCatalogSelections mirrors CHAOS-4323's three independent org-level
// reference-catalog toggles (sync_configurations.sync_options:
// auto_import_teams / auto_import_projects / auto_import_members). A false
// selection means that surface must not be collected or written for this
// provider, by any collector -- native or Python bridge.
type TeamCatalogSelections struct {
	Teams    bool
	Projects bool
	Members  bool
}

// Any reports whether at least one surface is selected. A caller with no
// selections enabled must skip this provider entirely (native collector and
// bridge alike) rather than calling a collector that would write nothing.
func (selections TeamCatalogSelections) Any() bool {
	return selections.Teams || selections.Projects || selections.Members
}

// TeamCatalogReference is the claim-free identifier set a native team/member/
// project-catalog collector needs (CHAOS-4431 ruling, team-lead 2026-08-28,
// option (c)). Team/project/member reference discovery runs once per sync
// run per provider -- it is never a claimed provider-unit, so there is
// deliberately no Claim and no lease here.
type TeamCatalogReference struct {
	OrgID         string
	SyncRunID     string
	IntegrationID string
	SourceID      string
	// SyncOptions is the run's own canonical sync_configurations.sync_options
	// (team-lead ruling, 2026-08-28): provider-specific config a collector
	// needs beyond the resolved credential -- e.g. GitLab's group_path
	// (credential.Config carries only auth material, never scope). Collectors
	// that need nothing beyond the credential (Linear today) simply ignore
	// it. Never nil when populated by a real resolver; may be nil in tests.
	SyncOptions map[string]any
	// Strict mirrors which Python call shape this collection run is
	// standing in for: true from the reference-discovery seam
	// (run_team_autoimport_strict -- propagates failures, no per-surface
	// selection gate upstream of the collector) and false from the
	// post-sync seam (run_team_autoimport -- the caller, not the collector,
	// degrades a failure to a non-fatal zero result; see
	// cmd/dev-health-worker/team_catalog_clients.go's teamCatalogAutoimportBridge).
	// A collector MAY use it to decide per-item soft-fail vs hard-fail
	// internally; none do yet (CHAOS-4431's Linear collector always
	// propagates, which already matches strict semantics, and the
	// non-strict safety net lives at the bridge boundary instead).
	Strict bool
	// SourceExternalIDs is the run's own provider-source external id set
	// (team-lead ruling, 2026-08-28), ported from the SAME
	// sync_run_units-JOIN-integration_sources join
	// reference_discovery.py:281-303 uses to build scope["source_external_ids"]
	// (and fails closed -- "reference discovery source inventory incomplete"
	// -- if any of this run's sync_run_units.source_id has no resolvable
	// integration_sources.external_id). A collector that must scope its walk
	// to the run's selected sources (e.g. GitLab's project catalog) reads
	// this instead of querying sync_run_units itself -- collectors stay
	// DB-free; the resolver carries it. Sorted, deduplicated. Collectors
	// that need nothing beyond the credential (Linear today) ignore it.
	SourceExternalIDs []string
}

func (ref TeamCatalogReference) validate() error {
	if ref.OrgID == "" || ref.SyncRunID == "" {
		return ErrInvalidConfiguration
	}
	return nil
}

// TeamCatalogResult reports rows written per destination table plus an
// outcome label, the shape the telemetry layer (rows_written per table +
// outcome: native|bridge|skipped_selection) reports directly.
type TeamCatalogResult struct {
	TeamsWritten       int
	MembersWritten     int
	MembershipsWritten int
	ProjectsWritten    int
	// OwnershipWritten is team_project_ownership rows (Linear, GitLab).
	OwnershipWritten int
	// ProjectsWithoutKey (CHAOS-4530, "a team key is not a project key")
	// counts, of ProjectsWritten, how many `projects` rows this call wrote
	// with a nil project_key. Zero for a collector that never writes
	// projects.project_key at all. Linear's collector no longer stamps the
	// owning team's key onto a real project's catalog/ownership rows (that
	// was the defect), and it has no genuine per-project key source yet, so
	// every real Linear project it writes currently counts here -- this is
	// the visible, per-run cost of that gap until a real per-project key
	// exists to close it.
	ProjectsWithoutKey int
	// RepoOwnershipWritten is team_repo_ownership rows (GitHub) -- a
	// DIFFERENT destination table from OwnershipWritten's
	// team_project_ownership (team-lead ruling, 2026-08-28: the two must
	// never share one field, or one provider's rows get the other's table
	// label in telemetry). Zero for collectors that don't write this table.
	RepoOwnershipWritten int
	// TeamKeys is the native_team_key set actually written to `teams` this
	// call (empty when Teams was not selected). It exists so a caller can
	// hand it to the existing ClickHouse readback verifier
	// (ReferenceReadbackVerifier/MissingTeamKeys) the same way the Python
	// bridge path's summary["reference_team_keys"] already does -- CHAOS-4431
	// keeps that verification for native providers too, not just bridge ones.
	TeamKeys []string
	// RosterPreservationFailed (team-lead ruling, 2026-08-28) is true when
	// the existing-manual_members pre-read (CHAOS-4446's
	// PreserveExistingTeamManualMembers) failed and the collector chose to
	// CONTINUE the write rather than hard-fail it. No collector sets this
	// today -- hard-failing is the safe default (it is exactly what CHAOS-4446
	// fixed: a quiet empty override is a silent data-loss bug) -- but the
	// field exists so a future collector's deliberate soft-fail of that one
	// step is never indistinguishable from a clean run in telemetry (see
	// jobruntime.TeamCatalogOutcomeRosterPreservationFailed).
	RosterPreservationFailed bool
	// SprintsWritten/SprintIDs are Linear's (and, once 4434/4432 wire their
	// own cycle equivalents, any provider's) unconditional sprint/cycle
	// reference discovery -- never gated on TeamCatalogSelections, written
	// whenever the collector runs at all (CHAOS-4431 codex review P1,
	// team_autoimport_linear.py:575-576: "sprints/cycles ... reference
	// data, not a category"). SprintIDs feeds the same ClickHouse readback
	// claim Python's reference_sprint_ids already fed before this cutover.
	SprintsWritten int
	SprintIDs      []string
	// TeamsSkippedPolicy counts teams this call deliberately left untouched
	// in `teams` because their CHAOS-2622 sync_policy is not the auto-apply
	// default (0) -- the fail-safe guard team-lead ruled for codex review
	// findings #3/#6 (2026-08-28), pending the full drift-aware projector
	// (CHAOS-4444 class). Zero for every org with no flagged-for-review or
	// manual teams, which today is every org -- the guard is additive-safe.
	TeamsSkippedPolicy int
	// MembershipsSkippedManualConflict counts team_memberships rows this call
	// deliberately left unwritten because the (member_id, team_id) pair
	// already has an active manual membership, or the member has an active
	// member-scoped manual_attribution_fallbacks row -- the fail-safe guard
	// team-lead ruled for codex review finding #6 (2026-08-28), pending the
	// full CHAOS-2622/CHAOS-4444 drift-aware projector. Unlike
	// TeamsSkippedPolicy, this gate applies regardless of sync_policy: the
	// membership conflict check is independent of team policy
	// (team-attribution.md:793-797).
	MembershipsSkippedManualConflict int
	// TeamsStagedForReview (CHAOS-4444) counts distinct teams for which this
	// call staged (or refreshed the last_seen_at of) at least one PENDING
	// team_drift_changes row -- the full clickhouse_team_drift_projector.py
	// parity this ticket adds on top of TeamsSkippedPolicy's plain-skip
	// interim guard. A team counted in TeamsSkippedPolicy is not
	// necessarily counted here too: a flagged-for-review team whose
	// observed values exactly match what is already persisted has nothing
	// to stage.
	TeamsStagedForReview int
	// MembershipsStagedForReview (CHAOS-4444) counts membership rows for
	// which this call staged (or refreshed) a PENDING identity-drift
	// team_drift_changes row -- the row-level counterpart to
	// TeamsStagedForReview, mirroring MembershipsSkippedManualConflict's
	// row-level granularity. Always <= MembershipsSkippedManualConflict: a
	// conflicting row whose change is already approved/dismissed is
	// excluded from the write but not re-staged.
	MembershipsStagedForReview int
	// DriftChangesSuperseded (CHAOS-4444) counts team_drift_changes rows
	// (both entity_type='team' field-level changes and
	// entity_type='identity' membership changes) this call transitioned to
	// STATUS_SUPERSEDED because a previously-pending diff for the same
	// field/member was replaced by a newer, different diff this run.
	// Deliberately does not also expose a "resolved" counter -- team-lead
	// ruling 2026-08-28 scoped telemetry to staged/superseded only; RESOLVED
	// transitions still happen (a team back on auto-apply, or a conflict
	// that cleared) but are not separately counted.
	DriftChangesSuperseded int
	// Skipped (team-lead ruling, 2026-08-28, GitLab's non-strict walk-
	// failure Python-parity fix -- CHAOS-4432) is true when a collector's
	// non-strict run hit a failure severe enough that it made NO writes at
	// all and is reporting a clean, successful zero result rather than an
	// error -- mirroring Python's own _zero_summary(reason=...) pattern.
	// Exists so this is never indistinguishable from "genuinely nothing to
	// do" in telemetry ("zero-row success = defect"): a caller that sees
	// Skipped=true must record jobruntime.TeamCatalogOutcomeSkipped, not
	// TeamCatalogOutcomeNative, even though err is nil. False for every
	// other outcome, including "nothing selected" (that path returns
	// before ever calling the collector, so it never reaches here) and a
	// genuinely empty but successful collection (e.g. an org with zero
	// GitLab groups).
	Skipped bool
	// SkipReason names why (e.g. "group_projects_fetch_failed"), mirroring
	// _zero_summary's own `reason` string. Empty when Skipped is false.
	SkipReason string
}

// TeamCatalogCollector is the shared, provider-neutral seam every native
// team/member/project-catalog collector implements (CHAOS-4431 activates
// Linear as the first; CHAOS-4434/CHAOS-4432 add GitHub/GitLab against this
// same interface). A caller resolves credentials/HTTP client exactly as
// cmd/dev-health-worker/provider_sync.go already does for claimed
// provider-units; this seam itself carries no lease and no Claim.
type TeamCatalogCollector interface {
	CollectTeamCatalog(
		ctx context.Context,
		ref TeamCatalogReference,
		credential providerfoundation.Credential,
		client *providerfoundation.HTTPClient,
		selections TeamCatalogSelections,
		normalizedAt time.Time,
	) (TeamCatalogResult, error)
}
