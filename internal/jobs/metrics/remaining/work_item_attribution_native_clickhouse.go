package remaining

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// WorkItemAttributionOutcome reports what one ComputeOrg call did, for
// telemetry and for the caller to decide whether a run marker is needed.
type WorkItemAttributionOutcome struct {
	OrgWide     bool
	RepoIDs     []string
	ProjectKeys []string
	ItemsSeen   int
	RowsWritten int
	SkippedNoop bool
}

// ComputeOrg re-derives work_item_team_attributions for one org's AFFECTED
// SCOPE and writes it, following the CHAOS-2433 write-then-marker protocol.
//
// # SCOPING RULE (CHAOS-3092 PR-B, team-lead ruling)
//
// Unlike the sync-time deriver (per-provider, re-derives on every item sync
// -- but only within an incremental watermark window, which is exactly the
// gap this backstop exists to close) and unlike the retired Python daily
// sweep (job_daily.py's unconditional compute_work_item_team_attributions,
// which re-derived every work item loaded that day regardless of whether
// anything about it could have changed), this backstop re-derives ONLY the
// scope whose OWNERSHIP changed since the last backstop run:
//
//   - A team_repo_ownership change since that repo's last covered run scopes
//     the rederive to that repo's items.
//   - A team_project_ownership change scopes to that project's items.
//   - An identities/teams (admin membership) change is ALWAYS org-wide:
//     admin membership has no single-repo/project scope to key on, and
//     "team = ownership" means a membership change can retarget ANY item's
//     assignee_membership/author_membership candidate ([[feedback_team_vs_member_attribution]]).
//
// A scoped run also rederives its linked_issue CLOSURE, not just the
// originally detected repo/project set -- an item one inheritable-dependency
// hop away from an affected item may need re-verifying too, in EITHER
// direction (see evaluateClosurePromotion). If that closure exceeds
// workItemAttributionClosurePromotionBound of the org's total item count,
// the run is PROMOTED to fully org-wide instead.
//
// A run that finds nothing changed is a genuine no-op: it writes NOTHING,
// not even a marker -- the EXISTING watermark is still the correct
// high-water mark, and publishing a fresh one would just be a needless
// write with no new coverage behind it.
func (executor *WorkItemAttributionExecutor) ComputeOrg(
	ctx context.Context, orgID string,
) (WorkItemAttributionOutcome, error) {
	if executor == nil || executor.conn == nil {
		return WorkItemAttributionOutcome{}, ErrWorkItemAttributionUnavailable
	}
	if strings.TrimSpace(orgID) == "" {
		return WorkItemAttributionOutcome{}, ErrWorkItemAttributionWriteInvalidState
	}
	now, err := executor.nowOrRefuse()
	if err != nil {
		return WorkItemAttributionOutcome{}, err
	}

	scope, err := executor.detectScope(ctx, orgID, now)
	if err != nil {
		return WorkItemAttributionOutcome{}, err
	}
	if scope.orgWide == false && len(scope.repoIDs) == 0 && len(scope.projectKeys) == 0 {
		outcome := WorkItemAttributionOutcome{SkippedNoop: true}
		executor.observeRun(orgID, outcome)
		return outcome, nil
	}

	subjects, err := executor.loadAffectedSubjects(ctx, orgID, scope)
	if err != nil {
		return WorkItemAttributionOutcome{}, err
	}
	outcome := WorkItemAttributionOutcome{
		OrgWide: scope.orgWide, RepoIDs: scope.repoIDs, ProjectKeys: scope.projectKeys,
		ItemsSeen: len(subjects),
	}
	if len(subjects) == 0 {
		// The scope's ownership changed, but nothing in it has any work
		// items yet -- a real, if unusual, outcome (e.g. a repo/project was
		// just given team ownership before any item synced). Still a
		// completed run: publish the marker so this scope is not
		// re-detected as changed forever.
		if err := executor.publishRunMarkers(ctx, orgID, scope, now); err != nil {
			return outcome, err
		}
		executor.observeRun(orgID, outcome)
		return outcome, nil
	}

	// affectedIDs pins which work items THIS run rederives and WRITES. It
	// starts as exactly the originally detected scope; the closure step
	// below grows it to include the closure (owed a rederive too, per
	// team-lead's ruling) or replaces it wholesale on promotion. What it
	// never gains is a donor pulled in ONLY to feed linked_issue inheritance
	// for OTHER items (the plain donor step further down, unrelated to the
	// closure) -- that kind of donor is never itself written, since writing
	// it would attribute an item nothing in this run's scope or closure
	// ever asked to be re-verified, stamped with a computed_at that makes
	// it look freshly checked when it was not.
	affectedIDs := make(map[string]struct{}, len(subjects))
	for id := range subjects {
		affectedIDs[id] = struct{}{}
	}

	facts, err := executor.loadFacts(ctx, orgID, now)
	if err != nil {
		return outcome, err
	}
	dependencies, err := executor.loadDependencyEdges(ctx, orgID, subjects)
	if err != nil {
		return outcome, err
	}

	// CLOSURE (team-lead's PR-B ruling): a scoped run ALWAYS rederives its
	// linked_issue closure too -- donors of affected items, and items whose
	// donor is affected, one hop each way -- never just the originally
	// detected repo/project set. If that closure is big enough to be
	// "effectively the whole org", the run is instead PROMOTED to fully
	// org-wide rather than writing a scoped marker for a set that size.
	// Only evaluated for a run detectScope did NOT already decide was
	// org-wide: an org-wide run is already a superset of any closure it
	// could compute.
	if !scope.orgWide {
		closureSubjects, promoted, reason, err := executor.evaluateClosurePromotion(
			ctx, orgID, affectedIDs, subjects, dependencies)
		if err != nil {
			return outcome, err
		}
		switch {
		case promoted:
			scope = workItemAttributionScopeDecision{orgWide: true, promotedReason: reason}
			subjects, err = executor.loadAffectedSubjects(ctx, orgID, scope)
			if err != nil {
				return outcome, err
			}
			affectedIDs = make(map[string]struct{}, len(subjects))
			for id := range subjects {
				affectedIDs[id] = struct{}{}
			}
			// Re-scoped to the WHOLE org as source: the closure-scoped
			// dependencies above only ever covered the original, smaller
			// affected set.
			dependencies, err = executor.loadDependencyEdges(ctx, orgID, subjects)
			if err != nil {
				return outcome, err
			}
			outcome.OrgWide, outcome.RepoIDs, outcome.ProjectKeys = true, nil, nil
			outcome.ItemsSeen = len(subjects)
		case len(closureSubjects) > 0:
			// Below the promotion bound: the RESOLVED closure (already
			// loaded by evaluateClosurePromotion, to size the promotion
			// decision correctly -- see its doc comment) is still owed a
			// rederive, so it's merged into BOTH subjects (for LinkedIssue
			// context) and affectedIDs (so BuildWorkItemAttributionRows
			// actually WRITES it) -- a closure item is no longer treated
			// the same as a donor pulled in only for context (see the
			// affectedIDs doc comment above, which predates this ruling).
			for id, subject := range closureSubjects {
				subjects[id] = subject
				affectedIDs[id] = struct{}{}
			}
			// Reload dependencies scoped to the WIDENED subjects: a
			// reverse-hop closure item (e.g. B, found because B --relates_to-->
			// A and A is affected) has its OWN outgoing edge (B->A), which the
			// FIRST loadDependencyEdges call above never saw -- that call was
			// scoped to the ORIGINAL affected set as source, before B was
			// known. Without this reload, BuildLinkedIssueIndex below never
			// sees B->A, so B resolves unassigned instead of inheriting A's
			// team via linked_issue -- codex round 2's P1 finding.
			dependencies, err = executor.loadDependencyEdges(ctx, orgID, subjects)
			if err != nil {
				return outcome, err
			}
			outcome.ItemsSeen = len(affectedIDs)
		}
	}

	donorIDs, donorKeys := workItemAttributionDonorTargets(dependencies, subjects)
	donors, err := executor.loadDonorSubjects(ctx, orgID, donorIDs, donorKeys)
	if err != nil {
		return outcome, err
	}
	for id, donor := range donors {
		if _, exists := subjects[id]; !exists {
			subjects[id] = donor
		}
	}

	derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
	linkedIssue, _, _ := derived.BuildLinkedIssueIndex("", subjects, dependencies, nil)
	derived.LinkedIssue = linkedIssue

	// CHAOS-5078 codex r1 F2 fix: exclude any item the native daily
	// `work_item_attribution` family has ALREADY written TODAY. Without
	// this, an ownership change detected the same day daily already ran
	// would make this backstop write a SECOND, newer row for the same
	// (org, repo, work_item) -- work_item_team_attributions'
	// ReplacingMergeTree(computed_at) does not collapse the two (source/
	// team_id differ, so the ORDER BY key differs), so both stay resident
	// and LoadWorkItemPrimaryTeamAttributions' (work_item_id,
	// max(computed_at)) fence picks whichever wrote LAST -- not necessarily
	// the row correct for the day being read. Deferring is safe: the
	// ownership change is picked up on THIS backstop's next run (the
	// watermark this run advances still reflects that the change was
	// observed, just not written today), or overwritten by tomorrow's daily
	// run regardless.
	coveredToday, err := executor.alreadyCoveredToday(ctx, orgID, now, affectedIDs)
	if err != nil {
		return outcome, err
	}
	for id := range coveredToday {
		delete(affectedIDs, id)
	}

	rows := BuildWorkItemAttributionRows(orgID, now, affectedIDs, subjects, derived)

	// Codex confirmation-pass P1 (#2276, found on re-review after the 7th-site
	// fix landed): WriteAttributions' own batch.Send() branch already
	// reports its TRUE row count on an ambiguous network error -- assigning
	// outcome.RowsWritten only on the success path discarded that count a
	// second time on a failure. Assign it BEFORE the error check, same
	// idiom as every other fixed site in this PR.
	written, err := executor.writer.WriteAttributions(ctx, rows)
	outcome.RowsWritten = written
	if err != nil {
		return outcome, err
	}
	if err := executor.publishRunMarkers(ctx, orgID, scope, now); err != nil {
		return outcome, err
	}
	executor.observeRun(orgID, outcome)
	return outcome, nil
}

// observeRun reports a completed run to the optional observer. Nil is
// tolerated, same discipline as SetObserver's own doc comment -- called on
// every SUCCESSFUL return path (no-op, empty-scope, and the full write),
// never on an error return, matching membership_backfill's
// ObserveMembershipRun call sites.
func (executor *WorkItemAttributionExecutor) observeRun(orgID string, outcome WorkItemAttributionOutcome) {
	if executor.observer == nil {
		return
	}
	executor.observer.ObserveWorkItemAttributionRun(orgID, outcome)
}

// BuildWorkItemAttributionRows maps resolved candidates onto the
// work_item_team_attributions row shape, for exactly the affected work items
// (never a donor pulled in only for linked_issue inheritance -- see
// ComputeOrg's affectedIDs doc comment). EXPORTED so the CHAOS-3092 PR-B
// differential test (internal/providersync's oracle harness) can drive this
// SAME mapping against a subjects/derivation-context pair built from an
// oracle case, rather than a hand-copied duplicate of ComputeOrg's former
// inline loop -- a divergence between the tested mapping and the one that
// actually writes to ClickHouse would defeat the point of the proof.
//
// affectedIDs is iterated in SORTED order: a map range would make row order
// (and therefore the dedupe/last-wins tie-break in
// workItemAttributionSortingKeyDedupe, and the oracle differential test's
// column-positional comparison) nondeterministic between runs, for no
// benefit -- ClickHouse itself does not care what order a batch's rows
// arrive in.
func BuildWorkItemAttributionRows(
	orgID string,
	computedAt time.Time,
	affectedIDs map[string]struct{},
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
	derived teamattribution.GithubWorkItemDerivationContext,
) []WorkItemAttributionRow {
	ids := make([]string, 0, len(affectedIDs))
	for id := range affectedIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	rows := make([]WorkItemAttributionRow, 0, len(affectedIDs))
	for _, id := range ids {
		subject := subjects[id]
		_, _, candidates := derived.Resolve(subject)
		for _, candidate := range candidates {
			var repoID *uuid.UUID
			if subject.RepoID != nil {
				if parsed, err := uuid.Parse(*subject.RepoID); err == nil {
					repoID = &parsed
				}
			}
			rows = append(rows, WorkItemAttributionRow{
				WorkItemID: subject.WorkItemID,
				Provider:   subject.Provider,
				Source:     candidate.Source,
				IsPrimary:  candidate.IsPrimary,
				Confidence: candidate.Confidence,
				Evidence:   candidate.Evidence,
				ComputedAt: computedAt,
				RepoID:     repoID,
				TeamID:     candidate.TeamID,
				TeamName:   candidate.TeamName,
				OrgID:      orgID,
			})
		}
	}
	return rows
}

// workItemAttributionScopeDecision is the affected set detectScope computes:
// EITHER org-wide (identities/teams changed -- team = ownership, admin
// membership has no single-repo/project scope to key on) OR a set of
// repo/project scopes, never both (an org-wide run is a strict superset).
//
// KNOWN LIMITATION, not yet addressed: repo scoping keys ONLY on
// team_repo_ownership.repo_id (a non-null UUID); a repo-name-only ownership
// row (repo_id NULL, repo_full_name set) is invisible to this detector.
// (Project scoping's OWN project_id-only gap is FIXED -- see detectScope's
// projectIDChanges query -- project scoping now keys on both project_key
// and project_id, matching Resolve()'s own projectByKey/projectByID
// matching exactly.) The repo-name case is deliberately NOT fixed the same
// way: Resolve()'s repo-by-name match (derived.repoByName) keys off
// subject.ProjectID, the SAME work_items column project-ownership-by-id
// matching also reads -- that field is overloaded between "this item's
// project id" and "this item's repo's full name" depending on context this
// package does not have enough information to disambiguate from the
// ownership-table side alone. Detecting a repo_full_name ownership change
// by comparing it against subject.ProjectID would risk firing (or
// silently NOT firing) on the wrong items, which is worse than the
// current honest gap. Left for a fast-follow with the fuller context to
// resolve the overload correctly, not attempted here.
type workItemAttributionScopeDecision struct {
	orgWide     bool
	repoIDs     []string
	projectKeys []string
	// promotedReason is set ONLY when a scoped run's linked_issue closure
	// exceeded workItemAttributionClosurePromotionBound and orgWide was
	// widened to true as a result (team-lead's PR-B ruling) -- never set
	// for a run that was org-wide from detectScope's own decision (an
	// identities/teams change). Recorded on the org-wide run marker so a
	// reader can tell the two apart.
	promotedReason string
}

// workItemAttributionEffectiveChangeSignal is the PURE, unit-testable
// reference for the SQL `greatest(updated_at, if(valid_from <= asOf, ...),
// ifNull(if(valid_to <= asOf, ...), ...))` expression detectScope's
// repo/project scope-change queries evaluate server-side (see their query
// text). It exists to let team-lead's "unit/fake-client tests now" ruling
// cover the LOGIC before the bigboy container pause lifts and the
// SQL-executing integration test
// (TestDetectScopeCatchesFutureOwnershipActivation) can actually run --
// this function is not itself called by production code, and a
// discrepancy between it and the live SQL is exactly what that deferred
// integration test is for.
//
// An ownership row's effective "this changed" instant is the LATEST of:
// updated_at (an edit right now); valid_from, but ONLY once it has already
// taken effect (asOf >= valid_from) -- a row inserted today with a FUTURE
// valid_from contributes nothing until the day it actually activates,
// since nothing else writes to the row at that moment; valid_to, but ONLY
// once it has already passed (asOf >= valid_to) -- an expired row needs a
// rederive too, even though it no longer counts as "currently owning"
// anything.
func workItemAttributionEffectiveChangeSignal(updatedAt, validFrom time.Time, validTo *time.Time, asOf time.Time) time.Time {
	signal := updatedAt
	if !validFrom.After(asOf) && validFrom.After(signal) {
		signal = validFrom
	}
	if validTo != nil && !validTo.After(asOf) && validTo.After(signal) {
		signal = *validTo
	}
	return signal
}

// detectScope reads the org-wide and scoped watermarks, compares them
// against ownership-table activity, and returns the affected set. See
// ComputeOrg's doc comment for the scoping rule itself.
func (executor *WorkItemAttributionExecutor) detectScope(
	ctx context.Context, orgID string, now time.Time,
) (workItemAttributionScopeDecision, error) {
	orgWatermark, err := executor.orgWatermark(ctx, orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	repoWatermarks, projectWatermarks, err := executor.scopedWatermarks(ctx, orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}

	// Org-wide triggers: identities/teams is the ADMIN membership layer
	// (CHAOS-4321); team_memberships is the PROVIDER FALLBACK layer
	// loadFacts also consumes (LoadProviderMembers) but detectScope
	// originally never watched; manual_attribution_fallbacks is the
	// override layer loadFacts consumes via LoadManualFallbacks. All three
	// share identities/teams' own reasoning for going org-wide rather than
	// scoped: a membership/fallback change can retarget an
	// assignee_membership/author_membership candidate for ANY item that
	// person (or scope) touches, with no single-repo/project scope to key
	// on the way ownership-table changes have.
	identitiesChanged, err := executor.maxUpdatedAt(ctx,
		"SELECT max(updated_at) FROM identities FINAL WHERE org_id = ?", orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	teamsChanged, err := executor.maxUpdatedAt(ctx,
		"SELECT max(updated_at) FROM teams FINAL WHERE org_id = ?", orgID)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	// team_memberships and manual_attribution_fallbacks are ALSO bitemporal
	// (valid_from/valid_to), the same as team_repo_ownership/
	// team_project_ownership -- LoadProviderMembers/LoadManualFallbacks
	// filter by validity the same way LoadRepos/LoadProjects do (see
	// teamattribution/cascade.go). A plain max(updated_at) here would leave
	// the SAME future-activation/expiry gap the repo/project queries below
	// were widened to close -- codex round 2's P1 finding on this exact
	// asymmetry. identities/teams do NOT get the same treatment: they are
	// admin-authored catalogs with no valid_from/valid_to columns at all.
	membershipsChanged, err := executor.maxEffectiveChangedAt(ctx, now, orgID, `
SELECT max(greatest(
    updated_at,
    if(valid_from <= ?, valid_from, toDateTime64(0, 3)),
    ifNull(if(valid_to <= ?, valid_to, toDateTime64(0, 3)), toDateTime64(0, 3))
  ))
FROM team_memberships FINAL
WHERE org_id = ?`)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	manualFallbacksChanged, err := executor.maxEffectiveChangedAt(ctx, now, orgID, `
SELECT max(greatest(
    updated_at,
    if(valid_from <= ?, valid_from, toDateTime64(0, 3)),
    ifNull(if(valid_to <= ?, valid_to, toDateTime64(0, 3)), toDateTime64(0, 3))
  ))
FROM manual_attribution_fallbacks FINAL
WHERE org_id = ?`)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	if identitiesChanged.After(orgWatermark) || teamsChanged.After(orgWatermark) ||
		membershipsChanged.After(orgWatermark) || manualFallbacksChanged.After(orgWatermark) {
		return workItemAttributionScopeDecision{orgWide: true}, nil
	}

	// The changedAt signal is the MAX of three things, not just updated_at:
	// updated_at (an insert/edit right now), valid_from IF it has already
	// taken effect (asOf >= valid_from -- an ownership row inserted TODAY
	// with a FUTURE valid_from stays invisible until the day it actually
	// activates, since nothing else writes to it at that moment), and
	// valid_to IF it has already passed (an ownership row that just
	// EXPIRED needs a rederive too, even though LoadRepos' own asOf filter
	// now excludes it -- the fact that changed is "this row no longer
	// applies", not any write to the row). Both guards compare against the
	// SAME asOf detectScope was called with, matching LoadRepos'/
	// LoadProjects' own effective-row filter exactly, so a row this query
	// says "changed as of asOf" is a row whose EFFECT actually changed as
	// of asOf, not just its storage.
	repoChanges, err := executor.scopeChanges(ctx, orgID, now, `
SELECT toString(repo_id), max(greatest(
    updated_at,
    if(valid_from <= ?, valid_from, toDateTime64(0, 3)),
    ifNull(if(valid_to <= ?, valid_to, toDateTime64(0, 3)), toDateTime64(0, 3))
  ))
FROM team_repo_ownership
WHERE org_id = ? AND repo_id IS NOT NULL
GROUP BY repo_id`)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	// Two independent queries, not one OR'd WHERE clause: project ownership
	// is keyed by BOTH project_key and project_id (Resolve() matches
	// either, see the KNOWN LIMITATION doc below), and a row can carry one
	// without the other -- project_key is Nullable, project_id is not.
	// Both feed the SAME projectKeys scope-id space; see
	// loadAffectedSubjects' matching comment for why that is safe.
	projectKeyChanges, err := executor.scopeChanges(ctx, orgID, now, `
SELECT project_key, max(greatest(
    updated_at,
    if(valid_from <= ?, valid_from, toDateTime64(0, 3)),
    ifNull(if(valid_to <= ?, valid_to, toDateTime64(0, 3)), toDateTime64(0, 3))
  ))
FROM team_project_ownership
WHERE org_id = ? AND project_key != ''
GROUP BY project_key`)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}
	projectIDChanges, err := executor.scopeChanges(ctx, orgID, now, `
SELECT project_id, max(greatest(
    updated_at,
    if(valid_from <= ?, valid_from, toDateTime64(0, 3)),
    ifNull(if(valid_to <= ?, valid_to, toDateTime64(0, 3)), toDateTime64(0, 3))
  ))
FROM team_project_ownership
WHERE org_id = ? AND project_id != ''
GROUP BY project_id`)
	if err != nil {
		return workItemAttributionScopeDecision{}, err
	}

	var decision workItemAttributionScopeDecision
	for scopeID, changedAt := range repoChanges {
		floor := orgWatermark
		if watermark, ok := repoWatermarks[scopeID]; ok && watermark.After(floor) {
			floor = watermark
		}
		if changedAt.After(floor) {
			decision.repoIDs = append(decision.repoIDs, scopeID)
		}
	}
	projectSeen := map[string]bool{}
	for _, changes := range []map[string]time.Time{projectKeyChanges, projectIDChanges} {
		for scopeID, changedAt := range changes {
			if projectSeen[scopeID] {
				continue
			}
			floor := orgWatermark
			if watermark, ok := projectWatermarks[scopeID]; ok && watermark.After(floor) {
				floor = watermark
			}
			if changedAt.After(floor) {
				decision.projectKeys = append(decision.projectKeys, scopeID)
				projectSeen[scopeID] = true
			}
		}
	}
	return decision, nil
}

// orgWatermark is the org's org-wide "attribution guaranteed correct as of"
// timestamp -- the zero time if no org-wide run has ever completed.
func (executor *WorkItemAttributionExecutor) orgWatermark(
	ctx context.Context, orgID string,
) (time.Time, error) {
	return executor.maxUpdatedAt(ctx,
		"SELECT max(completed_at) FROM work_item_attribution_backstop_runs WHERE org_id = ?", orgID)
}

// scopedWatermarks reads every repo- and project-scoped watermark for the
// org, keyed by scope_id.
func (executor *WorkItemAttributionExecutor) scopedWatermarks(
	ctx context.Context, orgID string,
) (repos map[string]time.Time, projects map[string]time.Time, err error) {
	rows, err := executor.conn.Query(ctx, `
SELECT scope_kind, scope_id, max(completed_at)
FROM work_item_attribution_backstop_scoped_runs
WHERE org_id = ?
GROUP BY scope_kind, scope_id`, orgID)
	if err != nil {
		return nil, nil, fmt.Errorf("query scoped watermarks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	repos = map[string]time.Time{}
	projects = map[string]time.Time{}
	for rows.Next() {
		var kind, id string
		var completedAt time.Time
		if err := rows.Scan(&kind, &id, &completedAt); err != nil {
			return nil, nil, fmt.Errorf("scan scoped watermark: %w", err)
		}
		switch kind {
		case "repo":
			repos[id] = completedAt
		case "project":
			projects[id] = completedAt
		}
	}
	return repos, projects, rows.Err()
}

// scopeChanges runs a `SELECT scope_id, max(updated_at) ... GROUP BY
// scope_id` query and returns the result as a map.
func (executor *WorkItemAttributionExecutor) scopeChanges(
	ctx context.Context, orgID string, asOf time.Time, query string,
) (map[string]time.Time, error) {
	rows, err := executor.conn.Query(ctx, query, asOf, asOf, orgID)
	if err != nil {
		return nil, fmt.Errorf("query scope changes: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string]time.Time{}
	for rows.Next() {
		var id string
		var changedAt time.Time
		if err := rows.Scan(&id, &changedAt); err != nil {
			return nil, fmt.Errorf("scan scope change: %w", err)
		}
		result[id] = changedAt
	}
	return result, rows.Err()
}

// maxUpdatedAt runs a single-row `SELECT max(...)` query, treating a
// ClickHouse NULL (nothing matched) as the zero time -- "nothing has ever
// happened here" is not an error.
func (executor *WorkItemAttributionExecutor) maxUpdatedAt(
	ctx context.Context, query string, orgID string,
) (time.Time, error) {
	rows, err := executor.conn.Query(ctx, query, orgID)
	if err != nil {
		return time.Time{}, fmt.Errorf("query max updated_at: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return time.Time{}, rows.Err()
	}
	var value *time.Time
	if err := rows.Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("scan max updated_at: %w", err)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	if value == nil {
		return time.Time{}, nil
	}
	return *value, nil
}

// maxEffectiveChangedAt is maxUpdatedAt's bitemporal sibling: query must be
// a single-row `SELECT max(greatest(updated_at, if(valid_from <= ?, ...),
// ifNull(if(valid_to <= ?, ...))))` expression (two `?` placeholders for
// asOf, matching workItemAttributionEffectiveChangeSignal's logic, then the
// org_id `?`) -- used for a table whose facts are ALSO filtered by validity
// window downstream (team_memberships, manual_attribution_fallbacks), so a
// row whose effect changes without a fresh updated_at write (activation,
// expiry) still registers here, the same way scopeChanges' repo/project
// queries do.
func (executor *WorkItemAttributionExecutor) maxEffectiveChangedAt(
	ctx context.Context, asOf time.Time, orgID string, query string,
) (time.Time, error) {
	rows, err := executor.conn.Query(ctx, query, asOf, asOf, orgID)
	if err != nil {
		return time.Time{}, fmt.Errorf("query max effective changed_at: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return time.Time{}, rows.Err()
	}
	var value *time.Time
	if err := rows.Scan(&value); err != nil {
		return time.Time{}, fmt.Errorf("scan max effective changed_at: %w", err)
	}
	if err := rows.Err(); err != nil {
		return time.Time{}, err
	}
	if value == nil {
		return time.Time{}, nil
	}
	return *value, nil
}

// loadAffectedSubjects loads every work item in the affected scope,
// org-wide or repo/project-scoped, keyed by work_item_id.
func (executor *WorkItemAttributionExecutor) loadAffectedSubjects(
	ctx context.Context, orgID string, scope workItemAttributionScopeDecision,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	query := `SELECT ` + WorkItemDerivationSubjectColumns + `
FROM work_items FINAL
WHERE org_id = ?`
	args := []any{orgID}
	if !scope.orgWide {
		if len(scope.repoIDs) == 0 && len(scope.projectKeys) == 0 {
			return map[string]teamattribution.GithubWorkItemDerivationSubject{}, nil
		}
		// scope.projectKeys carries BOTH project_key-triggered and
		// project_id-triggered scope ids in one list (detectScope's doc
		// comment on the project_id fix explains why) -- matched against
		// BOTH columns here for the same reason: a subject is affected if
		// EITHER its project_key or its project_id names a scope that
		// changed, regardless of which identifier space detectScope used
		// to notice the change.
		query += " AND (has(?, toString(repo_id)) OR has(?, project_key) OR has(?, project_id))"
		args = append(args, scope.repoIDs, scope.projectKeys, scope.projectKeys)
	}
	return executor.querySubjects(ctx, query, args...)
}

// loadDonorSubjects loads work items referenced as linked-issue donor
// TARGETS that fall outside the affected scope -- the same donor-loading
// shape as the sync-time deriver's loadDonors
// (internal/providersync/github_work_items_derivation_context.go), adapted
// to a plain org-wide/multi-provider read since this backstop has no
// per-provider claim to scope by.
func (executor *WorkItemAttributionExecutor) loadDonorSubjects(
	ctx context.Context, orgID string, donorIDs, donorKeys []string,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	return LoadWorkItemDonorSubjects(ctx, executor.conn, orgID, donorIDs, donorKeys)
}

// LoadWorkItemDonorSubjects is the free-function (conn-parameterised) form of
// loadDonorSubjects, exported for the daily `work_item_attribution` family
// (CHAOS-5078 codex r1 F1): that family needs the SAME linked-issue-donor
// loading this backstop does, without constructing a full backstop
// WorkItemAttributionExecutor (which would also pull in watermarks/
// detectScope/closure-promotion machinery the daily family has no use for --
// see internal/jobs/metrics/daily/work_item_attribution_native_executor.go's
// own doc comment on why it borrows only the loaders, not the backstop
// itself).
func LoadWorkItemDonorSubjects(
	ctx context.Context, conn driver.Conn, orgID string, donorIDs, donorKeys []string,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	if len(donorIDs) == 0 && len(donorKeys) == 0 {
		return map[string]teamattribution.GithubWorkItemDerivationSubject{}, nil
	}
	query := `SELECT ` + WorkItemDerivationSubjectColumns + `
FROM work_items FINAL
WHERE org_id = ? AND (
  has(?, work_item_id)
  OR (provider IN ('linear', 'jira') AND has(?, upper(splitByChar(':', work_item_id)[-1])))
)`
	return querySubjectsInto(ctx, conn, query, orgID, donorIDs, donorKeys)
}

// WorkItemDerivationSubjectColumns is the SELECT list every subject query must
// use. It is a constant, not repeated per query, because it is paired
// positionally with the Scan in QueryWorkItemDerivationSubjects -- a column
// added to one spelling of the list and not the others would mis-bind silently
// (repo_id into native_team_key, say), which no type error would catch since
// most of these are strings.
const WorkItemDerivationSubjectColumns = `work_item_id, provider, type, toString(repo_id), native_team_key,
       project_key, project_id, project_name, assignees, reporter, org_id`

// QueryWorkItemDerivationSubjects runs a subject query and scans it into the
// cascade's subject map.
//
// Exported (CHAOS-4283 PR2) so the metrics.daily `work_item_attribution` family
// can supply its OWN predicate -- scoped to one partition's (org, repo, day
// window) rather than this backstop's org-wide/changed-scope shape -- without
// re-writing the column list and its positional Scan. The predicate is the part
// that legitimately differs between the two callers; the column-to-field
// binding is the part that must not.
func QueryWorkItemDerivationSubjects(
	ctx context.Context, conn driver.Conn, query string, args ...any,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	return querySubjectsInto(ctx, conn, query, args...)
}

func (executor *WorkItemAttributionExecutor) querySubjects(
	ctx context.Context, query string, args ...any,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	return querySubjectsInto(ctx, executor.conn, query, args...)
}

func querySubjectsInto(
	ctx context.Context, conn driver.Conn, query string, args ...any,
) (map[string]teamattribution.GithubWorkItemDerivationSubject, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query work_items: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string]teamattribution.GithubWorkItemDerivationSubject{}
	for rows.Next() {
		var subject teamattribution.GithubWorkItemDerivationSubject
		var repoID string
		if err := rows.Scan(
			&subject.WorkItemID, &subject.Provider, &subject.Type, &repoID, &subject.NativeTeamKey,
			&subject.ProjectKey, &subject.ProjectID, &subject.ProjectName,
			&subject.Assignees, &subject.Reporter, &subject.OrgID,
		); err != nil {
			return nil, fmt.Errorf("scan work_items row: %w", err)
		}
		if repoID != "" && repoID != uuid.Nil.String() {
			subject.RepoID = &repoID
		}
		result[subject.WorkItemID] = subject
	}
	return result, rows.Err()
}

// loadDependencyEdges loads work_item_dependencies rows whose SOURCE is one
// of the affected subjects, converted to teamattribution's narrow
// dependency-edge shape.
func (executor *WorkItemAttributionExecutor) loadDependencyEdges(
	ctx context.Context, orgID string, subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]teamattribution.GithubWorkItemDerivationDependencyEdge, error) {
	return LoadWorkItemDependencyEdges(ctx, executor.conn, orgID, subjects)
}

// LoadWorkItemDependencyEdges is the free-function (conn-parameterised) form
// of loadDependencyEdges, exported for the daily family's reuse -- see
// LoadWorkItemDonorSubjects' doc comment for why this is a conn-parameterised
// export rather than a full backstop executor.
func LoadWorkItemDependencyEdges(
	ctx context.Context, conn driver.Conn, orgID string, subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]teamattribution.GithubWorkItemDerivationDependencyEdge, error) {
	if len(subjects) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(subjects))
	for id := range subjects {
		ids = append(ids, id)
	}
	rows, err := conn.Query(ctx, `
SELECT source_work_item_id, target_work_item_id, relationship_type, last_synced
FROM work_item_dependencies FINAL
WHERE org_id = ? AND has(?, source_work_item_id)`, orgID, ids)
	if err != nil {
		return nil, fmt.Errorf("query work_item_dependencies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var result []teamattribution.GithubWorkItemDerivationDependencyEdge
	for rows.Next() {
		var edge teamattribution.GithubWorkItemDerivationDependencyEdge
		if err := rows.Scan(&edge.SourceWorkItemID, &edge.TargetWorkItemID, &edge.RelationshipType, &edge.LastSynced); err != nil {
			return nil, fmt.Errorf("scan work_item_dependencies row: %w", err)
		}
		edge.OrgID = orgID
		result = append(result, edge)
	}
	return result, rows.Err()
}

// WorkItemAttributionDonorTargets is workItemAttributionDonorTargets, exported
// for the daily family's reuse (same CHAOS-5078 F1 fix).
func WorkItemAttributionDonorTargets(
	dependencies []teamattribution.GithubWorkItemDerivationDependencyEdge,
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]string, []string) {
	return workItemAttributionDonorTargets(dependencies, subjects)
}

// workItemAttributionDonorTargets mirrors githubWorkItemDerivationDonorTargets
// (internal/providersync/github_work_items_derivation_context.go): the
// dependency targets not already present among the affected subjects,
// split into plain work-item IDs and extkey: cross-provider issue keys.
func workItemAttributionDonorTargets(
	dependencies []teamattribution.GithubWorkItemDerivationDependencyEdge,
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
) ([]string, []string) {
	ids := map[string]struct{}{}
	keys := map[string]struct{}{}
	for _, dependency := range teamattribution.LatestGitHubWorkItemDerivationDependencies(dependencies) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[dependency.RelationshipType] {
			continue
		}
		target := strings.TrimSpace(dependency.TargetWorkItemID)
		if _, exists := subjects[target]; exists {
			continue
		}
		if strings.HasPrefix(target, "extkey:") {
			key := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(target, "extkey:")))
			if key != "" {
				keys[key] = struct{}{}
			}
		} else if target != "" {
			ids[target] = struct{}{}
		}
	}
	idList := make([]string, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}
	keyList := make([]string, 0, len(keys))
	for key := range keys {
		keyList = append(keyList, key)
	}
	return idList, keyList
}

// workItemAttributionClosurePromotionBound is the fraction of an org's
// total item count above which a scoped run's linked_issue closure gets
// PROMOTED to org-wide instead of writing a scoped marker for a set that is
// effectively the whole org anyway (team-lead's PR-B ruling).
const workItemAttributionClosurePromotionBound = 0.25

// evaluateClosurePromotion computes the ONE-HOP linked_issue closure around
// a scoped run's affected set:
//
//   - forward: donors OF affected items -- an affected item's own
//     inheritable-dependency target, read from forwardDependencies (already
//     loaded by ComputeOrg, scoped to the affected set as source).
//   - reverse: items WHOSE DONOR IS AFFECTED -- an item with an
//     inheritable-dependency edge pointing AT an affected item. Its OWN
//     attribution may now be stale too, since what it inherits from just
//     changed team ownership; this direction needs its own query, since
//     forwardDependencies never loaded edges scoped by TARGET.
//
// If the closure (affected ∪ forward ∪ reverse) exceeds
// workItemAttributionClosurePromotionBound of the org's total item count,
// the run is promoted: ComputeOrg treats it as fully org-wide rather than
// rederiving only the closure. Crossing the bound means "effectively the
// whole org", not "a slightly bigger scoped set" -- and deciding whether a
// SECOND hop also needs covering is more expensive than just doing the
// whole org once the first hop already crossed the line.
// evaluateClosurePromotion returns the closure ID set REGARDLESS of the
// promotion decision: a below-bound closure is still owed a rederive
// (team-lead's ruling: "rederives the affected set PLUS its linked_issue
// closure"), it just doesn't widen the whole run to org-wide. Only an
// ABOVE-bound closure promotes -- see ComputeOrg's caller, which merges the
// returned closureIDs into affectedIDs/subjects when NOT promoted, and
// discards them (org-wide already covers everything) when promoted.
//
// forwardDonorIDs/forwardDonorKeys reuse workItemAttributionDonorTargets --
// the SAME extkey-aware resolution the LinkedIssue-donor step already uses
// -- rather than reading forwardDependencies' TargetWorkItemID directly,
// which for a cross-provider linked issue is an unresolved "extkey:..."
// string, not a loadable work_item_id.
func (executor *WorkItemAttributionExecutor) evaluateClosurePromotion(
	ctx context.Context, orgID string,
	affectedIDs map[string]struct{},
	subjects map[string]teamattribution.GithubWorkItemDerivationSubject,
	forwardDependencies []teamattribution.GithubWorkItemDerivationDependencyEdge,
) (closureSubjects map[string]teamattribution.GithubWorkItemDerivationSubject, promoted bool, reason string, err error) {
	forwardDonorIDs, forwardDonorKeys := workItemAttributionDonorTargets(forwardDependencies, subjects)
	reverseSourceIDs, err := executor.loadInheritableDependencySourcesTargeting(ctx, orgID, affectedIDs)
	if err != nil {
		return nil, false, "", err
	}

	closureIDList := make([]string, 0, len(forwardDonorIDs)+len(reverseSourceIDs))
	closureIDList = append(closureIDList, forwardDonorIDs...)
	closureIDList = append(closureIDList, reverseSourceIDs...)

	if len(closureIDList) == 0 && len(forwardDonorKeys) == 0 {
		return nil, false, "", nil
	}

	// Resolve BEFORE sizing: closureIDList/forwardDonorKeys are RAW
	// candidates, not a proven closure -- an extkey with no matching synced
	// item resolves to nothing (a dangling cross-provider reference must
	// not inflate the count), and an ID and an extkey that both resolve to
	// the SAME work item must not be double-counted. loadDonorSubjects'
	// return is a map keyed by resolved work_item_id, so both cases
	// collapse correctly by construction -- sizing on len(closureSubjects)
	// (the actually-resolved set) rather than len(closureIDList)+len(forwardDonorKeys)
	// (the raw candidate count) is what makes that collapse count for
	// anything. Codex round 3's P2 finding: sizing on the raw counts let a
	// single dangling extkey push a 4-item org over the 25% bound with a
	// real closure of zero.
	closureSubjects, err = executor.loadDonorSubjects(ctx, orgID, closureIDList, forwardDonorKeys)
	if err != nil {
		return nil, false, "", err
	}
	closureSize := len(closureSubjects)
	if closureSize == 0 {
		return closureSubjects, false, "", nil
	}

	total, err := executor.orgItemCount(ctx, orgID)
	if err != nil {
		return nil, false, "", err
	}
	promoted, reason = workItemAttributionPromotionDecision(len(affectedIDs), closureSize, total)
	return closureSubjects, promoted, reason, nil
}

// workItemAttributionPromotionDecision is the PURE arithmetic half of
// evaluateClosurePromotion, split out specifically so it is unit-testable
// without a live ClickHouse connection (team-lead's PR-B ruling: unit/fake
// tests now, the live closure-integration proof once the bigboy container
// pause lifts). affected+closureSize crossing workItemAttributionClosurePromotionBound
// of total promotes; an unknown total (<=0, e.g. a construction-time
// refusal never reached this far in practice) never promotes -- there is
// nothing to divide by, and treating unknown as "definitely small" is the
// conservative direction (a scoped run, not an unwarranted org-wide one).
func workItemAttributionPromotionDecision(affected, closureSize, total int) (promoted bool, reason string) {
	if total <= 0 || float64(affected+closureSize)/float64(total) <= workItemAttributionClosurePromotionBound {
		return false, ""
	}
	return true, fmt.Sprintf(
		"linked_issue_closure_exceeded_%.0fpct: affected=%d closure=%d org_total=%d",
		workItemAttributionClosurePromotionBound*100, affected, closureSize, total,
	)
}

// loadInheritableDependencySourcesTargeting loads the SOURCE work item ids
// of every INHERITABLE work_item_dependencies edge whose TARGET is one of
// targetIDs and whose source is not itself already in targetIDs -- the
// reverse half of evaluateClosurePromotion's one-hop closure.
func (executor *WorkItemAttributionExecutor) loadInheritableDependencySourcesTargeting(
	ctx context.Context, orgID string, targetIDs map[string]struct{},
) ([]string, error) {
	if len(targetIDs) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(targetIDs))
	for id := range targetIDs {
		ids = append(ids, id)
	}
	rows, err := executor.conn.Query(ctx, `
SELECT source_work_item_id, target_work_item_id, relationship_type, last_synced
FROM work_item_dependencies FINAL
WHERE org_id = ? AND has(?, target_work_item_id)`, orgID, ids)
	if err != nil {
		return nil, fmt.Errorf("query work_item_dependencies (reverse closure): %w", err)
	}
	defer func() { _ = rows.Close() }()
	var edges []teamattribution.GithubWorkItemDerivationDependencyEdge
	for rows.Next() {
		var edge teamattribution.GithubWorkItemDerivationDependencyEdge
		if err := rows.Scan(
			&edge.SourceWorkItemID, &edge.TargetWorkItemID, &edge.RelationshipType, &edge.LastSynced,
		); err != nil {
			return nil, fmt.Errorf("scan work_item_dependencies row (reverse closure): %w", err)
		}
		edge.OrgID = orgID
		edges = append(edges, edge)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	seen := map[string]struct{}{}
	var sources []string
	for _, edge := range teamattribution.LatestGitHubWorkItemDerivationDependencies(edges) {
		if !teamattribution.GithubWorkItemDerivationInheritableRelationships[edge.RelationshipType] {
			continue
		}
		if _, alreadyAffected := targetIDs[edge.SourceWorkItemID]; alreadyAffected {
			continue
		}
		if _, duplicate := seen[edge.SourceWorkItemID]; duplicate {
			continue
		}
		seen[edge.SourceWorkItemID] = struct{}{}
		sources = append(sources, edge.SourceWorkItemID)
	}
	return sources, nil
}

// orgItemCount returns the org's total work-item count, the denominator
// evaluateClosurePromotion compares a scoped run's closure size against.
func (executor *WorkItemAttributionExecutor) orgItemCount(ctx context.Context, orgID string) (int, error) {
	row := executor.conn.QueryRow(ctx, `SELECT count() FROM work_items FINAL WHERE org_id = ?`, orgID)
	// count() is UInt64; clickhouse-go's Scan does not convert a UInt64
	// column into a Go *int (only *uint64), so the destination must match
	// the column's own width, not the width this file otherwise thinks in.
	var count uint64
	if err := row.Scan(&count); err != nil {
		return 0, fmt.Errorf("count work_items: %w", err)
	}
	return int(count), nil
}

// loadFacts loads the org-wide, provider-neutral ownership facts via
// teamattribution's own ClickHouseFactSource (PR-A) -- the SAME loader the
// sync-time deriver's providersync.githubWorkItemClickHouseDerivationContextSource
// wraps, so both writers read identical facts for identical inputs.
func (executor *WorkItemAttributionExecutor) loadFacts(
	ctx context.Context, orgID string, asOf time.Time,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	return LoadWorkItemDerivationFacts(ctx, executor.conn, orgID, asOf)
}

// LoadWorkItemDerivationFacts composes the six ClickHouseFactSource loaders
// into the fact set the attribution cascade needs, for one org as of one
// instant.
//
// Exported (CHAOS-4283 PR2) because the metrics.daily `work_item_attribution`
// family needs the IDENTICAL fact set, and this composition is not the trivial
// sequence it looks like: LoadMembers returns FOUR slices, and its fourth --
// provider-tagged roster members -- must be appended to ProviderMembers rather
// than assigned anywhere, because `teams.members` mixes admin-curated entries
// with unreviewed provider auto-import writes and therefore belongs in the
// FALLBACK layer, not the admin one (chris, 2026-08-26, after a codex HIGH
// finding). A second hand-written copy of that would be free to drop the
// append and silently promote provider rosters to authoritative overrides --
// exactly the defect that review caught. One implementation, two callers.
func LoadWorkItemDerivationFacts(
	ctx context.Context, conn driver.Conn, orgID string, asOf time.Time,
) (teamattribution.GithubWorkItemDerivationFacts, error) {
	loader := teamattribution.ClickHouseFactSource{Conn: conn}
	facts := teamattribution.GithubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = loader.LoadTeams(ctx, orgID); err != nil {
		return facts, err
	}
	if facts.Projects, err = loader.LoadProjects(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	if facts.Repos, err = loader.LoadRepos(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	var providerTaggedRosterMembers []teamattribution.GithubWorkItemDerivationMemberFact
	if facts.Members, facts.UntypedMembers, facts.ProviderUntypedMembers, providerTaggedRosterMembers, err = loader.LoadMembers(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	if facts.ProviderMembers, err = loader.LoadProviderMembers(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	facts.ProviderMembers = append(facts.ProviderMembers, providerTaggedRosterMembers...)
	if facts.ManualFallbacks, err = loader.LoadManualFallbacks(ctx, orgID, asOf); err != nil {
		return facts, err
	}
	return facts, nil
}

// alreadyCoveredToday returns the subset of ids that already have a
// work_item_team_attributions row computed on the SAME calendar day as now
// (UTC) -- items the native daily `work_item_attribution` family has already
// written for today. See the CHAOS-5078 codex r1 F2 fix comment at this
// function's call site in ComputeOrg for why this exclusion exists.
func (executor *WorkItemAttributionExecutor) alreadyCoveredToday(
	ctx context.Context, orgID string, now time.Time, ids map[string]struct{},
) (map[string]struct{}, error) {
	covered := map[string]struct{}{}
	if len(ids) == 0 {
		return covered, nil
	}
	idList := make([]string, 0, len(ids))
	for id := range ids {
		idList = append(idList, id)
	}
	rows, err := executor.conn.Query(ctx, `
SELECT DISTINCT work_item_id
FROM work_item_team_attributions
WHERE org_id = ? AND has(?, work_item_id) AND toDate(computed_at) = toDate(?)`,
		orgID, idList, now)
	if err != nil {
		return nil, fmt.Errorf("query already-covered-today attributions: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan already-covered-today row: %w", err)
		}
		covered[id] = struct{}{}
	}
	return covered, rows.Err()
}

// publishRunMarkers writes the completion marker(s) for a completed run:
// one org-wide marker, or one scoped marker per repo/project actually
// processed. CHAOS-2433 protocol: called only after WriteAttributions has
// already returned successfully.
func (executor *WorkItemAttributionExecutor) publishRunMarkers(
	ctx context.Context, orgID string, scope workItemAttributionScopeDecision, completedAt time.Time,
) error {
	runID := uuid.NewString()
	if scope.orgWide {
		return executor.writer.WriteAttributionRun(ctx, WorkItemAttributionRunRecord{
			OrgID: orgID, RunID: runID, CompletedAt: completedAt,
			PromotedReason: scope.promotedReason,
		})
	}
	records := make([]WorkItemAttributionScopedRunRecord, 0, len(scope.repoIDs)+len(scope.projectKeys))
	for _, repoID := range scope.repoIDs {
		records = append(records, WorkItemAttributionScopedRunRecord{
			OrgID: orgID, ScopeKind: "repo", ScopeID: repoID, RunID: runID, CompletedAt: completedAt,
		})
	}
	for _, projectKey := range scope.projectKeys {
		records = append(records, WorkItemAttributionScopedRunRecord{
			OrgID: orgID, ScopeKind: "project", ScopeID: projectKey, RunID: runID, CompletedAt: completedAt,
		})
	}
	return executor.writer.WriteScopedAttributionRuns(ctx, records)
}

// ComputePartition satisfies PartitionExecutor: the seam the partition
// handler drives.
//
// Unlike membership_backfill's ComputePartition, the decoded scope is NOT
// fed into the compute call: membership's caller-supplied repo_ids come from
// an upstream planner that already knows what changed, but this backstop's
// whole design point (team-lead's PR-B ruling) is that IT determines the
// affected scope itself, from ClickHouse watermarks, at compute time --
// staleness a scheduler-time planner cannot see coming. The partition's
// workItemAttributionScope is decoded only to fail the partition loudly on a
// shape a future producer got wrong (scopes.go's contract), matching the
// validation discipline every other decode in this package applies; ComputeOrg
// alone decides what is actually re-derived.
func (executor *WorkItemAttributionExecutor) ComputePartition(
	ctx context.Context, run Run, partition Partition,
) (CompatibilityOutcome, error) {
	if executor == nil || executor.conn == nil {
		return CompatibilityOutcome{}, ErrWorkItemAttributionUnavailable
	}
	if strings.TrimSpace(run.OrganizationID) == "" {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s has no organization", ErrInvalidState, partition.ID))
	}

	var scope workItemAttributionScope
	if err := json.Unmarshal(partition.Scope, &scope); err != nil {
		return CompatibilityOutcome{}, jobruntime.WithSafeCause(fmt.Errorf(
			"%w: partition %s scope: %v", ErrInvalidState, partition.ID, err))
	}

	outcome, err := executor.ComputeOrg(ctx, run.OrganizationID)
	written := outcome.RowsWritten
	if err != nil {
		return CompatibilityOutcome{RowsWritten: &written}, err
	}
	return CompatibilityOutcome{RowsWritten: &written}, nil
}

// A compile-time pin that this executor IS the seam the handler drives.
var _ PartitionExecutor = (*WorkItemAttributionExecutor)(nil)
