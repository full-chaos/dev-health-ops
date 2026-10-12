package providersync

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// team_repo_ownership_derivation_clickhouse.go loads the already-synced
// inputs deriveTeamRepoOwnership needs and writes its output, for one org,
// against ClickHouse. Never fetches from a provider -- CHAOS-4365 item 1b.
// Triggered as a post-sync River job (see native_post_sync.go /
// sync_dispatch.go), not from within any provider's own sync route.

var ErrTeamRepoOwnershipDerivationUnavailable = errors.New("team_repo_ownership derivation is unavailable")

// TeamRepoOwnershipDerivationService reads already-synced team_project_ownership,
// work_items, work_item_dependencies and work_graph_issue_pr rows for one org,
// runs the pure derivation, and writes team_repo_ownership (source='inferred').
type TeamRepoOwnershipDerivationService struct {
	Conn driver.Conn
}

// teamRepoOwnershipRepoInfo is one org's repos-table row, keyed by repo_id --
// needed to stamp the REPO's own provider (not the work item's tracker
// provider) and repo_full_name onto every written row, since
// team_repo_ownership.source=='provider_access' rows (team_autoimport_github.py)
// are read back by a (org_id, provider, repo_full_name) NAME join
// (providers/teams.py::load_team_repo_ownership_map) and repo_full_name is
// part of this table's ReplacingMergeTree ORDER BY key -- leaving it blank
// would collide two different repos owned by the same team onto the same
// sort key and silently drop one on merge.
type teamRepoOwnershipRepoInfo struct {
	FullName string
	Provider string
}

// Derive loads orgID's already-synced ownership/linkage rows, runs the pure
// derivation, writes the result to team_repo_ownership, and retracts any
// PRIOR inferred (team, repo) claim this run no longer derives. Returns the
// number of rows written, the number of rows retracted, and whether this
// org's INPUTS were present at all.
//
// inputsReady=false (team-lead ruling, CHAOS-4365 item 1b codex finding #4,
// 2026-08-28: "leave the no-prerequisite design... make it observable
// instead of sequenced") means this org had zero team_project_ownership
// rows, or zero linkage rows of any kind (work_items, work_item_dependencies,
// or work_graph_issue_pr) -- the first-sync gap this producer's deliberately
// unsequenced Fanout publish (no prerequisite completion key, same as
// team-autoimport) can hit: team_project_ownership from team-autoimport's
// async Python bridge, or work_graph_issue_pr from the workgraph builder,
// simply have not landed yet for a brand-new org. It converges on the NEXT
// qualifying sync (this producer is idempotent and re-triggered by every
// sync with git||hasWorkItems), so it is surfaced as its own telemetry
// outcome (inputs_not_ready) rather than treated as a failure or sequenced
// against those other producers' own completion. Retraction NEVER runs on
// an inputsReady=false evaluation -- an incomplete input snapshot is not a
// trustworthy basis for deciding a prior claim is gone.
//
// inputsReady=true with written=0 is the OTHER, unrelated zero-row case:
// inputs existed but genuinely produced nothing (every reached repo was a full tie,
// or matched no repo-bearing item) -- the designed-empty case, reported as
// no_signal (unless retracted>0, in which case every prior claim was
// retracted and none replaced it).
//
// Retraction (team-lead ruling, 2026-08-28, codex R3 finding: "removed
// ownership remains authorized indefinitely" -- confirmed pre-existing
// across every OTHER writer of this table too, github/jira/linear/gitlab
// autoimport, none of which ever sets valid_to; tracked separately as its
// own cross-writer ticket, CHAOS-4321-adjacent -- but THIS producer, being
// new, must not add to that exposure): every sync recomputes the org's
// COMPLETE (team_id, repo_full_name) inferred set from scratch, so a prior
// active inferred row absent from the new set is provably stale -- its
// project ownership or donor linkage was removed or reassigned, or another
// team now has the larger linked share of the repo. The set holds one owner
// per repo: when several teams reach a repo, the team with the most native
// links wins, then the most explicit_text links, then the most heuristic
// links (deriveTeamRepoOwnership). A repo is never dropped only because two
// teams have links to it. A full tie (equal counts at every tier) names no
// owner: the open rows of the tied teams stay open (no retraction), a row of
// any other team is retracted, and the run reports the tie in
// TeamRepoOwnershipDerivationStats.Ties.
// Retracted by writing a replacement row under the SAME (org_id, provider,
// repo_full_name, team_id, source, valid_from) ReplacingMergeTree key with
// valid_to=now and a newer updated_at, so FINAL/argMax(updated_at) readers
// (providers/teams.py::load_team_repo_ownership_map,
// native_status_change.py's _TEAM_REPOSITORIES_SQL -- both already filter
// on valid_to) see it as expired on their very next query.
// TeamRepoOwnershipDerivationStats is what one Derive run says about HOW MANY facts it derived, beyond what it wrote (CHAOS-8148). A run
// that writes nothing is one of two different things the written count cannot tell apart: it derived NOTHING (no_signal: the designed-empty
// case), or it derived facts that EVERY active row already carries unchanged (unchanged: the steady state since "stop re-writing unchanged
// ownership"). Both stay zero on an early return (inputs not ready, error).
type TeamRepoOwnershipDerivationStats struct {
	// Derived is the number of (team, repo) facts the run derived from the synced inputs.
	Derived int
	// Unchanged is how many of those an already-open row carried unchanged, so the run did not write them.
	Unchanged int
	// RetractionsHeld is how many open rows the run does not derive and kept open, because the input of a kind that can
	// derive them was not in the run (CHAOS-9045, team_repo_ownership_retraction_guard.go).
	RetractionsHeld int
	// Ties are the repos two or more teams reach with equal link counts at every tier: the run named no owner for them, kept
	// the tied teams' open rows and retracted any other team's. Set on every run that reaches the derivation, also when it
	// writes nothing.
	Ties []TeamRepoOwnershipTie
	// RoleChanges is how many open rows the run closed at the run because the
	// claim's role changed (main owner to co-owner, or co-owner to main owner);
	// the claim's row of its new role starts at the run.
	RoleChanges int
	// Superseded is how many open rows the run closed because the claim they
	// record now starts at its evidence time (a row at that time was written).
	Superseded int
}

// Derive is DeriveWithStats without the stats (the form the integration tests and the older callers use).
func (service TeamRepoOwnershipDerivationService) Derive(ctx context.Context, orgID string) (written int, retracted int, inputsReady bool, armCounts map[string]int, err error) {
	written, retracted, inputsReady, armCounts, _, err = service.DeriveWithStats(ctx, orgID)
	return written, retracted, inputsReady, armCounts, err
}

// DeriveWithStats is Derive plus the derived/unchanged counts of the run (CHAOS-8148).
func (service TeamRepoOwnershipDerivationService) DeriveWithStats(ctx context.Context, orgID string) (written int, retracted int, inputsReady bool, armCounts map[string]int, stats TeamRepoOwnershipDerivationStats, err error) {
	written, retracted, inputsReady, armCounts, err = service.derive(ctx, orgID, &stats)
	return written, retracted, inputsReady, armCounts, stats, err
}

func (service TeamRepoOwnershipDerivationService) derive(ctx context.Context, orgID string, stats *TeamRepoOwnershipDerivationStats) (written int, retracted int, inputsReady bool, armCounts map[string]int, err error) {
	if service.Conn == nil || ctx == nil || orgID == "" {
		return 0, 0, false, nil, ErrTeamRepoOwnershipDerivationUnavailable
	}
	now := time.Now().UTC()

	projectLinks, err := loadTeamRepoOwnershipProjectLinks(ctx, service.Conn, orgID, now)
	if err != nil {
		return 0, 0, false, nil, err
	}

	repos, err := loadTeamRepoOwnershipRepos(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, false, nil, err
	}
	if len(repos) == 0 {
		// No repos synced for this org yet -- nothing derivable can attach to
		// a repo regardless of which arm would otherwise resolve a team, so
		// this gate is unaffected by CHAOS-4537 and stays first.
		return 0, 0, false, nil, nil
	}
	repoIDs := make([]uuid.UUID, 0, len(repos))
	for id := range repos {
		repoIDs = append(repoIDs, id)
	}

	workItems, err := loadTeamRepoOwnershipWorkItems(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, false, nil, err
	}

	dependencyEdges, err := loadTeamRepoOwnershipDependencyEdges(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, false, nil, err
	}

	issuePRLinks, err := loadTeamRepoOwnershipIssuePRLinks(ctx, service.Conn, orgID, repoIDs)
	if err != nil {
		return 0, 0, false, nil, err
	}
	// CHAOS-4537: the early return that used to sit right after loading
	// projectLinks (before workItems/dependencyEdges/issuePRLinks were even
	// loaded) assumed every resolution arm required a team_project_ownership
	// row, which is no longer true: the linear_team_key arm now resolves
	// straight from a Linear work item's own native_team_key column (see
	// deriveTeamRepoOwnership's doc comment). An org with real,
	// already-synced Linear work items but a team_project_ownership table
	// that has not synced yet (a plausible ordering: work-items sync and
	// team autoimport are independent per-config selections, CHAOS-4323) is
	// NOT a first-sync gap for this arm. This guard is NOT gone, though
	// (round 1's fix unconditionally removed it; round 3's codex review
	// caught that this reopened a retraction hazard for the case with no
	// Linear-native signal -- see the guard reinstated below, after
	// knownTeams loads): it now only skips when a Linear-native signal is
	// actually present.
	//
	// The guard below (unchanged from before this ticket, codex review round
	// 1 P1: keep it) is a DIFFERENT, still-necessary check: if
	// workItems/dependencyEdges/issuePRLinks are ALL empty, nothing can be
	// derived by ANY arm regardless of projectLinks' state -- proceeding
	// anyway would treat a transient partial-sync snapshot (team_project_
	// ownership synced, work-items not yet -- the OPPOSITE ordering from
	// the paragraph above) as a genuine inputsReady=true, derived=[]
	// evaluation, and RETRACT every previously-derived row for this org via
	// diffTeamRepoOwnershipRetractions below. Removing only the
	// projectLinks-only guard, not this one, is what makes the Linear-only
	// case resolve while a transient linkage gap still reports
	// inputsReady=false instead of wiping prior rows.
	if len(workItems) == 0 && len(dependencyEdges) == 0 && len(issuePRLinks) == 0 {
		return 0, 0, false, nil, nil
	}

	// CHAOS-4537 codex review round 2 P1: the linear_team_key arm validates a
	// Linear work item's native_team_key against the org's CURRENT team
	// catalog before trusting it -- see TeamRepoOwnershipKnownTeam's doc
	// comment. Loaded here (before the guard below, not after) because that
	// guard now needs to know whether a Linear-native signal exists.
	knownTeams, err := loadTeamRepoOwnershipKnownTeams(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, false, nil, err
	}

	// CHAOS-4537 codex review round 3 P1 (confirmed real): removing the
	// projectLinks-only guard UNCONDITIONALLY (round 1's fix) reopened the
	// exact retraction hazard round 1 closed, mirrored onto the opposite
	// input combination. Consider team_project_ownership transiently empty
	// (the gap this ticket targets) for a NON-Linear org (or a Linear org
	// with no native_team_key signal): workItems/dependencyEdges/issuePRLinks
	// are non-empty, so the guard above does not fire, but with
	// projectLinks==nil neither arm can resolve anything (the project_id arm
	// has no projectToTeam entries; the linear_team_key arm never applies to
	// a non-Linear item) -- derived comes back empty, and the retraction
	// diff below would then wipe every previously-derived row for the org,
	// having nothing to protect them with. Only skip this guard (treat as
	// ready despite projectLinks==0) when a Linear-native signal is actually
	// present -- the one case removing the guard was meant to unblock.
	if len(projectLinks) == 0 && !hasResolvableLinearNativeTeamKey(workItems, knownTeams) {
		return 0, 0, false, nil, nil
	}

	derivation := deriveTeamRepoOwnership(orgID, projectLinks, workItems, dependencyEdges, issuePRLinks, knownTeams)

	activeRows, err := loadTeamRepoOwnershipActiveInferredRows(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, true, nil, err
	}
	// The tie decision runs first because a kept main owner takes the repo's
	// earliest evidence.
	derived := resolveTiedMainOwners(derivation.Rows, repos, activeRows)
	// CHAOS-4537 codex review round 3, second P1, and CHAOS-9045: the diff
	// below is one comparison over the org's whole activeRows, so "this run
	// does not derive the row" is a proof of absence only when the run read
	// the input that could derive it. That input is per provider and per
	// fact kind: a run with no open project link of provider A (a transient
	// gap, or a wrong upstream close of A's links) cannot reconfirm a row
	// that A's links derived, however many links provider B has, and a run
	// with no resolvable Linear team key cannot reconfirm a row of the
	// linear_team_key arm. The guard (team_repo_ownership_retraction_guard.go)
	// keeps every retraction whose kinds are not all proven, and reports it.
	linkHistory, err := loadTeamRepoOwnershipTeamLinkProviders(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, true, nil, err
	}
	guard := newTeamRepoOwnershipRetractionGuard(projectLinks, linkHistory, workItems, knownTeams)
	retractions := diffTeamRepoOwnershipRetractions(activeRows, derived, derivation.Ties, repos)
	toRetract, held, heldKinds := guard.retain(retractions)
	// Each claim's evidence is settled (set aside when it is in the future of
	// the run or older than the end of the claim's last retraction; a main
	// owner's moved to the end of another team's main tenure), then the start
	// of each claim is stamped, then role changes are dated at the run.
	tenure, err := loadTeamRepoOwnershipRetractedUntil(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, true, nil, err
	}
	settled, settledCounts := settleInferredEvidence(derived, repos, tenure, toRetract, now)
	if settledCounts != (inferredEvidenceSettleCounts{}) {
		slog.Default().InfoContext(ctx, "team_repo_ownership_derivation.evidence_start_set_aside",
			"org_id", orgID, "future", settledCounts.future, "before_retraction", settledCounts.beforeRetraction,
			"after_other_owner", settledCounts.afterOtherOwner)
	}
	derived, roleCloses, roleChanged := applyTeamRepoOwnershipRoleChanges(stampTeamRepoOwnershipClaimStarts(settled, repos, activeRows, now), repos, activeRows, now)
	// A role change closes the claim's open row of its old role at the run,
	// under the same proof as a retraction. A repository with a role change
	// and any held close or retraction keeps all its rows this run, so it
	// never has two main rows (or none) at one instant: it waits for a run
	// that proves them all.
	keptRoleCloses, heldRoles, heldRoleKinds := guard.retain(roleCloses)
	heldRepos := teamRepoOwnershipReposOf(withoutRows(retractions, toRetract), withoutRows(roleCloses, keptRoleCloses))
	derived, toRetract, keptRoleCloses, repoHeld := withoutHeldRoleChanges(derived, roleChanged, toRetract, keptRoleCloses, heldRepos, repos)
	if repoHeld > 0 {
		slog.Default().InfoContext(ctx, "team_repo_ownership_derivation.role_change_held",
			"org_id", orgID, "rows", repoHeld)
	}
	stats.RetractionsHeld = held + heldRoles + repoHeld
	reportTeamRepoOwnershipRetractionHeld(ctx, orgID, append(heldKinds, heldRoleKinds...))
	if len(toRetract) > 0 {
		retracted, err = retractTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, toRetract)
		if err != nil {
			return 0, 0, true, nil, err
		}
	}
	if len(keptRoleCloses) > 0 {
		changed, err := retractTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, keptRoleCloses)
		if err != nil {
			return 0, retracted, true, nil, err
		}
		stats.RoleChanges = changed
		slog.Default().InfoContext(ctx, "team_repo_ownership_derivation.role_changed", "org_id", orgID, "rows", changed)
	}
	activeRows = withoutRows(activeRows, keptRoleCloses)

	stats.Derived = len(derived)
	stats.Ties = derivation.Ties
	if len(derived) == 0 {
		return 0, retracted, true, nil, nil
	}
	// A run writes a new open row only for a fact this org's activeRows
	// snapshot (loaded above, before this run's own retractions) does not
	// already carry unchanged -- see filterUnchangedTeamRepoOwnershipRows'
	// doc comment for exactly what "unchanged" compares. A fact whose
	// attributes changed, or that has no active row at all (new, or
	// previously retracted and now derived again), is left in toWrite and
	// gets a fresh open row exactly like today's unconditional write did.
	toWrite := filterUnchangedTeamRepoOwnershipRows(derived, repos, activeRows)
	stats.Unchanged = len(derived) - len(toWrite)
	if len(toWrite) == 0 {
		// The claims are all written; an open row an earlier run left beside
		// one (a write that landed before its supersede) is closed here.
		superseded, err := supersedeTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, supersededTeamRepoOwnershipRows(derived, repos, activeRows, now))
		if err != nil {
			return 0, retracted, true, nil, err
		}
		stats.Superseded = superseded
		return 0, retracted, true, nil, nil
	}
	// armCounts (CHAOS-4458 part (b)) is computed by writeTeamRepoOwnershipRows
	// itself from the rows it actually COMMITS, not from `derived` up front
	// (codex adversarial review, 2026-08-29, confirmed finding): a derived
	// candidate can still be dropped (unresolvable repo_id) or the whole
	// batch can fail before Send() -- counting from `derived` would report a
	// linear_team_key/project_id row that was never actually written.
	written, armCounts, err = writeTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, toWrite, repos)
	if err != nil {
		return 0, retracted, true, nil, err
	}
	superseded, err := supersedeTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, supersededTeamRepoOwnershipRows(derived, repos, activeRows, now))
	if err != nil {
		return written, retracted, true, nil, err
	}
	stats.Superseded = superseded
	return written, retracted, true, armCounts, nil
}

// evidenceTime is t in UTC at the millisecond of team_repo_ownership's
// DateTime64(3) columns, or zero for a time that is no evidence (the zero
// time, or the epoch a DateTime64 column default reads as).
func evidenceTime(t time.Time) time.Time {
	if t.IsZero() || !t.After(time.Unix(0, 0)) {
		return time.Time{}
	}
	return t.UTC().Truncate(time.Millisecond)
}

// inferredValidFrom is the valid_from an inferred row is written with: the
// earliest evidence time of the claim (a pull or merge request of the
// repository reached through the team's work), so a read of a past day sees
// the owner the evidence shows. A claim with no timed evidence, or evidence
// in the future of this run, starts at the run.
func inferredValidFrom(row DerivedTeamRepoOwnershipRow, now time.Time) time.Time {
	if !row.validFrom.IsZero() {
		return row.validFrom
	}
	if row.EvidenceFrom.IsZero() || row.EvidenceFrom.After(now) {
		return now
	}
	return row.EvidenceFrom
}

// teamRepoOwnershipClaim is the identity of one team's claim on one
// repository: the table's key without source and valid_from.
type teamRepoOwnershipClaim struct{ provider, repoFullName, teamID string }

// stampTeamRepoOwnershipClaimStarts sets the valid_from each derived claim is
// written with. A claim with evidence in the past of the run starts at its
// evidence. A claim with no such evidence keeps the start of its earliest open
// row, so a new share or role replaces that row (same key) instead of opening
// a second row; a new claim with no evidence starts at the run.
func stampTeamRepoOwnershipClaimStarts(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	activeRows []teamRepoOwnershipActiveRow,
	now time.Time,
) []DerivedTeamRepoOwnershipRow {
	earliestOpen := map[teamRepoOwnershipClaim]time.Time{}
	for _, row := range activeRows {
		key := teamRepoOwnershipClaim{row.Provider, row.RepoFullName, row.TeamID}
		start := row.ValidFrom.UTC().Truncate(time.Millisecond)
		if held, ok := earliestOpen[key]; !ok || start.Before(held) {
			earliestOpen[key] = start
		}
	}
	out := make([]DerivedTeamRepoOwnershipRow, len(derived))
	for i, row := range derived {
		out[i] = row
		if !row.EvidenceFrom.IsZero() && !row.EvidenceFrom.After(now) {
			out[i].validFrom = row.EvidenceFrom
			continue
		}
		out[i].validFrom = now
		if info, ok := resolveTeamRepoOwnershipRepo(row.RepoID, repos); ok {
			if start, open := earliestOpen[teamRepoOwnershipClaim{info.Provider, info.FullName, row.TeamID}]; open {
				out[i].validFrom = start
			}
		}
	}
	return out
}

// applyTeamRepoOwnershipRoleChanges dates an observed change of owner at the
// run. A claim whose open rows all hold another role (a main owner that is now
// a co-owner, a co-owner that is now the main owner) starts its new row at the
// run, and its open rows of the old role are returned to be closed at the run
// (a real end, never a supersede: the old role held until this run). A new
// main owner of a repository whose open main row belongs to another team also
// starts at the run. Only a first observation is dated by its evidence, so a
// repository has one main owner at every instant. An open row with is_primary
// 0 (the one-owner derivation) is a main row.
func applyTeamRepoOwnershipRoleChanges(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	activeRows []teamRepoOwnershipActiveRow,
	now time.Time,
) ([]DerivedTeamRepoOwnershipRow, []teamRepoOwnershipActiveRow, map[teamRepoOwnershipClaim]bool) {
	isMain := func(isPrimary uint8) bool { return isPrimary != teamRepoOwnershipCoOwner }
	open := map[teamRepoOwnershipClaim][]teamRepoOwnershipActiveRow{}
	openMainTeams := map[teamRepoOwnershipRepoKey]map[string]bool{}
	for _, row := range activeRows {
		claim := teamRepoOwnershipClaim{row.Provider, row.RepoFullName, row.TeamID}
		open[claim] = append(open[claim], row)
		if isMain(row.IsPrimary) {
			key := teamRepoOwnershipRepoKey{row.Provider, row.RepoFullName}
			if openMainTeams[key] == nil {
				openMainTeams[key] = map[string]bool{}
			}
			openMainTeams[key][row.TeamID] = true
		}
	}
	out := make([]DerivedTeamRepoOwnershipRow, len(derived))
	var closes []teamRepoOwnershipActiveRow
	changed := map[teamRepoOwnershipClaim]bool{}
	for i, row := range derived {
		out[i] = row
		info, ok := resolveTeamRepoOwnershipRepo(row.RepoID, repos)
		if !ok {
			continue
		}
		claim := teamRepoOwnershipClaim{info.Provider, info.FullName, row.TeamID}
		main := row.IsPrimary == teamRepoOwnershipMainOwner
		rows := open[claim]
		sameRole := false
		for _, openRow := range rows {
			if isMain(openRow.IsPrimary) == main {
				sameRole = true
			}
		}
		roleChanged := len(rows) > 0 && !sameRole
		mainChanged := false
		if main && !sameRole {
			for team := range openMainTeams[teamRepoOwnershipRepoKey{info.Provider, info.FullName}] {
				if team != row.TeamID {
					mainChanged = true
				}
			}
		}
		if !roleChanged && !mainChanged {
			continue
		}
		out[i].validFrom = now
		changed[claim] = true
		if roleChanged {
			closes = append(closes, rows...)
		}
	}
	return out, closes, changed
}

// withoutHeldRoleChanges keeps every row of a repository whose role changes
// this run while the guard holds a close or retraction of it: the derived
// rows, retractions and closes of that repository are left out, so it keeps
// its open rows this run. It returns those and the count of rows it held.
func withoutHeldRoleChanges(
	derived []DerivedTeamRepoOwnershipRow,
	changed map[teamRepoOwnershipClaim]bool,
	retractions, closes []teamRepoOwnershipActiveRow,
	heldRepos map[teamRepoOwnershipRepoKey]bool,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) ([]DerivedTeamRepoOwnershipRow, []teamRepoOwnershipActiveRow, []teamRepoOwnershipActiveRow, int) {
	frozen := map[teamRepoOwnershipRepoKey]bool{}
	for claim := range changed {
		if key := (teamRepoOwnershipRepoKey{claim.provider, claim.repoFullName}); heldRepos[key] {
			frozen[key] = true
		}
	}
	if len(frozen) == 0 {
		return derived, retractions, closes, 0
	}
	heldRows := 0
	out := derived[:0:0]
	for _, row := range derived {
		if info, ok := resolveTeamRepoOwnershipRepo(row.RepoID, repos); ok && frozen[teamRepoOwnershipRepoKey{info.Provider, info.FullName}] {
			if changed[teamRepoOwnershipClaim{info.Provider, info.FullName, row.TeamID}] {
				heldRows++
			}
			continue
		}
		out = append(out, row)
	}
	keep := func(rows []teamRepoOwnershipActiveRow) []teamRepoOwnershipActiveRow {
		kept := rows[:0:0]
		for _, row := range rows {
			if frozen[teamRepoOwnershipRepoKey{row.Provider, row.RepoFullName}] {
				heldRows++
				continue
			}
			kept = append(kept, row)
		}
		return kept
	}
	keptRetractions, keptCloses := keep(retractions), keep(closes)
	return out, keptRetractions, keptCloses, heldRows
}

// teamRepoOwnershipRepoKey names one repository of the open rows.
type teamRepoOwnershipRepoKey struct{ provider, repoFullName string }

func teamRepoOwnershipReposOf(sets ...[]teamRepoOwnershipActiveRow) map[teamRepoOwnershipRepoKey]bool {
	out := map[teamRepoOwnershipRepoKey]bool{}
	for _, rows := range sets {
		for _, row := range rows {
			out[teamRepoOwnershipRepoKey{row.Provider, row.RepoFullName}] = true
		}
	}
	return out
}

// withoutRows is rows less every row of drop (by the table key).
func withoutRows(rows, drop []teamRepoOwnershipActiveRow) []teamRepoOwnershipActiveRow {
	if len(drop) == 0 {
		return rows
	}
	type key struct {
		provider, repoFullName, teamID string
		validFrom                      time.Time
	}
	dropped := map[key]bool{}
	for _, row := range drop {
		dropped[key{row.Provider, row.RepoFullName, row.TeamID, row.ValidFrom.UTC()}] = true
	}
	out := rows[:0:0]
	for _, row := range rows {
		if !dropped[key{row.Provider, row.RepoFullName, row.TeamID, row.ValidFrom.UTC()}] {
			out = append(out, row)
		}
	}
	return out
}

// resolveTiedMainOwners decides the main owner of each repository whose
// largest linked share is tied. The one tied team whose open inferred row of
// the repository is a main row (is_primary 1, or 0: a row of the one-owner
// derivation before co-owner rows) stays the main owner; with no such team, or
// with more than one, the repository has no main owner and every tied team is
// a co-owner. A tie never moves the main owner to another team. The kept main
// owner starts at the earliest evidence of any claim on the repository, as a
// main owner does.
func resolveTiedMainOwners(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	activeRows []teamRepoOwnershipActiveRow,
) []DerivedTeamRepoOwnershipRow {
	openMain := map[teamRepoOwnershipClaim]bool{}
	for _, row := range activeRows {
		if row.IsPrimary != teamRepoOwnershipCoOwner {
			openMain[teamRepoOwnershipClaim{row.Provider, row.RepoFullName, row.TeamID}] = true
		}
	}
	mains := map[string][]int{}
	out := make([]DerivedTeamRepoOwnershipRow, len(derived))
	for i, row := range derived {
		out[i] = row
		if !row.Tied {
			continue
		}
		info, ok := resolveTeamRepoOwnershipRepo(row.RepoID, repos)
		if ok && openMain[teamRepoOwnershipClaim{info.Provider, info.FullName, row.TeamID}] {
			mains[row.RepoID] = append(mains[row.RepoID], i)
		}
	}
	for _, indexes := range mains {
		if len(indexes) == 1 {
			out[indexes[0]].IsPrimary = teamRepoOwnershipMainOwner
			out[indexes[0]].EvidenceFrom = out[indexes[0]].repoEvidenceFrom
		}
	}
	return out
}

// resolveTeamRepoOwnershipRepo is the repos row a derived row's repo_id names,
// the same resolution writeTeamRepoOwnershipRows applies.
func resolveTeamRepoOwnershipRepo(id string, repos map[uuid.UUID]teamRepoOwnershipRepoInfo) (teamRepoOwnershipRepoInfo, bool) {
	repoID, err := uuid.Parse(id)
	if err != nil || repoID == uuid.Nil {
		return teamRepoOwnershipRepoInfo{}, false
	}
	info, ok := repos[repoID]
	return info, ok && info.FullName != ""
}

type inferredEvidenceSettleCounts struct{ future, beforeRetraction, afterOtherOwner int }

// settleInferredEvidence moves the evidence time of a claim to where its row
// must start:
//   - evidence in the future of this run is cleared (the row starts at the run
//     and an open row matches on the next runs, so a rerun writes nothing);
//   - evidence older than the end of the claim's own latest retraction is
//     cleared (that retraction already judged it; a row at the evidence time
//     would replace the retracted row, whose valid_from is the same key, and
//     erase the interval the claim did not own);
//   - else a main owner's evidence older than the end of another team's main
//     tenure on the repository (the latest close of its main row, or this
//     run, when this run retracts its open main row) moves to that end, so the
//     repository has one main owner at a time. Co-owners own the repository
//     beside the main owner, so a co-owner's start is never moved by another
//     team. The end is stored as that team's valid_to, so the next run reads
//     the same start and writes nothing.
func settleInferredEvidence(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	tenure teamRepoOwnershipTenureEnds,
	retractingNow []teamRepoOwnershipActiveRow,
	now time.Time,
) ([]DerivedTeamRepoOwnershipRow, inferredEvidenceSettleCounts) {
	type repoKey struct{ provider, repoFullName string }
	type tenureEnd struct {
		teamID string
		until  time.Time
	}
	ends := map[repoKey][]tenureEnd{}
	for claim, until := range tenure.mainUntil {
		key := repoKey{claim.provider, claim.repoFullName}
		ends[key] = append(ends[key], tenureEnd{claim.teamID, until})
	}
	runEnd := evidenceTime(now)
	for _, row := range retractingNow {
		if row.IsPrimary == teamRepoOwnershipCoOwner {
			continue
		}
		key := repoKey{row.Provider, row.RepoFullName}
		ends[key] = append(ends[key], tenureEnd{row.TeamID, runEnd})
	}

	var counts inferredEvidenceSettleCounts
	settled := make([]DerivedTeamRepoOwnershipRow, len(derived))
	for i, row := range derived {
		settled[i] = row
		if row.EvidenceFrom.IsZero() {
			continue
		}
		if row.EvidenceFrom.After(now) {
			settled[i].EvidenceFrom = time.Time{}
			counts.future++
			continue
		}
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			continue
		}
		if until, ok := tenure.retractedUntil[teamRepoOwnershipClaim{info.Provider, info.FullName, row.TeamID}]; ok && row.EvidenceFrom.Before(until) {
			settled[i].EvidenceFrom = time.Time{}
			counts.beforeRetraction++
			continue
		}
		if row.IsPrimary != teamRepoOwnershipMainOwner {
			continue
		}
		var otherEnd time.Time
		for _, end := range ends[repoKey{info.Provider, info.FullName}] {
			if end.teamID != row.TeamID && end.until.After(otherEnd) {
				otherEnd = end.until
			}
		}
		if otherEnd.After(row.EvidenceFrom) {
			settled[i].EvidenceFrom = otherEnd
			counts.afterOtherOwner++
		}
	}
	return settled, counts
}

// teamRepoOwnershipTenureEnds is, per inferred claim, the end of its latest
// retraction (any role, a role change's close included) and the end of its
// latest main row (is_primary other than 2).
type teamRepoOwnershipTenureEnds struct {
	retractedUntil map[teamRepoOwnershipClaim]time.Time
	mainUntil      map[teamRepoOwnershipClaim]time.Time
}

// loadTeamRepoOwnershipRetractedUntil loads the tenure ends of the org's
// inferred claims. A row closed at its own valid_from (superseded by its
// evidence start) is no retraction.
func loadTeamRepoOwnershipRetractedUntil(
	ctx context.Context, conn driver.Conn, orgID string,
) (teamRepoOwnershipTenureEnds, error) {
	tenure := teamRepoOwnershipTenureEnds{retractedUntil: map[teamRepoOwnershipClaim]time.Time{}, mainUntil: map[teamRepoOwnershipClaim]time.Time{}}
	rows, err := conn.Query(ctx, `
SELECT provider, repo_full_name, team_id, max(assumeNotNull(valid_to)),
       maxIf(assumeNotNull(valid_to), is_primary != 2), countIf(is_primary != 2)
FROM team_repo_ownership FINAL
WHERE org_id = ?
  AND source = 'inferred'
  AND valid_to IS NOT NULL
  AND valid_to > valid_from
  AND valid_to <= now64(3, 'UTC')
GROUP BY provider, repo_full_name, team_id`,
		orgID)
	if err != nil {
		return tenure, err
	}
	defer rows.Close()
	for rows.Next() {
		var claim teamRepoOwnershipClaim
		var until, mainUntil time.Time
		var mainRows uint64
		if err := rows.Scan(&claim.provider, &claim.repoFullName, &claim.teamID, &until, &mainUntil, &mainRows); err != nil {
			return tenure, err
		}
		tenure.retractedUntil[claim] = until.UTC()
		if mainRows > 0 {
			tenure.mainUntil[claim] = mainUntil.UTC()
		}
	}
	return tenure, rows.Err()
}

// teamRepoOwnershipRowSignature is the full set of values a written
// team_repo_ownership row carries beyond its identity key (org_id,
// provider, repo_full_name, team_id, source) -- the ATTRIBUTES
// filterUnchangedTeamRepoOwnershipRows compares to decide whether a fact
// this run derives is the SAME fact an already-open row already records,
// or a changed one that needs its own new generation. repo_id is included
// because a repo can be deleted and re-synced under a new UUID while
// keeping the same repo_full_name -- the identity key alone would then
// wrongly call that unchanged.
type teamRepoOwnershipRowSignature struct {
	TeamID       string
	RepoFullName string
	Provider     string
	RepoID       uuid.UUID
	IsPrimary    uint8
	Specificity  uint16
	Priority     int32
	MatchType    string
	HasShare     bool
	Share        float64
}

// filterUnchangedTeamRepoOwnershipRows returns the subset of derived that
// this run must actually write: a derived row whose full
// teamRepoOwnershipRowSignature (identity: team_id, repo_full_name,
// provider; attributes: repo_id, is_primary, specificity, priority,
// match_type, share) already matches SOME row in activeRows is left out -- the
// existing open row already records exactly this fact, so writing again
// would only stamp a new valid_from over an unchanged truth (the growth
// this producer exists to stop). Every other derived row -- a genuinely
// new fact, a fact whose attributes changed since its active row was
// written, or a fact that was previously retracted (closed, so it is no
// longer in activeRows at all) and is derived again -- is kept, and gets a
// fresh open row exactly like today's unconditional write produced for
// every row; a changed attribute is NOT a special case; it is simply a row
// with no matching signature in activeRows, so it reaches the same "write
// it" path a new fact does.
//
// Compares against activeRows as loaded before this run's own
// retractions: a fact this run is about to retract is, by construction
// (diffTeamRepoOwnershipRetractions), never also present in derived, so
// there is no ordering hazard in reading that snapshot here.
//
// Pure, no I/O, exhaustively unit-testable -- mirrors
// diffTeamRepoOwnershipRetractions' shape.
func filterUnchangedTeamRepoOwnershipRows(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	activeRows []teamRepoOwnershipActiveRow,
) []DerivedTeamRepoOwnershipRow {
	unchanged := make(map[teamRepoOwnershipRowSignature][]time.Time, len(activeRows))
	for _, row := range activeRows {
		key := teamRepoOwnershipRowSignature{
			TeamID:       row.TeamID,
			RepoFullName: row.RepoFullName,
			Provider:     row.Provider,
			RepoID:       row.RepoID,
			IsPrimary:    row.IsPrimary,
			Specificity:  row.Specificity,
			Priority:     row.Priority,
			MatchType:    row.MatchType,
		}
		if row.Share != nil {
			key.HasShare, key.Share = true, *row.Share
		}
		unchanged[key] = append(unchanged[key], row.ValidFrom.UTC().Truncate(time.Millisecond))
	}
	toWrite := make([]DerivedTeamRepoOwnershipRow, 0, len(derived))
	for _, row := range derived {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			// Same unresolvable-repo_id case writeTeamRepoOwnershipRows
			// itself skips -- keep it in toWrite so that existing skip
			// logic (and its own doc comment) stays the one place this is
			// handled, rather than silently dropping it here under a
			// different name.
			toWrite = append(toWrite, row)
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			toWrite = append(toWrite, row)
			continue
		}
		signature := teamRepoOwnershipRowSignature{
			TeamID:       row.TeamID,
			RepoFullName: info.FullName,
			Provider:     info.Provider,
			RepoID:       repoID,
			IsPrimary:    row.IsPrimary,
			Specificity:  row.Specificity,
			Priority:     teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred].Priority,
			MatchType:    "exact", // writeTeamRepoOwnershipRows always writes match_type="exact"
			HasShare:     true,
			Share:        row.Share,
		}
		if openRowMatches(unchanged[signature], row) {
			continue
		}
		toWrite = append(toWrite, row)
	}
	return toWrite
}

// openRowMatches reports whether one of the open rows of a signature records
// the derived claim: any of them for a claim with no evidence time (its start
// is the run that first wrote it), else the one that starts at the claim's
// evidence time. An open row that started at an earlier derive run is then
// not the claim: the claim gets a row at its evidence time and that open row
// is superseded (supersededTeamRepoOwnershipRows).
func openRowMatches(validFroms []time.Time, row DerivedTeamRepoOwnershipRow) bool {
	if len(validFroms) == 0 {
		return false
	}
	start := row.validFrom
	if start.IsZero() {
		start = row.EvidenceFrom
	}
	if start.IsZero() {
		return true
	}
	for _, validFrom := range validFroms {
		if validFrom.Equal(start) {
			return true
		}
	}
	return false
}

// supersededTeamRepoOwnershipRows is every open row of a derived claim's
// identity (provider, repo_full_name, team_id) that does not start at the
// claim's evidence time, when the claim has one: an earlier derive run wrote
// it at its own run time. Closing it at its own valid_from (an empty
// interval, never read) leaves ONE open row per claim, the one at the
// evidence time. A claim with no evidence time supersedes nothing.
func supersededTeamRepoOwnershipRows(
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
	activeRows []teamRepoOwnershipActiveRow,
	now time.Time,
) []teamRepoOwnershipActiveRow {
	type claim struct{ provider, repoFullName, teamID string }
	starts := map[claim]time.Time{}
	for _, row := range derived {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" || (row.EvidenceFrom.IsZero() && row.validFrom.IsZero()) {
			continue
		}
		starts[claim{info.Provider, info.FullName, row.TeamID}] = inferredValidFrom(row, now)
	}
	var superseded []teamRepoOwnershipActiveRow
	for _, row := range activeRows {
		start, ok := starts[claim{row.Provider, row.RepoFullName, row.TeamID}]
		if ok && !row.ValidFrom.UTC().Truncate(time.Millisecond).Equal(start) {
			superseded = append(superseded, row)
		}
	}
	return superseded
}

func loadTeamRepoOwnershipProjectLinks(
	ctx context.Context, conn driver.Conn, orgID string, asOf time.Time,
) ([]TeamRepoOwnershipProjectLink, error) {
	// GROUP BY + argMax(field, (updated_at, valid_from)), not FINAL: FINAL
	// only collapses rows sharing the exact ReplacingMergeTree ORDER BY key
	// (org_id, provider, project_id, team_id, source, valid_from) -- a
	// re-import that corrects specificity/is_primary under a NEW valid_from
	// is a DISTINCT key FINAL never merges, so a stale higher-specificity
	// generation could keep outranking a newer correction indefinitely.
	// Mirrors metrics/loaders/clickhouse.py's load_team_attribution_context
	// (same GROUP BY + argMax shape, same tie-break tuple) exactly (codex
	// adversarial review, 2026-08-28, confirmed finding).
	// GROUP BY + argMax(field, (updated_at, valid_from)), not FINAL: FINAL
	// only collapses rows sharing the exact ReplacingMergeTree ORDER BY key
	// (org_id, provider, project_id, team_id, source, valid_from) -- a
	// re-import that corrects specificity/is_primary under a NEW valid_from
	// is a DISTINCT key FINAL never merges, so a stale higher-specificity
	// generation could keep outranking a newer correction indefinitely.
	// Mirrors metrics/loaders/clickhouse.py's load_team_attribution_context
	// (same GROUP BY + argMax shape, same tie-break tuple) exactly (codex
	// adversarial review, 2026-08-28, confirmed finding).
	// Named {org_id:String}/{as_of:DateTime64(3, 'UTC')} parameters, not
	// positional `?`: this driver round-trips a positional `?` bound to a
	// time.Time at whole-SECOND precision (confirmed empirically -- SELECT
	// toString(?) with a sub-second time.Time argument returns the value
	// with its fraction dropped), so `valid_from <= ?` can read a row
	// written earlier in the SAME wall-clock second as not-yet-valid for up
	// to a full second. The named form preserves the DateTime64(3) scale
	// teamscope.go's RepoCondition already established for exactly this
	// reason (see its own doc comment on the same table).
	//
	// asOf is formatted as a literal string, not bound as a raw time.Time:
	// clickhouse-go renders a bare time.Time bound to a {name:DateTime64(...)}
	// placeholder as a `toDateTime(...)` expression, and the server REJECTS
	// that ("cannot be parsed as DateTime64(3, 'UTC')... isn't parsed
	// completely") -- a named parameter is parsed as a literal, not
	// evaluated as an expression. Same documented fix as
	// internal/jobs/metrics/remaining/dora_native_clickhouse.go's
	// dateTime64Argument/DateTime64Argument.
	//
	// Two levels. The inner one takes the newest stored version of each row
	// key, so its valid_to is the current one: a row is closed by writing
	// the same key again with valid_to set, and until a merge both versions
	// are stored. A valid_to filter on the stored rows would still see the
	// open version and return a closed link. The outer level then collapses
	// the generations of one (provider, project, team) as before. valid_to
	// is Nullable, so it is tuple-wrapped: argMax skips a NULL value.
	rows, err := conn.Query(ctx, `
SELECT
    o.provider,
    o.project_id,
    o.team_id,
    argMax(o.is_primary, (o.version_at, o.valid_from)) AS is_primary,
    argMax(o.specificity, (o.version_at, o.valid_from)) AS specificity
FROM (
    SELECT
        v.provider, v.project_id, v.team_id, v.source, v.valid_from,
        (argMax(tuple(v.valid_to), v.updated_at)).1 AS valid_to,
        argMax(v.is_primary, v.updated_at) AS is_primary,
        argMax(v.specificity, v.updated_at) AS specificity,
        max(v.updated_at) AS version_at
    FROM team_project_ownership AS v
    WHERE v.org_id = {org_id:String}
      AND v.project_id != ''
      AND v.team_id != ''
    GROUP BY v.provider, v.project_id, v.team_id, v.source, v.valid_from
) AS o
WHERE o.valid_from <= {as_of:DateTime64(3, 'UTC')}
  AND (o.valid_to IS NULL OR o.valid_to > {as_of:DateTime64(3, 'UTC')})
GROUP BY o.provider, o.project_id, o.team_id`,
		clickhouse.Named("org_id", orgID), clickhouse.Named("as_of", asOf.UTC().Format("2006-01-02 15:04:05.000")))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipProjectLink
	for rows.Next() {
		var link TeamRepoOwnershipProjectLink
		var isPrimary uint8
		if err := rows.Scan(&link.Provider, &link.ProjectID, &link.TeamID, &isPrimary, &link.Specificity); err != nil {
			return nil, err
		}
		link.IsPrimary = isPrimary == 1
		out = append(out, link)
	}
	return out, rows.Err()
}

// loadTeamRepoOwnershipTeamLinkProviders reads every (provider, team) pair a
// team_project_ownership row of the org names, open or closed: the providers
// whose links can have derived a row of the team (the retraction guard).
func loadTeamRepoOwnershipTeamLinkProviders(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipTeamLinkProvider, error) {
	rows, err := conn.Query(ctx, `
SELECT DISTINCT provider, team_id
FROM team_project_ownership
WHERE org_id = ? AND team_id != ''`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipTeamLinkProvider
	for rows.Next() {
		var link TeamRepoOwnershipTeamLinkProvider
		if err := rows.Scan(&link.Provider, &link.TeamID); err != nil {
			return nil, err
		}
		out = append(out, link)
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipRepos(
	ctx context.Context, conn driver.Conn, orgID string,
) (map[uuid.UUID]teamRepoOwnershipRepoInfo, error) {
	rows, err := conn.Query(ctx, `
SELECT id, provider, repo
FROM repos FINAL
WHERE org_id = ?`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[uuid.UUID]teamRepoOwnershipRepoInfo{}
	for rows.Next() {
		var id uuid.UUID
		var info teamRepoOwnershipRepoInfo
		if err := rows.Scan(&id, &info.Provider, &info.FullName); err != nil {
			return nil, err
		}
		if id == uuid.Nil {
			continue
		}
		out[id] = info
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipWorkItems(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipWorkItem, error) {
	// native_team_key (migration 050) alongside project_id -- CHAOS-4458
	// part (b): a Linear work item's project_id is a raw Linear Project UUID,
	// a SEPARATE id space from team_project_ownership's Linear rows (keyed
	// "{org_id}:linear:{team_key}"); native_team_key carries the raw Linear
	// team key (issue.team.key) that DOES match. Empty string (ClickHouse's
	// column default, migration 050) for every non-Linear provider.
	rows, err := conn.Query(ctx, `
SELECT work_item_id, provider, type, repo_id, project_id, native_team_key, created_at
FROM work_items FINAL
WHERE org_id = ?`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipWorkItem
	for rows.Next() {
		var item TeamRepoOwnershipWorkItem
		var repoID uuid.UUID
		if err := rows.Scan(&item.WorkItemID, &item.Provider, &item.Type, &repoID, &item.ProjectID, &item.NativeTeamKey, &item.CreatedAt); err != nil {
			return nil, err
		}
		item.CreatedAt = evidenceTime(item.CreatedAt)
		if repoID != uuid.Nil {
			item.RepoID = repoID.String()
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

// loadTeamRepoOwnershipKnownTeams loads this org's CURRENT team catalog
// (CHAOS-4537 codex review, round 2, P1) -- deriveTeamRepoOwnership's
// linear_team_key arm uses this to validate a Linear work item's
// native_team_key before trusting it as a resolved team_id, exactly like
// every other native-team resolution in this codebase (see
// TeamRepoOwnershipKnownTeam's doc comment). `teams` has been keyed
// org_id-first since migration 061 (ReplacingMergeTree ORDER BY
// (org_id, id)); GROUP BY + argMax on `is_active`, scoped by the WHERE
// org_id clause below, follows the same tuple-argMax convention this
// codebase's other `teams` readers use rather than reading via a plain
// `FINAL`. Scoped to provider='linear' since that is the only provider this
// validation set is used for.
func loadTeamRepoOwnershipKnownTeams(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipKnownTeam, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, id, argMax(is_active, (updated_at, last_synced, is_active)) AS is_active,
       ifNull((argMax(tuple(native_team_key), (updated_at, last_synced, ifNull(native_team_key, '')))).1, '') AS native_team_key
FROM teams
WHERE org_id = ? AND provider = 'linear'
GROUP BY provider, id`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipKnownTeam
	for rows.Next() {
		var provider, id, nativeTeamKey string
		var isActive uint8
		if err := rows.Scan(&provider, &id, &isActive, &nativeTeamKey); err != nil {
			return nil, err
		}
		if isActive != 0 {
			out = append(out, TeamRepoOwnershipKnownTeam{Provider: provider, ID: id, NativeTeamKey: nativeTeamKey})
		}
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipDependencyEdges(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipDependencyEdge, error) {
	// work_item_dependencies gained org_id in 024_add_org_id.sql -- filter
	// directly on it. (An earlier version of this query scoped indirectly
	// through an INNER JOIN against work_items, on the mistaken belief this
	// table had no org_id column at all; that let a same-named work_item_id
	// from a DIFFERENT org's dependency row leak into this org's donor walk
	// whenever ids collided across tenants -- codex adversarial review,
	// 2026-08-28, confirmed high-severity tenant-isolation finding.)
	rows, err := conn.Query(ctx, `
SELECT d.source_work_item_id, d.target_work_item_id, d.relationship_type, d.last_synced
FROM work_item_dependencies AS d FINAL
WHERE d.org_id = ?`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipDependencyEdge
	for rows.Next() {
		var edge TeamRepoOwnershipDependencyEdge
		if err := rows.Scan(
			&edge.SourceWorkItemID, &edge.TargetWorkItemID,
			&edge.RelationshipType, &edge.LastSynced,
		); err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, rows.Err()
}

func loadTeamRepoOwnershipIssuePRLinks(
	ctx context.Context, conn driver.Conn, orgID string, repoIDs []uuid.UUID,
) ([]TeamRepoOwnershipIssuePRLink, error) {
	if len(repoIDs) == 0 {
		return nil, nil
	}
	// work_graph_issue_pr gained org_id in 024_add_org_id.sql -- filter on
	// it directly, same fix as loadTeamRepoOwnershipDependencyEdges. The
	// repo_id IN (?) filter is kept too (defense in depth: this org's own
	// repo set, loaded separately), but org_id is the authoritative scope.
	rows, err := conn.Query(ctx, `
SELECT l.repo_id, l.work_item_id, l.pr_number, l.provenance, pr.created_at, pr.matched
FROM (
    SELECT repo_id, work_item_id, pr_number, provenance
    FROM work_graph_issue_pr FINAL
    WHERE org_id = ? AND repo_id IN (?)
) AS l
LEFT JOIN (
    SELECT repo_id, number, created_at, 1 AS matched
    FROM git_pull_requests FINAL
    WHERE org_id = ? AND repo_id IN (?)
) AS pr ON pr.repo_id = l.repo_id AND pr.number = l.pr_number`,
		orgID, repoIDs, orgID, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipIssuePRLink
	for rows.Next() {
		var link TeamRepoOwnershipIssuePRLink
		var repoID uuid.UUID
		var prCreatedAt time.Time
		var matched uint8
		if err := rows.Scan(&repoID, &link.WorkItemID, &link.PRNumber, &link.Provenance, &prCreatedAt, &matched); err != nil {
			return nil, err
		}
		// An unmatched LEFT JOIN reads the type's zero, never NULL: only a
		// matched pull request gives an evidence time.
		if matched == 1 {
			link.EvidenceAt = evidenceTime(prCreatedAt)
		}
		if repoID == uuid.Nil {
			continue
		}
		link.RepoID = repoID.String()
		out = append(out, link)
	}
	return out, rows.Err()
}

// teamRepoOwnershipActiveRow is one currently-active (valid_to IS NULL or
// still in the future) source='inferred' row for this org -- the "prior
// claim" snapshot retraction diffs the new derivation against. Carries
// every column the ReplacingMergeTree ORDER BY key needs so a retraction
// can replace it exactly (org_id, provider, repo_full_name, team_id,
// source, valid_from), plus the remaining columns so the replacement row is
// otherwise identical to the row it retracts.
type teamRepoOwnershipActiveRow struct {
	Provider     string
	TeamID       string
	RepoID       uuid.UUID
	RepoFullName string
	MatchType    string
	IsPrimary    uint8
	Specificity  uint16
	Priority     int32
	ValidFrom    time.Time
	// Share is the row's stored share; nil for a row with none (written before
	// the column, or by another writer).
	Share *float64
}

func loadTeamRepoOwnershipActiveInferredRows(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]teamRepoOwnershipActiveRow, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, team_id, repo_id, repo_full_name, match_type, is_primary, specificity, priority, valid_from, share
FROM team_repo_ownership FINAL
WHERE org_id = ?
  AND source = 'inferred'
  AND (valid_to IS NULL OR valid_to > now64(3, 'UTC'))`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []teamRepoOwnershipActiveRow
	for rows.Next() {
		var row teamRepoOwnershipActiveRow
		var repoID uuid.UUID
		if err := rows.Scan(
			&row.Provider, &row.TeamID, &repoID, &row.RepoFullName, &row.MatchType,
			&row.IsPrimary, &row.Specificity, &row.Priority, &row.ValidFrom, &row.Share,
		); err != nil {
			return nil, err
		}
		row.RepoID = repoID
		out = append(out, row)
	}
	return out, rows.Err()
}

// diffTeamRepoOwnershipRetractions returns every activeRows entry whose
// (provider, repo_full_name, team_id) identity is absent from the
// newly-derived set, except the open row of a team in a tie on that repo: a
// full tie names no owner, so a tied team's existing open row stays. A row
// of a team that is not in the tie is retracted. The identity is the one the
// table keys on, so a same-named repo of another provider is a different
// repo and neither a derived row nor a tie on one protects the other.
// Pure, no I/O. Resolves each derived row's repo through the SAME repos
// snapshot writeTeamRepoOwnershipRows uses, so a derived row that
// writeTeamRepoOwnershipRows would itself skip (unresolvable repo_id) never
// wrongly protects an active row from retraction.
func diffTeamRepoOwnershipRetractions(
	activeRows []teamRepoOwnershipActiveRow,
	derived []DerivedTeamRepoOwnershipRow,
	ties []TeamRepoOwnershipTie,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) []teamRepoOwnershipActiveRow {
	type claim struct{ provider, repoFullName, teamID string }
	resolve := func(id string) (teamRepoOwnershipRepoInfo, bool) {
		repoID, err := uuid.Parse(id)
		if err != nil || repoID == uuid.Nil {
			return teamRepoOwnershipRepoInfo{}, false
		}
		info, ok := repos[repoID]
		return info, ok && info.FullName != ""
	}
	desired := make(map[claim]bool, len(derived))
	for _, row := range derived {
		if info, ok := resolve(row.RepoID); ok {
			desired[claim{provider: info.Provider, repoFullName: info.FullName, teamID: row.TeamID}] = true
		}
	}
	tied := make(map[claim]bool)
	for _, tie := range ties {
		if info, ok := resolve(tie.RepoID); ok {
			for _, teamID := range tie.TeamIDs {
				tied[claim{provider: info.Provider, repoFullName: info.FullName, teamID: teamID}] = true
			}
		}
	}
	var toRetract []teamRepoOwnershipActiveRow
	for _, row := range activeRows {
		key := claim{provider: row.Provider, repoFullName: row.RepoFullName, teamID: row.TeamID}
		if desired[key] || tied[key] {
			continue
		}
		toRetract = append(toRetract, row)
	}
	return toRetract
}

// retractTeamRepoOwnershipRows closes out every row in toRetract: a
// replacement under the SAME ReplacingMergeTree key (org_id, provider,
// repo_full_name, team_id, source, valid_from) with valid_to=now and a
// newer updated_at, so FINAL/argMax(updated_at) readers see it as expired
// on their very next query.
func retractTeamRepoOwnershipRows(
	ctx context.Context,
	conn driver.Conn,
	orgID string,
	now time.Time,
	toRetract []teamRepoOwnershipActiveRow,
) (int, error) {
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipShareInsert)
	if err != nil {
		return 0, err
	}
	defer releaseTeamRepoOwnershipBatch(ctx, orgID, batch)
	for _, row := range toRetract {
		if err := batch.Append(
			orgID,
			row.Provider,
			row.TeamID,
			row.RepoID,
			row.RepoFullName,
			row.MatchType,
			"inferred",
			row.IsPrimary,
			row.Specificity,
			row.Priority,
			row.ValidFrom,
			now,
			now,
			row.Share,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(toRetract), nil
}

// supersedeTeamRepoOwnershipRows closes each row at its own valid_from (a
// replacement under the same key with valid_to = valid_from and a newer
// updated_at): the row then covers no instant, and the claim's row at its
// evidence time is the one open row. Logged with the count.
func supersedeTeamRepoOwnershipRows(
	ctx context.Context,
	conn driver.Conn,
	orgID string,
	now time.Time,
	rows []teamRepoOwnershipActiveRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipShareInsert)
	if err != nil {
		return 0, err
	}
	defer releaseTeamRepoOwnershipBatch(ctx, orgID, batch)
	for _, row := range rows {
		if err := batch.Append(
			orgID, row.Provider, row.TeamID, row.RepoID, row.RepoFullName, row.MatchType, "inferred",
			row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidFrom, now, row.Share,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	slog.Default().InfoContext(ctx, "team_repo_ownership_derivation.superseded_by_evidence_start", "org_id", orgID, "rows", len(rows))
	return len(rows), nil
}

// releaseTeamRepoOwnershipBatch aborts a batch that was never sent. A
// prepared batch holds a pool connection until Send or Abort, so a return
// without either (no writable row, an Append error) leaks the connection.
func releaseTeamRepoOwnershipBatch(ctx context.Context, orgID string, batch driver.Batch) {
	if batch.IsSent() {
		return
	}
	if err := batch.Abort(); err != nil {
		slog.Default().WarnContext(ctx, "team_repo_ownership_derivation.batch_abort_failed", "org_id", orgID, "error", err)
	}
}

const teamRepoOwnershipInsert = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

// teamRepoOwnershipShareInsert is teamRepoOwnershipInsert with the inferred
// row's share: the insert of this derivation's own writes, retractions and
// supersedes. teamRepoOwnershipInsert keeps its column list for its other
// users (the Jira project-as-team retire builds an INSERT ... SELECT on it).
const teamRepoOwnershipShareInsert = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share)`

// writeTeamRepoOwnershipRows batch-inserts the derived rows. A derived row
// whose repo_id has no matching repos entry (should not happen -- every
// RepoID came from either work_items.repo_id or work_graph_issue_pr.repo_id,
// both scoped to repos already loaded for this org -- but a race between the
// repos snapshot and the work_items/work_graph_issue_pr snapshot is possible
// under concurrent sync) is skipped rather than written with a blank
// repo_full_name, which would collide this table's ORDER BY key across
// unrelated repos owned by the same team.
//
// armCounts (CHAOS-4458 part (b)) is tallied from the SAME rows that make it
// into the batch, and is only meaningful once Send() has actually succeeded
// -- an error return (from Append or Send) always pairs with a nil armCounts,
// mirroring the existing written=0-on-error contract, so a caller can never
// export a resolution-arm count for a row that was not actually committed
// (codex adversarial review, 2026-08-29, confirmed finding: counting
// pre-write candidates let the metric report rows that were skipped for a
// missing repo snapshot, or never committed at all because the whole batch
// failed).
func writeTeamRepoOwnershipRows(
	ctx context.Context,
	conn driver.Conn,
	orgID string,
	now time.Time,
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) (int, map[string]int, error) {
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipShareInsert)
	if err != nil {
		return 0, nil, err
	}
	defer releaseTeamRepoOwnershipBatch(ctx, orgID, batch)
	written := 0
	armCounts := map[string]int{}
	for _, row := range derived {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			continue
		}
		if err := batch.Append(
			orgID,
			info.Provider,
			row.TeamID,
			repoID,
			info.FullName,
			"exact",
			"inferred",
			// 1 for the main owner, 2 for a co-owner (ruling D5988).
			row.IsPrimary,
			row.Specificity,
			teamRepoOwnershipPrecedence[teamRepoOwnershipSourceKindInferred].Priority,
			inferredValidFrom(row, now),
			nil,
			now,
			row.Share,
		); err != nil {
			return written, nil, err
		}
		written++
		if row.ResolutionArm != "" {
			armCounts[row.ResolutionArm]++
		}
	}
	if written == 0 {
		return 0, nil, nil
	}
	if err := batch.Send(); err != nil {
		return 0, nil, err
	}
	return written, armCounts, nil
}
