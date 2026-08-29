package providersync

import (
	"context"
	"errors"
	"time"

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
// inputs existed but genuinely produced nothing (every candidate conflicted,
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
// project ownership or donor linkage was removed or reassigned. Retracted
// by writing a replacement row under the SAME (org_id, provider,
// repo_full_name, team_id, source, valid_from) ReplacingMergeTree key with
// valid_to=now and a newer updated_at, so FINAL/argMax(updated_at) readers
// (providers/teams.py::load_team_repo_ownership_map,
// native_status_change.py's _TEAM_REPOSITORIES_SQL -- both already filter
// on valid_to) see it as expired on their very next query.
func (service TeamRepoOwnershipDerivationService) Derive(ctx context.Context, orgID string) (written int, retracted int, inputsReady bool, armCounts map[string]int, err error) {
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

	derived := deriveTeamRepoOwnership(orgID, projectLinks, workItems, dependencyEdges, issuePRLinks, knownTeams)

	activeRows, err := loadTeamRepoOwnershipActiveInferredRows(ctx, service.Conn, orgID)
	if err != nil {
		return 0, 0, true, nil, err
	}
	// CHAOS-4537 codex review round 3, second P1 (confirmed real, traced
	// through diffTeamRepoOwnershipRetractions below): that diff is a single
	// GLOBAL comparison over the whole org's activeRows vs. derived -- it is
	// not scoped per resolution arm. The guard above can let a cycle proceed
	// with projectLinks empty (a Linear-native signal from ONE work item was
	// enough), but `derived` can never reproduce a project_id-arm-derived
	// pair in that state (the arm has no projectToTeam entries to resolve
	// from at all). Diffing anyway would treat "this cycle cannot reconfirm
	// them" as "they're no longer true" and retract every previously-good
	// project_id-arm row for the org, including ones for repos the single
	// Linear item never touches (a mixed-org false retraction). Skip
	// retraction entirely whenever projectLinks is empty; still derive and
	// write any newly-resolvable linear_team_key rows. A later cycle that
	// re-syncs team_project_ownership resumes normal retraction.
	var toRetract []teamRepoOwnershipActiveRow
	if len(projectLinks) > 0 {
		toRetract = diffTeamRepoOwnershipRetractions(activeRows, derived, repos)
	}
	if len(toRetract) > 0 {
		retracted, err = retractTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, toRetract)
		if err != nil {
			return 0, 0, true, nil, err
		}
	}

	if len(derived) == 0 {
		return 0, retracted, true, nil, nil
	}
	// armCounts (CHAOS-4458 part (b)) is computed by writeTeamRepoOwnershipRows
	// itself from the rows it actually COMMITS, not from `derived` up front
	// (codex adversarial review, 2026-08-29, confirmed finding): a derived
	// candidate can still be dropped (unresolvable repo_id) or the whole
	// batch can fail before Send() -- counting from `derived` would report a
	// linear_team_key/project_id row that was never actually written.
	written, armCounts, err = writeTeamRepoOwnershipRows(ctx, service.Conn, orgID, now, derived, repos)
	if err != nil {
		return 0, retracted, true, nil, err
	}
	return written, retracted, true, armCounts, nil
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
	rows, err := conn.Query(ctx, `
SELECT
    provider,
    project_id,
    team_id,
    argMax(is_primary, (updated_at, valid_from)) AS is_primary,
    argMax(specificity, (updated_at, valid_from)) AS specificity
FROM team_project_ownership
WHERE org_id = ?
  AND project_id != ''
  AND team_id != ''
  AND valid_from <= ?
  AND (valid_to IS NULL OR valid_to > ?)
GROUP BY provider, project_id, team_id`,
		orgID, asOf, asOf)
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
		link.IsPrimary = isPrimary != 0
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
SELECT work_item_id, provider, repo_id, project_id, native_team_key
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
		if err := rows.Scan(&item.WorkItemID, &item.Provider, &repoID, &item.ProjectID, &item.NativeTeamKey); err != nil {
			return nil, err
		}
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
// TeamRepoOwnershipKnownTeam's doc comment). GROUP BY + argMax on `is_active`
// mirrors github_work_items_derivation_context.go's loadTeams -- the
// established convention for reading `teams` here, since its
// ReplacingMergeTree ORDER BY is `(id)` alone (no org_id), so a plain `FINAL`
// filtered only by a WHERE clause is not itself a safe per-org collapse.
// Scoped to provider='linear' since that is the only provider this
// validation set is used for.
func loadTeamRepoOwnershipKnownTeams(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]TeamRepoOwnershipKnownTeam, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, id, argMax(is_active, (updated_at, last_synced, is_active)) AS is_active
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
		var provider, id string
		var isActive uint8
		if err := rows.Scan(&provider, &id, &isActive); err != nil {
			return nil, err
		}
		if isActive != 0 {
			out = append(out, TeamRepoOwnershipKnownTeam{Provider: provider, ID: id})
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
SELECT repo_id, work_item_id, pr_number
FROM work_graph_issue_pr FINAL
WHERE org_id = ? AND repo_id IN (?)`,
		orgID, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TeamRepoOwnershipIssuePRLink
	for rows.Next() {
		var link TeamRepoOwnershipIssuePRLink
		var repoID uuid.UUID
		if err := rows.Scan(&repoID, &link.WorkItemID, &link.PRNumber); err != nil {
			return nil, err
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
	IsPrimary    bool
	Specificity  uint16
	Priority     int32
	ValidFrom    time.Time
}

func loadTeamRepoOwnershipActiveInferredRows(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]teamRepoOwnershipActiveRow, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, team_id, repo_id, repo_full_name, match_type, is_primary, specificity, priority, valid_from
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
		var isPrimary uint8
		if err := rows.Scan(
			&row.Provider, &row.TeamID, &repoID, &row.RepoFullName, &row.MatchType,
			&isPrimary, &row.Specificity, &row.Priority, &row.ValidFrom,
		); err != nil {
			return nil, err
		}
		row.RepoID = repoID
		row.IsPrimary = isPrimary != 0
		out = append(out, row)
	}
	return out, rows.Err()
}

// diffTeamRepoOwnershipRetractions returns every activeRows entry whose
// (team_id, repo_full_name) pair is absent from the newly-derived set --
// pure, no I/O, exhaustively unit-testable. Resolves each derived row's
// repo_full_name via the SAME repos snapshot writeTeamRepoOwnershipRows
// uses, so a derived row that writeTeamRepoOwnershipRows would itself skip
// (unresolvable repo_id) never wrongly protects an active row from
// retraction.
func diffTeamRepoOwnershipRetractions(
	activeRows []teamRepoOwnershipActiveRow,
	derived []DerivedTeamRepoOwnershipRow,
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) []teamRepoOwnershipActiveRow {
	type pair struct{ teamID, repoFullName string }
	desired := make(map[pair]bool, len(derived))
	for _, row := range derived {
		repoID, err := uuid.Parse(row.RepoID)
		if err != nil || repoID == uuid.Nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			continue
		}
		desired[pair{teamID: row.TeamID, repoFullName: info.FullName}] = true
	}
	var toRetract []teamRepoOwnershipActiveRow
	for _, row := range activeRows {
		if desired[pair{teamID: row.TeamID, repoFullName: row.RepoFullName}] {
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
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipInsert)
	if err != nil {
		return 0, err
	}
	for _, row := range toRetract {
		if err := batch.Append(
			orgID,
			row.Provider,
			row.TeamID,
			row.RepoID,
			row.RepoFullName,
			row.MatchType,
			"inferred",
			uint8(0),
			row.Specificity,
			row.Priority,
			row.ValidFrom,
			now,
			now,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(toRetract), nil
}

const teamRepoOwnershipInsert = `INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at)`

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
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipInsert)
	if err != nil {
		return 0, nil, err
	}
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
			// is_primary=0, NEVER 1: the read path
			// (providers/teams.py::load_team_repo_ownership_map) orders
			// "ORDER BY is_primary DESC, specificity DESC" -- is_primary is
			// checked BEFORE specificity, so an is_primary=1 inferred row
			// would always outrank a real is_primary=0 direct GitHub grant
			// (team_autoimport_github.py writes every row is_primary=0),
			// silently overriding authoritative ownership with a weaker
			// inferred signal. codex adversarial review, 2026-08-28,
			// confirmed high-severity finding. teamRepoOwnershipInferredSpecificity's
			// low value is what keeps this row losing to a direct row when
			// both are is_primary=0; when this is the ONLY row for a repo,
			// is_primary=0 does not stop it from being read -- there is no
			// competing row to lose to.
			uint8(0),
			row.Specificity,
			int32(0),
			now,
			nil,
			now,
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
