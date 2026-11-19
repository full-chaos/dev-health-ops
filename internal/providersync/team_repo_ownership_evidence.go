package providersync

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks/witness"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/linktier"
)

// The input evidence of the derived team_repo_ownership rows (CHAOS-9349).
//
// A derived row is retracted when the run no longer derives it. The kind
// guard (team_repo_ownership_retraction_guard.go) proves that the run read
// the inputs of the row's provider and arm; it cannot tell a changed input row
// from a missing one. A missing row (a sync gap, a partial write, a delete
// that is not a removal) derives nothing, the same as a row that no longer
// names the team. So every run records, for every claim it derives, the keys
// of the input rows of every chain that gave the claim a link: the work item,
// the dependency edge and the donor (the chosen donor only: a donor that
// teamattribution.LinkedIssueDonorRefusal refuses is never chosen, so it
// records no key), the work_graph_issue_pr row, and the project link or the
// Linear team that names the team. A key is the sort key of the stored row
// (team_project_ownership: (provider, project_id, team_id), coarser than its
// sort key, ruling 10). A claim the run does not derive closes only when EVERY
// recorded key of it is held by the run: present in the run's rows, with any
// value (AbsenceByInputEvidence). A missing key holds the close. A
// work_graph_issue_pr key is also held when the native-first rule removed it
// (issueprlinks.PlanOutrankedFallbacks): a native link of its pull request or
// issue is in the run, or its issue side is a pull request.
//
// A claim with no recorded key (written before the store) follows the kind
// guard alone and is logged with the reason no_recorded_evidence: a residual.

const (
	teamRepoOwnershipEvidenceWorkItem    = "work_item"
	teamRepoOwnershipEvidenceDependency  = "dependency"
	teamRepoOwnershipEvidenceIssuePR     = "issue_pr"
	teamRepoOwnershipEvidenceProjectLink = "project_link"
	teamRepoOwnershipEvidenceLinearTeam  = "linear_team"

	// teamRepoOwnershipRetractionKindInputEvidence is the held kind of a close
	// whose recorded input row is not in the run.
	teamRepoOwnershipRetractionKindInputEvidence = "input_evidence"
	// TeamRepoOwnershipEvidenceRowMissing prefixes the reason of a held close:
	// evidence_row_missing_<input>.
	TeamRepoOwnershipEvidenceRowMissing = "evidence_row_missing_"
	// TeamRepoOwnershipNoRecordedEvidence is the reason of a close of a claim
	// with no recorded key (the bootstrap rule).
	TeamRepoOwnershipNoRecordedEvidence = "no_recorded_evidence"
	// TeamRepoOwnershipRetractedWithoutEvidenceLog is the INFO line of those closes.
	TeamRepoOwnershipRetractedWithoutEvidenceLog = "team_repo_ownership_derivation.retracted_without_evidence"
)

// teamRepoOwnershipEvidenceKey is one input row: the input table's name and
// the row's key, a JSON array of the key's columns without org_id.
type teamRepoOwnershipEvidenceKey struct{ Input, Key string }

func newTeamRepoOwnershipEvidenceKey(input string, columns ...string) teamRepoOwnershipEvidenceKey {
	encoded, _ := json.Marshal(columns)
	return teamRepoOwnershipEvidenceKey{Input: input, Key: string(encoded)}
}

// teamRepoOwnershipChain is a resolved team and the keys of the rows that
// resolved it.
type teamRepoOwnershipChain struct {
	teamID, arm string
	evidence    []teamRepoOwnershipEvidenceKey
}

// work_items ORDER BY (org_id, repo_id, work_item_id); "" is the nil repo.
func teamRepoOwnershipWorkItemEvidence(item TeamRepoOwnershipWorkItem) teamRepoOwnershipEvidenceKey {
	return newTeamRepoOwnershipEvidenceKey(teamRepoOwnershipEvidenceWorkItem, item.RepoID, item.WorkItemID)
}

// work_item_dependencies ORDER BY (org_id, source, target, relationship_type):
// the raw stored target, an extkey text included.
func teamRepoOwnershipDependencyEvidence(edge TeamRepoOwnershipDependencyEdge) teamRepoOwnershipEvidenceKey {
	return newTeamRepoOwnershipEvidenceKey(teamRepoOwnershipEvidenceDependency,
		edge.SourceWorkItemID, edge.TargetWorkItemID, edge.RelationshipType)
}

// work_graph_issue_pr ORDER BY (org_id, repo_id, work_item_id, pr_number), no provenance.
func teamRepoOwnershipIssuePREvidence(link TeamRepoOwnershipIssuePRLink) teamRepoOwnershipEvidenceKey {
	return newTeamRepoOwnershipEvidenceKey(teamRepoOwnershipEvidenceIssuePR,
		link.RepoID, link.WorkItemID, strconv.FormatUint(uint64(link.PRNumber), 10))
}

func teamRepoOwnershipProjectLinkEvidence(provider, storedProjectID, teamID string) teamRepoOwnershipEvidenceKey {
	return newTeamRepoOwnershipEvidenceKey(teamRepoOwnershipEvidenceProjectLink, provider, storedProjectID, teamID)
}

// teams ORDER BY (org_id, id).
func teamRepoOwnershipLinearTeamEvidence(teamID string) teamRepoOwnershipEvidenceKey {
	return newTeamRepoOwnershipEvidenceKey(teamRepoOwnershipEvidenceLinearTeam, teamID)
}

// teamRepoOwnershipTeamEvidence gives the key of the row that names an item's
// team: the stored project link of the resolved team (the smallest stored
// project_id when a remap joined two), or the Linear team.
func teamRepoOwnershipTeamEvidence(
	links []TeamRepoOwnershipProjectLink, projectToTeam map[teamRepoOwnershipProjectRef]string,
) func(TeamRepoOwnershipWorkItem, string, string) []teamRepoOwnershipEvidenceKey {
	stored := map[teamRepoOwnershipProjectRef]string{}
	for _, link := range links {
		ref := teamRepoOwnershipProjectRef{Provider: link.Provider, ProjectID: link.ProjectID}
		if link.ProjectID == "" || link.TeamID == "" || projectToTeam[ref] != link.TeamID {
			continue
		}
		id := link.StoredProjectID
		if id == "" {
			id = link.ProjectID
		}
		if held, ok := stored[ref]; !ok || id < held {
			stored[ref] = id
		}
	}
	return func(item TeamRepoOwnershipWorkItem, teamID, arm string) []teamRepoOwnershipEvidenceKey {
		switch arm {
		case TeamRepoOwnershipResolutionArmProjectID:
			ref := teamRepoOwnershipProjectRef{Provider: item.Provider, ProjectID: item.ProjectID}
			if id, ok := stored[ref]; ok {
				return []teamRepoOwnershipEvidenceKey{teamRepoOwnershipProjectLinkEvidence(item.Provider, id, teamID)}
			}
		case TeamRepoOwnershipResolutionArmLinearTeamKey:
			return []teamRepoOwnershipEvidenceKey{teamRepoOwnershipLinearTeamEvidence(teamID)}
		}
		return nil
	}
}

// teamRepoOwnershipEvidenceHeld is the set of input rows one run holds, and
// the native links of the run: the witnesses of
// issueprlinks.PlanOutrankedFallbacks, by the one rule of the leaf package
// witness (providersync must not import issueprlinks: its logging would join
// this package's dependency set).
type teamRepoOwnershipEvidenceHeld struct {
	keys   map[teamRepoOwnershipEvidenceKey]struct{}
	native witness.NativeLinks
}

// newTeamRepoOwnershipEvidenceHeld indexes the run's rows. linkKeys is every
// stored project link key of the org, open or closed (a closed link is a
// changed row, not a missing one); linearTeams is every Linear team id of the
// catalog, active or not.
func newTeamRepoOwnershipEvidenceHeld(
	workItems []TeamRepoOwnershipWorkItem,
	edges []TeamRepoOwnershipDependencyEdge,
	issuePRLinks []TeamRepoOwnershipIssuePRLink,
	linkKeys []teamRepoOwnershipEvidenceKey,
	linearTeams []string,
) teamRepoOwnershipEvidenceHeld {
	held := teamRepoOwnershipEvidenceHeld{keys: map[teamRepoOwnershipEvidenceKey]struct{}{}, native: witness.NewNativeLinks()}
	for _, item := range workItems {
		held.keys[teamRepoOwnershipWorkItemEvidence(item)] = struct{}{}
	}
	for _, edge := range edges {
		held.keys[teamRepoOwnershipDependencyEvidence(edge)] = struct{}{}
	}
	for _, link := range issuePRLinks {
		held.keys[teamRepoOwnershipIssuePREvidence(link)] = struct{}{}
		if link.Provenance != linktier.Native {
			continue
		}
		if repoID, err := uuid.Parse(link.RepoID); err == nil {
			held.native.Add(repoID, link.PRNumber, link.WorkItemID)
		}
	}
	for _, key := range linkKeys {
		held.keys[key] = struct{}{}
	}
	for _, teamID := range linearTeams {
		held.keys[teamRepoOwnershipLinearTeamEvidence(teamID)] = struct{}{}
	}
	return held
}

// holds reports whether the run holds the row of key, or (a
// work_graph_issue_pr row) a witness of its removal by the native-first rule.
func (held teamRepoOwnershipEvidenceHeld) holds(key teamRepoOwnershipEvidenceKey) bool {
	if _, ok := held.keys[key]; ok {
		return true
	}
	if key.Input != teamRepoOwnershipEvidenceIssuePR {
		return false
	}
	var columns []string
	if err := json.Unmarshal([]byte(key.Key), &columns); err != nil || len(columns) != 3 {
		return false
	}
	repoID, err := uuid.Parse(columns[0])
	number, numberErr := strconv.ParseUint(columns[2], 10, 32)
	if err != nil || numberErr != nil {
		return false
	}
	return held.native.Outranks(repoID, uint32(number), columns[1]) || witness.IsPullRequestWorkItem(columns[1])
}

// teamRepoOwnershipEvidence is the recorded keys of the org's claims and the
// rows the run holds: the absence proof of the kind inferred_repo_ownership.
type teamRepoOwnershipEvidence struct {
	recorded map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}
	held     teamRepoOwnershipEvidenceHeld
	absence  AbsenceProof[InferredRepoOwnershipClaim]
}

func newTeamRepoOwnershipEvidence(
	recorded map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}, held teamRepoOwnershipEvidenceHeld,
) teamRepoOwnershipEvidence {
	evidence := teamRepoOwnershipEvidence{recorded: recorded, held: held}
	evidence.absence = AbsenceByInputEvidence(evidence.missing)
	return evidence
}

// missing is the reasons of the recorded keys of a claim that the run does
// not hold, sorted; empty when it holds every one (or none is recorded).
func (evidence teamRepoOwnershipEvidence) missing(claim InferredRepoOwnershipClaim) []string {
	reasons := map[string]struct{}{}
	for key := range evidence.recorded[claim] {
		if !evidence.held.holds(key) {
			reasons[TeamRepoOwnershipEvidenceRowMissing+key.Input] = struct{}{}
		}
	}
	out := make([]string, 0, len(reasons))
	for reason := range reasons {
		out = append(out, reason)
	}
	sort.Strings(out)
	return out
}

// proves reports whether the absence of the claim is proven.
func (evidence teamRepoOwnershipEvidence) proves(claim InferredRepoOwnershipClaim) bool {
	return InferredRepoOwnershipKind().Holds(claim) && evidence.absence.of(claim) == SnapshotAbsenceProven
}

// hasRecord reports whether the claim has a recorded key.
func (evidence teamRepoOwnershipEvidence) hasRecord(claim InferredRepoOwnershipClaim) bool {
	return len(evidence.recorded[claim]) > 0
}

func inferredClaimOf(row teamRepoOwnershipActiveRow) InferredRepoOwnershipClaim {
	return InferredRepoOwnershipClaim{Provider: row.Provider, RepoFullName: row.RepoFullName, TeamID: row.TeamID}
}

// retain keeps the closes whose claim's absence is proven, and reports the
// rest by provider and reasons (kind input_evidence).
func (evidence teamRepoOwnershipEvidence) retain(rows []teamRepoOwnershipActiveRow) ([]teamRepoOwnershipActiveRow, int, []teamRepoOwnershipRetractionHeld) {
	var kept []teamRepoOwnershipActiveRow
	heldRows := 0
	byReasons := map[[2]string]*teamRepoOwnershipRetractionHeld{}
	for _, row := range rows {
		claim := inferredClaimOf(row)
		if evidence.proves(claim) {
			kept = append(kept, row)
			continue
		}
		heldRows++
		reasons := evidence.missing(claim)
		group := [2]string{row.Provider, strings.Join(reasons, ",")}
		entry := byReasons[group]
		if entry == nil {
			entry = &teamRepoOwnershipRetractionHeld{Kind: teamRepoOwnershipRetractionKindInputEvidence, Provider: row.Provider, Reasons: reasons}
			byReasons[group] = entry
		}
		entry.Rows++
	}
	held := make([]teamRepoOwnershipRetractionHeld, 0, len(byReasons))
	for _, entry := range byReasons {
		held = append(held, *entry)
	}
	sort.Slice(held, func(i, j int) bool {
		if held[i].Provider != held[j].Provider {
			return held[i].Provider < held[j].Provider
		}
		return strings.Join(held[i].Reasons, ",") < strings.Join(held[j].Reasons, ",")
	})
	return kept, heldRows, held
}

// withoutRecord counts the rows whose claim has no recorded key.
func (evidence teamRepoOwnershipEvidence) withoutRecord(groups ...[]teamRepoOwnershipActiveRow) int {
	count := 0
	for _, rows := range groups {
		for _, row := range rows {
			if !evidence.hasRecord(inferredClaimOf(row)) {
				count++
			}
		}
	}
	return count
}

// teamRepoOwnershipEvidenceWrite is one row of team_repo_ownership_evidence.
type teamRepoOwnershipEvidenceWrite struct {
	claim   InferredRepoOwnershipClaim
	key     teamRepoOwnershipEvidenceKey
	deleted bool
}

// upkeep plans the store's writes after the run's closes. A derived claim
// gets the keys of its chains that are not recorded; a recorded key that the
// run holds and that is in no chain of the claim any more is deleted; a
// recorded key the run does not hold stays (a missing row is never taken as
// gone). Every key of a closed claim (its close was proven, so the run holds
// all of them) is deleted.
func (evidence teamRepoOwnershipEvidence) upkeep(
	derived map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{},
	closed map[InferredRepoOwnershipClaim]struct{},
) []teamRepoOwnershipEvidenceWrite {
	var writes []teamRepoOwnershipEvidenceWrite
	for claim, keys := range derived {
		if _, gone := closed[claim]; gone {
			continue
		}
		recorded := evidence.recorded[claim]
		for key := range keys {
			if _, ok := recorded[key]; !ok {
				writes = append(writes, teamRepoOwnershipEvidenceWrite{claim: claim, key: key})
			}
		}
		for key := range recorded {
			if _, ok := keys[key]; !ok && evidence.held.holds(key) {
				writes = append(writes, teamRepoOwnershipEvidenceWrite{claim: claim, key: key, deleted: true})
			}
		}
	}
	for claim := range closed {
		for key := range evidence.recorded[claim] {
			writes = append(writes, teamRepoOwnershipEvidenceWrite{claim: claim, key: key, deleted: true})
		}
	}
	sort.Slice(writes, func(i, j int) bool {
		a, b := writes[i], writes[j]
		if a.claim != b.claim {
			if a.claim.Provider != b.claim.Provider {
				return a.claim.Provider < b.claim.Provider
			}
			if a.claim.RepoFullName != b.claim.RepoFullName {
				return a.claim.RepoFullName < b.claim.RepoFullName
			}
			return a.claim.TeamID < b.claim.TeamID
		}
		if a.key.Input != b.key.Input {
			return a.key.Input < b.key.Input
		}
		return a.key.Key < b.key.Key
	})
	return writes
}

// teamRepoOwnershipDerivedEvidence keys the derivation's evidence by claim.
func teamRepoOwnershipDerivedEvidence(
	evidence map[teamRepoOwnershipClaimRef]map[teamRepoOwnershipEvidenceKey]struct{},
	repos map[uuid.UUID]teamRepoOwnershipRepoInfo,
) map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{} {
	out := map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}{}
	for ref, keys := range evidence {
		repoID, err := uuid.Parse(ref.RepoID)
		if err != nil {
			continue
		}
		info, ok := repos[repoID]
		if !ok || info.FullName == "" {
			continue
		}
		claim := InferredRepoOwnershipClaim{Provider: info.Provider, RepoFullName: info.FullName, TeamID: ref.TeamID}
		if out[claim] == nil {
			out[claim] = map[teamRepoOwnershipEvidenceKey]struct{}{}
		}
		for key := range keys {
			out[claim][key] = struct{}{}
		}
	}
	return out
}

// loadTeamRepoOwnershipEvidence reads the org's live recorded keys: the
// newest version of each key (a delete wins a tie), then is_deleted.
func loadTeamRepoOwnershipEvidence(
	ctx context.Context, conn driver.Conn, orgID string,
) (map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}, error) {
	rows, err := conn.Query(ctx, `
SELECT provider, repo_full_name, team_id, input, input_key
FROM (
    SELECT provider, repo_full_name, team_id, input, input_key,
           argMax(is_deleted, (seen_at, is_deleted)) AS deleted
    FROM team_repo_ownership_evidence
    WHERE org_id = ?
    GROUP BY provider, repo_full_name, team_id, input, input_key
)
WHERE deleted = 0`,
		orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[InferredRepoOwnershipClaim]map[teamRepoOwnershipEvidenceKey]struct{}{}
	for rows.Next() {
		var claim InferredRepoOwnershipClaim
		var key teamRepoOwnershipEvidenceKey
		if err := rows.Scan(&claim.Provider, &claim.RepoFullName, &claim.TeamID, &key.Input, &key.Key); err != nil {
			return nil, err
		}
		if out[claim] == nil {
			out[claim] = map[teamRepoOwnershipEvidenceKey]struct{}{}
		}
		out[claim][key] = struct{}{}
	}
	return out, rows.Err()
}

// loadTeamRepoOwnershipLinkKeys reads every stored team_project_ownership key
// of the org, open or closed: the project link keys a run holds. The same rows
// give the (provider, team) pairs of the retraction guard.
func loadTeamRepoOwnershipLinkKeys(
	ctx context.Context, conn driver.Conn, orgID string,
) ([]teamRepoOwnershipEvidenceKey, []TeamRepoOwnershipTeamLinkProvider, error) {
	rows, err := conn.Query(ctx, `
SELECT DISTINCT provider, project_id, team_id
FROM team_project_ownership
WHERE org_id = ? AND team_id != ''`,
		orgID)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var keys []teamRepoOwnershipEvidenceKey
	var providers []TeamRepoOwnershipTeamLinkProvider
	seen := map[TeamRepoOwnershipTeamLinkProvider]struct{}{}
	for rows.Next() {
		var provider, projectID, teamID string
		if err := rows.Scan(&provider, &projectID, &teamID); err != nil {
			return nil, nil, err
		}
		if projectID != "" {
			keys = append(keys, teamRepoOwnershipProjectLinkEvidence(provider, projectID, teamID))
		}
		pair := TeamRepoOwnershipTeamLinkProvider{Provider: provider, TeamID: teamID}
		if _, dup := seen[pair]; !dup {
			seen[pair] = struct{}{}
			providers = append(providers, pair)
		}
	}
	return keys, providers, rows.Err()
}

const teamRepoOwnershipEvidenceInsert = `INSERT INTO team_repo_ownership_evidence
(org_id, provider, repo_full_name, team_id, input, input_key, seen_at, is_deleted)`

// writeTeamRepoOwnershipEvidence writes the planned rows at the run's clock,
// so a fixture world's pinned run writes the same rows every time. Two runs
// at one instant (a pinned clock only) give a key one version: a retire wins
// that tie (argMax over (seen_at, is_deleted)), so the store errs toward fewer
// keys, the side that closes as before the store.
func writeTeamRepoOwnershipEvidence(ctx context.Context, conn driver.Conn, orgID string, now time.Time, writes []teamRepoOwnershipEvidenceWrite) error {
	if len(writes) == 0 {
		return nil
	}
	batch, err := conn.PrepareBatch(ctx, teamRepoOwnershipEvidenceInsert)
	if err != nil {
		return err
	}
	defer releaseTeamRepoOwnershipBatch(ctx, orgID, batch)
	seenAt := now.UTC().Truncate(time.Millisecond)
	for _, write := range writes {
		deleted := uint8(0)
		if write.deleted {
			deleted = 1
		}
		if err := batch.Append(orgID, write.claim.Provider, write.claim.RepoFullName, write.claim.TeamID,
			write.key.Input, write.key.Key, seenAt, deleted); err != nil {
			return err
		}
	}
	return batch.Send()
}

// proves reports whether the guard proves every kind that can have derived a
// row of the team.
func (guard teamRepoOwnershipRetractionGuard) proves(teamID string) bool {
	for _, kind := range guard.kindsOf(teamID) {
		if proof, made := guard.proofs[kind]; !made || !proof.Proven() {
			return false
		}
	}
	return true
}

// provenFloorVoids keeps the closed rows of floor-dropped co-owners that the
// run may void: a claim with an open row only when that open row is closed by
// the run (it passed every proof and hold), a claim with no open row only when
// the guard and the recorded evidence prove its absence.
func provenFloorVoids(
	earlier, activeRows, toRetract []teamRepoOwnershipActiveRow,
	guard teamRepoOwnershipRetractionGuard, evidence teamRepoOwnershipEvidence,
) []teamRepoOwnershipActiveRow {
	open := map[InferredRepoOwnershipClaim]bool{}
	for _, row := range activeRows {
		open[inferredClaimOf(row)] = true
	}
	closing := map[InferredRepoOwnershipClaim]bool{}
	for _, row := range toRetract {
		closing[inferredClaimOf(row)] = true
	}
	var out []teamRepoOwnershipActiveRow
	for _, row := range earlier {
		claim := inferredClaimOf(row)
		switch {
		case open[claim]:
			if closing[claim] {
				out = append(out, row)
			}
		case guard.proves(row.TeamID) && evidence.proves(claim):
			out = append(out, row)
		}
	}
	return out
}
