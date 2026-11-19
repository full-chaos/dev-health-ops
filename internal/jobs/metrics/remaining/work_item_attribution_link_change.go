package remaining

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining/stepcause"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/linktier"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// Why an issue is owed a rederive because of its work_graph_issue_pr links.
const (
	linkChangeGone       = "gone"
	linkChangeTier       = "tier_changed"
	linkChangeBelowBest  = "below_best_tier"
	linkChangeNewerLink  = "newer_link"
	linkChangeOwnedLink  = "owned_link"
	linkChangeEvidenceIn = "work_graph_issue_pr="
)

// linkChangeStored is what an issue's newest stored row set says about its
// links: the primary row, the set's computed_at, and the link each
// link-derived repo_ownership row names (any is_primary).
type linkChangeStored struct {
	PrimarySource string
	PrimaryTeam   string
	ComputedAt    time.Time
	Named         []teamattribution.GithubWorkItemDerivationLinkedPullRequest
}

// linkChangeLink is one current work_graph_issue_pr row of an issue.
type linkChangeLink struct {
	teamattribution.GithubWorkItemDerivationLinkedPullRequest
	LastSynced time.Time
}

// linkChangeReason says why the stored rows of subject no longer match its
// current links, or "" when they do:
//   - a named link is gone (removed or retracted);
//   - a named link's tier changed;
//   - a named link is below the issue's best tier now;
//   - a best-tier link was written after the stored rows.
//
// A pull or merge request never takes this path.
func linkChangeReason(
	subject teamattribution.GithubWorkItemDerivationSubject, stored linkChangeStored, links []linkChangeLink,
) string {
	if teamattribution.GithubWorkItemDerivationIsPullOrMergeRequestType(subject.Provider, subject.Type) {
		return ""
	}
	best := linkChangeBestRank(links)
	for _, named := range stored.Named {
		current, ok := linkChangeFind(links, named)
		switch {
		case !ok:
			return linkChangeGone
		case current.Tier != named.Tier:
			return linkChangeTier
		case linktier.Rank(current.Tier) < best:
			return linkChangeBelowBest
		}
	}
	for _, link := range links {
		if best > 0 && linktier.Rank(link.Tier) == best && strings.TrimSpace(link.RepoID) != "" &&
			link.LastSynced.After(stored.ComputedAt) {
			return linkChangeNewerLink
		}
	}
	return ""
}

func linkChangeBestRank(links []linkChangeLink) int {
	best := 0
	for _, link := range links {
		if rank := linktier.Rank(link.Tier); rank > best && strings.TrimSpace(link.RepoID) != "" {
			best = rank
		}
	}
	return best
}

func linkChangeFind(links []linkChangeLink, named teamattribution.GithubWorkItemDerivationLinkedPullRequest) (linkChangeLink, bool) {
	for _, link := range links {
		if strings.TrimSpace(link.RepoID) == strings.TrimSpace(named.RepoID) && link.PRNumber == named.PRNumber {
			return link, true
		}
	}
	return linkChangeLink{}, false
}

// linkChangeOwnedLinkCandidate is an issue whose stored primary is
// unassigned or manual_fallback and that has a best-tier link: the cascade may
// now give it a link's repository owner although no link changed after its row
// (a native link carries its dependency row's time).
func linkChangeOwnedLinkCandidate(
	subject teamattribution.GithubWorkItemDerivationSubject, stored linkChangeStored, links []linkChangeLink,
) bool {
	if teamattribution.GithubWorkItemDerivationIsPullOrMergeRequestType(subject.Provider, subject.Type) {
		return false
	}
	if stored.PrimarySource != "unassigned" && stored.PrimarySource != "manual_fallback" {
		return false
	}
	return linkChangeBestRank(links) > 0
}

// linkChangePrimaryDiffers is true when the cascade on derived (the facts of
// the item's day and its links) gives subject another primary row than the
// stored one: the same Resolve the rederive writes with
// (BuildWorkItemAttributionRows), so a rederive makes it false.
func linkChangePrimaryDiffers(
	derived teamattribution.GithubWorkItemDerivationContext,
	subject teamattribution.GithubWorkItemDerivationSubject, stored linkChangeStored,
) bool {
	_, _, candidates := derived.Resolve(subject)
	for _, candidate := range candidates {
		if candidate.IsPrimary == teamattribution.AttributionPrimary {
			return candidate.Source != stored.PrimarySource ||
				teamattribution.GithubWorkItemDerivationStringValue(candidate.TeamID) != stored.PrimaryTeam
		}
	}
	return false
}

// loadLinkChangedIssues returns the issues owed a rederive because of their
// work_graph_issue_pr links (CHAOS-14149). Like the donor-changed dependents,
// the stored rows decide on every run, with no watermark: a link removed by a
// mutation leaves no trace a watermark could read. After the rederive the
// item's rows name only its current best-tier links, are newer than every
// link, and carry the primary the cascade gives, so the next run finds
// nothing.
func (executor *WorkItemAttributionExecutor) loadLinkChangedIssues(
	ctx context.Context, orgID string, now time.Time,
) ([]string, error) {
	candidates, err := executor.loadLinkChangeCandidates(ctx, orgID)
	if err != nil || len(candidates) == 0 {
		return nil, err
	}
	changed := map[string]struct{}{}
	type ownedCandidate struct {
		subject teamattribution.GithubWorkItemDerivationSubject
		stored  linkChangeStored
		links   []linkChangeLink
	}
	var owned []ownedCandidate
	for _, chunk := range chunkStringsByRenderedBytes(candidates, workItemAttributionMaxArrayBytes) {
		stored, err := executor.loadLinkChangeStoredRows(ctx, orgID, chunk)
		if err != nil {
			return nil, err
		}
		links, err := executor.loadLinkChangeLinks(ctx, orgID, chunk)
		if err != nil {
			return nil, err
		}
		subjects, err := executor.loadDonorSubjects(ctx, orgID, chunk, nil)
		if err != nil {
			return nil, err
		}
		for _, id := range chunk {
			subject, known := subjects[id]
			row, rowed := stored[id]
			if !known || !rowed {
				continue
			}
			if linkChangeReason(subject, row, links[id]) != "" {
				changed[id] = struct{}{}
			} else if linkChangeOwnedLinkCandidate(subject, row, links[id]) {
				owned = append(owned, ownedCandidate{subject: subject, stored: row, links: links[id]})
			}
		}
	}

	if len(owned) > 0 {
		today := workItemAttributionUTCDay(now)
		derivedAt := map[time.Time]teamattribution.GithubWorkItemDerivationContext{}
		linked := map[string][]teamattribution.GithubWorkItemDerivationLinkedPullRequest{}
		for _, candidate := range owned {
			for _, link := range candidate.links {
				linked[candidate.subject.WorkItemID] = append(linked[candidate.subject.WorkItemID], link.GithubWorkItemDerivationLinkedPullRequest)
			}
		}
		derivedOn := func(day time.Time) (teamattribution.GithubWorkItemDerivationContext, error) {
			if derived, ok := derivedAt[day]; ok {
				return derived, nil
			}
			facts, err := executor.loadFacts(ctx, orgID, day)
			if err != nil {
				return teamattribution.GithubWorkItemDerivationContext{}, err
			}
			derived := teamattribution.NewGitHubWorkItemDerivationContext(facts)
			derived.LinkedPullRequests = linked
			derivedAt[day] = derived
			return derived, nil
		}
		todays, err := derivedOn(today)
		if err != nil {
			return nil, err
		}
		var completedDays map[string]time.Time
		pastDayRechecks := 0
		for _, candidate := range owned {
			if !linkChangePrimaryDiffers(todays, candidate.subject, candidate.stored) {
				continue
			}
			if completedDays == nil {
				if completedDays, err = executor.loadCompletedDays(ctx, orgID); err != nil {
					return nil, err
				}
			}
			day := WorkItemAttributionItemDay(completedDays, candidate.subject.WorkItemID, now)
			if day.Equal(today) {
				changed[candidate.subject.WorkItemID] = struct{}{}
				continue
			}
			derived, err := derivedOn(day)
			if err != nil {
				return nil, err
			}
			if linkChangePrimaryDiffers(derived, candidate.subject, candidate.stored) {
				changed[candidate.subject.WorkItemID] = struct{}{}
			} else {
				pastDayRechecks++
			}
		}
		if pastDayRechecks > 0 {
			slog.Default().InfoContext(ctx, "work_item_attribution.link_change_past_day_rechecks",
				"org_id", orgID, "count", pastDayRechecks)
		}
	}

	out := make([]string, 0, len(changed))
	for id := range changed {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// loadLinkChangeCandidates is every item that has a work_graph_issue_pr row
// or a stored link-derived repo_ownership row: one statement, no list of ids.
func (executor *WorkItemAttributionExecutor) loadLinkChangeCandidates(ctx context.Context, orgID string) ([]string, error) {
	rows, err := executor.conn.Query(ctx, `
SELECT DISTINCT work_item_id FROM (
    SELECT work_item_id FROM work_graph_issue_pr FINAL WHERE org_id = ?
    UNION ALL
    SELECT work_item_id FROM work_item_team_attributions
    WHERE org_id = ? AND source = 'repo_ownership' AND position(evidence, '`+linkChangeEvidenceIn+`') > 0
)
ORDER BY work_item_id`, orgID, orgID)
	if err != nil {
		return nil, stepcause.Failure(stepcause.QueryLinkChangeCandidates, err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, stepcause.Failure(stepcause.ScanLinkChangeCandidate, err)
		}
		ids = append(ids, id)
	}
	return ids, stepcause.Failure(stepcause.IterateLinkChangeCandidates, rows.Err())
}

// loadLinkChangeStoredRows reads the newest row set of each item of ids: its
// primary row and its link-derived repo_ownership rows.
func (executor *WorkItemAttributionExecutor) loadLinkChangeStoredRows(
	ctx context.Context, orgID string, ids []string,
) (map[string]linkChangeStored, error) {
	rows, err := executor.conn.Query(ctx, `
SELECT work_item_id, toString(source), ifNull(team_id, ''), is_primary = 1, evidence, computed_at
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND has(?, work_item_id)
  AND (work_item_id, computed_at) IN (
      SELECT work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE org_id = ? AND has(?, work_item_id)
      GROUP BY work_item_id)
  AND (is_primary = 1 OR (source = 'repo_ownership' AND position(evidence, '`+linkChangeEvidenceIn+`') > 0))`,
		orgID, ids, orgID, ids)
	if err != nil {
		return nil, stepcause.Failure(stepcause.QueryNewestRowsLinkChange, err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string]linkChangeStored{}
	for rows.Next() {
		var id, source, team, evidence string
		var primary bool
		var computedAt time.Time
		if err := rows.Scan(&id, &source, &team, &primary, &evidence, &computedAt); err != nil {
			return nil, stepcause.Failure(stepcause.ScanNewestRowLinkChange, err)
		}
		row := result[id]
		row.ComputedAt = computedAt
		if primary {
			row.PrimarySource, row.PrimaryTeam = source, team
		}
		if source == "repo_ownership" {
			if link, ok := teamattribution.ParseLinkedPullRequestRepoEvidence(evidence); ok {
				row.Named = append(row.Named, link)
			}
		}
		result[id] = row
	}
	return result, stepcause.Failure(stepcause.IterateNewestRowsLinkChange, rows.Err())
}

// loadLinkChangeLinks reads the current work_graph_issue_pr rows of ids.
func (executor *WorkItemAttributionExecutor) loadLinkChangeLinks(
	ctx context.Context, orgID string, ids []string,
) (map[string][]linkChangeLink, error) {
	rows, err := executor.conn.Query(ctx, `
SELECT work_item_id, toString(repo_id), pr_number, provenance, last_synced
FROM work_graph_issue_pr FINAL
WHERE org_id = ? AND has(?, work_item_id)`, orgID, ids)
	if err != nil {
		return nil, stepcause.Failure(stepcause.QueryLinksLinkChange, err)
	}
	defer func() { _ = rows.Close() }()
	result := map[string][]linkChangeLink{}
	for rows.Next() {
		var id string
		var link linkChangeLink
		if err := rows.Scan(&id, &link.RepoID, &link.PRNumber, &link.Tier, &link.LastSynced); err != nil {
			return nil, stepcause.Failure(stepcause.ScanLinkLinkChange, err)
		}
		result[id] = append(result[id], link)
	}
	return result, stepcause.Failure(stepcause.IterateLinksLinkChange, rows.Err())
}
