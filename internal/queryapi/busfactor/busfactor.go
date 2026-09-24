// Package busfactor serves the busFactor GraphQL field: how concentrated
// the churn (additions plus deletions) of an org, a team's repositories or
// one repository is across authors, and how many authors it takes to cover
// 80% of it.
//
// Evidence is every git_commit_stats row joined to its commit and (left) to
// its repository, read across all time. The org is always the caller's own
// org. A team scope selects the repositories the team owns through the
// shared team-ownership condition (team_repo_ownership), never the
// repositories a team's members happened to commit in.
package busfactor

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

const (
	// busFactorThreshold is the share of churn the counted authors must reach.
	busFactorThreshold = 0.8
	orgTopLimit        = 5
	repoTopLimit       = 3
)

// evidenceRow is one commit-stat row reduced to what the calculation reads.
type evidenceRow struct {
	repoID   string
	repoName string
	identity string
	churn    int64
}

// parseRepoID reads a repository id the way a UUID constructor does: the
// "urn:" and "uuid:" prefixes, surrounding braces and every hyphen are
// dropped, and what is left must be 32 hexadecimal digits. Anything else is
// not a scope: it returns ok=false and the request is served unscoped.
func parseRepoID(raw *string) (string, bool) {
	if raw == nil || *raw == "" {
		return "", false
	}
	h := strings.ReplaceAll(*raw, "urn:", "")
	h = strings.ReplaceAll(h, "uuid:", "")
	h = strings.Trim(h, "{}")
	h = strings.ReplaceAll(h, "-", "")
	if len(h) != 32 {
		return "", false
	}
	h = strings.ToLower(h)
	for _, r := range h {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", false
		}
	}
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], true
}

// identityOf is the author key: email, else name, else "unknown"; an empty
// value counts as absent.
func identityOf(email, name *string) string {
	if email != nil && *email != "" {
		return *email
	}
	if name != nil && *name != "" {
		return *name
	}
	return "unknown"
}

// authorChurn sums churn per author in first-appearance order.
type authorChurn struct {
	order []string
	sum   map[string]int64
}

func newAuthorChurn() *authorChurn { return &authorChurn{sum: map[string]int64{}} }

func (a *authorChurn) add(author string, churn int64) {
	if _, seen := a.sum[author]; !seen {
		a.order = append(a.order, author)
	}
	a.sum[author] = a.sum[author] + churn
}

// computeBusFactor is the smallest number of authors, taken in descending
// churn order, whose cumulative churn reaches 80% of the total; 0 when the
// total is 0.
func computeBusFactor(rows []evidenceRow) int {
	if len(rows) == 0 {
		return 0
	}
	ac := newAuthorChurn()
	var total int64
	for _, r := range rows {
		ac.add(r.identity, r.churn)
		total += r.churn
	}
	if total == 0 {
		return 0
	}
	sums := make([]int64, 0, len(ac.order))
	for _, a := range ac.order {
		sums = append(sums, ac.sum[a])
	}
	sort.SliceStable(sums, func(i, j int) bool { return sums[i] > sums[j] })
	target := float64(total) * busFactorThreshold
	var cumulative int64
	count := 0
	for _, c := range sums {
		cumulative += c
		count++
		if float64(cumulative) >= target {
			break
		}
	}
	return count
}

// topMaintainers returns up to limit authors by share of the positive churn,
// ties keeping first-appearance order.
func topMaintainers(rows []evidenceRow, limit int) []model.MaintainerShare {
	ac := newAuthorChurn()
	var total int64
	for _, r := range rows {
		if r.churn <= 0 {
			continue
		}
		ac.add(r.identity, r.churn)
		total += r.churn
	}
	out := []model.MaintainerShare{}
	if total == 0 {
		return out
	}
	authors := append([]string(nil), ac.order...)
	// Equal churn orders by author key so the answer does not depend on the
	// order ClickHouse returns rows in.
	sort.SliceStable(authors, func(i, j int) bool {
		if ac.sum[authors[i]] != ac.sum[authors[j]] {
			return ac.sum[authors[i]] > ac.sum[authors[j]]
		}
		return authors[i] < authors[j]
	})
	if len(authors) > limit {
		authors = authors[:limit]
	}
	for _, a := range authors {
		out = append(out, model.MaintainerShare{
			Author:       a,
			SharePercent: float64(ac.sum[a]) / float64(total) * 100.0,
		})
	}
	return out
}

// Resolve answers busFactor for the org; scope narrows it to a team's
// owned repositories, one repository, or both (their intersection).
func Resolve(ctx context.Context, client QueryClient, orgID string, scope *model.BusFactorScopeInput, asOf time.Time) (*model.BusFactor, error) {
	if client == nil {
		return nil, errors.New("busfactor: clickhouse client is required")
	}
	var rawRepo, teamID *string
	if scope != nil {
		rawRepo, teamID = scope.RepoID, scope.TeamID
	}
	repoID, hasRepo := parseRepoID(rawRepo)

	rows, err := loadEvidence(ctx, client, orgID, repoID, hasRepo, teamID, asOf)
	if err != nil {
		return nil, err
	}

	byRepo := map[string][]evidenceRow{}
	var repoOrder []string
	names := map[string]string{}
	for _, r := range rows {
		if _, seen := byRepo[r.repoID]; !seen {
			repoOrder = append(repoOrder, r.repoID)
		}
		byRepo[r.repoID] = append(byRepo[r.repoID], r)
		names[r.repoID] = r.repoName
	}
	repos := make([]model.RepoBusFactor, 0, len(repoOrder))
	for _, id := range repoOrder {
		rr := byRepo[id]
		repos = append(repos, model.RepoBusFactor{
			RepoID:              id,
			RepoName:            names[id],
			Value:               computeBusFactor(rr),
			TopMaintainers:      topMaintainers(rr, repoTopLimit),
			EvidenceSampleCount: len(rr),
		})
	}
	sort.SliceStable(repos, func(i, j int) bool {
		if repos[i].Value != repos[j].Value {
			return repos[i].Value < repos[j].Value
		}
		if repos[i].RepoName != repos[j].RepoName {
			return repos[i].RepoName < repos[j].RepoName
		}
		return repos[i].RepoID < repos[j].RepoID
	})

	var scopeRepo *string
	if hasRepo {
		id := repoID
		scopeRepo = &id
	}
	return &model.BusFactor{
		OrgID:               orgID,
		Scope:               &model.BusFactorScope{RepoID: scopeRepo, TeamID: teamID},
		Value:               computeBusFactor(rows),
		TopMaintainers:      topMaintainers(rows, orgTopLimit),
		Repos:               repos,
		EvidenceSampleCount: len(rows),
	}, nil
}

func loadEvidence(ctx context.Context, client QueryClient, orgID, repoID string, hasRepo bool, teamID *string, asOf time.Time) ([]evidenceRow, error) {
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	var where []string
	if hasRepo {
		where = append(where, "gc.repo_id = {repo_id:UUID}")
		bindings = append(bindings, clickhouse.Binding{Name: "repo_id", Value: repoID})
	}
	if teamID != nil {
		cond, teamBindings := teamscope.RepoCondition(orgID, "toString(gc.repo_id)", []string{*teamID}, asOf)
		if cond != "" {
			where = append(where, cond)
			bindings = append(bindings, teamBindings...)
		}
	}
	extra := ""
	for _, w := range where {
		extra += "\n  AND " + w
	}
	query := `SELECT
    toString(gc.repo_id) AS repo_id,
    coalesce(r.repo, toString(gc.repo_id)) AS repo_name,
    gc.author_email AS author_email,
    gc.author_name AS author_name,
    gcs.additions AS additions,
    gcs.deletions AS deletions
FROM git_commit_stats AS gcs
INNER JOIN git_commits AS gc
    ON gc.repo_id = gcs.repo_id
   AND gc.hash = gcs.commit_hash
   AND gc.org_id = gcs.org_id
LEFT JOIN repos AS r
    ON r.id = gc.repo_id
   AND r.org_id = gc.org_id
WHERE 1 = 1
  AND gc.org_id = {org_id:String}
  AND gcs.org_id = {org_id:String}` + extra

	rs, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("busfactor: evidence query: %w", err)
	}
	defer rs.Close()
	var out []evidenceRow
	for rs.Next() {
		var id, name string
		var email, author *string
		var add, del int32
		if err := rs.Scan(&id, &name, &email, &author, &add, &del); err != nil {
			return nil, fmt.Errorf("busfactor: evidence scan: %w", err)
		}
		if name == "" {
			name = id
		}
		out = append(out, evidenceRow{repoID: id, repoName: name, identity: identityOf(email, author), churn: int64(add) + int64(del)})
	}
	if err := rs.Err(); err != nil {
		return nil, fmt.Errorf("busfactor: evidence rows: %w", err)
	}
	return out, nil
}
