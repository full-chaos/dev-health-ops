package workgraph

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// prDetailIDPattern is a direct port of resolvers/pr.py's `_PR_ID_RE`
// (`^(?P<repo>[0-9a-fA-F-]{36})(?:#pr|#|:|/pr/)(?P<number>\d+)$`): a
// 36-character UUID-shaped repo id, one of four separators, then a decimal
// PR number, anchored on both ends the same way Python's `.match` plus a
// `$`-terminated pattern is.
var prDetailIDPattern = regexp.MustCompile(`^([0-9a-fA-F-]{36})(?:#pr|#|:|/pr/)(\d+)$`)

// ParsePRDetailID parses the query-api `Query.pr` `id` argument into a
// lower-cased repo id and PR number, mirroring `parse_pr_id`
// (resolvers/pr.py:30-36) exactly, including lower-casing the repo id.
// Returns ok=false for anything that doesn't match -- same as Python's
// `None` return, which the Pr resolver treats as "no such PR" (a nil
// result, no GraphQL error), never a parse error surfaced to the client.
func ParsePRDetailID(id string) (repoID string, number int, ok bool) {
	m := prDetailIDPattern.FindStringSubmatch(strings.TrimSpace(id))
	if m == nil {
		return "", 0, false
	}
	n, err := strconv.Atoi(m[2])
	if err != nil {
		return "", 0, false
	}
	return strings.ToLower(m[1]), n, true
}

// ResolveLinkedIssues builds the `linkedIssues` field of PullRequestDetail
// via fetchLinkedIssueRows (issuepr.go) -- CHAOS-4924's previously-unwired
// reader, wired into the query-api Pr resolver by CHAOS-4980.
// fetchLinkedIssueRows itself picks the fast (argMax/version_rank) path or
// the FINAL oracle path per investmentMaterializeNativeEnabled(); this
// function is flag-oblivious by construction -- it only shapes whatever
// fetchLinkedIssueRows returns into the GraphQL model, identically
// regardless of which path served the rows. That sameness is exactly the
// parity contract CHAOS-4924's reader exists to uphold (see
// fetchLinkedIssueRows's own doc comment); pr_test.go and the reader-level
// golden proof in issuepr_integration_test.go both pin it, and
// pr_integration_test.go pins it again at this mapping layer against a
// real seeded fixture.
//
// SCOPE NOTE (CHAOS-4980): this covers only the `linkedIssues` sub-field.
// The rest of PullRequestDetail -- the PR core row, reviews, and commits
// (resolve_pr's `_fetch_pr_row`/`_fetch_reviews`/`_fetch_commits` in
// pr.py) -- has not been ported to Go yet and is out of this ticket's
// scope. The Pr resolver (schema.resolvers.go) that calls this therefore
// returns a PARTIAL PullRequestDetail (id/orgId/repoId/number/linkedIssues
// only) and never returns nil for an unknown PR the way resolve_pr does --
// there is no core-row fetch yet to detect "unknown". Tracked as a
// follow-up, not silently dropped; see this ticket's own body.
func ResolveLinkedIssues(ctx context.Context, client QueryClient, orgID, repoID string, number int) ([]model.PullRequestIssueLink, error) {
	rows, err := fetchLinkedIssueRows(ctx, client, orgID, repoID, number)
	if err != nil {
		return nil, err
	}
	out := make([]model.PullRequestIssueLink, 0, len(rows))
	for _, row := range rows {
		out = append(out, model.PullRequestIssueLink{
			WorkItemID: row.workItemID,
			Confidence: row.confidence,
			Provenance: row.provenance,
			Evidence:   row.evidence,
		})
	}
	return out, nil
}
