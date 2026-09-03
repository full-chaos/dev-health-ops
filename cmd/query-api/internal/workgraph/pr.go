package workgraph

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// prDetailIDPattern is a direct port of resolvers/pr.py's `_PR_ID_RE`
// (`^(?P<repo>[0-9a-fA-F-]{36})(?:#pr|#|:|/pr/)(?P<number>\d+)$`): a
// 36-character UUID-shaped repo id, one of four separators, then a decimal
// PR number, anchored on both ends the same way Python's `.match` plus a
// `$`-terminated pattern is.
//
// KNOWN DIVERGENCE (codex round 1 on #2190, P2, ARGUED and verified via a
// real `python3` run): Python's bare (str-pattern) `\d` matches every
// Unicode decimal-digit codepoint (category Nd), not just ASCII 0-9, and
// Python's `int()` accepts the same codepoints -- so Python's parse_pr_id
// resolves an id ending in e.g. `#pr١٢` (Arabic-Indic digits) to number
// 12. Go's RE2 `\d` is ASCII-only with no Unicode-digit mode, so this
// pattern rejects that id outright and ParsePRDetailID returns ok=false --
// the Pr resolver then returns nil without ever querying the existing PR,
// where Python would have found and returned it.
//
// NOT fixed here, deliberately, same "capability gap, not a vulnerability"
// convention schema.resolvers.go's Analytics resolver doc comment already
// establishes for a different divergence: this canonical id is ALWAYS
// server-constructed with `fmt.Sprintf("%s#pr%d", repoID, number)`
// (schema.resolvers.go) or Python's equivalent f-string -- both use
// ASCII digits exclusively, so no real PR id ever contains a non-ASCII
// digit. The divergence is reachable only via a hand-crafted, adversarial
// client request, and Go's direction (reject something Python would
// accept) is the safe one for a parser deciding what identifies a
// resource: nothing leaks, it just doesn't resolve. Replicating Python's
// behavior exactly would need a hand-rolled Unicode decimal-digit-value
// table (Go's standard library has no such lookup, and RE2 cannot express
// a `\d`-equivalent Unicode class match-and-convert in one step) for a
// case with no legitimate traffic. TestParsePRDetailID's
// "unicode-digit id is rejected (KNOWN DIVERGENCE from Python)" case pins
// this current, safe behavior so a future change to this pattern doesn't
// silently regress it into an unnoticed acceptance either way.
var prDetailIDPattern = regexp.MustCompile(`^([0-9a-fA-F-]{36})(?:#pr|#|:|/pr/)(\d+)$`)

// ParsePRDetailID parses the query-api `Query.pr` `id` argument into a
// lower-cased repo id and PR number, mirroring `parse_pr_id`
// (resolvers/pr.py:30-36) exactly, including lower-casing the repo id.
// Returns ok=false for anything that doesn't match -- same as Python's
// `None` return, which the Pr resolver treats as "no such PR" (a nil
// result, no GraphQL error), never a parse error surfaced to the client.
// See prDetailIDPattern's own doc comment for a known, deliberate,
// safe-direction divergence on non-ASCII decimal digits.
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

// PRCoreRowExists is a cheap existence check against `git_pull_requests` --
// the same core-row table `_fetch_pr_row` (pr.py:44-56) reads, but asking
// only "does at least one row exist for this identity", not fetching any
// of its columns. This is what lets the Pr resolver return nil for an
// unknown PR/org/repo, exactly like Python's `resolve_pr` does when
// `_fetch_pr_row` comes back empty (pr.py:223-224), without needing the
// full core-row port (title/body/state/... -- CHAOS-4980's own follow-up
// ticket) to do it.
//
// Deliberately WITHOUT `FINAL`: `git_pull_requests` is a
// `ReplacingMergeTree(last_synced)` with no `is_deleted` marker
// (000_raw_tables.sql) -- it only collapses duplicate physical rows for
// the same (repo_id, number) by version, it never makes a row that exists
// stop existing. An unmerged duplicate can change which row FINAL would
// pick, but never whether at least one row is present -- so a plain
// (non-FINAL) read answers "does it exist" exactly as correctly as FINAL
// would, at a fraction of the cost.
func PRCoreRowExists(ctx context.Context, client QueryClient, orgID, repoID string, number int) (bool, error) {
	const query = `
        SELECT number
        FROM git_pull_requests
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND number = {number:UInt32}
        LIMIT 1
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("workgraph: pr core-row existence rows: %w", err)
		}
		return false, nil
	}
	var n uint32
	if err := rows.Scan(&n); err != nil {
		return false, fmt.Errorf("workgraph: pr core-row existence scan: %w", err)
	}
	return true, nil
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
// SCOPE NOTE (CHAOS-4980): this, plus PRCoreRowExists above, covers only
// the "does the PR exist" question and the `linkedIssues` sub-field. The
// rest of PullRequestDetail -- the PR core row's OWN columns, reviews, and
// commits (resolve_pr's `_fetch_pr_row`/`_fetch_reviews`/`_fetch_commits`
// in pr.py) -- has not been ported to Go yet and is out of this ticket's
// scope. The Pr resolver (schema.resolvers.go) that calls this therefore
// returns a PARTIAL PullRequestDetail (id/orgId/repoId/number/linkedIssues
// only) for a PR that DOES exist, and nil for one that doesn't -- matching
// resolve_pr's nil-for-unknown behavior exactly, while every other field
// stays at its zero value until the follow-up ticket lands. Tracked, not
// silently dropped; see this ticket's own body and query-api's
// routeswitch registry (routeswitch.go / query_route.go's
// digestByOperation), where `pr` has no registered document and therefore
// cannot serve traffic at all yet regardless of this resolver's
// completeness -- see pr_operation_not_registered_test.go.
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
