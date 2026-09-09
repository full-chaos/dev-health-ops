package workgraph

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

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
// SCOPE NOTE (CHAOS-4980, UPDATED by CHAOS-4991): this, plus
// PRCoreRowExists above, covers the "does the PR exist" question and the
// `linkedIssues` sub-field. CHAOS-4980 shipped the Pr resolver with only
// those wired in -- a PARTIAL PullRequestDetail
// (id/orgId/repoId/number/linkedIssues only) for a PR that DOES exist.
// CHAOS-4991 ports the rest: the PR core row's OWN columns
// (FetchPRCoreRow), reviews (ResolveReviews), and commits (ResolveCommits)
// -- resolve_pr's `_fetch_pr_row`/`_fetch_reviews`/`_fetch_commits` in
// pr.py -- and wires all three into schema.resolvers.go's Pr resolver, so
// it now returns the FULL PullRequestDetail for a PR that exists, and nil
// for one that doesn't (unchanged). CHAOS-4991 also registers `pr` in
// query_route.go's digestByOperation (see registeredPrDetailDocument's own
// doc comment there for what registration does and does not mean) --
// pr_operation_not_registered_test.go, which asserted the OPPOSITE (that
// `pr` must stay unregistered until this port landed), is removed by that
// same change now that its guarded condition has been met.
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

// PRCoreRow is FetchPRCoreRow's result shape: the PR core row's OWN
// columns from `git_pull_requests`, plus `repo_name` pulled in via the
// same LEFT JOIN against `repos` that _fetch_pr_row (pr.py:44-71) uses --
// column for column, same nullability as the schema
// (000_raw_tables.sql): every field but CreatedAt is nullable and is
// passed through as-is, exactly as Python's dict-of-row-values does with
// no coalescing.
type PRCoreRow struct {
	RepoName              *string
	Title                 *string
	Body                  *string
	State                 *string
	AuthorName            *string
	AuthorEmail           *string
	CreatedAt             time.Time
	MergedAt              *time.Time
	ClosedAt              *time.Time
	HeadBranch            *string
	BaseBranch            *string
	Additions             *int
	Deletions             *int
	ChangedFiles          *int
	FirstReviewAt         *time.Time
	FirstCommentAt        *time.Time
	ChangesRequestedCount int
	ReviewsCount          int
	CommentsCount         int
}

// FetchPRCoreRow is the Go port of _fetch_pr_row (pr.py:44-71): the PR
// core row's own columns, deliberately NOT fetched by the cheap
// PRCoreRowExists check above. Returns ok=false when no row exists -- the
// same nil-for-unknown contract PRCoreRowExists already establishes, so a
// caller (schema.resolvers.go's Pr resolver) that already confirmed
// existence via PRCoreRowExists should not normally see ok=false here
// (barring an accepted TOCTOU race between the two calls, same as any
// two-query resolver).
//
// FINAL on both sides (unlike PRCoreRowExists, which deliberately omits
// it): this reads actual COLUMN VALUES, not just "does a row exist" --
// git_pull_requests is a ReplacingMergeTree(last_synced) and repos is
// too, so FINAL is required here to collapse to the single winning
// physical row per identity before reading its columns, exactly like
// pr.py's own query (which reads via the same ClickHouse FINAL
// semantics).
//
// GROUP BY every non-aggregated SELECTed column, mirroring pr.py's GROUP
// BY list exactly: FINAL already collapses git_pull_requests to one
// physical row per (org_id, repo_id, number), but the LEFT JOIN against
// `repos FINAL` still needs a GROUP BY to pair with anyLast(repos.repo)
// -- same shape as pr.py's own query, not an independent Go design
// choice.
//
// repo_name via a plain (non-double) *string, not **string: repos.repo is
// a non-nullable String column (000_raw_tables.sql), and this stack's
// ClickHouse client runs with the default join_use_nulls=0 (see
// analytics/investment.go's repoAllocationInvestmentSource doc comment
// for the same convention elsewhere in this package) -- an unmatched LEFT
// JOIN yields the column's zero value (empty string), never SQL NULL, so
// anyLast(repos.repo) stays typed String (non-nullable) even across the
// join. An empty string is treated as "no repo name" (nil) at the mapping
// layer below, not surfaced as a literal empty string to the client.
func FetchPRCoreRow(ctx context.Context, client QueryClient, orgID, repoID string, number int) (PRCoreRow, bool, error) {
	const query = `
        SELECT
            anyLast(repos.repo) AS repo_name,
            pr.title AS title,
            pr.body AS body,
            pr.state AS state,
            pr.author_name AS author_name,
            pr.author_email AS author_email,
            pr.created_at AS created_at,
            pr.merged_at AS merged_at,
            pr.closed_at AS closed_at,
            pr.head_branch AS head_branch,
            pr.base_branch AS base_branch,
            pr.additions AS additions,
            pr.deletions AS deletions,
            pr.changed_files AS changed_files,
            pr.first_review_at AS first_review_at,
            pr.first_comment_at AS first_comment_at,
            pr.changes_requested_count AS changes_requested_count,
            pr.reviews_count AS reviews_count,
            pr.comments_count AS comments_count
        FROM git_pull_requests AS pr FINAL
        LEFT JOIN repos AS repos FINAL
            ON repos.org_id = pr.org_id AND repos.id = pr.repo_id
        WHERE pr.org_id = {org_id:String}
          AND toString(pr.repo_id) = {repo_id:String}
          AND pr.number = {number:UInt32}
        GROUP BY
            pr.title, pr.body, pr.state, pr.author_name, pr.author_email,
            pr.created_at, pr.merged_at, pr.closed_at, pr.head_branch,
            pr.base_branch, pr.additions, pr.deletions, pr.changed_files,
            pr.first_review_at, pr.first_comment_at,
            pr.changes_requested_count, pr.reviews_count, pr.comments_count
        LIMIT 1
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return PRCoreRow{}, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return PRCoreRow{}, false, fmt.Errorf("workgraph: pr core row: %w", err)
		}
		return PRCoreRow{}, false, nil
	}

	var (
		repoName                                           string
		title, body, state, authorName, authorEmail        *string
		headBranch, baseBranch                             *string
		createdAt                                          time.Time
		mergedAt, closedAt, firstReviewAt, firstCommentAt  *time.Time
		additions, deletions, changedFiles                 *uint32
		changesRequestedCount, reviewsCount, commentsCount uint32
	)
	if err := rows.Scan(
		&repoName, &title, &body, &state, &authorName, &authorEmail,
		&createdAt, &mergedAt, &closedAt, &headBranch, &baseBranch,
		&additions, &deletions, &changedFiles,
		&firstReviewAt, &firstCommentAt,
		&changesRequestedCount, &reviewsCount, &commentsCount,
	); err != nil {
		return PRCoreRow{}, false, fmt.Errorf("workgraph: pr core row scan: %w", err)
	}

	row := PRCoreRow{
		RepoName:              nilIfEmpty(repoName),
		Title:                 title,
		Body:                  body,
		State:                 state,
		AuthorName:            authorName,
		AuthorEmail:           authorEmail,
		CreatedAt:             createdAt,
		MergedAt:              mergedAt,
		ClosedAt:              closedAt,
		HeadBranch:            headBranch,
		BaseBranch:            baseBranch,
		Additions:             uint32PtrToIntPtr(additions),
		Deletions:             uint32PtrToIntPtr(deletions),
		ChangedFiles:          uint32PtrToIntPtr(changedFiles),
		FirstReviewAt:         firstReviewAt,
		FirstCommentAt:        firstCommentAt,
		ChangesRequestedCount: int(changesRequestedCount),
		ReviewsCount:          int(reviewsCount),
		CommentsCount:         int(commentsCount),
	}
	return row, true, nil
}

// nilIfEmpty is FetchPRCoreRow's helper for the join_use_nulls=0 case
// documented on FetchPRCoreRow itself: an empty string coming back from
// anyLast(repos.repo) means "no matching repo row", not "a repo whose
// name is the empty string" (repo names are never empty in practice --
// GitHub/GitLab reject that), so it maps to nil rather than a pointer to
// "".
func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// uint32PtrToIntPtr adapts a scanned Nullable(UInt32) ClickHouse column
// (driver type *uint32) to the Go model's *int fields (Additions,
// Deletions, ChangedFiles on model.PullRequestDetail) -- a narrowing
// widen (uint32 -> int is always lossless on the 64-bit platforms this
// service runs on), never surfaced to a caller as a distinct error case.
func uint32PtrToIntPtr(v *uint32) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}

// ResolveReviews is the Go port of _fetch_reviews (pr.py:74-90): every
// review row for one PR, ordered exactly as Python orders them
// (submitted_at ASC, review_id ASC) and capped at the same LIMIT 500.
// git_pull_request_reviews is a ReplacingMergeTree(last_synced) with no
// FINAL here -- deliberately: same "would rather over-return a
// not-yet-merged duplicate than pay for a forced merge on every read"
// posture is NOT what's happening here, this is a bit-exact port of
// pr.py's own query text, which itself has no FINAL. If a future need for
// dedup-under-load arises here, that is a conscious follow-up, not
// silently added by this port.
func ResolveReviews(ctx context.Context, client QueryClient, orgID, repoID string, number int) ([]model.PullRequestReview, error) {
	const query = `
        SELECT review_id, reviewer, state, submitted_at
        FROM git_pull_request_reviews
        WHERE org_id = {org_id:String}
          AND toString(repo_id) = {repo_id:String}
          AND number = {number:UInt32}
        ORDER BY submitted_at ASC, review_id ASC
        LIMIT 500
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.PullRequestReview{}
	for rows.Next() {
		var reviewID, reviewer, state string
		var submittedAt time.Time
		if scanErr := rows.Scan(&reviewID, &reviewer, &state, &submittedAt); scanErr != nil {
			return nil, fmt.Errorf("workgraph: pr reviews scan: %w", scanErr)
		}
		out = append(out, model.PullRequestReview{
			ReviewID:    reviewID,
			Reviewer:    reviewer,
			State:       state,
			SubmittedAt: submittedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: pr reviews rows: %w", err)
	}
	return out, nil
}

// ResolveCommits is the Go port of _fetch_commits (pr.py:93-124): every
// commit linked to one PR via work_graph_pr_commit, LEFT JOINed to
// git_commits for the commit's own metadata, ordered exactly as Python
// orders them (author_when ASC, commit_hash ASC, via anyLast(...) inside
// ORDER BY -- pr.py orders by the SAME anyLast(commit.author_when)
// expression the SELECT list computes) and capped at the same LIMIT 500.
//
// toFloat64(argMax(link.confidence, link.last_synced)): work_graph_pr_commit.confidence
// is Float32 (014_work_graph.sql) and argMax() preserves its input's
// ClickHouse type -- the native Go driver refuses to scan a Float32
// result column into *float64 outright, the SAME trap
// fetchLinkedIssueRowsFinal's doc comment (issuepr.go) already documents
// for work_graph_issue_pr's identically-typed confidence column. Cast
// here for the same reason, not a value-changing divergence from Python
// (Python's driver has no such restriction).
//
// author_when is scanned as a plain (non-nullable) time.Time: git_commits.author_when
// is DateTime64 NOT NULL (000_raw_tables.sql), and under this stack's
// default join_use_nulls=0 an unmatched LEFT JOIN yields that column's
// zero value rather than SQL NULL (same convention FetchPRCoreRow's own
// doc comment documents for repo_name) -- so anyLast(commit.author_when)
// stays non-nullable even across the join. That zero-value edge case
// (a work_graph_pr_commit row whose linked commit_hash has no matching
// git_commits row) is passed through as a real, if meaningless, zero
// time rather than manufactured as nil -- matching what Python's
// dict-of-row-values would also carry through unchanged.
func ResolveCommits(ctx context.Context, client QueryClient, orgID, repoID string, number int) ([]model.PullRequestCommit, error) {
	const query = `
        SELECT
            link.commit_hash AS hash,
            anyLast(commit.message) AS message,
            anyLast(commit.author_name) AS author_name,
            anyLast(commit.author_email) AS author_email,
            anyLast(commit.author_when) AS author_when,
            toFloat64(argMax(link.confidence, link.last_synced)) AS confidence,
            argMax(link.provenance, link.last_synced) AS provenance,
            argMax(link.evidence, link.last_synced) AS evidence
        FROM work_graph_pr_commit AS link FINAL
        LEFT JOIN git_commits AS commit FINAL
            ON commit.org_id = link.org_id AND commit.repo_id = link.repo_id AND commit.hash = link.commit_hash
        WHERE link.org_id = {org_id:String}
          AND toString(link.repo_id) = {repo_id:String}
          AND link.pr_number = {number:UInt32}
        GROUP BY link.commit_hash
        ORDER BY anyLast(commit.author_when) ASC, link.commit_hash ASC
        LIMIT 500
    `
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_id", Value: repoID},
		{Name: "number", Value: number},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []model.PullRequestCommit{}
	for rows.Next() {
		var hash string
		var message, authorName, authorEmail *string
		var authorWhen time.Time
		var confidence float64
		var provenance, evidence string
		if scanErr := rows.Scan(&hash, &message, &authorName, &authorEmail, &authorWhen, &confidence, &provenance, &evidence); scanErr != nil {
			return nil, fmt.Errorf("workgraph: pr commits scan: %w", scanErr)
		}
		out = append(out, model.PullRequestCommit{
			Hash:        hash,
			Message:     message,
			AuthorName:  authorName,
			AuthorEmail: authorEmail,
			AuthorWhen:  &authorWhen,
			Confidence:  &confidence,
			Provenance:  &provenance,
			Evidence:    &evidence,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("workgraph: pr commits rows: %w", err)
	}
	return out, nil
}
