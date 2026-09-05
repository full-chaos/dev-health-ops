package reviewedges

import (
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// NormalizeGitIdentity ports normalize_git_identity's NO-RESOLVER branch
// (providers/identity.py:218-226). job_daily.py:1608 calls
// compute_review_edges_daily WITHOUT an identity_resolver, so the alias
// cascade above that branch is unreachable from this family and is
// deliberately not ported -- porting it would be dead code with no oracle.
//
// The order is email, then display name, then the literal "unknown", and each
// candidate must be non-empty AFTER stripping.
//
// STRIPPING IS pythonparity.Strip, NOT strings.TrimSpace. Python's str.strip()
// treats U+001C..U+001F (the file/group/record/unit separators) as whitespace
// and Go's unicode.IsSpace does not, so TrimSpace is a strict subset --
// already pinned by pythonparity/whitespace_test.go's
// TestGoUnicodeIsSpaceIsAStrictSubset. A name of "\x1c" strips to "" in Python
// (falling through to "unknown") but survives TrimSpace as a one-rune
// identity, which would silently split one contributor's edges in two.
func NormalizeGitIdentity(email, displayName string) string {
	if email != "" {
		if normalized := pythonparity.Strip(email); normalized != "" {
			return normalized
		}
	}
	if displayName != "" {
		if normalized := pythonparity.Strip(displayName); normalized != "" {
			return normalized
		}
	}
	return "unknown"
}

// edgeKey is the (repo, reviewer, author) triple edges are counted on.
type edgeKey struct {
	repoID   uuid.UUID
	reviewer string
	author   string
}

// prKey is the (repo, number) pair the author map is keyed on.
type prKey struct {
	repoID uuid.UUID
	number uint32
}

// ComputeReviewEdgesDaily ports compute_review_edges_daily (reviews.py:22-73).
//
// Counting is pure integer arithmetic -- there is no float anywhere in this
// family, so none of the FMA or compensated-summation concerns that dominate
// the other two families in this lane apply here.
//
// `day` must be the UTC calendar day; the window is [day, day+1).
func ComputeReviewEdgesDaily(
	day time.Time,
	pullRequests []PullRequestRow,
	reviews []ReviewRow,
	computedAt time.Time,
	orgID string,
) []Record {
	// Python's early return: no reviews means no edges, and the PR rows are
	// never even walked (reviews.py:33-34).
	if len(reviews) == 0 {
		return nil
	}

	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	// LAST ROW WINS on a duplicate (repo_id, number), because Python's dict
	// assignment does (reviews.py:44). The loader hands rows in a
	// deterministic order and deduplicates upstream, so the "last" here is
	// well-defined -- see clickhouse.go's note on how that differs from
	// Python's own un-ORDER-BY'd, un-deduplicated query.
	authors := make(map[prKey]string, len(pullRequests))
	for _, pr := range pullRequests {
		authors[prKey{pr.RepoID, pr.Number}] = NormalizeGitIdentity(pr.AuthorEmail, pr.AuthorName)
	}

	counts := make(map[edgeKey]uint32)
	for _, review := range reviews {
		submitted := review.SubmittedAt.UTC()
		// Python re-checks the window even though the loader already filtered
		// on it (reviews.py:49-50); kept, so the compute is correct on any
		// caller's rows rather than only the loader's.
		if submitted.Before(start) || !submitted.Before(end) {
			continue
		}
		// Reviewer resolves from the display name only -- Python passes
		// email=None here (reviews.py:51).
		reviewer := NormalizeGitIdentity("", review.Reviewer)
		author, ok := authors[prKey{review.RepoID, review.Number}]
		// `if not author: continue` (reviews.py:53-54). Because the loader
		// coerces both author columns to "" and the fallback returns
		// "unknown", a PRESENT entry is never empty -- so this only fires when
		// the PR is ABSENT from the map, which is the dropped-edge quirk
		// documented on the package.
		if !ok || author == "" {
			continue
		}
		counts[edgeKey{review.RepoID, reviewer, author}]++
	}

	records := make([]Record, 0, len(counts))
	for key, count := range counts {
		records = append(records, Record{
			RepoID:       key.repoID,
			Day:          start,
			Reviewer:     key.reviewer,
			Author:       key.author,
			ReviewsCount: count,
			ComputedAt:   computedAt.UTC(),
			OrgID:        orgID,
		})
	}

	// Python sorts by (str(repo_id), reviewer, author) (reviews.py:59-61).
	// Python compares str by code point and Go compares string by byte, but
	// for UTF-8 those orders coincide, so a plain < is the same total order.
	// str(uuid.UUID) is canonical lowercase-hyphenated, which is exactly what
	// uuid.UUID.String() produces.
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if leftID, rightID := left.RepoID.String(), right.RepoID.String(); leftID != rightID {
			return leftID < rightID
		}
		if left.Reviewer != right.Reviewer {
			return left.Reviewer < right.Reviewer
		}
		return left.Author < right.Author
	})
	return records
}
