package reviewedges

import (
	"regexp"
	"strings"
	"testing"
)

// TestBothLoaderQueriesReadFINAL is a STATIC FLOOR under the dedup, and it
// exists because the oracle layer above it is blind to this exact change.
//
// #2231 codex r1 established the gap: deleting `FINAL` from either loader query
// would be accepted by BOTH the frozen compute golden
// (compute_test.go) and the live rot guard (golden_rot_guard_test.go), because
// neither exercises SQL loading at all -- the golden feeds already-materialized
// rows straight into the compute function, and the rot guard only re-runs the
// Python generator. A silent revert to raw reads would restore Python's
// double-counting of a re-synced review, and every non-container test in this
// package would stay green.
//
// The integration tests DO catch it (review_edges_integration_test.go asserts
// reviews_count 1 and not 2 for a duplicated review). They need a real
// ClickHouse, so they cannot run in the default test path. This test is the
// floor that runs everywhere: it costs nothing, needs no container, and fails
// the moment either query stops reading FINAL.
//
// WHAT THIS TEST IS NOT. It is a floor, not a proof of behaviour. It asserts
// the query TEXT contains the keyword; it cannot tell you the dedup is correct,
// that the right table is deduplicated, or that the window predicate still
// applies after the collapse. Only the integration layer can. If this test is
// ever the only thing standing behind the dedup, the dedup is under-tested --
// it is deliberately narrow so that it is cheap enough to always run, and
// saying so here is part of the assertion.
func TestBothLoaderQueriesReadFINAL(t *testing.T) {
	cases := []struct {
		name  string
		query string
		table string
	}{
		{"pullRequestsQuery", pullRequestsQuery, "git_pull_requests"},
		{"reviewsQuery", reviewsQuery, "git_pull_request_reviews"},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			// Anchored to the table, so moving FINAL onto a DIFFERENT table in
			// the same query cannot satisfy this. A bare strings.Contains(q,
			// "FINAL") would pass for `FROM other_table FINAL`, which
			// deduplicates the wrong thing -- and would also pass on the word
			// appearing in a comment.
			pattern := regexp.MustCompile(`(?m)^FROM\s+` + regexp.QuoteMeta(testCase.table) + `\s+FINAL\s*$`)
			matches := pattern.FindAllString(testCase.query, -1)
			if len(matches) != 1 {
				t.Errorf(
					"%s must read `FROM %s FINAL` exactly once, found %d. Without FINAL this "+
						"loader reads the ReplacingMergeTree raw, a re-synced row is returned "+
						"twice, and reviews_count silently reinflates to Python's double count. "+
						"Neither the compute golden nor the rot guard can see that, which is why "+
						"this floor exists (#2231 codex r1).",
					testCase.name, testCase.table, len(matches))
			}
		})
	}

	// The dedup must not be reintroduced as argMax alongside FINAL. The fleet
	// three-arm rule puts an ADDED dedup on a ReplacingMergeTree source on the
	// FINAL arm; argMax here would mean someone reverted the ruling without
	// removing FINAL, leaving two dedups disagreeing about which row wins.
	for _, testCase := range cases {
		if strings.Contains(testCase.query, "argMax") {
			t.Errorf(
				"%s contains argMax as well as FINAL. An ADDED dedup on a ReplacingMergeTree "+
					"source is FINAL, not argMax (fleet three-arm rule); having both means two "+
					"dedup policies in one query.", testCase.name)
		}
	}
}
