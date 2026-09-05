// Package reviewedges is the native Go port of the Python `review_edges`
// daily metrics family (CHAOS-4279):
// `compute_review_edges_daily` (src/dev_health_ops/metrics/reviews.py:22),
// which builds reviewer -> author edge counts for reviews submitted in a day.
//
// # Write mode
//
// review_edges_daily is a PLAIN MergeTree
// (migrations/clickhouse/004_quality_delivery_metrics.sql:11-20),
// PARTITION BY toYYYYMM(day), ORDER BY (repo_id, reviewer, author, day). It is
// append-only, and `org_id` was bolted on later by migration 024 and is NOT in
// the sort key. Readers deduplicate on READ via clickhouse_dedup.py:115
// (`LIMIT 1 BY (org_id, repo_id, reviewer, author, day) ORDER BY computed_at
// DESC`), so a recompute leaves duplicate rows behind physically. The Python
// write-skip guard is therefore load-bearing, not cosmetic.
//
// # The dropped-edge quirk, mirrored deliberately
//
// The author of a reviewed PR is resolved from a map built over the day's PR
// rows, and the PR loader's window is `created_at ∈ [day, day+1)` OR
// `merged_at ∈ [day, day+1)` (loaders/clickhouse.py:300-307). A review
// submitted today on a PR that was neither created nor merged today therefore
// finds NO author and its edge is silently DROPPED (reviews.py:52-54). That is
// almost certainly a defect in the Python producer, but it is not this port's
// to fix: it is reproduced exactly, pinned by a golden case, and recorded.
package reviewedges

import (
	"time"

	"github.com/google/uuid"
)

// PullRequestRow is the subset of git_pull_requests `compute_review_edges_daily`
// reads. The Python loader coerces both author columns with `or ""`
// (loaders/clickhouse.py:361-362), so an absent value arrives as the empty
// string rather than as NULL, and the identity fallback -- not a nil check --
// is what turns it into "unknown".
type PullRequestRow struct {
	RepoID      uuid.UUID
	Number      uint32
	AuthorEmail string
	AuthorName  string
}

// ReviewRow is the subset of git_pull_request_reviews the compute reads. The
// Python loader coerces `reviewer` with `or "unknown"`
// (loaders/clickhouse.py:389), so an empty or NULL reviewer is already the
// literal "unknown" before compute sees it.
type ReviewRow struct {
	RepoID      uuid.UUID
	Number      uint32
	Reviewer    string
	SubmittedAt time.Time
}

// Record is one review_edges_daily row, field-for-field ReviewEdgeDailyRecord
// (schemas.py:613-620).
type Record struct {
	RepoID       uuid.UUID
	Day          time.Time
	Reviewer     string
	Author       string
	ReviewsCount uint32
	ComputedAt   time.Time
	OrgID        string
}
