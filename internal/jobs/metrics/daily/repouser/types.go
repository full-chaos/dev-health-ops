// Package repouser is the native Go port of the repo_user_commit family
// (CHAOS-4275) -- the first family to leave metrics.daily's whole-partition
// HTTP compatibility bridge (internal/jobs/metrics/daily/compatibility_http.go),
// and the reference implementation the other 20 families in
// internal/jobs/metrics/daily/families.json are meant to copy.
//
// The Python authority is src/dev_health_ops/metrics/compute.py:149
// (compute_daily_metrics), plus three small helper kernels it depends on for
// repo-level knowledge/quality signals: compute_rework_churn_ratio and
// compute_single_owner_file_ratio (quality.py), and compute_bus_factor /
// compute_code_ownership_gini (knowledge.py). All four are reproduced here
// (quality.go) because compute_daily_metrics takes their output as plain
// float/int maps -- porting the aggregation without them would still leave
// those four fields (rework_churn_ratio_30d, single_owner_file_ratio_30d,
// bus_factor, code_ownership_gini) computed by a still-running Python
// process, which is not a port.
//
// # Scope: team attribution is NOT ported here
//
// Python's compute_daily_metrics accepts team_resolver/repo_team_resolver
// and uses them to set UserMetricsDailyRecord.team_id/team_name. That
// resolution is the CHAOS-2600 ClickHouse-native team/identity attribution
// engine (docs/contribute/architecture/team-attribution.md) -- a separate,
// actively-evolving subsystem with no Go implementation anywhere in this
// repo today (verified by grep before writing this package). Reimplementing
// its staged precedence here would make this "reference implementation"
// port also the reference implementation for team attribution, which is a
// different ticket's scope.
//
// # Scope: identity alias resolution is NOT ported either
//
// Python also passes a real IdentityResolver (load_identity_resolver,
// providers/identity.py) into compute_daily_metrics, which maps configured
// author aliases (e.g. two emails for one person) onto one canonical
// identity before grouping commits/PRs/reviews into user_metrics_daily rows.
// That resolver is driven by a YAML config file
// (src/dev_health_ops/config/identity_mapping.yaml) that ships with
// `identities: []` -- empty -- by default; an org only diverges from the
// plain "email, else name, else unknown" fallback (normalize_git_identity
// with no resolver match) once an operator has explicitly populated it. This
// package always uses that plain fallback (DefaultNormalizeIdentity). An org
// with a populated identity_mapping.yaml will see MORE distinct
// user_metrics_daily rows per day under the Go path than Python produced --
// one per raw identity instead of one per canonical alias group. Documented
// here and in PR RISK-NOTES for CHAOS-4275; porting the YAML-driven resolver
// is a small, bounded follow-up if an org needs it.
//
// This package therefore always resolves team_id="unassigned",
// team_name="Unassigned" for every user_metrics_daily row it writes -- the
// same fallback Python itself uses when neither resolver matches. The
// DIVERGENCE from Python is exactly the set of rows where a real resolver
// WOULD have matched a team: those rows lose their team label under the Go
// path. This is a known, deliberate, documented gap (see PR RISK-NOTES for
// CHAOS-4275), not a silent one, and should be closed by a dedicated
// follow-up ticket before any org that depends on per-user team dashboards
// is switched to the Go path in a way that matters operationally.
package repouser

import (
	"time"

	"github.com/google/uuid"
)

// CommitStatRow is one file-level delta within one commit -- the LEFT JOIN of
// git_commits and git_commit_stats in load_git_rows (loaders/clickhouse.py).
// FilePath is empty for a commit with no stat row at all (the join found no
// match), mirroring Python's `file_path: str | None`.
type CommitStatRow struct {
	RepoID        uuid.UUID
	CommitHash    string
	AuthorEmail   string
	AuthorName    string
	CommitterWhen time.Time
	FilePath      string
	Additions     int
	Deletions     int
}

// PullRequestRow mirrors dev_health_ops.metrics.schemas.PullRequestRow.
type PullRequestRow struct {
	RepoID                uuid.UUID
	Number                int
	AuthorEmail           string
	AuthorName            string
	CreatedAt             time.Time
	MergedAt              *time.Time
	FirstReviewAt         *time.Time
	FirstCommentAt        *time.Time
	ChangesRequestedCount int
	ReviewsCount          int
	CommentsCount         int
	Additions             int
	Deletions             int
	ChangedFiles          int
	// Title feeds Compute's revert-PR detection (isRevertTitle), but
	// ClickHouseLoader never populates it -- kept parity-DEAD on purpose.
	// Python's PullRequestRow (schemas.py) has no "title" field at all, and
	// loaders/clickhouse.py's real pr_query never SELECTs
	// git_pull_requests.title either, so compute.py's own
	// `pr.get("title")` always sees None in production (never "revert").
	// Populating this from ClickHouse here would make the Go port detect
	// reverts Python's production path never does -- a real behavior
	// divergence dressed up as a bug fix. If Python's gap is ever closed,
	// close this one in the same change, not before.
	Title string
}

// PullRequestReviewRow mirrors dev_health_ops.metrics.schemas.PullRequestReviewRow.
type PullRequestReviewRow struct {
	RepoID      uuid.UUID
	Number      int
	Reviewer    string
	SubmittedAt time.Time
	State       string
}

// RepoMetric mirrors dev_health_ops.metrics.schemas.RepoMetricsDailyRecord,
// restricted to the fields compute_daily_metrics actually sets (see doc
// comment on Compute for the fields Python leaves at their dataclass
// defaults, which this type does not carry at all).
type RepoMetric struct {
	RepoID                     uuid.UUID
	Day                        time.Time
	CommitsCount               int
	TotalLOCTouched            int
	AvgCommitSizeLOC           float64
	LargeCommitRatio           float64
	PRsMerged                  int
	MedianPRCycleHours         float64
	PRCycleP75Hours            float64
	PRCycleP90Hours            float64
	PRsWithFirstReview         int
	PRFirstReviewP50Hours      *float64
	PRFirstReviewP90Hours      *float64
	PRReviewTimeP50Hours       *float64
	PRPickupTimeP50Hours       *float64
	LargePRRatio               float64
	PRReworkRatio              float64
	PRSizeP50LOC               *float64
	PRSizeP90LOC               *float64
	PRCommentsPer100LOC        *float64
	PRReviewsPer100LOC         *float64
	ReworkChurnRatio30d        float64
	SingleOwnerFileRatio30d    float64
	ReviewLoadTopReviewerRatio float64
	BusFactor                  int
	CodeOwnershipGini          float64
	MTTRHours                  *float64
	ChangeFailureRate          float64
	ComputedAt                 time.Time
}

// UserMetric mirrors dev_health_ops.metrics.schemas.UserMetricsDailyRecord,
// restricted the same way. TeamID/TeamName are always "unassigned"/
// "Unassigned" -- see the package doc comment.
type UserMetric struct {
	RepoID                uuid.UUID
	Day                   time.Time
	AuthorEmail           string
	CommitsCount          int
	LOCAdded              int
	LOCDeleted            int
	FilesChanged          int
	LargeCommitsCount     int
	AvgCommitSizeLOC      float64
	PRsAuthored           int
	PRsMerged             int
	AvgPRCycleHours       float64
	MedianPRCycleHours    float64
	PRCycleP75Hours       float64
	PRCycleP90Hours       float64
	PRsWithFirstReview    int
	PRFirstReviewP50Hours *float64
	PRFirstReviewP90Hours *float64
	PRReviewTimeP50Hours  *float64
	PRPickupTimeP50Hours  *float64
	ReviewsGiven          int
	ChangesRequestedGiven int
	ReviewsReceived       int
	ReviewReciprocity     float64
	PRInterruptionLoad    int
	ContextSpreadCount    int
	ReviewRequestLoad     int
	TeamID                string
	TeamName              string
	ActiveHours           float64
	WeekendDays           int
	IdentityID            string
	ComputedAt            time.Time
}

// CommitMetric mirrors dev_health_ops.metrics.schemas.CommitMetricsRecord.
type CommitMetric struct {
	RepoID       uuid.UUID
	CommitHash   string
	Day          time.Time
	AuthorEmail  string
	TotalLOC     int
	FilesChanged int
	SizeBucket   string
	ComputedAt   time.Time
}

// Result is the return value of Compute, mirroring
// dev_health_ops.metrics.schemas.DailyMetricsResult.
type Result struct {
	RepoMetrics   []RepoMetric
	UserMetrics   []UserMetric
	CommitMetrics []CommitMetric
}
