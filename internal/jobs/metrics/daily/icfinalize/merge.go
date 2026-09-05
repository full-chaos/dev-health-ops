package icfinalize

import (
	"sort"

	"github.com/google/uuid"
)

// GitUserMetric mirrors UserMetricsDailyRecord as compute_ic_metrics_daily sees
// it -- EVERY column of the identity's already-written user_metrics_daily row,
// not just the ones this family's own derivation reads. CHAOS-5151's third
// class: compute_ic.py:143 sets `base = g`, and `dataclasses.replace(base,
// ...)` at compute_ic.py:169 carries every field of `base` FORWARD UNCHANGED
// except the ones explicitly named (identity_id, team_id, loc_touched,
// prs_opened, work_items_completed, work_items_active, delivery_units,
// cycle_p50_hours, cycle_p90_hours). A Go port that reads back only the
// columns it needs for derivation and writes back only the derived ones drops
// every OTHER column to its ClickHouse table default on the newly-inserted
// (later computed_at) row -- and since user_metrics_daily's dedup reads
// `ORDER BY computed_at DESC LIMIT 1 BY (...)`, that later row is the one
// every downstream reader sees. The column list here is
// repouser.UserMetric's own write list (internal/jobs/metrics/daily/repouser/
// clickhouse.go) -- the two are the SAME table row at two different points in
// its life, repouser writes it first per partition, this family reads it back
// and re-writes it with only the IC-derived fields changed.
type GitUserMetric struct {
	AuthorEmail string
	TeamID      string
	// RepoID is the real repo_id this identity's git row already carries.
	// compute_ic.py:143 sets `base = g` for a git-backed identity, i.e. the
	// OUTPUT row keeps the INPUT row's own repo_id verbatim -- it is never
	// replaced with a placeholder. See ICUserMetric.SynthesizedRepoID for the
	// other half: an identity with NO git record gets a synthesized one instead.
	RepoID                uuid.UUID
	LOCAdded              int64
	LOCDeleted            int64
	PRsAuthored           int64
	PRsMerged             int64
	MedianPRCycleHours    float64
	PRCycleP90Hours       float64
	CommitsCount          int64
	FilesChanged          int64
	LargeCommitsCount     int64
	AvgCommitSizeLOC      float64
	AvgPRCycleHours       float64
	PRCycleP75Hours       float64
	PRsWithFirstReview    int64
	PRFirstReviewP50Hours *float64
	PRFirstReviewP90Hours *float64
	PRReviewTimeP50Hours  *float64
	PRPickupTimeP50Hours  *float64
	ReviewsGiven          int64
	ChangesRequestedGiven int64
	ReviewsReceived       int64
	ReviewReciprocity     float64
	PRInterruptionLoad    int64
	ContextSpreadCount    int64
	ReviewRequestLoad     int64
	TeamName              string
	ActiveHours           float64
	WeekendDays           int64
}

// WorkItemUserMetric is the subset of WorkItemUserMetricsDailyRecord read here.
type WorkItemUserMetric struct {
	UserIdentity     string
	Provider         string
	WorkScopeID      string
	TeamID           string
	TeamName         string
	ItemsStarted     int64
	ItemsCompleted   int64
	WIPCountEndOfDay int64
	CycleTimeP50Hrs  float64
	CycleTimeP90Hrs  float64
}

// ICUserMetric is one merged output row.
type ICUserMetric struct {
	IdentityID        string
	TeamID            string
	LOCTouched        int64
	PRsOpened         int64
	WorkItemsComplete int64
	WorkItemsActive   int64
	DeliveryUnits     int64
	CycleP50Hours     float64
	CycleP90Hours     float64
	// RepoID is the real repo_id for a git-backed identity (base.repo_id in
	// compute_ic.py, carried through verbatim), meaningful only when
	// SynthesizedRepoID is false. The caller (writeUserMetrics) must not
	// substitute a placeholder here -- doing so was CHAOS-5151's second defect:
	// every git-backed row landed on the all-zero landscape placeholder instead
	// of its own repo, so it never deduped against the row that was already
	// on disk for that (org, repo, author, day) and every finalize attempt
	// added a NEW key rather than superseding.
	RepoID uuid.UUID
	// SynthesizedRepoID marks a row with no git record, where the reference
	// invents `repo_id=uuid.uuid4()`. The caller supplies the UUID so this
	// function stays deterministic and testable; see the doc comment.
	SynthesizedRepoID bool
	// PassThrough carries every user_metrics_daily column this family does not
	// itself derive, verbatim from the git-backed identity's own row (Python's
	// `replace(base, ...)`, see GitUserMetric's doc comment). Zero-valued for a
	// synthesized (work-item-only) identity, matching the reference's
	// zero-filled synthesized UserMetricsDailyRecord (compute_ic.py's else
	// branch) field-for-field -- Go's zero value and the reference's explicit
	// `=0`/`=0.0` literals agree on every field the reference sets, and on
	// every field it leaves to the dataclass's own default (also 0/0.0/None).
	PassThrough GitUserMetric
}

// AggregateWorkItems ports the multi-provider fold at compute_ic.py:96-124.
//
// It is explicitly "Crude aggregation" in the reference and is replicated as
// such: counts SUM, but the cycle percentiles take the MAX of the two
// providers, and `provider`/`work_scope_id` are both overwritten with the
// literal "mixed". A max of two p50s is not a p50 of anything, but it is what
// the reference computes and this is a port.
//
// `existing.cycle_time_p50_hours or 0` treats Python's None AND 0.0 alike
// (both falsy), so a provider reporting a genuine 0.0 is indistinguishable
// from one reporting nothing — replicated by using the zero value, since Go's
// float64 has the same collapse for this expression.
//
// team_id/team_name come from the LAST record folded in, not the first: the
// reference reads `wi_record.team_id`, i.e. the incoming one, on every fold.
func AggregateWorkItems(records []WorkItemUserMetric) map[string]WorkItemUserMetric {
	byIdentity := map[string]WorkItemUserMetric{}
	for _, record := range records {
		existing, seen := byIdentity[record.UserIdentity]
		if !seen {
			byIdentity[record.UserIdentity] = record
			continue
		}
		byIdentity[record.UserIdentity] = WorkItemUserMetric{
			UserIdentity:     record.UserIdentity,
			Provider:         "mixed",
			WorkScopeID:      "mixed",
			TeamID:           record.TeamID,
			TeamName:         record.TeamName,
			ItemsStarted:     existing.ItemsStarted + record.ItemsStarted,
			ItemsCompleted:   existing.ItemsCompleted + record.ItemsCompleted,
			WIPCountEndOfDay: existing.WIPCountEndOfDay + record.WIPCountEndOfDay,
			CycleTimeP50Hrs:  maxFloat(existing.CycleTimeP50Hrs, record.CycleTimeP50Hrs),
			CycleTimeP90Hrs:  maxFloat(existing.CycleTimeP90Hrs, record.CycleTimeP90Hrs),
		}
	}
	return byIdentity
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// MergeICUserMetrics ports compute_ic_metrics_daily (compute_ic.py:74).
//
// ORDER. The reference iterates `set(git) | set(wi)`, so its output row order
// is not stable across runs — Python set iteration depends on hash seed and
// insertion history. A port cannot reproduce an order the reference does not
// have, so this returns identities in SORTED order. That is a canonical choice
// and the values are order-invariant; it is recorded because "the port sorts
// and the reference does not" reads as a divergence until explained.
//
// SYNTHESIZED ROWS. An identity present only in work items has no git record,
// and the reference builds a base row with `repo_id=uuid.uuid4()` — a fresh
// random UUID on every run, written into a column that is part of
// user_metrics_daily's dedup key (org_id, repo_id, author_email, day). Those
// rows therefore never collapse across re-runs; each re-drive appends another.
// This function does not mint the UUID: it FLAGS the row via SynthesizedRepoID
// and leaves the decision to the caller, so the quirk is visible at the write
// site rather than buried in a merge. Replicating it is team-lead's Q1 ruling;
// the suspected accumulation defect is filed separately, Python untouched.
func MergeICUserMetrics(
	gitMetrics []GitUserMetric,
	workItems []WorkItemUserMetric,
	resolveTeam TeamResolver,
) []ICUserMetric {
	gitByIdentity := map[string]GitUserMetric{}
	for _, metric := range gitMetrics {
		gitByIdentity[metric.AuthorEmail] = metric
	}
	wiByIdentity := AggregateWorkItems(workItems)

	identities := make([]string, 0, len(gitByIdentity)+len(wiByIdentity))
	for identity := range gitByIdentity {
		identities = append(identities, identity)
	}
	for identity := range wiByIdentity {
		if _, alsoGit := gitByIdentity[identity]; !alsoGit {
			identities = append(identities, identity)
		}
	}
	sort.Strings(identities)

	results := make([]ICUserMetric, 0, len(identities))
	for _, identity := range identities {
		git, hasGit := gitByIdentity[identity]
		wi, hasWI := wiByIdentity[identity]

		// team_map wins over the base record's team_id, matching
		// `team_id = team_map.get(identity) or base.team_id`.
		teamID := git.TeamID
		if resolveTeam != nil {
			if mapped, ok := resolveTeam(identity); ok && mapped != "" {
				teamID = mapped
			}
		}

		var completed, active int64
		if hasWI {
			completed, active = wi.ItemsCompleted, wi.WIPCountEndOfDay
		}

		results = append(results, ICUserMetric{
			IdentityID:        identity,
			TeamID:            teamID,
			LOCTouched:        git.LOCAdded + git.LOCDeleted,
			PRsOpened:         git.PRsAuthored, // prs_authored maps to prs_opened
			WorkItemsComplete: completed,
			WorkItemsActive:   active,
			DeliveryUnits:     git.PRsMerged + completed,
			CycleP50Hours:     git.MedianPRCycleHours,
			CycleP90Hours:     git.PRCycleP90Hours,
			RepoID:            git.RepoID,
			SynthesizedRepoID: !hasGit,
			// Zero-valued when !hasGit, matching the reference's zero-filled
			// synthesized base -- see PassThrough's doc comment.
			PassThrough: git,
		})
	}
	return results
}
