package icfinalize

import "sort"

// GitUserMetric is the subset of UserMetricsDailyRecord compute_ic_metrics_daily
// reads or rewrites. Fields it passes through untouched are not modelled here.
type GitUserMetric struct {
	AuthorEmail        string
	TeamID             string
	LOCAdded           int64
	LOCDeleted         int64
	PRsAuthored        int64
	PRsMerged          int64
	MedianPRCycleHours float64
	PRCycleP90Hours    float64
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
	// SynthesizedRepoID marks a row with no git record, where the reference
	// invents `repo_id=uuid.uuid4()`. The caller supplies the UUID so this
	// function stays deterministic and testable; see the doc comment.
	SynthesizedRepoID bool
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
	teamMap map[string]string,
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
		if mapped, ok := teamMap[identity]; ok && mapped != "" {
			teamID = mapped
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
			SynthesizedRepoID: !hasGit,
		})
	}
	return results
}
