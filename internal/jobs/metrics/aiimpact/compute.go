// Package aiimpact is the native Go port of the `ai_impact` metrics.daily
// family (CHAOS-4280) -- compute_ai_impact_metrics_daily (metrics/ai_impact.py:312)
// and every helper it calls.
//
// # The float rules, stated once here and enforced per call site below
//
//  1. _avg (ai_impact.py:95) is `sum(values) / len(values)` over FLOATS.
//     CPython >= 3.12's sum() is Neumaier-COMPENSATED, so a Go `total +=` loop
//     disagrees on ~16% of inputs with three or more summands. Every average
//     here goes through pythonparity.Sum. Affects cycle_time_avg_hours,
//     baseline_cycle_time_avg_hours, and transitively
//     ai_cycle_time_delta_hours and leverage_cycle_time_component.
//
//  2. _ratio (:101) and _component_delta (:300) are plain `/`, `1.0 - r` and
//     `r - 1.0`. There is NO `x*y + z` anywhere in this family, so there is no
//     arm64 FMA fusion site -- a literal transcription is bit-exact. The
//     float64(...) barriers are kept anyway, per CHAOS-4818 discipline, so
//     that a later edit introducing a product cannot silently fuse.
//
//  3. cycle_hours is (merged_at - created_at).total_seconds() / 3600.0.
//     CPython computes total_seconds() as an EXACT integer microsecond count
//     divided by 10**6, then this divides by 3600.0 -- two divisions, in that
//     order. Go's Duration.Seconds() is float64(nanoseconds)/1e9, a DIFFERENT
//     division, and Duration.Hours() is different again. Neither is used here;
//     see cycleHours.
//
// All output float columns are Nullable(Float64) (migration 036:22-49), so
// unlike the work-graph edge tables there is no Float32 quantisation step.
package aiimpact

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// AttributionBucket mirrors the StrEnum of the same name (ai_impact.py:23).
// The string values are also the storage representation in
// ai_impact_metrics_daily.attribution_bucket.
type AttributionBucket string

const (
	BucketAIAssisted   AttributionBucket = "ai_assisted"
	BucketAgentCreated AttributionBucket = "agent_created"
	BucketAIReview     AttributionBucket = "ai_review"
	BucketHuman        AttributionBucket = "human"
	BucketUnknown      AttributionBucket = "unknown"
)

// isAIBucket mirrors AI_BUCKETS (ai_impact.py:40).
func isAIBucket(bucket AttributionBucket) bool {
	return bucket == BucketAIAssisted || bucket == BucketAgentCreated || bucket == BucketAIReview
}

// bucketEmitOrder is Python's `(*sorted(AI_BUCKETS), NON_AI_BUCKET, UNKNOWN_BUCKET)`
// (:417). sorted() over a frozenset of StrEnum sorts by STRING VALUE, giving
// agent_created, ai_assisted, ai_review -- not declaration order, which would
// put ai_assisted first. Row order within a group is observable, so this is
// pinned as a literal rather than recomputed.
var bucketEmitOrder = []AttributionBucket{
	BucketAgentCreated, BucketAIAssisted, BucketAIReview, BucketHuman, BucketUnknown,
}

// PullRequestRow is the subset of PullRequestRow (metrics/schemas.py) this
// family reads. Pointer fields are the ones Python reads with .get(), where
// absent and NULL are distinguishable from a real zero.
type PullRequestRow struct {
	RepoID                uuid.UUID
	Number                int64
	CreatedAt             time.Time
	MergedAt              *time.Time
	ReviewsCount          *uint32
	ChangesRequestedCount *uint32
	Additions             *uint32
	Deletions             *uint32
	ChangedFiles          *uint32
}

type PullRequestReviewRow struct {
	RepoID      uuid.UUID
	Number      int64
	State       *string
	SubmittedAt *time.Time
}

type AttributionRow struct {
	RepoID   uuid.UUID
	Number   int64
	Kind     *string
	WorkType *string
	TeamID   *string
}

type IncidentRow struct {
	RepoID    uuid.UUID
	StartedAt time.Time
}

// CommitStatRow carries the PR-commit linkage columns _is_test_path and
// _followup_commits_by_pr read.
type CommitStatRow struct {
	FilePath      *string
	CommitHash    *string
	CommitterWhen *time.Time
	Evidence      *string
}

// Record is one ai_impact_metrics_daily row (migration 036:7).
type Record struct {
	OrgID                     string
	TeamID                    *string
	RepoID                    uuid.UUID
	WorkType                  string
	Day                       time.Time
	AttributionBucket         AttributionBucket
	PRsTotal                  uint32
	PRsMerged                 uint32
	AIAssistedPRs             uint32
	AgentCreatedPRs           uint32
	HumanPRs                  uint32
	UnknownPRs                uint32
	AIAssistedPRRatio         *float64
	AgentCreatedPRCount       uint32
	CycleTimeAvgHours         *float64
	BaselineCycleTimeAvgHours *float64
	AICycleTimeDeltaHours     *float64
	ReviewsPerPR              *float64
	BaselineReviewsPerPR      *float64
	AIReviewAmplification     *float64
	ChangesRequestedPerPR     *float64
	ReworkPRs                 uint32
	ReworkDragRate            *float64
	FollowupCommitsCount      uint32
	RevertPRs                 uint32
	RevertRate                *float64
	IncidentsCount            uint32
	IncidentDragRate          *float64
	TestGapPRs                uint32
	TestGapRate               *float64
	LeveragePRsComponent      float64
	LeverageCycleTime         *float64
	LeverageReview            *float64
	LeverageRework            *float64
	LeverageTest              *float64
	LeverageIncident          *float64
}

// TeamResolver mirrors the team_resolver callable (:20). It is consulted ONLY
// when the attribution row carries no team_id (:360).
type TeamResolver func(repoID uuid.UUID, repoName string) *string

// Params carries compute_ai_impact_metrics_daily's keyword arguments.
//
// PRCommitStats nil vs empty is LOAD-BEARING and must survive the call
// boundary: nil means the PR-commit linkage was UNAVAILABLE (the query failed,
// or was never run), which makes has_test_change unknown for every PR and
// test_gap_rate null. An empty non-nil map means the linkage ran and found
// nothing. Collapsing the two recreates CHAOS-2183's 100%-inflation bug.
type Params struct {
	Day            time.Time
	OrgID          string
	PullRequests   []PullRequestRow
	Reviews        []PullRequestReviewRow
	Attributions   []AttributionRow
	Incidents      []IncidentRow
	PRCommitStats  map[PRKey][]CommitStatRow
	HasCommitStats bool
	TeamResolver   TeamResolver
	RepoNamesByID  map[uuid.UUID]string
}

// PRKey is the (repo_id, number) tuple Python uses as a dict key throughout.
type PRKey struct {
	RepoID uuid.UUID
	Number int64
}

type prFact struct {
	repoID           uuid.UUID
	number           int64
	bucket           AttributionBucket
	workType         string
	teamID           *string
	merged           bool
	cycleHours       *float64
	reviews          uint32
	changesRequested uint32
	additions        uint32
	deletions        uint32
	changedFiles     uint32
	hasTestChange    *bool // nil = commit linkage unavailable for this PR
	followupCommits  uint32
}

type aggregate struct {
	prsTotal              uint32
	prsMerged             uint32
	cycleAvg              *float64
	reviewsPerPR          *float64
	changesRequestedPerPR *float64
	reworkPRs             uint32
	reworkRate            *float64
	followupCommits       uint32
	revertPRs             uint32
	revertRate            *float64
	incidentsCount        uint32
	incidentRate          *float64
	testGapPRs            uint32
	testGapRate           *float64
}

// mean ports _avg (:95). pythonparity.Sum is REQUIRED, not a stylistic
// choice: CPython >= 3.12's sum() over floats is Neumaier-compensated.
func mean(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	result := float64(pythonparity.Sum(values)) / float64(len(values))
	return &result
}

// ratio ports _ratio (:101): None when the denominator is zero, else a plain
// float division. Python coerces both operands with float() first, which for
// ints is exact.
func ratio(numerator, denominator float64) *float64 {
	if denominator == 0 {
		return nil
	}
	result := float64(numerator) / float64(denominator)
	return &result
}

// componentDelta ports _component_delta (:300).
func componentDelta(value, baseline *float64, lowerIsBetter bool) *float64 {
	if value == nil || baseline == nil || *baseline == 0 {
		return nil
	}
	r := float64(*value) / float64(*baseline)
	var result float64
	if lowerIsBetter {
		result = float64(1.0 - r)
	} else {
		result = float64(r - 1.0)
	}
	return &result
}

// cycleHours ports `(merged_at - created_at).total_seconds() / 3600.0` (:373).
//
// CPython's timedelta.total_seconds() is
// ((days*86400 + seconds) * 10**6 + microseconds) / 10**6 -- an EXACT integer
// numerator divided ONCE, at microsecond resolution. Go's
// Duration.Seconds() divides a NANOSECOND count by 1e9, and Duration.Hours()
// divides by 3.6e12; both are different roundings of the same quantity.
// Reproducing Python means building the microsecond integer, dividing by 1e6,
// and only then dividing by 3600.0 -- two divisions, in that order.
//
// The column is DateTime64(3), so inputs are millisecond-resolution and the
// microsecond truncation below is exact for every real value.
func cycleHours(createdAt, mergedAt time.Time) float64 {
	microseconds := mergedAt.Sub(createdAt).Microseconds()
	totalSeconds := float64(microseconds) / 1e6
	return float64(totalSeconds) / 3600.0
}

// safeBucket ports _safe_bucket (:107).
//
// ASCII-CONTAINMENT NOTE (required by pythonparity.Lower's doc comment):
// Lower carries a bounded-Final_Sigma divergence from CPython, which can only
// change an answer when a comparand is non-ASCII. Every bucket name compared
// below is ASCII, so a value differing only in sigma form fails every
// comparison either way and lands in "unknown" identically. Pinned by
// TestSigmaFormCannotChangeABucket.
func safeBucket(kind *string) AttributionBucket {
	if kind == nil || *kind == "" {
		return BucketUnknown
	}
	normalized := strings.ReplaceAll(pythonparity.Lower(pythonparity.Strip(*kind)), "-", "_")
	switch AttributionBucket(normalized) {
	case BucketAIAssisted, BucketAgentCreated, BucketAIReview, BucketHuman:
		return AttributionBucket(normalized)
	}
	return BucketUnknown
}

// isTestPath ports _is_test_path (:116). Same ASCII-containment argument as
// safeBucket: every literal compared here is ASCII.
func isTestPath(path *string) bool {
	if path == nil || *path == "" {
		return false
	}
	lower := pythonparity.Lower(*path)
	return strings.Contains(lower, "/test/") ||
		strings.Contains(lower, "/tests/") ||
		strings.HasPrefix(lower, "test/") ||
		strings.HasPrefix(lower, "tests/") ||
		strings.HasSuffix(lower, "_test.py") ||
		strings.HasSuffix(lower, ".test.ts") ||
		strings.HasSuffix(lower, ".test.tsx") ||
		strings.HasSuffix(lower, ".spec.ts") ||
		strings.HasSuffix(lower, ".spec.tsx")
}

// mergeArtifactEvidence ports _MERGE_ARTIFACT_EVIDENCE (:190) -- the evidence
// values marking a PR's OWN merge/squash commit, which is the merge artifact
// rather than follow-up work.
func isMergeArtifactEvidence(evidence *string) bool {
	if evidence == nil {
		return false
	}
	return *evidence == "commit_message_pr_ref" || *evidence == "commit_message_squash_pr_ref"
}

// toUTC ports _to_utc (:89). ClickHouse hands us UTC already; this keeps the
// conversion explicit rather than rebuilding a wall clock.
func toUTC(value time.Time) time.Time { return value.UTC() }

// Compute ports compute_ai_impact_metrics_daily (:312).
func Compute(params Params) []Record {
	dayUTC := params.Day.UTC()
	start := time.Date(dayUTC.Year(), dayUTC.Month(), dayUTC.Day(), 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 0, 1)

	attributionByPR := indexAttributions(params.Attributions)
	reviewCounts := reviewsByPR(params.Reviews)
	firstReviewAt := firstReviewAtByPR(params.Reviews)
	prIndex := make(map[PRKey]PullRequestRow, len(params.PullRequests))
	for _, pr := range params.PullRequests {
		prIndex[PRKey{RepoID: pr.RepoID, Number: pr.Number}] = pr
	}
	testChanges := testChangesByPR(params.PRCommitStats, params.HasCommitStats)
	followupByPR := followupCommitsByPR(params.PRCommitStats, params.HasCommitStats, prIndex, firstReviewAt)

	facts := make([]prFact, 0, len(params.PullRequests))
	for _, pr := range params.PullRequests {
		createdAt := toUTC(pr.CreatedAt)
		var mergedAt *time.Time
		if pr.MergedAt != nil {
			merged := toUTC(*pr.MergedAt)
			mergedAt = &merged
		}
		eventAt := createdAt
		if mergedAt != nil {
			eventAt = *mergedAt
		}
		if eventAt.Before(start) || !eventAt.Before(end) {
			continue
		}

		key := PRKey{RepoID: pr.RepoID, Number: pr.Number}
		attribution, hasAttribution := attributionByPR[key]

		var kind *string
		workType := "pull_request"
		var teamID *string
		if hasAttribution {
			kind = attribution.Kind
			// `str(attr.get("work_type") or "pull_request")` -- an empty string
			// is falsy in Python, so it too falls back to "pull_request".
			if attribution.WorkType != nil && *attribution.WorkType != "" {
				workType = *attribution.WorkType
			}
			teamID = attribution.TeamID
		}
		bucket := safeBucket(kind)
		if teamID == nil && params.TeamResolver != nil {
			teamID = params.TeamResolver(pr.RepoID, params.RepoNamesByID[pr.RepoID])
		}

		derivedReviews, derivedChangesRequested := reviewCounts[key][0], reviewCounts[key][1]
		// `int(pr.get("reviews_count", reviews) or reviews)` -- Python's `or`
		// means a reviews_count of 0 (or NULL) falls BACK to the count derived
		// from review rows, rather than winning as a real zero. Same for
		// changes_requested_count. Reproducing the truthiness is load-bearing:
		// treating the column as authoritative would zero out every PR whose
		// denormalised counter has not been backfilled.
		reviews := derivedReviews
		if pr.ReviewsCount != nil && *pr.ReviewsCount != 0 {
			reviews = *pr.ReviewsCount
		}
		changesRequested := derivedChangesRequested
		if pr.ChangesRequestedCount != nil && *pr.ChangesRequestedCount != 0 {
			changesRequested = *pr.ChangesRequestedCount
		}

		var cycle *float64
		if mergedAt != nil {
			hours := cycleHours(createdAt, *mergedAt)
			cycle = &hours
		}

		facts = append(facts, prFact{
			repoID: pr.RepoID, number: pr.Number, bucket: bucket, workType: workType,
			teamID: teamID, merged: mergedAt != nil, cycleHours: cycle,
			reviews: reviews, changesRequested: changesRequested,
			additions:    derefUint32(pr.Additions),
			deletions:    derefUint32(pr.Deletions),
			changedFiles: derefUint32(pr.ChangedFiles),
			// nil (not false) when the linkage was unavailable or this PR was
			// absent from it -- unknown, never a gap.
			hasTestChange:   testChanges[key],
			followupCommits: followupByPR[key],
		})
	}

	incidentsByRepo := make(map[uuid.UUID]uint32)
	for _, incident := range params.Incidents {
		startedAt := toUTC(incident.StartedAt)
		if !startedAt.Before(start) && startedAt.Before(end) {
			incidentsByRepo[incident.RepoID]++
		}
	}

	return buildRecords(params, start, facts, incidentsByRepo)
}

func derefUint32(value *uint32) uint32 {
	if value == nil {
		return 0
	}
	return *value
}

// indexAttributions ports _attribution_index (:133): last row wins per key.
func indexAttributions(rows []AttributionRow) map[PRKey]AttributionRow {
	indexed := make(map[PRKey]AttributionRow, len(rows))
	for _, row := range rows {
		indexed[PRKey{RepoID: row.RepoID, Number: row.Number}] = row
	}
	return indexed
}

// reviewsByPR ports _reviews_by_pr (:142).
//
// The CHANGES_REQUESTED test is `str(row.get("state") or "").upper()`, so it
// uses CPython's FULL uppercase mapping. pythonparity.Upper reproduces it; the
// comparand is ASCII, so the bounded-Final_Sigma divergence cannot change the
// answer (same containment argument as safeBucket).
func reviewsByPR(rows []PullRequestReviewRow) map[PRKey][2]uint32 {
	counts := make(map[PRKey][2]uint32)
	for _, row := range rows {
		key := PRKey{RepoID: row.RepoID, Number: row.Number}
		entry := counts[key]
		entry[0]++
		state := ""
		if row.State != nil {
			state = *row.State
		}
		if pythonparity.Upper(state) == "CHANGES_REQUESTED" {
			entry[1]++
		}
		counts[key] = entry
	}
	return counts
}

// testChangesByPR ports _test_changes_by_pr (:154). When the linkage is
// unavailable it returns an EMPTY map, so every lookup yields nil (unknown)
// rather than false (a gap) -- never manufacturing coverage evidence from
// repo-level commits.
func testChangesByPR(stats map[PRKey][]CommitStatRow, hasStats bool) map[PRKey]*bool {
	result := make(map[PRKey]*bool)
	if !hasStats {
		return result
	}
	for key, rows := range stats {
		found := false
		for _, row := range rows {
			if isTestPath(row.FilePath) {
				found = true
				break
			}
		}
		value := found
		result[key] = &value
	}
	return result
}

// firstReviewAtByPR ports _first_review_at_by_pr (:168): the earliest review
// timestamp per PR, which is the feedback boundary for rework.
func firstReviewAtByPR(rows []PullRequestReviewRow) map[PRKey]time.Time {
	result := make(map[PRKey]time.Time)
	for _, row := range rows {
		if row.SubmittedAt == nil {
			continue
		}
		key := PRKey{RepoID: row.RepoID, Number: row.Number}
		timestamp := toUTC(*row.SubmittedAt)
		if current, seen := result[key]; !seen || timestamp.Before(current) {
			result[key] = timestamp
		}
	}
	return result
}

// followupCommitsByPR ports _followup_commits_by_pr (:195).
//
// A follow-up commit is a DISTINCT commit linked to the PR that landed after
// the feedback boundary (first review time, else PR creation). Two guards keep
// the merge artifact itself from counting as rework, so a squash-merge PR
// contributes 0 rather than a phantom 1:
//
//   - evidence: commits linked as the PR's own merge/squash commit are
//     excluded. On a squash-merge repo that commit is the ONLY one the linkage
//     attaches, and its committer_when sits just BEFORE merged_at, so a
//     timestamp bound alone would miss it.
//   - merged_at: a defensive `>= merged_at` cut for any merge commit reaching
//     here without artifact evidence.
//
// The artifact exclusion is two-phase in Python and must stay so: a hash is
// collected into artifact_hashes and then POPPED from commit_times, so a
// commit seen first with ordinary evidence and later as the merge artifact is
// still excluded. A single-pass `continue` would keep it.
func followupCommitsByPR(
	stats map[PRKey][]CommitStatRow, hasStats bool,
	prIndex map[PRKey]PullRequestRow, firstReviewAt map[PRKey]time.Time,
) map[PRKey]uint32 {
	result := make(map[PRKey]uint32)
	if !hasStats || len(stats) == 0 {
		return result
	}
	for key, rows := range stats {
		pr, known := prIndex[key]
		if !known {
			continue
		}
		boundary, reviewed := firstReviewAt[key]
		if !reviewed {
			boundary = toUTC(pr.CreatedAt)
		}
		var mergedAt *time.Time
		if pr.MergedAt != nil {
			merged := toUTC(*pr.MergedAt)
			mergedAt = &merged
		}

		commitTimes := make(map[string]time.Time)
		artifactHashes := make(map[string]struct{})
		for _, row := range rows {
			if row.CommitHash == nil || *row.CommitHash == "" || row.CommitterWhen == nil {
				continue
			}
			hash := *row.CommitHash
			if isMergeArtifactEvidence(row.Evidence) {
				artifactHashes[hash] = struct{}{}
				continue
			}
			commitTimes[hash] = toUTC(*row.CommitterWhen)
		}
		for hash := range artifactHashes {
			delete(commitTimes, hash)
		}

		var count uint32
		for _, when := range commitTimes {
			if !when.After(boundary) {
				continue
			}
			if mergedAt != nil && !when.Before(*mergedAt) {
				continue
			}
			count++
		}
		result[key] = count
	}
	return result
}

// aggregateFacts ports _aggregate (:261).
func aggregateFacts(facts []prFact, incidentsCount uint32) aggregate {
	cycles := make([]float64, 0, len(facts))
	var prsMerged, reworkPRs, followupCommits, revertPRs uint32
	var knownTestPRs, testGapPRs uint32
	// WIDENED TO uint64 ON PURPOSE (codex round chaos-4280-r1, finding 3).
	// Each fact.reviews/fact.changesRequested is a UInt32 wire value, so any
	// ONE of them fits uint32 -- but Python sums arbitrary-precision integers
	// across the whole group, and a uint32 ACCUMULATOR wraps once the group's
	// total crosses 2**32. Measured: two PRs each with reviews_count
	// 3_000_000_000 sum to 6_000_000_000 in Python; a uint32 accumulator wraps
	// that to 1_705_032_704, corrupting reviews_per_pr for the whole group.
	var reviews, changesRequested uint64
	for _, fact := range facts {
		if fact.cycleHours != nil {
			cycles = append(cycles, *fact.cycleHours)
		}
		if fact.merged {
			prsMerged++
		}
		reviews += uint64(fact.reviews)
		changesRequested += uint64(fact.changesRequested)
		if fact.changesRequested > 0 || fact.followupCommits > 0 {
			reworkPRs++
		}
		followupCommits += fact.followupCommits
		// Integer comparison, exactly as Python: deletions > additions*2 AND
		// deletions >= 50. No float anywhere, so no FMA site.
		//
		// WIDENED TO uint64 ON PURPOSE. additions is UInt32 on the wire, and
		// CPython's ints are arbitrary precision, so Python evaluates
		// additions*2 exactly. In uint32 the same product WRAPS: at
		// additions = 2**31 (a perfectly valid column value) additions*2
		// becomes 0, and `deletions > 0` flips this PR to a revert when Python
		// says it is not one. Measured, not theorised -- python gives False,
		// uint32 Go gives True for (additions=2**31, deletions=100).
		// Pinned by TestRevertRuleDoesNotWrapOnLargeAdditions.
		if uint64(fact.deletions) > uint64(fact.additions)*2 && fact.deletions >= 50 {
			revertPRs++
		}
		// Only PRs whose test-change status is KNOWN contribute to the gap
		// denominator (CHAOS-2183): counting unknowns as gaps recreates the
		// 100%-inflation bug whenever the linkage table is missing.
		if fact.hasTestChange != nil {
			knownTestPRs++
			if !*fact.hasTestChange {
				testGapPRs++
			}
		}
	}
	prsTotal := uint32(len(facts))
	return aggregate{
		prsTotal:              prsTotal,
		prsMerged:             prsMerged,
		cycleAvg:              mean(cycles),
		reviewsPerPR:          ratio(float64(reviews), float64(prsTotal)),
		changesRequestedPerPR: ratio(float64(changesRequested), float64(prsTotal)),
		reworkPRs:             reworkPRs,
		reworkRate:            ratio(float64(reworkPRs), float64(prsTotal)),
		followupCommits:       followupCommits,
		revertPRs:             revertPRs,
		revertRate:            ratio(float64(revertPRs), float64(prsTotal)),
		incidentsCount:        incidentsCount,
		incidentRate:          ratio(float64(incidentsCount), float64(prsMerged)),
		testGapPRs:            testGapPRs,
		testGapRate:           ratio(float64(testGapPRs), float64(knownTestPRs)),
	}
}

type groupKey struct {
	teamID   string
	hasTeam  bool
	repoID   uuid.UUID
	workType string
}

// buildRecords ports the grouping and emission loop (:407-495).
//
// ORDER: Python groups into a plain dict and iterates it in INSERTION order
// (:412), i.e. the order facts were built, which follows the loader's row
// order -- and the loader has no ORDER BY, so Python's row order is already
// nondeterministic. A canonical sort is emitted instead; the differential
// oracle asserts set equality keyed by the table's ORDER BY key plus a
// separate order-invariance property, rather than pretending an order exists.
func buildRecords(
	params Params, day time.Time, facts []prFact, incidentsByRepo map[uuid.UUID]uint32,
) []Record {
	grouped := make(map[groupKey][]prFact)
	var order []groupKey
	for _, fact := range facts {
		key := groupKey{repoID: fact.repoID, workType: fact.workType}
		if fact.teamID != nil {
			key.teamID, key.hasTeam = *fact.teamID, true
		}
		if _, seen := grouped[key]; !seen {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], fact)
	}
	sort.SliceStable(order, func(i, j int) bool {
		left, right := order[i], order[j]
		if left.teamID != right.teamID {
			return left.teamID < right.teamID
		}
		if left.hasTeam != right.hasTeam {
			return !left.hasTeam
		}
		if left.repoID != right.repoID {
			return left.repoID.String() < right.repoID.String()
		}
		return left.workType < right.workType
	})

	var records []Record
	for _, key := range order {
		groupFacts := grouped[key]
		baseline := aggregateFacts(filterBucket(groupFacts, BucketHuman), 0)
		groupTotal := uint32(len(groupFacts))

		var aiCount, aiAssistedCount, agentCreatedCount, humanCount, unknownCount uint32
		for _, fact := range groupFacts {
			if isAIBucket(fact.bucket) {
				aiCount++
			}
			switch fact.bucket {
			case BucketAIAssisted:
				aiAssistedCount++
			case BucketAgentCreated:
				agentCreatedCount++
			case BucketHuman:
				humanCount++
			case BucketUnknown:
				unknownCount++
			}
		}

		var teamID *string
		if key.hasTeam {
			team := key.teamID
			teamID = &team
		}

		for _, bucket := range bucketEmitOrder {
			bucketFacts := filterBucket(groupFacts, bucket)
			// The unknown bucket ALWAYS emits a row, even when empty (:419).
			if len(bucketFacts) == 0 && bucket != BucketUnknown {
				continue
			}
			var bucketIncidents uint32
			if isAIBucket(bucket) {
				bucketIncidents = incidentsByRepo[key.repoID]
			}
			agg := aggregateFacts(bucketFacts, bucketIncidents)

			var cycleDelta *float64
			if agg.cycleAvg != nil && baseline.cycleAvg != nil {
				delta := float64(*agg.cycleAvg - *baseline.cycleAvg)
				cycleDelta = &delta
			}
			reviewAmplification := componentDelta(agg.reviewsPerPR, baseline.reviewsPerPR, false)

			records = append(records, Record{
				OrgID: params.OrgID, TeamID: teamID, RepoID: key.repoID,
				WorkType: key.workType, Day: day, AttributionBucket: bucket,
				PRsTotal: agg.prsTotal, PRsMerged: agg.prsMerged,
				AIAssistedPRs: aiAssistedCount, AgentCreatedPRs: agentCreatedCount,
				HumanPRs: humanCount, UnknownPRs: unknownCount,
				AIAssistedPRRatio:   ratio(float64(aiAssistedCount), float64(groupTotal)),
				AgentCreatedPRCount: agentCreatedCount,
				CycleTimeAvgHours:   agg.cycleAvg, BaselineCycleTimeAvgHours: baseline.cycleAvg,
				AICycleTimeDeltaHours: cycleDelta,
				ReviewsPerPR:          agg.reviewsPerPR, BaselineReviewsPerPR: baseline.reviewsPerPR,
				AIReviewAmplification: reviewAmplification,
				ChangesRequestedPerPR: agg.changesRequestedPerPR,
				ReworkPRs:             agg.reworkPRs, ReworkDragRate: agg.reworkRate,
				FollowupCommitsCount: agg.followupCommits,
				RevertPRs:            agg.revertPRs, RevertRate: agg.revertRate,
				IncidentsCount: agg.incidentsCount, IncidentDragRate: agg.incidentRate,
				TestGapPRs: agg.testGapPRs, TestGapRate: agg.testGapRate,
				LeveragePRsComponent: float64(aiCount),
				LeverageCycleTime:    componentDelta(agg.cycleAvg, baseline.cycleAvg, true),
				LeverageReview:       reviewAmplification,
				LeverageRework:       componentDelta(agg.reworkRate, baseline.reworkRate, true),
				LeverageTest:         componentDelta(agg.testGapRate, baseline.testGapRate, true),
				LeverageIncident:     componentDelta(agg.incidentRate, baseline.incidentRate, true),
			})
		}
	}
	return records
}

func filterBucket(facts []prFact, bucket AttributionBucket) []prFact {
	filtered := make([]prFact, 0, len(facts))
	for _, fact := range facts {
		if fact.bucket == bucket {
			filtered = append(filtered, fact)
		}
	}
	return filtered
}
