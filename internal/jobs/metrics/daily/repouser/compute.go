package repouser

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// CommitSizeBucket mirrors commit_size_bucket (compute.py:22).
func CommitSizeBucket(totalLOC int) string {
	switch {
	case totalLOC <= 50:
		return "small"
	case totalLOC <= 300:
		return "medium"
	default:
		return "large"
	}
}

func utcDayWindow(day time.Time) (time.Time, time.Time) {
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	return start, start.Add(24 * time.Hour)
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

// percentile mirrors compute.py's module-level _percentile: linear
// interpolation between closest ranks, NOT the truncating integer variant
// numerical.IntegerPercentiles uses for the capacity family. The two are
// different kernels in Python (compute.py._percentile vs
// compute_capacity._percentile) and must stay different here too.
func percentile(values []float64, pct float64) float64 {
	if len(values) == 0 {
		return 0
	}
	switch {
	case pct <= 0:
		return minFloat(values)
	case pct >= 100:
		return maxFloat(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := float64(len(sorted)-1) * (pct / 100.0)
	lo := int(rank)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func minFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}

func maxFloat(values []float64) float64 {
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}

func ptr(value float64) *float64 { return &value }

// commitAgg mirrors compute.py's _CommitAgg: one row per (repo_id, commit_hash).
type commitAgg struct {
	repoID         uuid.UUID
	commitHash     string
	authorIdentity string
	committerWhen  time.Time
	additions      int
	deletions      int
	files          map[string]struct{}
}

func (agg *commitAgg) totalLOC() int     { return agg.additions + agg.deletions }
func (agg *commitAgg) filesChanged() int { return len(agg.files) }

// userAgg mirrors compute.py's _UserAgg: one row per (repo_id, author_identity).
type userAgg struct {
	repoID                uuid.UUID
	commitsCount          int
	locAdded              int
	locDeleted            int
	files                 map[string]struct{}
	largeCommitsCount     int
	prsAuthored           int
	prsMerged             int
	prCycleTimes          []float64
	prFirstReviewTimes    []float64
	prReviewTimes         []float64
	prPickupTimes         []float64
	prsWithFirstReview    int
	reviewsGiven          int
	changesRequestedGiven int
	reviewsReceived       int
	reviewedPRNumbers     map[int]struct{}
	receivedReviewPRs     map[int]struct{}
	activityTimestamps    []time.Time
}

func newUserAgg(repoID uuid.UUID) *userAgg {
	return &userAgg{
		repoID:            repoID,
		files:             map[string]struct{}{},
		reviewedPRNumbers: map[int]struct{}{},
		receivedReviewPRs: map[int]struct{}{},
	}
}

type userKey struct {
	repoID   uuid.UUID
	identity string
}

// Compute mirrors compute_daily_metrics (compute.py:149). day is the target
// UTC day (time-of-day/location ignored, matching Python's `date`). The four
// by-repo maps are the outputs of ReworkChurnRatio/SingleOwnerFileRatio/
// BusFactor/CodeOwnershipGini (quality.go) and MTTRByRepo, computed by the
// caller over the same 30-day window Python uses (h_commit_rows in
// job_daily.py). A nil map is treated as "no data for any repo", matching
// Python's `if rework_churn_ratio_by_repo else 0.0` guard.
//
// normalizeIdentity mirrors normalize_git_identity: Python resolves an
// author's canonical identity through an optional IdentityResolver before
// grouping. This package has no such resolver (identity resolution is a
// separate, ClickHouse-backed subsystem with no Go port yet, same rationale
// as the team-attribution gap in the package doc comment); callers pass a
// function that returns the identity to group by -- production callers pass
// a function that lower-cases/trims the email the same way Python's
// no-resolver fallback does, tests can pass identity.
func Compute(
	day time.Time,
	commits []CommitStatRow,
	pullRequests []PullRequestRow,
	reviews []PullRequestReviewRow,
	computedAt time.Time,
	normalizeIdentity func(authorEmail, authorName string) string,
	largePRTotalLOCThreshold int,
	mttrByRepo map[uuid.UUID]float64,
	reworkChurnRatioByRepo map[uuid.UUID]float64,
	singleOwnerFileRatioByRepo map[uuid.UUID]float64,
	busFactorByRepo map[uuid.UUID]int,
	codeOwnershipGiniByRepo map[uuid.UUID]float64,
) Result {
	start, end := utcDayWindow(day)
	computedAtUTC := computedAt.UTC()

	// 1) Build per-commit aggregates.
	commitAggs := map[[2]string]*commitAgg{}
	commitOrder := make([][2]string, 0, len(commits))
	for _, row := range commits {
		key := [2]string{row.RepoID.String(), row.CommitHash}
		agg, ok := commitAggs[key]
		if !ok {
			agg = &commitAgg{
				repoID:         row.RepoID,
				commitHash:     row.CommitHash,
				authorIdentity: normalizeIdentity(row.AuthorEmail, row.AuthorName),
				committerWhen:  row.CommitterWhen.UTC(),
				files:          map[string]struct{}{},
			}
			commitAggs[key] = agg
			commitOrder = append(commitOrder, key)
		}
		agg.additions += maxInt(0, row.Additions)
		agg.deletions += maxInt(0, row.Deletions)
		if row.FilePath != "" {
			agg.files[row.FilePath] = struct{}{}
		}
	}

	// 2) Roll up commit aggregates to per-user.
	userAggs := map[userKey]*userAgg{}
	userOrder := make([]userKey, 0)
	getOrCreateUser := func(repoID uuid.UUID, identity string) *userAgg {
		key := userKey{repoID: repoID, identity: identity}
		agg, ok := userAggs[key]
		if !ok {
			agg = newUserAgg(repoID)
			userAggs[key] = agg
			userOrder = append(userOrder, key)
		}
		return agg
	}
	for _, key := range commitOrder {
		agg := commitAggs[key]
		ua := getOrCreateUser(agg.repoID, agg.authorIdentity)
		ua.commitsCount++
		ua.locAdded += agg.additions
		ua.locDeleted += agg.deletions
		for path := range agg.files {
			ua.files[path] = struct{}{}
		}
		if agg.totalLOC() > 300 {
			ua.largeCommitsCount++
		}
		ua.activityTimestamps = append(ua.activityTimestamps, agg.committerWhen)
	}

	// 3) Process PR rows in the day window.
	repoCycleTimes := map[uuid.UUID][]float64{}
	repoFirstReviewTimes := map[uuid.UUID][]float64{}
	repoReviewTimes := map[uuid.UUID][]float64{}
	repoPickupTimes := map[uuid.UUID][]float64{}
	repoLargePRs := map[uuid.UUID]int{}
	repoReworkPRs := map[uuid.UUID]int{}
	repoRevertPRs := map[uuid.UUID]int{}
	repoPRsWithFirstReview := map[uuid.UUID]int{}
	repoPRSizes := map[uuid.UUID][]float64{}
	repoPRLOCTotals := map[uuid.UUID]int{}
	repoPRCommentTotals := map[uuid.UUID]int{}
	repoPRReviewTotals := map[uuid.UUID]int{}
	repoReviewers := map[uuid.UUID]map[string]int{}
	type prKey struct {
		repoID uuid.UUID
		number int
	}
	prAuthorByKey := map[prKey]string{}

	for _, pr := range pullRequests {
		authorIdentity := normalizeIdentity(pr.AuthorEmail, pr.AuthorName)
		prAuthorByKey[prKey{repoID: pr.RepoID, number: pr.Number}] = authorIdentity
		ua := getOrCreateUser(pr.RepoID, authorIdentity)

		createdAt := pr.CreatedAt.UTC()
		if !createdAt.Before(start) && createdAt.Before(end) {
			ua.prsAuthored++
			ua.activityTimestamps = append(ua.activityTimestamps, createdAt)
		}

		if pr.MergedAt == nil {
			continue
		}
		mergedAt := pr.MergedAt.UTC()
		if mergedAt.Before(start) || !mergedAt.Before(end) {
			continue
		}
		ua.prsMerged++
		cycleHours := mergedAt.Sub(createdAt).Seconds() / 3600.0
		ua.prCycleTimes = append(ua.prCycleTimes, cycleHours)
		repoCycleTimes[pr.RepoID] = append(repoCycleTimes[pr.RepoID], cycleHours)

		additions := maxInt(0, pr.Additions)
		deletions := maxInt(0, pr.Deletions)
		totalLOC := additions + deletions
		if totalLOC >= largePRTotalLOCThreshold {
			repoLargePRs[pr.RepoID]++
		}
		if totalLOC > 0 {
			repoPRSizes[pr.RepoID] = append(repoPRSizes[pr.RepoID], float64(totalLOC))
			repoPRLOCTotals[pr.RepoID] += totalLOC
			repoPRCommentTotals[pr.RepoID] += pr.CommentsCount
			repoPRReviewTotals[pr.RepoID] += pr.ReviewsCount
		}

		if pr.ChangesRequestedCount > 0 {
			repoReworkPRs[pr.RepoID]++
		}

		if isRevertTitle(pr.Title) {
			repoRevertPRs[pr.RepoID]++
		}

		if pr.FirstReviewAt != nil {
			firstReviewAt := pr.FirstReviewAt.UTC()
			ua.prsWithFirstReview++
			repoPRsWithFirstReview[pr.RepoID]++

			frHours := firstReviewAt.Sub(createdAt).Seconds() / 3600.0
			ua.prFirstReviewTimes = append(ua.prFirstReviewTimes, frHours)
			repoFirstReviewTimes[pr.RepoID] = append(repoFirstReviewTimes[pr.RepoID], frHours)

			reviewHours := mergedAt.Sub(firstReviewAt).Seconds() / 3600.0
			ua.prReviewTimes = append(ua.prReviewTimes, reviewHours)
			repoReviewTimes[pr.RepoID] = append(repoReviewTimes[pr.RepoID], reviewHours)
		}

		var interactionAt *time.Time
		if pr.FirstCommentAt != nil {
			t := pr.FirstCommentAt.UTC()
			interactionAt = &t
		}
		if pr.FirstReviewAt != nil {
			frUTC := pr.FirstReviewAt.UTC()
			if interactionAt == nil || frUTC.Before(*interactionAt) {
				interactionAt = &frUTC
			}
		}
		if interactionAt != nil {
			pickupHours := interactionAt.Sub(createdAt).Seconds() / 3600.0
			ua.prPickupTimes = append(ua.prPickupTimes, pickupHours)
			repoPickupTimes[pr.RepoID] = append(repoPickupTimes[pr.RepoID], pickupHours)
		}
	}

	// 3b) Review participation.
	for _, review := range reviews {
		submittedAt := review.SubmittedAt.UTC()
		if submittedAt.Before(start) || !submittedAt.Before(end) {
			continue
		}
		reviewerIdentity := normalizeIdentity("", review.Reviewer)
		rua := getOrCreateUser(review.RepoID, reviewerIdentity)
		rua.reviewsGiven++
		rua.activityTimestamps = append(rua.activityTimestamps, submittedAt)
		rua.reviewedPRNumbers[review.Number] = struct{}{}
		if review.State == "CHANGES_REQUESTED" {
			rua.changesRequestedGiven++
		}

		if prAuthor, ok := prAuthorByKey[prKey{repoID: review.RepoID, number: review.Number}]; ok {
			if aua, ok := userAggs[userKey{repoID: review.RepoID, identity: prAuthor}]; ok {
				aua.reviewsReceived++
				aua.receivedReviewPRs[review.Number] = struct{}{}
			}
		}

		reviewers := repoReviewers[review.RepoID]
		if reviewers == nil {
			reviewers = map[string]int{}
			repoReviewers[review.RepoID] = reviewers
		}
		reviewers[reviewerIdentity]++
	}

	authorRepoIDs := map[string]map[uuid.UUID]struct{}{}
	for key := range userAggs {
		set := authorRepoIDs[key.identity]
		if set == nil {
			set = map[uuid.UUID]struct{}{}
			authorRepoIDs[key.identity] = set
		}
		set[key.repoID] = struct{}{}
	}

	// 4) Finalize user metrics, sorted by (repo_id string, author_identity)
	// to match Python's `sorted(user_aggs.items(), key=lambda kv: (str(kv[0][0]), kv[0][1]))`.
	sort.Slice(userOrder, func(i, j int) bool {
		a, b := userOrder[i], userOrder[j]
		if a.repoID.String() != b.repoID.String() {
			return a.repoID.String() < b.repoID.String()
		}
		return a.identity < b.identity
	})

	userMetrics := make([]UserMetric, 0, len(userOrder))
	for _, key := range userOrder {
		ua := userAggs[key]
		commitsCount := ua.commitsCount
		totalLOCTouched := ua.locAdded + ua.locDeleted
		avgCommitSizeLOC := 0.0
		if commitsCount > 0 {
			avgCommitSizeLOC = float64(totalLOCTouched) / float64(commitsCount)
		}

		var prFirstReviewP50, prFirstReviewP90, prReviewTimeP50, prPickupP50 *float64
		if len(ua.prFirstReviewTimes) > 0 {
			prFirstReviewP50 = ptr(percentile(ua.prFirstReviewTimes, 50.0))
			prFirstReviewP90 = ptr(percentile(ua.prFirstReviewTimes, 90.0))
		}
		if len(ua.prReviewTimes) > 0 {
			prReviewTimeP50 = ptr(percentile(ua.prReviewTimes, 50.0))
		}
		if len(ua.prPickupTimes) > 0 {
			prPickupP50 = ptr(percentile(ua.prPickupTimes, 50.0))
		}

		activeHours := 0.0
		var dayTimestamps []time.Time
		for _, t := range ua.activityTimestamps {
			if !t.Before(start) && !t.After(end) {
				dayTimestamps = append(dayTimestamps, t)
			}
		}
		if len(dayTimestamps) > 1 {
			minT, maxT := dayTimestamps[0], dayTimestamps[0]
			for _, t := range dayTimestamps[1:] {
				if t.Before(minT) {
					minT = t
				}
				if t.After(maxT) {
					maxT = t
				}
			}
			activeHours = maxT.Sub(minT).Seconds() / 3600.0
		}

		weekendDays := 0
		isWeekend := day.Weekday() == time.Saturday || day.Weekday() == time.Sunday
		if activeHours > 0 && isWeekend {
			weekendDays = 1
		}

		reviewReciprocity := float64(minInt(ua.reviewsGiven, ua.reviewsReceived)) /
			float64(maxInt(1, maxInt(ua.reviewsGiven, ua.reviewsReceived)))

		metric := UserMetric{
			RepoID:                key.repoID,
			Day:                   day,
			AuthorEmail:           key.identity,
			CommitsCount:          commitsCount,
			LOCAdded:              ua.locAdded,
			LOCDeleted:            ua.locDeleted,
			FilesChanged:          len(ua.files),
			LargeCommitsCount:     ua.largeCommitsCount,
			AvgCommitSizeLOC:      avgCommitSizeLOC,
			PRsAuthored:           ua.prsAuthored,
			PRsMerged:             ua.prsMerged,
			AvgPRCycleHours:       mean(ua.prCycleTimes),
			MedianPRCycleHours:    median(ua.prCycleTimes),
			PRCycleP75Hours:       percentile(ua.prCycleTimes, 75.0),
			PRCycleP90Hours:       percentile(ua.prCycleTimes, 90.0),
			PRsWithFirstReview:    ua.prsWithFirstReview,
			PRFirstReviewP50Hours: prFirstReviewP50,
			PRFirstReviewP90Hours: prFirstReviewP90,
			PRReviewTimeP50Hours:  prReviewTimeP50,
			PRPickupTimeP50Hours:  prPickupP50,
			ReviewsGiven:          ua.reviewsGiven,
			ChangesRequestedGiven: ua.changesRequestedGiven,
			ReviewsReceived:       ua.reviewsReceived,
			ReviewReciprocity:     reviewReciprocity,
			PRInterruptionLoad:    len(ua.reviewedPRNumbers),
			ContextSpreadCount:    len(authorRepoIDs[key.identity]),
			ReviewRequestLoad:     len(ua.receivedReviewPRs),
			TeamID:                "unassigned",
			TeamName:              "Unassigned",
			ActiveHours:           activeHours,
			WeekendDays:           weekendDays,
			IdentityID:            key.identity,
			ComputedAt:            computedAtUTC,
		}
		userMetrics = append(userMetrics, metric)
	}

	// 5) Roll up to per-repo metrics.
	repoSet := map[uuid.UUID]struct{}{}
	for key := range userAggs {
		repoSet[key.repoID] = struct{}{}
	}
	for _, pr := range pullRequests {
		repoSet[pr.RepoID] = struct{}{}
	}
	repoIDs := make([]uuid.UUID, 0, len(repoSet))
	for repoID := range repoSet {
		repoIDs = append(repoIDs, repoID)
	}
	sort.Slice(repoIDs, func(i, j int) bool { return repoIDs[i].String() < repoIDs[j].String() })

	repoMetrics := make([]RepoMetric, 0, len(repoIDs))
	for _, repoID := range repoIDs {
		var commitsCount, totalLOCTouched, largeCommitsCount, prsMerged int
		for key, ua := range userAggs {
			if key.repoID != repoID {
				continue
			}
			commitsCount += ua.commitsCount
			totalLOCTouched += ua.locAdded + ua.locDeleted
			largeCommitsCount += ua.largeCommitsCount
			prsMerged += ua.prsMerged
		}

		avgCommitSizeLOC := 0.0
		largeCommitRatio := 0.0
		if commitsCount > 0 {
			avgCommitSizeLOC = float64(totalLOCTouched) / float64(commitsCount)
			largeCommitRatio = float64(largeCommitsCount) / float64(commitsCount)
		}

		repoCycles := repoCycleTimes[repoID]
		repoFirstReviews := repoFirstReviewTimes[repoID]
		repoReviewsList := repoReviewTimes[repoID]
		repoPickups := repoPickupTimes[repoID]
		repoSizes := repoPRSizes[repoID]
		repoLOCTotal := repoPRLOCTotals[repoID]
		repoCommentTotal := repoPRCommentTotals[repoID]
		repoReviewTotal := repoPRReviewTotals[repoID]

		largePRCount := repoLargePRs[repoID]
		reworkPRCount := repoReworkPRs[repoID]
		revertPRCount := repoRevertPRs[repoID]
		totalPRsMerged := prsMerged
		if totalPRsMerged == 0 {
			totalPRsMerged = 1
		}
		changeFailureRate := float64(revertPRCount) / float64(totalPRsMerged)
		prsWithFirstReview := repoPRsWithFirstReview[repoID]

		largePRRatio, prReworkRatio := 0.0, 0.0
		if prsMerged > 0 {
			largePRRatio = float64(largePRCount) / float64(prsMerged)
			prReworkRatio = float64(reworkPRCount) / float64(prsMerged)
		}

		var prSizeP50, prSizeP90, prCommentsPer100LOC, prReviewsPer100LOC *float64
		if len(repoSizes) > 0 {
			prSizeP50 = ptr(percentile(repoSizes, 50.0))
			prSizeP90 = ptr(percentile(repoSizes, 90.0))
		}
		if repoLOCTotal > 0 {
			prCommentsPer100LOC = ptr(float64(repoCommentTotal) / float64(repoLOCTotal) * 100.0)
			prReviewsPer100LOC = ptr(float64(repoReviewTotal) / float64(repoLOCTotal) * 100.0)
		}

		reviewerCounts := repoReviewers[repoID]
		totalReviews := 0
		maxReviewer := 0
		for _, count := range reviewerCounts {
			totalReviews += count
			if count > maxReviewer {
				maxReviewer = count
			}
		}
		topReviewerRatio := 0.0
		if totalReviews > 0 && len(reviewerCounts) > 0 {
			topReviewerRatio = float64(maxReviewer) / float64(totalReviews)
		}

		var mttrHours *float64
		if mttrByRepo != nil {
			if value, ok := mttrByRepo[repoID]; ok {
				mttrHours = ptr(value)
			}
		}
		reworkChurn := 0.0
		if reworkChurnRatioByRepo != nil {
			reworkChurn = reworkChurnRatioByRepo[repoID]
		}
		singleOwner := 0.0
		if singleOwnerFileRatioByRepo != nil {
			singleOwner = singleOwnerFileRatioByRepo[repoID]
		}
		busFactor := 0
		if busFactorByRepo != nil {
			busFactor = busFactorByRepo[repoID]
		}
		gini := 0.0
		if codeOwnershipGiniByRepo != nil {
			gini = codeOwnershipGiniByRepo[repoID]
		}

		repoMetrics = append(repoMetrics, RepoMetric{
			RepoID:                     repoID,
			Day:                        day,
			CommitsCount:               commitsCount,
			TotalLOCTouched:            totalLOCTouched,
			AvgCommitSizeLOC:           avgCommitSizeLOC,
			LargeCommitRatio:           largeCommitRatio,
			PRsMerged:                  prsMerged,
			MedianPRCycleHours:         median(repoCycles),
			PRCycleP75Hours:            percentile(repoCycles, 75.0),
			PRCycleP90Hours:            percentile(repoCycles, 90.0),
			PRsWithFirstReview:         prsWithFirstReview,
			PRFirstReviewP50Hours:      percentileOrNil(repoFirstReviews, 50.0),
			PRFirstReviewP90Hours:      percentileOrNil(repoFirstReviews, 90.0),
			PRReviewTimeP50Hours:       percentileOrNil(repoReviewsList, 50.0),
			PRPickupTimeP50Hours:       percentileOrNil(repoPickups, 50.0),
			LargePRRatio:               largePRRatio,
			PRReworkRatio:              prReworkRatio,
			PRSizeP50LOC:               prSizeP50,
			PRSizeP90LOC:               prSizeP90,
			PRCommentsPer100LOC:        prCommentsPer100LOC,
			PRReviewsPer100LOC:         prReviewsPer100LOC,
			ReworkChurnRatio30d:        reworkChurn,
			SingleOwnerFileRatio30d:    singleOwner,
			ReviewLoadTopReviewerRatio: topReviewerRatio,
			BusFactor:                  busFactor,
			CodeOwnershipGini:          gini,
			MTTRHours:                  mttrHours,
			ChangeFailureRate:          changeFailureRate,
			ComputedAt:                 computedAtUTC,
		})
	}

	// 6) Per-commit metrics, sorted by (repo_id string, commit_hash).
	sort.Slice(commitOrder, func(i, j int) bool {
		a, b := commitAggs[commitOrder[i]], commitAggs[commitOrder[j]]
		if a.repoID.String() != b.repoID.String() {
			return a.repoID.String() < b.repoID.String()
		}
		return a.commitHash < b.commitHash
	})
	commitMetrics := make([]CommitMetric, 0, len(commitOrder))
	for _, key := range commitOrder {
		agg := commitAggs[key]
		commitMetrics = append(commitMetrics, CommitMetric{
			RepoID:       agg.repoID,
			CommitHash:   agg.commitHash,
			Day:          day,
			AuthorEmail:  agg.authorIdentity,
			TotalLOC:     agg.totalLOC(),
			FilesChanged: agg.filesChanged(),
			SizeBucket:   CommitSizeBucket(agg.totalLOC()),
			ComputedAt:   computedAtUTC,
		})
	}

	return Result{RepoMetrics: repoMetrics, UserMetrics: userMetrics, CommitMetrics: commitMetrics}
}

func percentileOrNil(values []float64, pct float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	return ptr(percentile(values, pct))
}

// isRevertTitle mirrors: title.strip().lower(); startswith("revert") or "revert" in title.
func isRevertTitle(title string) bool {
	normalized := strings.ToLower(strings.TrimSpace(title))
	return strings.HasPrefix(normalized, "revert") || strings.Contains(normalized, "revert")
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
