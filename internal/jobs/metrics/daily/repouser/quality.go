package repouser

import (
	"math/big"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ReworkChurnRatio mirrors compute_rework_churn_ratio (quality.py:16): the
// fraction of a repo's 30-day window churn (additions+deletions) that landed
// on a file touched by more than one commit in the window.
func ReworkChurnRatio(repoID uuid.UUID, windowStats []CommitStatRow) float64 {
	type fileStats struct {
		churn   int
		commits map[string]struct{}
	}
	byFile := map[string]*fileStats{}
	for _, row := range windowStats {
		if row.RepoID != repoID || row.FilePath == "" {
			continue
		}
		stats := byFile[row.FilePath]
		if stats == nil {
			stats = &fileStats{commits: map[string]struct{}{}}
			byFile[row.FilePath] = stats
		}
		stats.churn += maxInt(0, row.Additions) + maxInt(0, row.Deletions)
		stats.commits[row.CommitHash] = struct{}{}
	}
	var totalChurn, reworkChurn int
	for _, stats := range byFile {
		totalChurn += stats.churn
		if len(stats.commits) > 1 {
			reworkChurn += stats.churn
		}
	}
	if totalChurn == 0 {
		return 0.0
	}
	return float64(reworkChurn) / float64(totalChurn)
}

// SingleOwnerFileRatio mirrors compute_single_owner_file_ratio (quality.py:52):
// the fraction of touched files where one author's commits make up at least
// ownerThreshold (Python default 0.75) of the file's commit count.
func SingleOwnerFileRatio(
	repoID uuid.UUID,
	windowStats []CommitStatRow,
	ownerThreshold float64,
	normalizeIdentity func(authorEmail, authorName string) string,
) float64 {
	fileAuthorCommits := map[string]map[string]map[string]struct{}{}
	for _, row := range windowStats {
		if row.RepoID != repoID || row.FilePath == "" {
			continue
		}
		author := normalizeIdentity(row.AuthorEmail, row.AuthorName)
		byAuthor := fileAuthorCommits[row.FilePath]
		if byAuthor == nil {
			byAuthor = map[string]map[string]struct{}{}
			fileAuthorCommits[row.FilePath] = byAuthor
		}
		commits := byAuthor[author]
		if commits == nil {
			commits = map[string]struct{}{}
			byAuthor[author] = commits
		}
		commits[row.CommitHash] = struct{}{}
	}
	if len(fileAuthorCommits) == 0 {
		return 0.0
	}
	singleOwnerFiles := 0
	for _, byAuthor := range fileAuthorCommits {
		total := 0
		max := 0
		for _, commits := range byAuthor {
			count := len(commits)
			total += count
			if count > max {
				max = count
			}
		}
		if total == 0 {
			continue
		}
		if float64(max)/float64(total) >= ownerThreshold {
			singleOwnerFiles++
		}
	}
	return float64(singleOwnerFiles) / float64(len(fileAuthorCommits))
}

// BusFactor mirrors compute_bus_factor (knowledge.py:9): the smallest number
// of authors (by churn, descending) whose combined churn reaches
// thresholdPercent (Python default 0.5) of the repo's total 30-day churn.
// Identity is the RAW author_email/author_name pair -- compute_bus_factor
// never normalizes through an IdentityResolver, unlike compute_daily_metrics.
func BusFactor(repoID uuid.UUID, windowStats []CommitStatRow, thresholdPercent float64) int {
	if len(windowStats) == 0 {
		return 0
	}
	authorChurn := map[string]int{}
	var totalChurn int
	for _, row := range windowStats {
		if row.RepoID != repoID {
			continue
		}
		identity := rawIdentity(row.AuthorEmail, row.AuthorName)
		churn := row.Additions + row.Deletions
		authorChurn[identity] += churn
		totalChurn += churn
	}
	if totalChurn == 0 {
		return 0
	}
	churns := make([]int, 0, len(authorChurn))
	for _, churn := range authorChurn {
		churns = append(churns, churn)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(churns)))

	target := float64(totalChurn) * thresholdPercent
	var cumulative float64
	busFactor := 0
	for _, churn := range churns {
		cumulative += float64(churn)
		busFactor++
		if cumulative >= target {
			break
		}
	}
	return busFactor
}

// CodeOwnershipGini mirrors compute_code_ownership_gini (knowledge.py:62):
// the Gini coefficient of per-author churn distribution over the repo's
// 30-day window. 0 = perfectly equal, 1 = one author owns everything.
func CodeOwnershipGini(repoID uuid.UUID, windowStats []CommitStatRow) float64 {
	if len(windowStats) == 0 {
		return 0.0
	}
	// CHAOS-4824 (rounds 1-4, codex): Python's Gini formula is EXACT integer
	// arithmetic -- `churns` holds arbitrary-precision Python ints -- until
	// ONE true division at the very end, `2*numerator/denominator`, which
	// CPython computes as the correctly-rounded float64 nearest the exact
	// rational. Converting churn totals to float64 (or even to a FIXED-WIDTH
	// int64) at any EARLIER point loses precision or silently wraps in a way
	// Python never does, and each earlier attempt here closed one
	// construction while leaving a strictly more extreme one reachable:
	// float64 terms (round 1: accumulated numerator crossing 2**53 across
	// many representable per-author totals; round 2: a SINGLE author's own
	// total exceeding 2**53 before summation began); then int64 accumulation
	// of that same per-author total (round 4: 2.1 BILLION int32-max rows for
	// one author overflows int64 and silently wraps negative, discarding the
	// author entirely at the `> 0` filter below). Mirroring Python's
	// conversion POINT instead of patching each construction: EVERY
	// arithmetic step from the first row read to the final division is
	// math/big.Int (or the one big.Rat-mediated division CPython's own
	// int/int does), so there is no fixed-width integer or float anywhere
	// upstream of it for a large construction to overflow or round.
	authorChurn := map[string]*big.Int{}
	for _, row := range windowStats {
		if row.RepoID != repoID {
			continue
		}
		identity := rawIdentity(row.AuthorEmail, row.AuthorName)
		rowChurn := new(big.Int).Add(big.NewInt(int64(row.Additions)), big.NewInt(int64(row.Deletions)))
		if total, ok := authorChurn[identity]; ok {
			total.Add(total, rowChurn)
		} else {
			authorChurn[identity] = rowChurn
		}
	}
	churns := make([]*big.Int, 0, len(authorChurn))
	for _, churn := range authorChurn {
		if churn.Sign() > 0 {
			churns = append(churns, churn)
		}
	}
	if len(churns) == 0 {
		return 0.0
	}
	sort.Slice(churns, func(i, j int) bool { return churns[i].Cmp(churns[j]) < 0 })
	n := len(churns)

	numerator := new(big.Int)
	denominatorSum := new(big.Int)
	term := new(big.Int)
	for index, churn := range churns {
		term.Mul(big.NewInt(int64(index+1)), churn)
		numerator.Add(numerator, term)
		denominatorSum.Add(denominatorSum, churn)
	}
	denominator := new(big.Int).Mul(big.NewInt(int64(n)), denominatorSum)
	if denominator.Sign() == 0 {
		return 0.0
	}
	numeratorTimes2 := new(big.Int).Mul(big.NewInt(2), numerator)
	// 2*numerator/denominator, correctly rounded to the nearest float64 --
	// matches CPython's int/int true division exactly, regardless of
	// magnitude.
	quotient, _ := new(big.Rat).SetFrac(numeratorTimes2, denominator).Float64()
	// (n+1)/n: n is len(churns), always a small int far below 2**53, so a
	// plain float64 division is already correctly rounded and identical to
	// Python's -- no big.Rat needed for this term.
	gini := quotient - float64(n+1)/float64(n)
	if gini < 0 {
		return 0.0
	}
	if gini > 1 {
		return 1.0
	}
	return gini
}

// rawIdentity mirrors the RAW (never resolver-normalized) identity fallback
// compute_bus_factor/compute_code_ownership_gini use directly:
// `row.get("author_email") or row.get("author_name") or "unknown"`.
// Deliberately different from DefaultNormalizeIdentity (which trims): Python
// does not strip either field here.
func rawIdentity(authorEmail, authorName string) string {
	if authorEmail != "" {
		return authorEmail
	}
	if authorName != "" {
		return authorName
	}
	return "unknown"
}

// BugWorkItem is the narrow slice of a work_items row MTTRByRepo needs:
// type == "bug" rows with both timestamps set.
type BugWorkItem struct {
	RepoID      uuid.UUID
	StartedAt   time.Time
	CompletedAt time.Time
}

// MTTRByRepo mirrors the inline MTTR computation in job_daily.py (around
// line 995): for bug work items completed within the day window, the mean
// hours between started_at and completed_at, per repo. Callers pass only
// type=="bug" rows with non-zero StartedAt/CompletedAt (job_daily.py filters
// `item.type == "bug" and item.completed_at and item.started_at` before this
// loop runs).
func MTTRByRepo(day time.Time, bugItems []BugWorkItem) map[uuid.UUID]float64 {
	start, end := utcDayWindow(day)
	hoursByRepo := map[uuid.UUID][]float64{}
	for _, item := range bugItems {
		completedAt := item.CompletedAt.UTC()
		if completedAt.Before(start) || !completedAt.Before(end) {
			continue
		}
		hours := completedAt.Sub(item.StartedAt.UTC()).Seconds() / 3600.0
		hoursByRepo[item.RepoID] = append(hoursByRepo[item.RepoID], hours)
	}
	if len(hoursByRepo) == 0 {
		return map[uuid.UUID]float64{}
	}
	result := make(map[uuid.UUID]float64, len(hoursByRepo))
	for repoID, hours := range hoursByRepo {
		result[repoID] = mean(hours)
	}
	return result
}
