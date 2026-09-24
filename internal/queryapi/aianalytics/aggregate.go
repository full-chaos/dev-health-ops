// Package aianalytics serves the AI workflow analytics GraphQL fields that
// read the pre-computed ai_impact_metrics_daily rollups: the impact summary,
// the AI-versus-baseline comparison and the review load. Nothing here
// categorises or recomputes at request time; the rollups are read, weighted
// and projected.
//
// The org is always the caller's own org. Every table the readers touch that
// carries an org column is filtered on it, in every join and subquery.
package aianalytics

import (
	"math"
	"sort"
	"time"
)

// Bucket names as stored in ai_impact_metrics_daily.attribution_bucket.
const (
	bucketHuman = "human"
)

// aiBuckets are the AI-coded attribution buckets.
var aiBuckets = map[string]bool{"ai_assisted": true, "agent_created": true, "ai_review": true}

// dailyRow is one deduplicated (team, repo, work type, day, bucket) rollup
// row of ai_impact_metrics_daily.
type dailyRow struct {
	TeamID string
	RepoID string
	Day    string
	Bucket string

	PrsTotal            int64
	PrsMerged           int64
	AIAssistedPrs       int64
	AgentCreatedPrs     int64
	HumanPrs            int64
	UnknownPrs          int64
	AIAssistedPrRatio   *float64
	CycleTimeAvgHours   *float64
	AICycleTimeDelta    *float64
	ReviewsPerPr        *float64
	AIReviewAmp         *float64
	ChangesRequestedPer *float64
	ReworkPrs           int64
	ReworkDragRate      *float64
	FollowupCommits     int64
	RevertPrs           int64
	RevertRate          *float64
	IncidentsCount      int64
	IncidentDragRate    *float64
	TestGapPrs          int64
	TestGapRate         *float64

	LevPrs      float64
	LevCycle    *float64
	LevReview   *float64
	LevRework   *float64
	LevTest     *float64
	LevIncident *float64

	ComputedAt time.Time
}

// pair is one per-day average and its sample weight.
type pair struct {
	value  *float64
	weight float64
}

// weightedAvg combines per-day averages weighted by sample size. A missing
// value or a non-positive weight is skipped; no usable weight is nil.
//
// The product is converted explicitly so a fused multiply-add can never
// change the last bit against the reference's separate multiply and add.
func weightedAvg(pairs []pair) *float64 {
	totalWeight := 0.0
	totalValue := 0.0
	for _, p := range pairs {
		if p.value == nil || p.weight <= 0 {
			continue
		}
		totalWeight = totalWeight + p.weight
		totalValue = totalValue + float64((*p.value)*p.weight)
	}
	if totalWeight == 0 {
		return nil
	}
	out := totalValue / totalWeight
	return &out
}

// ratio is numerator/denominator, nil for a zero denominator.
func ratio(numerator, denominator float64) *float64 {
	if denominator == 0 {
		return nil
	}
	out := numerator / denominator
	return &out
}

// delta is a-b, nil when either side is missing.
func delta(a, b *float64) *float64 {
	if a == nil || b == nil {
		return nil
	}
	out := *a - *b
	return &out
}

// reviewTotalForRow is round-half-even(reviews_per_pr * prs_total).
func reviewTotalForRow(r dailyRow) int64 {
	perPr := 0.0
	if r.ReviewsPerPr != nil {
		perPr = *r.ReviewsPerPr
	}
	return int64(math.RoundToEven(float64(perPr * float64(r.PrsTotal))))
}

func pairsOf(rows []dailyRow, value func(dailyRow) *float64, weight func(dailyRow) int64) []pair {
	out := make([]pair, 0, len(rows))
	for _, r := range rows {
		out = append(out, pair{value: value(r), weight: float64(weight(r))})
	}
	return out
}

func byPrsTotal(r dailyRow) int64  { return r.PrsTotal }
func byPrsMerged(r dailyRow) int64 { return r.PrsMerged }

// groupByBucket groups rows by bucket, keeping row order inside a group and
// returning the bucket names in ascending order.
func groupByBucket(rows []dailyRow) (map[string][]dailyRow, []string) {
	groups := map[string][]dailyRow{}
	var names []string
	for _, r := range rows {
		if _, seen := groups[r.Bucket]; !seen {
			names = append(names, r.Bucket)
		}
		groups[r.Bucket] = append(groups[r.Bucket], r)
	}
	sort.Strings(names)
	return groups, names
}

func sumInt(rows []dailyRow, f func(dailyRow) int64) int64 {
	var total int64
	for _, r := range rows {
		total = total + f(r)
	}
	return total
}

func sortFloats(v []float64) { sort.Float64s(v) }
