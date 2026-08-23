// Package numerical contains deterministic numerical kernels shared by the
// remaining-metrics River migration. Querying, tenancy, leases, and ClickHouse
// persistence stay outside this package.
package numerical

import (
	"math"
	"math/big"
	"sort"
	"strings"
	"time"
)

var failedDeploymentStatuses = map[string]struct{}{
	"failure":  {},
	"failed":   {},
	"error":    {},
	"canceled": {},
}

type Deployment struct {
	RepoID     string
	Status     string
	DeployedAt time.Time
	StartedAt  time.Time
	MergedAt   time.Time
}

type Incident struct {
	RepoID     string
	StartedAt  time.Time
	ResolvedAt time.Time
}

type DORAMetric struct {
	RepoID string
	Name   string
	Value  float64
}

type deployBucket struct {
	total     int
	failed    int
	leadTimes []float64
}

// ComputeDORA mirrors compute_dora_metrics_daily over already tenant-scoped,
// provider-neutral deployment and incident rows.
func ComputeDORA(day time.Time, deployments []Deployment, incidents []Incident) []DORAMetric {
	start := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	deploys := make(map[string]*deployBucket)
	for _, deployment := range deployments {
		deployedAt := deployment.DeployedAt
		if deployedAt.IsZero() {
			deployedAt = deployment.StartedAt
		}
		deployedAt = deployedAt.UTC()
		if deployedAt.IsZero() || deployedAt.Before(start) || !deployedAt.Before(end) {
			continue
		}
		bucket := deploys[deployment.RepoID]
		if bucket == nil {
			bucket = &deployBucket{}
			deploys[deployment.RepoID] = bucket
		}
		bucket.total++
		if _, failed := failedDeploymentStatuses[strings.ToLower(strings.TrimSpace(deployment.Status))]; failed {
			bucket.failed++
		}
		if !deployment.MergedAt.IsZero() {
			lead := deployedAt.Sub(deployment.MergedAt.UTC()).Seconds()
			if lead >= 0 {
				bucket.leadTimes = append(bucket.leadTimes, lead)
			}
		}
	}
	incidentDurations := make(map[string][]float64)
	for _, incident := range incidents {
		resolvedAt := incident.ResolvedAt.UTC()
		if resolvedAt.IsZero() || resolvedAt.Before(start) || !resolvedAt.Before(end) || incident.StartedAt.IsZero() {
			continue
		}
		duration := resolvedAt.Sub(incident.StartedAt.UTC()).Seconds()
		if duration >= 0 {
			incidentDurations[incident.RepoID] = append(incidentDurations[incident.RepoID], duration)
		}
	}

	repoIDs := make([]string, 0, len(deploys))
	for repoID := range deploys {
		repoIDs = append(repoIDs, repoID)
	}
	sort.Strings(repoIDs)
	result := make([]DORAMetric, 0, len(repoIDs)*3+len(incidentDurations))
	for _, repoID := range repoIDs {
		bucket := deploys[repoID]
		result = append(result, DORAMetric{RepoID: repoID, Name: "deployment_frequency", Value: float64(bucket.total)})
		if bucket.total > 0 {
			result = append(result, DORAMetric{
				RepoID: repoID,
				Name:   "change_failure_rate",
				Value:  float64(bucket.failed) / float64(bucket.total),
			})
		}
		if len(bucket.leadTimes) > 0 {
			result = append(result, DORAMetric{RepoID: repoID, Name: "lead_time_for_changes", Value: median(bucket.leadTimes)})
		}
	}
	repoIDs = repoIDs[:0]
	for repoID := range incidentDurations {
		repoIDs = append(repoIDs, repoID)
	}
	sort.Strings(repoIDs)
	for _, repoID := range repoIDs {
		result = append(result, DORAMetric{
			RepoID: repoID,
			Name:   "time_to_restore_service",
			Value:  median(incidentDurations[repoID]),
		})
	}
	return result
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

type CapacityStatistics struct {
	Mean   float64
	Stddev float64
}

func ThroughputStatistics(values []int) CapacityStatistics {
	if len(values) == 0 {
		return CapacityStatistics{}
	}
	var sum float64
	for _, value := range values {
		sum += float64(value)
	}
	mean := sum / float64(len(values))
	if len(values) == 1 {
		return CapacityStatistics{Mean: mean}
	}
	return CapacityStatistics{Mean: mean, Stddev: populationStddev(values)}
}

// populationStddev reproduces statistics.pstdev for integer data.
//
// The obvious two-pass float accumulation -- sum the squared deviations in
// float64 and take math.Sqrt -- is WRONG against Python, and wrong in a way
// that is invisible on small inputs: for the throughput history
// [3 8 1 5 13 2 9 4 7 2 9 1 6 3] it returns 3.4677817411252443 where CPython
// returns 3.4677817411252447. Four ULP is not a rounding preference when the
// value is compared for exact equality against Python's output.
//
// CPython does not compute sqrt(float(variance)). statistics.pstdev keeps the
// sum of squared deviations as an EXACT Fraction and then takes a correctly
// rounded square root of that rational (_float_sqrt_of_frac), which is closer
// to the true value than converting to float first and rooting afterwards --
// float(pvariance) then sqrt also gives ...443, so even exact-variance-then-
// float-sqrt does not agree. The rounding has to happen once, at the end.
//
// For integer data the exact mean sum of squares reduces to
//
//	(count * Sum(x^2) - Sum(x)^2) / count^2
//
// which is computed here in big.Int and rooted through the same round-to-odd
// construction CPython uses.
func populationStddev(values []int) float64 {
	count := int64(len(values))
	sum := new(big.Int)
	sumSquares := new(big.Int)
	term := new(big.Int)
	for _, value := range values {
		term.SetInt64(int64(value))
		sum.Add(sum, term)
		term.Mul(term, term)
		sumSquares.Add(sumSquares, term)
	}
	countBig := big.NewInt(count)
	numerator := new(big.Int).Mul(countBig, sumSquares)
	numerator.Sub(numerator, new(big.Int).Mul(sum, sum))
	denominator := new(big.Int).Mul(countBig, countBig)
	if numerator.Sign() <= 0 {
		return 0
	}
	// Reduce first: CPython's Fraction is normalised before its numerator and
	// denominator reach _float_sqrt_of_frac, and the bit lengths of the
	// REDUCED pair drive the scaling exponent below.
	divisor := new(big.Int).GCD(nil, nil, numerator, denominator)
	numerator.Div(numerator, divisor)
	denominator.Div(denominator, divisor)
	return floatSqrtOfFraction(numerator, denominator)
}

// sqrtBitWidth mirrors statistics._sqrt_bit_width.
const sqrtBitWidth = 109

// floatSqrtOfFraction ports statistics._float_sqrt_of_frac: the square root of
// n/m as a float64, correctly rounded.
func floatSqrtOfFraction(n, m *big.Int) float64 {
	// Python's // is FLOOR division, which differs from Go's truncation for
	// negative values -- and this quotient is negative for every fraction
	// below one, which is most of them. Truncating here shifts by one bit and
	// silently returns a differently rounded answer.
	q := floorDiv(int64(n.BitLen()-m.BitLen()-sqrtBitWidth), 2)

	var numerator, denominator *big.Int
	if q >= 0 {
		scaled := new(big.Int).Lsh(m, uint(2*q))
		numerator = new(big.Int).Lsh(integerSqrtOfFractionRoundToOdd(n, scaled), uint(q))
		denominator = big.NewInt(1)
	} else {
		scaled := new(big.Int).Lsh(n, uint(-2*q))
		numerator = integerSqrtOfFractionRoundToOdd(scaled, m)
		denominator = new(big.Int).Lsh(big.NewInt(1), uint(-q))
	}
	value, _ := new(big.Rat).SetFrac(numerator, denominator).Float64()
	return value
}

// integerSqrtOfFractionRoundToOdd ports _integer_sqrt_of_frac_rto:
// floor(sqrt(n/m)), with the low bit forced set when the root is inexact.
//
// The round-to-odd step is what makes the final conversion correctly rounded
// rather than merely close; dropping it loses the information that the true
// root lies strictly above the floor, and the last conversion then rounds the
// wrong way on ties.
func integerSqrtOfFractionRoundToOdd(n, m *big.Int) *big.Int {
	quotient := new(big.Int).Div(n, m)
	root := new(big.Int).Sqrt(quotient)
	exact := new(big.Int).Mul(root, root)
	exact.Mul(exact, m)
	if exact.Cmp(n) != 0 {
		root.Or(root, big.NewInt(1))
	}
	return root
}

func floorDiv(numerator, denominator int64) int64 {
	quotient := numerator / denominator
	if (numerator%denominator != 0) && ((numerator < 0) != (denominator < 0)) {
		quotient--
	}
	return quotient
}

// IntegerPercentiles matches compute_capacity._percentile, including its
// truncating linear interpolation.
func IntegerPercentiles(values []int, percentiles []float64) []int {
	if len(values) == 0 {
		return make([]int, len(percentiles))
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	result := make([]int, 0, len(percentiles))
	for _, percentile := range percentiles {
		switch {
		case percentile <= 0:
			result = append(result, sorted[0])
		case percentile >= 100:
			result = append(result, sorted[len(sorted)-1])
		default:
			rank := float64(len(sorted)-1) * percentile / 100
			low := int(rank)
			high := min(low+1, len(sorted)-1)
			fraction := rank - float64(low)
			value := float64(sorted[low])*(1-fraction) + float64(sorted[high])*fraction
			result = append(result, int(value))
		}
	}
	return result
}

type ComplexityFile struct {
	LOC                int
	CyclomaticTotal    int
	HighComplexity     int
	VeryHighComplexity int
}

type ComplexitySummary struct {
	LOCTotal           int
	CyclomaticTotal    int
	CyclomaticPerKLOC  float64
	HighComplexity     int
	VeryHighComplexity int
}

func AggregateComplexity(files []ComplexityFile) ComplexitySummary {
	var result ComplexitySummary
	for _, file := range files {
		result.LOCTotal += file.LOC
		result.CyclomaticTotal += file.CyclomaticTotal
		result.HighComplexity += file.HighComplexity
		result.VeryHighComplexity += file.VeryHighComplexity
	}
	if result.LOCTotal > 0 {
		result.CyclomaticPerKLOC = float64(result.CyclomaticTotal) / (float64(result.LOCTotal) / 1000)
	}
	return result
}

func ReleaseImpactConfidence(coverageRatio float64, totalSessions, concurrentDeploys, minimumSessions int) float64 {
	sampleScore := 1.0
	if minimumSessions > 0 {
		sampleScore = math.Min(float64(totalSessions)/float64(minimumSessions), 1)
	}
	confoundScore := 1 / (1 + float64(concurrentDeploys))
	score := 0.35*coverageRatio + 0.35*sampleScore + 0.30*confoundScore
	return math.Max(0, math.Min(1, score))
}
