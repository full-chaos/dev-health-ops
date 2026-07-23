// Package numerical contains deterministic numerical kernels shared by the
// remaining-metrics River migration. Querying, tenancy, leases, and ClickHouse
// persistence stay outside this package.
package numerical

import (
	"math"
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
			result = append(result, DORAMetric{RepoID: repoID, Name: "lead_time_for_changes", Value: Median(bucket.leadTimes)})
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
			Value:  Median(incidentDurations[repoID]),
		})
	}
	return result
}

func Median(values []float64) float64 {
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
	var squared float64
	for _, value := range values {
		difference := float64(value) - mean
		squared += difference * difference
	}
	return CapacityStatistics{Mean: mean, Stddev: math.Sqrt(squared / float64(len(values)))}
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
