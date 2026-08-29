package analytics

import (
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
)

// Cost limits -- port of cost.py's CostLimits dataclass defaults
// (cost.py:12-26). maxTopN already lives in breakdown.go.
const (
	maxDays          = 3650
	maxBuckets       = 100
	maxSankeyNodes   = 100
	maxSankeyEdges   = 500
	maxSubRequests   = 10
	queryTimeoutSecs = 30
)

// validateDateRange ports validate_date_range (cost.py:34-59).
func validateDateRange(start, end graphqldate.Date) error {
	if end.Time().Before(start.Time()) {
		return newValidationError("date_range", nil, "end_date must be >= start_date")
	}
	days := int(end.Time().Sub(start.Time()).Hours()/24) + 1
	if days > maxDays {
		return newValidationError("max_days", days, "Date range of %d days exceeds limit of %d", days, maxDays)
	}
	return nil
}

// validateBuckets ports validate_buckets (cost.py:182-212) exactly,
// including its own fallback ("else: buckets = days" for any interval
// string not day/week/month -- unreachable through this port's own
// BucketInterval enum, kept for parity with the Python shape).
func validateBuckets(start, end graphqldate.Date, interval BucketInterval) error {
	days := int(end.Time().Sub(start.Time()).Hours()/24) + 1
	var buckets int
	switch interval {
	case BucketIntervalDay:
		buckets = days
	case BucketIntervalWeek:
		buckets = (days + 6) / 7
	case BucketIntervalMonth:
		buckets = (days + 29) / 30
	default:
		buckets = days
	}
	if buckets > maxBuckets {
		return newValidationError("max_buckets", buckets, "Estimated %d buckets exceeds limit of %d", buckets, maxBuckets)
	}
	return nil
}

// validateSankeyLimits ports validate_sankey_limits (cost.py:97-141).
func validateSankeyLimits(maxNodes, maxEdges int) error {
	if maxNodes <= 0 {
		return newValidationError("max_nodes", maxNodes, "max_nodes must be positive")
	}
	if maxNodes > maxSankeyNodes {
		return newValidationError("max_sankey_nodes", maxNodes, "max_nodes of %d exceeds limit of %d", maxNodes, maxSankeyNodes)
	}
	if maxEdges <= 0 {
		return newValidationError("max_edges", maxEdges, "max_edges must be positive")
	}
	if maxEdges > maxSankeyEdges {
		return newValidationError("max_sankey_edges", maxEdges, "max_edges of %d exceeds limit of %d", maxEdges, maxSankeyEdges)
	}
	return nil
}

// validateSubRequestCount ports validate_sub_request_count (cost.py:144-179).
// This is the "up to 10 parallel sub-requests" limit the ticket cites --
// it counts REQUESTED analyses, not real ClickHouse queries; sankey and
// flowMatrix each expand internally (see sankey.go/flowmatrix.go), so a
// saturated batch issues more than 10 real queries. Porting the count
// faithfully, not "fixing" it -- this is documented, working Python
// behavior (RISK-NOTES).
func validateSubRequestCount(timeseriesCount, breakdownsCount int, hasSankey, hasFlowMatrix bool) error {
	total := timeseriesCount + breakdownsCount
	if hasSankey {
		total++
	}
	if hasFlowMatrix {
		total++
	}
	if total > maxSubRequests {
		return newValidationError("max_sub_requests", total, "Total sub-requests (%d) exceeds limit of %d", total, maxSubRequests)
	}
	return nil
}
