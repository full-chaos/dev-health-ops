// Package workitemengine holds the SINGLE Go implementation of the work-item
// daily destinations that depend on the config-driven status mapping and the
// daily completions:
//
//   - issue_type_metrics_daily         -> ComputeIssueTypeMetricsDaily
//   - work_item_completion_daily       -> ComputeWorkItemCompletionDaily
//
// (The investment rule classifier and its two tables are gone: CHAOS-9200.)
//
// # Why this package exists
//
// This arithmetic was written for the sync-time deriver in
// internal/providersync (github_work_item_engine_destinations.go), where the
// frozen-production cases and the engine tests cover it. The daily metric job
// needs the SAME computation over STORED rows: a sync unit holds only the
// items it fetched, so the rows it derives for a day are partial, and the
// owner's design is that the daily job computes every derived table from
// stored rows. A second copy would be free to disagree with the first, so the
// compute moved HERE with no logic edit, and providersync calls it through a
// thin adapter. The same shape, for the same reason, as
// internal/jobs/metrics/workitemmetrics.
//
// internal/jobs/metrics/daily must not import internal/providersync (the
// provider tests import the daily package), so the engine comes in through
// TypeNormalizer.
//
// # What is in scope
//
// ONLY the arithmetic and the INSERT statements. Team resolution is the
// caller's, through TeamResolver: providersync runs the live attribution
// cascade over the facts it loaded, the daily family reads the cascade's stored
// result from work_item_team_attributions. Tenancy checks, the claim, the
// stamp, and the effect wire format stay with their callers.
package workitemengine

import (
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const unassignedTeamID = "unassigned"

// ErrUnrepresentableDeliveryUnits reports story points that cannot become a
// delivery-unit count: not finite, or outside the 64-bit integer range.
var ErrUnrepresentableDeliveryUnits = errors.New("workitemengine: story points are not a representable delivery-unit count")

// Item is one work item, reduced to exactly the fields the two computations
// read.
type Item struct {
	WorkItemID  string
	Provider    string
	Type        string
	Title       string
	Labels      []string
	RepoID      *uuid.UUID
	CreatedAt   time.Time
	StartedAt   *time.Time
	CompletedAt *time.Time
	StoryPoints *float64
	// LinkedPRs are the pull requests linked to the item through work_graph_issue_pr. The daily completion
	// compute reads them for churn_loc; no other compute does.
	LinkedPRs []LinkedPR
}

// Link tiers of work_graph_issue_pr.provenance, strongest first. All three count as a link; a consumer names the
// tier and never presents a lower one as native.
const (
	LinkTierNative       = "native"
	LinkTierExplicitText = "explicit_text"
	LinkTierHeuristic    = "heuristic"
)

// LinkedPR is one pull request linked to a work item. RepoID and Number are the pull request's own key (the
// repo_id of work_graph_issue_pr is the pull request's repository, not the item's). Additions and Deletions are
// nil when the pull request row is missing or holds no size: the size is unknown, which is not zero lines.
type LinkedPR struct {
	RepoID     uuid.UUID
	Number     uint32
	Provenance string
	Additions  *uint32
	Deletions  *uint32
}

// TeamResolver returns the raw team id of the item at `index` of the item
// slice the caller passed, or nil when the item has none. The compute applies
// the unassigned rule itself.
type TeamResolver func(index int) *string

// TypeNormalizer is the status-mapping engine's type rule.
type TypeNormalizer interface {
	NormalizeType(provider, typeRaw string, labels []string) string
}

// IssueTypeMetricsDailyRow is one issue_type_metrics_daily row without the
// caller's stamp (day, computed_at, org_id). RepoID is nil for an item with no
// repository: the column is nullable.
type IssueTypeMetricsDailyRow struct {
	RepoID         *uuid.UUID
	Provider       string
	TeamID         string
	IssueTypeNorm  string
	CreatedCount   int
	CompletedCount int
	ActiveCount    int
	CycleP50Hours  float64
	CycleP90Hours  float64
	LeadP50Hours   float64
}

// normalizeTeamID mirrors normalize_team_id (providers/teams.py:36): None/blank
// becomes "unassigned", otherwise the value is stripped.
func normalizeTeamID(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return unassignedTeamID
	}
	return strings.TrimSpace(*value)
}

type nullableUUIDKey struct {
	value uuid.UUID
	valid bool
}

func newNullableUUIDKey(value *uuid.UUID) nullableUUIDKey {
	if value == nil || *value == uuid.Nil {
		return nullableUUIDKey{}
	}
	return nullableUUIDKey{value: *value, valid: true}
}

func (key nullableUUIDKey) pointer() *uuid.UUID {
	if !key.valid {
		return nil
	}
	value := key.value
	return &value
}

type issueTypeMetricsKey struct {
	repoID                          nullableUUIDKey
	provider, teamID, issueTypeNorm string
}

type issueTypeMetricsBucket struct {
	created, completed, active int
	cycleHours                 []float64
	leadHours                  []float64
}

// ComputeIssueTypeMetricsDaily mirrors the issue-type path in the production
// Python work-item engine helper. In particular, the bucket is opened before any
// time check, so a future-only item still materializes an all-zero row (D16).
//
// dayUTC is the UTC midnight of the day and end the next midnight.
func ComputeIssueTypeMetricsDaily(
	items []Item,
	dayUTC, end time.Time,
	resolveTeam TeamResolver,
	normalizer TypeNormalizer,
) []IssueTypeMetricsDailyRow {
	buckets := make(map[issueTypeMetricsKey]*issueTypeMetricsBucket)
	order := make([]issueTypeMetricsKey, 0, len(items))
	for index, item := range items {
		teamID := resolveTeam(index)
		key := issueTypeMetricsKey{
			repoID:        newNullableUUIDKey(item.RepoID),
			provider:      item.Provider,
			teamID:        normalizeTeamID(teamID),
			issueTypeNorm: normalizer.NormalizeType(item.Provider, item.Type, item.Labels),
		}
		bucket := buckets[key]
		if bucket == nil {
			bucket = &issueTypeMetricsBucket{}
			buckets[key] = bucket
			order = append(order, key)
		}
		created := item.CreatedAt.UTC()
		if !created.Before(dayUTC) && created.Before(end) {
			bucket.created++
		}
		if item.CompletedAt != nil {
			completed := item.CompletedAt.UTC()
			if !completed.Before(dayUTC) && completed.Before(end) {
				bucket.completed++
				if item.StartedAt != nil {
					cycle := completed.Sub(item.StartedAt.UTC()).Hours()
					if cycle >= 0 {
						bucket.cycleHours = append(bucket.cycleHours, cycle)
					}
				}
				// Lead time runs from creation to completion. An item with no
				// created_at (the zero time) or a creation after its
				// completion has no lead time: it is left out of the p50 (see
				// LeadTimeInputs), never taken as 0 hours.
				if lead := completed.Sub(created).Hours(); !item.CreatedAt.IsZero() && lead >= 0 {
					bucket.leadHours = append(bucket.leadHours, lead)
				}
			}
		}
		if created.Before(end) &&
			(item.CompletedAt == nil || !item.CompletedAt.UTC().Before(dayUTC)) {
			bucket.active++
		}
	}

	result := make([]IssueTypeMetricsDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		sort.Float64s(bucket.cycleHours)
		sort.Float64s(bucket.leadHours)
		var p50, p90, leadP50 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
			p90 = bucket.cycleHours[int(float64(len(bucket.cycleHours))*0.9)]
		}
		if len(bucket.leadHours) > 0 {
			leadP50 = bucket.leadHours[len(bucket.leadHours)/2]
		}
		result = append(result, IssueTypeMetricsDailyRow{
			RepoID:         key.repoID.pointer(),
			Provider:       key.provider,
			TeamID:         key.teamID,
			IssueTypeNorm:  key.issueTypeNorm,
			CreatedCount:   bucket.created,
			CompletedCount: bucket.completed,
			ActiveCount:    bucket.active,
			CycleP50Hours:  p50,
			CycleP90Hours:  p90,
			LeadP50Hours:   leadP50,
		})
	}
	return result
}

type completionBucket struct {
	deliveryUnits, completed, churn int
	cycleHours                      []float64
	prs                             map[linkedPRKey]struct{}
}

// LeadTimeInputs counts the items completed in [dayUTC, end) and, of those, the ones left out of the lead-time p50
// because they have no created_at or were created after they completed. The caller logs it; this package holds no
// logger.
func LeadTimeInputs(items []Item, dayUTC, end time.Time) (completed, excluded int) {
	for _, item := range items {
		if item.CompletedAt == nil {
			continue
		}
		done := item.CompletedAt.UTC()
		if done.Before(dayUTC) || !done.Before(end) {
			continue
		}
		completed++
		if item.CreatedAt.IsZero() || done.Sub(item.CreatedAt.UTC()) < 0 {
			excluded++
		}
	}
	return completed, excluded
}
