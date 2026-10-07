// Package workitemengine holds the SINGLE Go implementation of the three
// work-item daily destinations that depend on the two config-driven engines
// (the status mapping and the investment classifier):
//
//   - issue_type_metrics_daily         -> ComputeIssueTypeMetricsDaily
//   - investment_classifications_daily -> ComputeInvestmentDaily
//   - investment_metrics_daily         -> ComputeInvestmentDaily
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
// provider tests import the daily package), so the two engines come in through
// TypeNormalizer and InvestmentClassifier, and the artifact and classification
// value types live here; providersync aliases them.
//
// # What is in scope
//
// ONLY the arithmetic and the three INSERT statements. Team resolution is the
// caller's, through TeamResolver: providersync runs the live attribution
// cascade over the facts it loaded, the daily family reads the cascade's stored
// result from work_item_team_attributions. Tenancy checks, the claim, the
// stamp, and the effect wire format stay with their callers.
package workitemengine

import (
	"errors"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// ArtifactType is the artifact_type of every classification row this package
// produces.
const ArtifactType = "work_item"

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
}

// TeamResolver returns the raw team id of the item at `index` of the item
// slice the caller passed, or nil when the item has none. The compute applies
// the unassigned rule itself.
type TeamResolver func(index int) *string

// TypeNormalizer is the status-mapping engine's type rule.
type TypeNormalizer interface {
	NormalizeType(provider, typeRaw string, labels []string) string
}

// InvestmentClassification is one rule decision.
//
// Three of its four fields are *string rather than string because Python can
// and does put None in each of them: `id:`, `investment_area:` and
// `project_stream:` are all read with `.get(key, default)`, so a key that is
// PRESENT AND NULL returns None rather than the default. The Python dataclass
// annotates investment_area and rule_id as `str`, but a dataclass does not
// enforce annotations, so None reaches the call site regardless. A plain Go
// string would have to invent "product"/"general"/"legacy_rule" there, which is
// a silent value divergence in the fail-open direction.
type InvestmentClassification struct {
	InvestmentArea *string `json:"investment_area"`
	ProjectStream  *string `json:"project_stream"`
	Confidence     float64 `json:"confidence"`
	RuleID         *string `json:"rule_id"`
}

// InvestmentArtifact is the classifier's input. Python passes a plain dict and
// the call site (job_work_items.py:1377) supplies exactly four keys: labels,
// component, title and provider.
//
// Title and Provider are carried here even though NOTHING reads them, because
// the Python matcher does not read them either -- it inspects only labels,
// paths and component. Dropping them would make the Go signature quietly
// narrower than the contract it ports, and the docstring's claim that `title`
// and `epic` participate would then have no visible counter-evidence.
type InvestmentArtifact struct {
	Labels []string
	// Paths is what the matcher's path_prefix arm reads. The work-item call
	// site never populates it, which is precisely why every path_prefix rule is
	// dead on that path; it stays here because the field is what makes that
	// deadness a property of the CALLER rather than of this engine.
	Paths []string
	// Component is a POINTER because Python reads it with
	// `artifact.get("component")` -- no default -- so an absent key yields None,
	// which is a different value from "" for both the `in` membership test AND
	// the bare-string containment path (`None in "analytics"` raises where
	// `"" in "analytics"` is True). The work-item call site always supplies the
	// key, and always as "" (WorkItem has no `component` attribute, so
	// `getattr(item, "component", "")` cannot return anything else), so nil is
	// unreachable from production -- but the contract this ports can express it
	// and so must this.
	Component *string
	// Read by neither engine. See the type comment.
	Title    string
	Provider string
}

// InvestmentClassifier is the investment-rule engine.
type InvestmentClassifier interface {
	Classify(InvestmentArtifact) (InvestmentClassification, error)
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

// InvestmentClassificationDailyRow is one investment_classifications_daily row
// without the caller's stamp. InvestmentArea and RuleID remain pointers: the
// legacy Python dataclass accepts None from a present-and-null rule output,
// even though ClickHouse cannot persist a null investment_area. The writer
// owns that storage refusal; inventing a value in the compute layer would be a
// Python/Go divergence.
type InvestmentClassificationDailyRow struct {
	RepoID         *uuid.UUID
	ArtifactType   string
	ArtifactID     string
	Provider       string
	InvestmentArea *string
	ProjectStream  string
	Confidence     float64
	RuleID         *string
}

// InvestmentMetricsDailyRow is one investment_metrics_daily row without the
// caller's stamp. TeamID is the empty string for Python's unassigned team: the
// inline producer first maps "unassigned" to None and then applies `or ""` at
// the record boundary.
type InvestmentMetricsDailyRow struct {
	RepoID             *uuid.UUID
	TeamID             string
	InvestmentArea     *string
	ProjectStream      string
	DeliveryUnits      int
	WorkItemsCompleted int
	PRsMerged          int
	ChurnLOC           int
	CycleP50Hours      float64
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
		var p50, p90 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
			p90 = bucket.cycleHours[int(float64(len(bucket.cycleHours))*0.9)]
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
			LeadP50Hours:   0,
		})
	}
	return result
}

type investmentMetricKey struct {
	repoID               nullableUUIDKey
	teamID, area, stream string
	areaValid            bool
}

type investmentMetricBucket struct {
	deliveryUnits, completed, churn int
	cycleHours                      []float64
}

func newInvestmentMetricKey(
	repoID *uuid.UUID,
	teamID string,
	classification InvestmentClassification,
) investmentMetricKey {
	key := investmentMetricKey{
		repoID: newNullableUUIDKey(repoID), teamID: teamID,
	}
	if classification.InvestmentArea != nil {
		key.area = *classification.InvestmentArea
		key.areaValid = true
	}
	if classification.ProjectStream != nil {
		key.stream = *classification.ProjectStream
	}
	return key
}

func (key investmentMetricKey) areaPointer() *string {
	if !key.areaValid {
		return nil
	}
	value := key.area
	return &value
}

// ComputeInvestmentDaily mirrors the investment path in the production Python
// work-item engine helper. Classifications cover active items; aggregate
// metrics are the completed-in-day subset of those same active items.
//
// dayUTC is the UTC midnight of the day and end the next midnight.
func ComputeInvestmentDaily(
	items []Item,
	dayUTC, end time.Time,
	resolveTeam TeamResolver,
	classifier InvestmentClassifier,
) ([]InvestmentClassificationDailyRow, []InvestmentMetricsDailyRow, error) {
	classifications := make([]InvestmentClassificationDailyRow, 0, len(items))
	buckets := make(map[investmentMetricKey]*investmentMetricBucket)
	order := make([]investmentMetricKey, 0, len(items))
	emptyComponent := ""
	for index, item := range items {
		created := item.CreatedAt.UTC()
		if !created.Before(end) ||
			(item.CompletedAt != nil && item.CompletedAt.UTC().Before(dayUTC)) {
			continue
		}
		classification, err := classifier.Classify(InvestmentArtifact{
			Labels: item.Labels, Component: &emptyComponent,
			Title: item.Title, Provider: item.Provider,
		})
		if err != nil {
			return nil, nil, err
		}
		stream := ""
		if classification.ProjectStream != nil {
			stream = *classification.ProjectStream
		}
		classifications = append(classifications, InvestmentClassificationDailyRow{
			RepoID:         newNullableUUIDKey(item.RepoID).pointer(),
			ArtifactType:   ArtifactType,
			ArtifactID:     item.WorkItemID,
			Provider:       item.Provider,
			InvestmentArea: classification.InvestmentArea,
			ProjectStream:  stream,
			Confidence:     classification.Confidence,
			RuleID:         classification.RuleID,
		})

		if item.CompletedAt == nil {
			continue
		}
		completed := item.CompletedAt.UTC()
		if completed.Before(dayUTC) || !completed.Before(end) {
			continue
		}
		teamID := resolveTeam(index)
		team := normalizeTeamID(teamID)
		if team == unassignedTeamID {
			team = ""
		}
		key := newInvestmentMetricKey(item.RepoID, team, classification)
		bucket := buckets[key]
		if bucket == nil {
			bucket = &investmentMetricBucket{}
			buckets[key] = bucket
			order = append(order, key)
		}
		bucket.completed++
		points := 1.0
		if item.StoryPoints != nil && *item.StoryPoints != 0 {
			points = *item.StoryPoints
		}
		if math.IsNaN(points) || math.IsInf(points, 0) ||
			points >= float64(math.MaxInt64) || points < float64(math.MinInt64) {
			return nil, nil, ErrUnrepresentableDeliveryUnits
		}
		bucket.deliveryUnits += int(math.Trunc(points))
		if item.StartedAt != nil {
			cycle := completed.Sub(item.StartedAt.UTC()).Hours()
			if cycle >= 0 {
				bucket.cycleHours = append(bucket.cycleHours, cycle)
			}
		}
	}

	metrics := make([]InvestmentMetricsDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		sort.Float64s(bucket.cycleHours)
		var p50 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
		}
		metrics = append(metrics, InvestmentMetricsDailyRow{
			RepoID:             key.repoID.pointer(),
			TeamID:             key.teamID,
			InvestmentArea:     key.areaPointer(),
			ProjectStream:      key.stream,
			DeliveryUnits:      bucket.deliveryUnits,
			WorkItemsCompleted: bucket.completed,
			PRsMerged:          0,
			ChurnLOC:           bucket.churn,
			CycleP50Hours:      p50,
		})
	}
	return classifications, metrics, nil
}
