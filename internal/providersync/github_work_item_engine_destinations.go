package providersync

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
)

const (
	githubIssueTypeMetricsDestination             = "issue_type_metrics_daily"
	githubInvestmentClassificationsDestination    = "investment_classifications_daily"
	githubInvestmentMetricsDestination            = "investment_metrics_daily"
	githubWorkItemEngineDestinationStampPrecision = time.Second
	githubWorkItemEngineArtifactType              = "work_item"
)

// githubIssueTypeMetricsDailyRow mirrors IssueTypeMetricsRecord. repo_id is
// nullable in the migration schema; a nil pointer represents Python's
// uuid.UUID(int=0) sentinel after it is converted back to None at row creation.
type githubIssueTypeMetricsDailyRow struct {
	RepoID         *uuid.UUID               `json:"repo_id"`
	Day            githubWorkItemDerivedDay `json:"day"`
	Provider       string                   `json:"provider"`
	TeamID         string                   `json:"team_id"`
	IssueTypeNorm  string                   `json:"issue_type_norm"`
	CreatedCount   int                      `json:"created_count"`
	CompletedCount int                      `json:"completed_count"`
	ActiveCount    int                      `json:"active_count"`
	CycleP50Hours  float64                  `json:"cycle_p50_hours"`
	CycleP90Hours  float64                  `json:"cycle_p90_hours"`
	LeadP50Hours   float64                  `json:"lead_p50_hours"`
	ComputedAt     time.Time                `json:"computed_at"`
	OrgID          string                   `json:"org_id"`
}

// githubInvestmentClassificationDailyRow mirrors
// InvestmentClassificationRecord. InvestmentArea and RuleID remain pointers:
// the legacy Python dataclass accepts None from a present-and-null rule output,
// even though ClickHouse cannot persist a null investment_area. The adapter
// owns that storage refusal; inventing a value in the compute layer would be a
// Python/Go divergence.
type githubInvestmentClassificationDailyRow struct {
	RepoID         *uuid.UUID               `json:"repo_id"`
	Day            githubWorkItemDerivedDay `json:"day"`
	ArtifactType   string                   `json:"artifact_type"`
	ArtifactID     string                   `json:"artifact_id"`
	Provider       string                   `json:"provider"`
	InvestmentArea *string                  `json:"investment_area"`
	ProjectStream  string                   `json:"project_stream"`
	Confidence     float64                  `json:"confidence"`
	RuleID         *string                  `json:"rule_id"`
	ComputedAt     time.Time                `json:"computed_at"`
	OrgID          string                   `json:"org_id"`
}

// githubInvestmentMetricsDailyRow mirrors InvestmentMetricsRecord. TeamID is
// the empty string for Python's unassigned team: the inline producer first
// maps "unassigned" to None and then applies `or ""` at the record boundary.
type githubInvestmentMetricsDailyRow struct {
	RepoID             *uuid.UUID               `json:"repo_id"`
	Day                githubWorkItemDerivedDay `json:"day"`
	TeamID             string                   `json:"team_id"`
	InvestmentArea     *string                  `json:"investment_area"`
	ProjectStream      string                   `json:"project_stream"`
	DeliveryUnits      int                      `json:"delivery_units"`
	WorkItemsCompleted int                      `json:"work_items_completed"`
	PRsMerged          int                      `json:"prs_merged"`
	ChurnLOC           int                      `json:"churn_loc"`
	CycleP50Hours      float64                  `json:"cycle_p50_hours"`
	ComputedAt         time.Time                `json:"computed_at"`
	OrgID              string                   `json:"org_id"`
}

// GitHubWorkItemEngineDeriver is the concrete implementation of the three
// per-day destinations that depend on config-driven engines. Both engines are
// one atomic capability: constructing or running a partial engine fails closed
// instead of fabricating an empty destination for the missing half.
type GitHubWorkItemEngineDeriver struct {
	statusMapping        *StatusMapping
	investmentClassifier *InvestmentClassifier
}

// githubWorkItemEngineRows is the concrete, schema-aligned result of one
// engine invocation. The legacy GitHub deriver still projects it onto its
// historical JSON seam, while the GitLab port consumes these typed fields
// directly so no provider result needs a generic destination map.
type githubWorkItemEngineRows struct {
	IssueTypes        []githubIssueTypeMetricsDailyRow
	Classifications   []githubInvestmentClassificationDailyRow
	InvestmentMetrics []githubInvestmentMetricsDailyRow
}

func NewGitHubWorkItemEngineDeriver(
	statusMapping *StatusMapping,
	investmentClassifier *InvestmentClassifier,
) (*GitHubWorkItemEngineDeriver, error) {
	if statusMapping == nil || investmentClassifier == nil {
		return nil, ErrInvalidConfiguration
	}
	return &GitHubWorkItemEngineDeriver{
		statusMapping:        statusMapping,
		investmentClassifier: investmentClassifier,
	}, nil
}

// Derive computes exactly one day's rows. GitHubWorkItemDeriver owns the day
// loop and appends this map through githubWorkItemMergeEngineRows, so multi-day
// windows cannot accidentally retain only the last day.
func (engine *GitHubWorkItemEngineDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	engineRows, err := engine.deriveRowsForProvider(
		ctx, "github", claim, rows, day, computedAt, derived,
	)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]json.RawMessage, len(githubWorkItemDerivedEngineDestinations))
	for _, destination := range githubWorkItemDerivedEngineDestinations {
		var encoded []json.RawMessage
		var err error
		switch destination {
		case githubIssueTypeMetricsDestination:
			encoded, err = marshalGitHubWorkItemDerivedRows(engineRows.IssueTypes)
		case githubInvestmentClassificationsDestination:
			encoded, err = marshalGitHubWorkItemDerivedRows(engineRows.Classifications)
		case githubInvestmentMetricsDestination:
			encoded, err = marshalGitHubWorkItemDerivedRows(engineRows.InvestmentMetrics)
		default:
			return nil, ErrInvalidConfiguration
		}
		if err != nil {
			return nil, err
		}
		result[destination] = encoded
	}
	if len(result) != len(githubWorkItemDerivedEngineDestinations) {
		return nil, ErrInvalidConfiguration
	}
	return result, nil
}

func (engine *GitHubWorkItemEngineDeriver) deriveRowsForProvider(
	ctx context.Context,
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
) (githubWorkItemEngineRows, error) {
	if ctx == nil || engine == nil || engine.statusMapping == nil ||
		engine.investmentClassifier == nil || claim.Validate() != nil ||
		claim.Provider != provider || !isWorkItemFamilyDataset(claim.Dataset) ||
		day.IsZero() || computedAt.IsZero() {
		return githubWorkItemEngineRows{}, ErrInvalidConfiguration
	}
	for _, item := range rows.WorkItems {
		// Validate before every time-window skip. A foreign future row is still a
		// foreign row and must not become harmless merely because it is inactive.
		if err := assertGitHubWorkItemDerivedTenancy(claim, item); err != nil {
			return githubWorkItemEngineRows{}, err
		}
	}

	dayUTC := githubWorkItemDerivedUTCDate(day)
	end := dayUTC.AddDate(0, 0, 1)
	stamp := githubWorkItemDerivedStamp(
		computedAt, githubWorkItemEngineDestinationStampPrecision,
	)
	issueTypes := buildGitHubIssueTypeMetricsDaily(
		claim, rows, dayUTC, end, stamp, derived, engine.statusMapping,
	)
	classifications, metrics, err := buildGitHubInvestmentDestinationsDaily(
		claim, rows, dayUTC, end, stamp, derived, engine.investmentClassifier,
	)
	if err != nil {
		return githubWorkItemEngineRows{}, err
	}
	return githubWorkItemEngineRows{
		IssueTypes: issueTypes, Classifications: classifications,
		InvestmentMetrics: metrics,
	}, nil
}

type githubNullableUUIDKey struct {
	value uuid.UUID
	valid bool
}

func newGitHubNullableUUIDKey(value *uuid.UUID) githubNullableUUIDKey {
	if value == nil || *value == uuid.Nil {
		return githubNullableUUIDKey{}
	}
	return githubNullableUUIDKey{value: *value, valid: true}
}

func (key githubNullableUUIDKey) pointer() *uuid.UUID {
	if !key.valid {
		return nil
	}
	value := key.value
	return &value
}

type githubIssueTypeMetricsKey struct {
	repoID                          githubNullableUUIDKey
	provider, teamID, issueTypeNorm string
}

type githubIssueTypeMetricsBucket struct {
	created, completed, active int
	cycleHours                 []float64
}

// buildGitHubIssueTypeMetricsDaily mirrors the issue-type path in the production
// Python work-item engine helper. In particular, the bucket is opened before any
// time check, so a future-only item still materializes an all-zero row (D16).
func buildGitHubIssueTypeMetricsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived githubWorkItemDerivationContext,
	statusMapping *StatusMapping,
) []githubIssueTypeMetricsDailyRow {
	buckets := make(map[githubIssueTypeMetricsKey]*githubIssueTypeMetricsBucket)
	order := make([]githubIssueTypeMetricsKey, 0, len(rows.WorkItems))
	for _, item := range rows.WorkItems {
		teamID, _, _ := derived.resolve(githubWorkItemDerivationSubjectFromRow(item))
		key := githubIssueTypeMetricsKey{
			repoID:        newGitHubNullableUUIDKey(item.RepoID),
			provider:      item.Provider,
			teamID:        normalizeGitHubWorkItemDerivedTeamID(teamID),
			issueTypeNorm: statusMapping.NormalizeType(item.Provider, item.Type, item.Labels),
		}
		bucket := buckets[key]
		if bucket == nil {
			bucket = &githubIssueTypeMetricsBucket{}
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

	result := make([]githubIssueTypeMetricsDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		sort.Float64s(bucket.cycleHours)
		var p50, p90 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
			p90 = bucket.cycleHours[int(float64(len(bucket.cycleHours))*0.9)]
		}
		result = append(result, githubIssueTypeMetricsDailyRow{
			RepoID:         key.repoID.pointer(),
			Day:            newGitHubWorkItemDerivedDay(dayUTC),
			Provider:       key.provider,
			TeamID:         key.teamID,
			IssueTypeNorm:  key.issueTypeNorm,
			CreatedCount:   bucket.created,
			CompletedCount: bucket.completed,
			ActiveCount:    bucket.active,
			CycleP50Hours:  p50,
			CycleP90Hours:  p90,
			LeadP50Hours:   0,
			ComputedAt:     computedAt,
			OrgID:          claim.OrgID,
		})
	}
	return result
}

type githubInvestmentMetricKey struct {
	repoID               githubNullableUUIDKey
	teamID, area, stream string
	areaValid            bool
}

type githubInvestmentMetricBucket struct {
	deliveryUnits, completed, churn int
	cycleHours                      []float64
}

func newGitHubInvestmentMetricKey(
	repoID *uuid.UUID,
	teamID string,
	classification InvestmentClassification,
) githubInvestmentMetricKey {
	key := githubInvestmentMetricKey{
		repoID: newGitHubNullableUUIDKey(repoID), teamID: teamID,
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

func (key githubInvestmentMetricKey) areaPointer() *string {
	if !key.areaValid {
		return nil
	}
	value := key.area
	return &value
}

// buildGitHubInvestmentDestinationsDaily mirrors the investment path in the
// production Python work-item engine helper. Classifications cover active items;
// aggregate metrics are the completed-in-day subset of those same active items.
func buildGitHubInvestmentDestinationsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived githubWorkItemDerivationContext,
	classifier *InvestmentClassifier,
) ([]githubInvestmentClassificationDailyRow, []githubInvestmentMetricsDailyRow, error) {
	classifications := make([]githubInvestmentClassificationDailyRow, 0, len(rows.WorkItems))
	buckets := make(map[githubInvestmentMetricKey]*githubInvestmentMetricBucket)
	order := make([]githubInvestmentMetricKey, 0, len(rows.WorkItems))
	emptyComponent := ""
	for _, item := range rows.WorkItems {
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
		classifications = append(classifications, githubInvestmentClassificationDailyRow{
			RepoID:         newGitHubNullableUUIDKey(item.RepoID).pointer(),
			Day:            newGitHubWorkItemDerivedDay(dayUTC),
			ArtifactType:   githubWorkItemEngineArtifactType,
			ArtifactID:     item.WorkItemID,
			Provider:       item.Provider,
			InvestmentArea: classification.InvestmentArea,
			ProjectStream:  stream,
			Confidence:     classification.Confidence,
			RuleID:         classification.RuleID,
			ComputedAt:     computedAt,
			OrgID:          claim.OrgID,
		})

		if item.CompletedAt == nil {
			continue
		}
		completed := item.CompletedAt.UTC()
		if completed.Before(dayUTC) || !completed.Before(end) {
			continue
		}
		teamID, _, _ := derived.resolve(githubWorkItemDerivationSubjectFromRow(item))
		team := normalizeGitHubWorkItemDerivedTeamID(teamID)
		if team == githubWorkItemUnassignedTeamID {
			team = ""
		}
		key := newGitHubInvestmentMetricKey(item.RepoID, team, classification)
		bucket := buckets[key]
		if bucket == nil {
			bucket = &githubInvestmentMetricBucket{}
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
			return nil, nil, ErrInvalidConfiguration
		}
		bucket.deliveryUnits += int(math.Trunc(points))
		if item.StartedAt != nil {
			cycle := completed.Sub(item.StartedAt.UTC()).Hours()
			if cycle >= 0 {
				bucket.cycleHours = append(bucket.cycleHours, cycle)
			}
		}
	}

	metrics := make([]githubInvestmentMetricsDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		sort.Float64s(bucket.cycleHours)
		var p50 float64
		if len(bucket.cycleHours) > 0 {
			p50 = bucket.cycleHours[len(bucket.cycleHours)/2]
		}
		metrics = append(metrics, githubInvestmentMetricsDailyRow{
			RepoID:             key.repoID.pointer(),
			Day:                newGitHubWorkItemDerivedDay(dayUTC),
			TeamID:             key.teamID,
			InvestmentArea:     key.areaPointer(),
			ProjectStream:      key.stream,
			DeliveryUnits:      bucket.deliveryUnits,
			WorkItemsCompleted: bucket.completed,
			PRsMerged:          0,
			ChurnLOC:           bucket.churn,
			CycleP50Hours:      p50,
			ComputedAt:         computedAt,
			OrgID:              claim.OrgID,
		})
	}
	return classifications, metrics, nil
}

var _ githubWorkItemEngineDeriver = (*GitHubWorkItemEngineDeriver)(nil)
