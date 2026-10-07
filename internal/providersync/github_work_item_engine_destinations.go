package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/google/uuid"
)

const (
	githubIssueTypeMetricsDestination             = "issue_type_metrics_daily"
	githubInvestmentClassificationsDestination    = "investment_classifications_daily"
	githubInvestmentMetricsDestination            = "investment_metrics_daily"
	githubWorkItemEngineDestinationStampPrecision = time.Second
	githubWorkItemEngineArtifactType              = workitemengine.ArtifactType
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
	derived teamattribution.GithubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	return engine.deriveForProvider(
		ctx, "github", claim, rows, day, computedAt, derived,
	)
}

func (engine *GitHubWorkItemEngineDeriver) deriveForProvider(
	ctx context.Context,
	provider string,
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived teamattribution.GithubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	engineRows, err := engine.deriveRowsForProvider(
		ctx, provider, claim, rows, day, computedAt, derived,
	)
	if err != nil {
		return nil, err
	}

	var issueTypeMetrics, investmentClassifications, metrics []json.RawMessage
	for _, destination := range githubWorkItemDerivedEngineDestinations {
		var err error
		switch destination {
		case githubIssueTypeMetricsDestination:
			issueTypeMetrics, err = marshalGitHubWorkItemDerivedRows(engineRows.IssueTypes)
		case githubInvestmentClassificationsDestination:
			investmentClassifications, err = marshalGitHubWorkItemDerivedRows(engineRows.Classifications)
		case githubInvestmentMetricsDestination:
			metrics, err = marshalGitHubWorkItemDerivedRows(engineRows.InvestmentMetrics)
		default:
			return nil, ErrInvalidConfiguration
		}
		if err != nil {
			return nil, err
		}
	}
	result := map[string][]json.RawMessage{
		githubIssueTypeMetricsDestination:          issueTypeMetrics,
		githubInvestmentClassificationsDestination: investmentClassifications,
		githubInvestmentMetricsDestination:         metrics,
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
	derived teamattribution.GithubWorkItemDerivationContext,
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

// githubWorkItemEngineItems projects the unit's rows onto the fields the shared
// compute reads. The slice keeps the order of rows.WorkItems, so an index of
// one is an index of the other.
func githubWorkItemEngineItems(rows githubWorkItemRows) []workitemengine.Item {
	items := make([]workitemengine.Item, 0, len(rows.WorkItems))
	for _, item := range rows.WorkItems {
		items = append(items, workitemengine.Item{
			WorkItemID: item.WorkItemID, Provider: item.Provider, Type: item.Type,
			Title: item.Title, Labels: item.Labels, RepoID: item.RepoID,
			CreatedAt: item.CreatedAt, StartedAt: item.StartedAt,
			CompletedAt: item.CompletedAt, StoryPoints: item.StoryPoints,
		})
	}
	return items
}

// githubWorkItemEngineTeamResolver answers from the live attribution cascade
// over the facts this unit loaded.
func githubWorkItemEngineTeamResolver(
	rows githubWorkItemRows,
	derived teamattribution.GithubWorkItemDerivationContext,
) workitemengine.TeamResolver {
	return func(index int) *string {
		teamID, _, _ := derived.Resolve(githubWorkItemDerivationSubjectFromRow(rows.WorkItems[index]))
		return teamID
	}
}

// buildGitHubIssueTypeMetricsDaily is the sync deriver's adapter over
// workitemengine.ComputeIssueTypeMetricsDaily: the arithmetic lives there, the
// stamp (day, computed_at, org_id) is added here.
func buildGitHubIssueTypeMetricsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived teamattribution.GithubWorkItemDerivationContext,
	statusMapping *StatusMapping,
) []githubIssueTypeMetricsDailyRow {
	computed := workitemengine.ComputeIssueTypeMetricsDaily(
		githubWorkItemEngineItems(rows), dayUTC, end,
		githubWorkItemEngineTeamResolver(rows, derived), statusMapping,
	)
	result := make([]githubIssueTypeMetricsDailyRow, 0, len(computed))
	for _, row := range computed {
		result = append(result, githubIssueTypeMetricsDailyRow{
			RepoID:         row.RepoID,
			Day:            newGitHubWorkItemDerivedDay(dayUTC),
			Provider:       row.Provider,
			TeamID:         row.TeamID,
			IssueTypeNorm:  row.IssueTypeNorm,
			CreatedCount:   row.CreatedCount,
			CompletedCount: row.CompletedCount,
			ActiveCount:    row.ActiveCount,
			CycleP50Hours:  row.CycleP50Hours,
			CycleP90Hours:  row.CycleP90Hours,
			LeadP50Hours:   row.LeadP50Hours,
			ComputedAt:     computedAt,
			OrgID:          claim.OrgID,
		})
	}
	return result
}

// buildGitHubInvestmentDestinationsDaily is the sync deriver's adapter over
// workitemengine.ComputeInvestmentDaily.
func buildGitHubInvestmentDestinationsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived teamattribution.GithubWorkItemDerivationContext,
	classifier *InvestmentClassifier,
) ([]githubInvestmentClassificationDailyRow, []githubInvestmentMetricsDailyRow, error) {
	computedClassifications, computedMetrics, err := workitemengine.ComputeInvestmentDaily(
		githubWorkItemEngineItems(rows), dayUTC, end,
		githubWorkItemEngineTeamResolver(rows, derived), classifier,
	)
	if err != nil {
		if errors.Is(err, workitemengine.ErrUnrepresentableDeliveryUnits) {
			return nil, nil, ErrInvalidConfiguration
		}
		return nil, nil, err
	}
	classifications := make([]githubInvestmentClassificationDailyRow, 0, len(computedClassifications))
	for _, row := range computedClassifications {
		classifications = append(classifications, githubInvestmentClassificationDailyRow{
			RepoID:         row.RepoID,
			Day:            newGitHubWorkItemDerivedDay(dayUTC),
			ArtifactType:   row.ArtifactType,
			ArtifactID:     row.ArtifactID,
			Provider:       row.Provider,
			InvestmentArea: row.InvestmentArea,
			ProjectStream:  row.ProjectStream,
			Confidence:     row.Confidence,
			RuleID:         row.RuleID,
			ComputedAt:     computedAt,
			OrgID:          claim.OrgID,
		})
	}
	metrics := make([]githubInvestmentMetricsDailyRow, 0, len(computedMetrics))
	for _, row := range computedMetrics {
		metrics = append(metrics, githubInvestmentMetricsDailyRow{
			RepoID:             row.RepoID,
			Day:                newGitHubWorkItemDerivedDay(dayUTC),
			TeamID:             row.TeamID,
			InvestmentArea:     row.InvestmentArea,
			ProjectStream:      row.ProjectStream,
			DeliveryUnits:      row.DeliveryUnits,
			WorkItemsCompleted: row.WorkItemsCompleted,
			PRsMerged:          row.PRsMerged,
			ChurnLOC:           row.ChurnLOC,
			CycleP50Hours:      row.CycleP50Hours,
			ComputedAt:         computedAt,
			OrgID:              claim.OrgID,
		})
	}
	return classifications, metrics, nil
}

var _ githubWorkItemEngineDeriver = (*GitHubWorkItemEngineDeriver)(nil)
