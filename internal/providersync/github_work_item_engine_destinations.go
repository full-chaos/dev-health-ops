package providersync

import (
	"context"
	"encoding/json"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/google/uuid"
)

const (
	githubIssueTypeMetricsDestination             = "issue_type_metrics_daily"
	githubWorkItemEngineDestinationStampPrecision = time.Second
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
	LeadP50Hours   *float64                 `json:"lead_p50_hours"`
	ComputedAt     time.Time                `json:"computed_at"`
	OrgID          string                   `json:"org_id"`
}

// GitHubWorkItemEngineDeriver is the concrete implementation of the per-day
// destination that depends on a config-driven engine (the status mapping).
// The investment rule classifier and its two destinations are gone
// (CHAOS-9200).
type GitHubWorkItemEngineDeriver struct {
	statusMapping *StatusMapping
}

// githubWorkItemEngineRows is the concrete, schema-aligned result of one
// engine invocation. The legacy GitHub deriver still projects it onto its
// historical JSON seam, while the GitLab port consumes these typed fields
// directly so no provider result needs a generic destination map.
type githubWorkItemEngineRows struct {
	IssueTypes []githubIssueTypeMetricsDailyRow
}

func NewGitHubWorkItemEngineDeriver(
	statusMapping *StatusMapping,
) (*GitHubWorkItemEngineDeriver, error) {
	if statusMapping == nil {
		return nil, ErrInvalidConfiguration
	}
	return &GitHubWorkItemEngineDeriver{statusMapping: statusMapping}, nil
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

	var issueTypeMetrics []json.RawMessage
	for _, destination := range githubWorkItemDerivedEngineDestinations {
		var err error
		switch destination {
		case githubIssueTypeMetricsDestination:
			issueTypeMetrics, err = marshalGitHubWorkItemDerivedRows(engineRows.IssueTypes)
		default:
			return nil, ErrInvalidConfiguration
		}
		if err != nil {
			return nil, err
		}
	}
	result := map[string][]json.RawMessage{
		githubIssueTypeMetricsDestination: issueTypeMetrics,
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
	if ctx == nil || engine == nil || engine.statusMapping == nil || claim.Validate() != nil ||
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
	return githubWorkItemEngineRows{IssueTypes: issueTypes}, nil
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

var _ githubWorkItemEngineDeriver = (*GitHubWorkItemEngineDeriver)(nil)
