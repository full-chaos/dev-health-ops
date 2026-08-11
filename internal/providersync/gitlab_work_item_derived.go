package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var ErrGitLabWorkItemDerivedProducerUnavailable = errors.New(
	"gitlab work-item authoritative derived producer is unavailable",
)

// The GitLab derived family is the ten schema-backed projections emitted by
// Python's work-item job. AI attribution is normalized while the route still
// owns the original merge-request payload; the other nine projections are
// computed from the six normalized work-item facts below.
var gitlabWorkItemDerivedDestinations = []string{
	"ai_attribution",
	"estimate_coverage_metrics_daily",
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_item_state_durations_daily",
	"work_item_team_attributions",
	"work_item_user_metrics_daily",
}

type gitlabEstimateCoverageMetricsDailyRow = githubEstimateCoverageMetricsDailyRow
type gitlabWorkItemTeamAttributionRow = githubWorkItemTeamAttributionRow
type gitlabWorkItemStateDurationDailyRow = githubWorkItemStateDurationDailyRow
type gitlabWorkItemMetricsDailyRow = githubWorkItemMetricsDailyRow
type gitlabWorkItemUserMetricsDailyRow = githubWorkItemUserMetricsDailyRow
type gitlabWorkItemCycleTimeRecord = githubWorkItemCycleTimeRecord
type gitlabWorkItemCycleTimePersistenceRow = githubWorkItemCycleTimePersistenceRow
type gitlabIssueTypeMetricsDailyRow = githubIssueTypeMetricsDailyRow
type gitlabInvestmentClassificationDailyRow = githubInvestmentClassificationDailyRow
type gitlabInvestmentMetricsDailyRow = githubInvestmentMetricsDailyRow

// GitLabWorkItemDerivedGap is a typed explanation of a destination that was
// evaluated and deliberately withheld because its authoritative producer did
// not complete. A complete result carries no gaps and may advance watermark.
type GitLabWorkItemDerivedGap struct {
	Destination           string `json:"destination"`
	AuthoritativeProducer string `json:"authoritative_producer"`
	Reason                string `json:"reason"`
}

// GitLabWorkItemDerivedRows is the provider result at the compute boundary.
// Every destination is a concrete ClickHouse row slice, so callers cannot
// accidentally smuggle an untyped map or generic evidence envelope into the
// effect ledger.
type GitLabWorkItemDerivedRows struct {
	AIAttributions                 []gitlabAIAttributionRow
	EstimateCoverageMetricsDaily   []gitlabEstimateCoverageMetricsDailyRow
	InvestmentClassificationsDaily []gitlabInvestmentClassificationDailyRow
	InvestmentMetricsDaily         []gitlabInvestmentMetricsDailyRow
	IssueTypeMetricsDaily          []gitlabIssueTypeMetricsDailyRow
	WorkItemCycleTimes             []gitlabWorkItemCycleTimePersistenceRow
	WorkItemMetricsDaily           []gitlabWorkItemMetricsDailyRow
	WorkItemStateDurationsDaily    []gitlabWorkItemStateDurationDailyRow
	WorkItemTeamAttributions       []gitlabWorkItemTeamAttributionRow
	WorkItemUserMetricsDaily       []gitlabWorkItemUserMetricsDailyRow
	Gaps                           []GitLabWorkItemDerivedGap
	Watermark                      *time.Time
}

func (rows GitLabWorkItemDerivedRows) producedDestinations() []string {
	if len(rows.Gaps) > 0 {
		return nil
	}
	return append([]string(nil), gitlabWorkItemDerivedDestinations...)
}

func gitlabWorkItemRowsAsGitHub(rows gitlabWorkItemRows) githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems:         rows.WorkItems,
		StatusTransitions: rows.StatusTransitions,
		Dependencies:      rows.Dependencies,
		ReopenEvents:      rows.ReopenEvents,
		Interactions:      rows.Interactions,
		Sprints:           rows.Sprints,
		AIAttributions:    rows.AIAttributions,
	}
}

// GitLabWorkItemDeriver owns only the compute boundary. It is intentionally
// unregistered: the provider route/cutover wiring remains a separate slice.
type GitLabWorkItemDeriver struct {
	Source               githubWorkItemDerivationContextSource
	statusMapping        *StatusMapping
	investmentClassifier *InvestmentClassifier
}

// NewGitLabWorkItemDeriver loads the same governed status and investment
// configuration used by Python's work-item job. Both paths are required so a
// missing engine cannot be mistaken for an empty destination.
func NewGitLabWorkItemDeriver(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	statusMappingConfigPath string,
	investmentConfigPath string,
) (*GitLabWorkItemDeriver, error) {
	if conn == nil || lease == nil || strings.TrimSpace(statusMappingConfigPath) == "" ||
		strings.TrimSpace(investmentConfigPath) == "" {
		return nil, ErrInvalidConfiguration
	}
	statusMapping, err := LoadStatusMapping(statusMappingConfigPath)
	if err != nil {
		return nil, err
	}
	classifier, err := NewInvestmentClassifier(investmentConfigPath)
	if err != nil {
		return nil, err
	}
	return &GitLabWorkItemDeriver{
		Source:        githubWorkItemClickHouseDerivationContextSource{Conn: conn, Lease: lease},
		statusMapping: statusMapping, investmentClassifier: classifier,
	}, nil
}

// Derive computes all ten destinations for the claim window. AI attribution
// was already produced at the provider normalization boundary from the raw MR
// payload; this compute boundary passes those rows through and derives the
// other nine surfaces. Context is loaded once, before the day loop, matching
// the Python job's donor/ownership snapshot semantics.
func (deriver GitLabWorkItemDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows gitlabWorkItemRows,
	normalizedAt time.Time,
) (GitLabWorkItemDerivedRows, error) {
	if ctx == nil || deriver.Source == nil || deriver.statusMapping == nil ||
		deriver.investmentClassifier == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || claim.Dataset != "work-items" || normalizedAt.IsZero() {
		return GitLabWorkItemDerivedRows{}, ErrInvalidConfiguration
	}
	days, err := githubWorkItemDerivedDays(claim, normalizedAt)
	if err != nil {
		return GitLabWorkItemDerivedRows{}, err
	}
	contextFacts, err := loadWorkItemDerivationContextForProvider(
		ctx, "gitlab", claim, gitlabWorkItemRowsAsGitHub(rows), deriver.Source, normalizedAt,
	)
	if err != nil {
		return GitLabWorkItemDerivedRows{}, err
	}
	var watermark *time.Time
	if claim.BeforeAt != nil {
		value := claim.BeforeAt.UTC()
		watermark = &value
	}
	result := GitLabWorkItemDerivedRows{
		AIAttributions:                 append([]gitlabAIAttributionRow(nil), rows.AIAttributions...),
		EstimateCoverageMetricsDaily:   []gitlabEstimateCoverageMetricsDailyRow{},
		InvestmentClassificationsDaily: []gitlabInvestmentClassificationDailyRow{},
		InvestmentMetricsDaily:         []gitlabInvestmentMetricsDailyRow{},
		IssueTypeMetricsDaily:          []gitlabIssueTypeMetricsDailyRow{},
		WorkItemCycleTimes:             []gitlabWorkItemCycleTimePersistenceRow{},
		WorkItemMetricsDaily:           []gitlabWorkItemMetricsDailyRow{},
		WorkItemStateDurationsDaily:    []gitlabWorkItemStateDurationDailyRow{},
		WorkItemTeamAttributions:       []gitlabWorkItemTeamAttributionRow{},
		WorkItemUserMetricsDaily:       []gitlabWorkItemUserMetricsDailyRow{},
		Watermark:                      watermark,
		Gaps:                           []GitLabWorkItemDerivedGap{},
	}
	for _, day := range days {
		triplet, err := buildWorkItemMetricTripletForProvider(
			"gitlab", claim, gitlabWorkItemRowsAsGitHub(rows), day, normalizedAt, contextFacts,
		)
		if err != nil {
			return GitLabWorkItemDerivedRows{}, err
		}
		result.WorkItemMetricsDaily = append(result.WorkItemMetricsDaily, triplet.MetricsDaily...)
		result.WorkItemUserMetricsDaily = append(result.WorkItemUserMetricsDaily, triplet.UserMetricsDaily...)
		for _, cycle := range triplet.CycleTimes {
			result.WorkItemCycleTimes = append(result.WorkItemCycleTimes, cycle.persistenceRow())
		}

		surfaces, err := buildWorkItemDerivedSurfacesForProvider(
			"gitlab", claim, gitlabWorkItemRowsAsGitHub(rows), day, normalizedAt, contextFacts,
		)
		if err != nil {
			return GitLabWorkItemDerivedRows{}, err
		}
		result.EstimateCoverageMetricsDaily = append(result.EstimateCoverageMetricsDaily, surfaces.EstimateCoverage...)
		result.WorkItemTeamAttributions = append(result.WorkItemTeamAttributions, surfaces.TeamAttributions...)
		result.WorkItemStateDurationsDaily = append(result.WorkItemStateDurationsDaily, surfaces.StateDurations...)

		engine := GitHubWorkItemEngineDeriver{
			statusMapping:        deriver.statusMapping,
			investmentClassifier: deriver.investmentClassifier,
		}
		engineRows, err := engine.deriveRowsForProvider(
			ctx, "gitlab", claim, gitlabWorkItemRowsAsGitHub(rows), day, normalizedAt, contextFacts,
		)
		if err != nil {
			return GitLabWorkItemDerivedRows{}, err
		}
		result.IssueTypeMetricsDaily = append(result.IssueTypeMetricsDaily, engineRows.IssueTypes...)
		result.InvestmentClassificationsDaily = append(result.InvestmentClassificationsDaily, engineRows.Classifications...)
		result.InvestmentMetricsDaily = append(result.InvestmentMetricsDaily, engineRows.InvestmentMetrics...)
	}
	return result, nil
}

// GitLabWorkItemDerivedEffectRows is the explicit effect projection. It has
// one field per landed destination and therefore cannot silently omit a row
// family through a string-keyed map.
type GitLabWorkItemDerivedEffectRows struct {
	AIAttributions                 []gitlabAIAttributionRow
	EstimateCoverageMetricsDaily   []gitlabEstimateCoverageMetricsDailyRow
	InvestmentClassificationsDaily []gitlabInvestmentClassificationDailyRow
	InvestmentMetricsDaily         []gitlabInvestmentMetricsDailyRow
	IssueTypeMetricsDaily          []gitlabIssueTypeMetricsDailyRow
	WorkItemCycleTimes             []gitlabWorkItemCycleTimePersistenceRow
	WorkItemMetricsDaily           []gitlabWorkItemMetricsDailyRow
	WorkItemStateDurationsDaily    []gitlabWorkItemStateDurationDailyRow
	WorkItemTeamAttributions       []gitlabWorkItemTeamAttributionRow
	WorkItemUserMetricsDaily       []gitlabWorkItemUserMetricsDailyRow
}

func (rows GitLabWorkItemDerivedRows) EffectRows() GitLabWorkItemDerivedEffectRows {
	return GitLabWorkItemDerivedEffectRows{
		AIAttributions:                 rows.AIAttributions,
		EstimateCoverageMetricsDaily:   rows.EstimateCoverageMetricsDaily,
		InvestmentClassificationsDaily: rows.InvestmentClassificationsDaily,
		InvestmentMetricsDaily:         rows.InvestmentMetricsDaily,
		IssueTypeMetricsDaily:          rows.IssueTypeMetricsDaily,
		WorkItemCycleTimes:             rows.WorkItemCycleTimes,
		WorkItemMetricsDaily:           rows.WorkItemMetricsDaily,
		WorkItemStateDurationsDaily:    rows.WorkItemStateDurationsDaily,
		WorkItemTeamAttributions:       rows.WorkItemTeamAttributions,
		WorkItemUserMetricsDaily:       rows.WorkItemUserMetricsDaily,
	}
}

// BuildGitLabWorkItemDerivedEffects serializes only concrete typed row slices,
// in canonical destination order.
func BuildGitLabWorkItemDerivedEffects(rows GitLabWorkItemDerivedEffectRows) ([]EffectBatch, error) {
	effects := make([]EffectBatch, 0, len(gitlabWorkItemDerivedDestinations))
	for _, destination := range gitlabWorkItemDerivedDestinations {
		var effect EffectBatch
		var err error
		switch destination {
		case "ai_attribution":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.AIAttributions)
		case "estimate_coverage_metrics_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.EstimateCoverageMetricsDaily)
		case "investment_classifications_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.InvestmentClassificationsDaily)
		case "investment_metrics_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.InvestmentMetricsDaily)
		case "issue_type_metrics_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.IssueTypeMetricsDaily)
		case "work_item_cycle_times":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.WorkItemCycleTimes)
		case "work_item_metrics_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.WorkItemMetricsDaily)
		case "work_item_state_durations_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.WorkItemStateDurationsDaily)
		case "work_item_team_attributions":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.WorkItemTeamAttributions)
		case "work_item_user_metrics_daily":
			effect, err = buildGitLabTypedDerivedEffect(destination, rows.WorkItemUserMetricsDaily)
		default:
			return nil, ErrInvalidConfiguration
		}
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

func buildGitLabTypedDerivedEffect[T any](
	destination string,
	rows []T,
) (EffectBatch, error) {
	encoded, err := json.Marshal(rows)
	if err != nil {
		return EffectBatch{}, err
	}
	var effectRows []json.RawMessage
	if err := json.Unmarshal(encoded, &effectRows); err != nil {
		return EffectBatch{}, ErrEffectRecoveryUnsafe
	}
	return BuildEffectBatch(destination, EffectReadbackRequired, effectRows)
}

func newGitLabWorkItemDerivedEffectIdentity(
	claim Claim,
	effect EffectBatch,
) (GitLabWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		!gitlabWorkItemDerivedDestination(effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) ||
		effect.PayloadBytes < 0 {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest ||
		rebuilt.PayloadBytes != effect.PayloadBytes {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := GitLabWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

func gitlabWorkItemDerivedDestination(destination string) bool {
	for _, candidate := range gitlabWorkItemDerivedDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

func gitlabDerivedGitHubIdentity(
	identity GitLabWorkItemEffectIdentity,
) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: identity.OrgID, Provider: identity.Provider, Dataset: identity.Dataset,
		Generation: identity.Generation, Destination: identity.Destination,
		ContentDigest: identity.ContentDigest, RowCount: identity.RowCount,
	}
}

// GitLabWorkItemDerivedClickHouseEffects is the ten-destination provider
// sink. Its fields are concrete adapter types and its dispatcher is an
// explicit switch, so a destination cannot be accepted without a corresponding
// schema-specific write/readback implementation.
type GitLabWorkItemDerivedClickHouseEffects struct {
	Lease                          providerfoundation.LeaseGuard
	AIAttribution                  GitLabAIAttributionClickHouseAdapter
	EstimateCoverageMetricsDaily   GitHubEstimateCoverageClickHouseEffects
	InvestmentClassificationsDaily GitHubInvestmentClassificationsClickHouseEffects
	InvestmentMetricsDaily         GitHubInvestmentMetricsClickHouseEffects
	IssueTypeMetricsDaily          GitHubIssueTypeMetricsClickHouseEffects
	WorkItemCycleTimes             GitHubWorkItemCycleTimesClickHouseEffects
	WorkItemMetricsDaily           GitHubWorkItemMetricsDailyClickHouseEffects
	WorkItemStateDurationsDaily    GitHubWorkItemStateDurationsClickHouseEffects
	WorkItemTeamAttributions       GitHubWorkItemTeamAttributionsClickHouseEffects
	WorkItemUserMetricsDaily       GitHubWorkItemUserMetricsDailyClickHouseEffects
}

func NewGitLabWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (GitLabWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return GitLabWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	sink := GitLabWorkItemDerivedClickHouseEffects{
		Lease:                          lease,
		AIAttribution:                  GitLabAIAttributionClickHouseAdapter{Conn: conn},
		EstimateCoverageMetricsDaily:   GitHubEstimateCoverageClickHouseEffects{Conn: conn, Lease: lease},
		InvestmentClassificationsDaily: GitHubInvestmentClassificationsClickHouseEffects{Conn: conn, Lease: lease},
		InvestmentMetricsDaily:         GitHubInvestmentMetricsClickHouseEffects{Conn: conn, Lease: lease},
		IssueTypeMetricsDaily:          GitHubIssueTypeMetricsClickHouseEffects{Conn: conn, Lease: lease},
		WorkItemCycleTimes:             GitHubWorkItemCycleTimesClickHouseEffects{Conn: conn, Lease: lease},
		WorkItemMetricsDaily:           GitHubWorkItemMetricsDailyClickHouseEffects{Conn: conn, Lease: lease},
		WorkItemStateDurationsDaily:    GitHubWorkItemStateDurationsClickHouseEffects{Conn: conn, Lease: lease},
		WorkItemTeamAttributions:       GitHubWorkItemTeamAttributionsClickHouseEffects{Conn: conn, Lease: lease},
		WorkItemUserMetricsDaily:       GitHubWorkItemUserMetricsDailyClickHouseEffects{Conn: conn, Lease: lease},
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink GitLabWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(gitlabWorkItemDerivedDestinations))
	checks := []struct {
		name  string
		conn  driver.Conn
		lease providerfoundation.LeaseGuard
	}{
		{"ai_attribution", sink.AIAttribution.Conn, sink.Lease},
		{"estimate_coverage_metrics_daily", sink.EstimateCoverageMetricsDaily.Conn, sink.EstimateCoverageMetricsDaily.Lease},
		{"investment_classifications_daily", sink.InvestmentClassificationsDaily.Conn, sink.InvestmentClassificationsDaily.Lease},
		{"investment_metrics_daily", sink.InvestmentMetricsDaily.Conn, sink.InvestmentMetricsDaily.Lease},
		{"issue_type_metrics_daily", sink.IssueTypeMetricsDaily.Conn, sink.IssueTypeMetricsDaily.Lease},
		{"work_item_cycle_times", sink.WorkItemCycleTimes.Conn, sink.WorkItemCycleTimes.Lease},
		{"work_item_metrics_daily", sink.WorkItemMetricsDaily.Conn, sink.WorkItemMetricsDaily.Lease},
		{"work_item_state_durations_daily", sink.WorkItemStateDurationsDaily.Conn, sink.WorkItemStateDurationsDaily.Lease},
		{"work_item_team_attributions", sink.WorkItemTeamAttributions.Conn, sink.WorkItemTeamAttributions.Lease},
		{"work_item_user_metrics_daily", sink.WorkItemUserMetricsDaily.Conn, sink.WorkItemUserMetricsDaily.Lease},
	}
	for _, check := range checks {
		if check.conn == nil || check.lease == nil {
			missing = append(missing, check.name)
		}
	}
	return missing
}

func (sink GitLabWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	identity, err := newGitLabWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	gitHubIdentity := gitlabDerivedGitHubIdentity(identity)
	switch effect.Destination {
	case "ai_attribution":
		err = sink.AIAttribution.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "estimate_coverage_metrics_daily":
		err = sink.EstimateCoverageMetricsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "investment_classifications_daily":
		err = sink.InvestmentClassificationsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "investment_metrics_daily":
		err = sink.InvestmentMetricsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "issue_type_metrics_daily":
		err = sink.IssueTypeMetricsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_cycle_times":
		err = sink.WorkItemCycleTimes.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_metrics_daily":
		err = sink.WorkItemMetricsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_state_durations_daily":
		err = sink.WorkItemStateDurationsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_team_attributions":
		err = sink.WorkItemTeamAttributions.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_user_metrics_daily":
		err = sink.WorkItemUserMetricsDaily.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	default:
		return ErrInvalidConfiguration
	}
	if err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitLabWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	identity, err := newGitLabWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	gitHubIdentity := gitlabDerivedGitHubIdentity(identity)
	var inspection EffectInspection
	switch effect.Destination {
	case "ai_attribution":
		inspection, err = sink.AIAttribution.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "estimate_coverage_metrics_daily":
		inspection, err = sink.EstimateCoverageMetricsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "investment_classifications_daily":
		inspection, err = sink.InvestmentClassificationsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "investment_metrics_daily":
		inspection, err = sink.InvestmentMetricsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "issue_type_metrics_daily":
		inspection, err = sink.IssueTypeMetricsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_cycle_times":
		inspection, err = sink.WorkItemCycleTimes.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_metrics_daily":
		inspection, err = sink.WorkItemMetricsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_state_durations_daily":
		inspection, err = sink.WorkItemStateDurationsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_team_attributions":
		inspection, err = sink.WorkItemTeamAttributions.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	case "work_item_user_metrics_daily":
		inspection, err = sink.WorkItemUserMetricsDaily.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
	if err != nil {
		return EffectConflict, err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch inspection {
	case EffectExact, EffectAbsent, EffectConflict:
		return inspection, nil
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ EffectSink = GitLabWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = GitLabWorkItemDerivedClickHouseEffects{}

// GitLabAIAttributionClickHouseAdapter preserves the provider fence before
// delegating the shared ai_attribution column projection. The shared adapter
// correctly fences tenant UUIDs but intentionally serves multiple providers,
// so this provider-local boundary must reject a recovered or forged GitHub row
// carried by a GitLab effect.
type GitLabAIAttributionClickHouseAdapter struct{ Conn driver.Conn }

func (adapter GitLabAIAttributionClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if !validGitLabAIAttributionEffect(identity, effect) {
		return ErrInvalidConfiguration
	}
	return (GitHubAIAttributionClickHouseAdapter{Conn: adapter.Conn}).
		WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter GitLabAIAttributionClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if !validGitLabAIAttributionEffect(identity, effect) {
		return EffectConflict, ErrInvalidConfiguration
	}
	return (GitHubAIAttributionClickHouseAdapter{Conn: adapter.Conn}).
		InspectGitHubWorkItemEffect(ctx, identity, effect)
}

func validGitLabAIAttributionEffect(
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) bool {
	if identity.Provider != "gitlab" || identity.Dataset != "work-items" ||
		identity.Destination != "ai_attribution" || effect.Destination != "ai_attribution" {
		return false
	}
	rows, err := decodeEffectRows[gitlabAIAttributionRow](effect)
	if err != nil || identity.RowCount != len(rows) {
		return false
	}
	claim := Claim{Unit: Unit{
		OrgID: identity.OrgID, Provider: identity.Provider, Dataset: identity.Dataset,
	}}
	for _, row := range rows {
		if validateGitLabAIAttributionRow(row, claim) != nil {
			return false
		}
	}
	return true
}

var _ GitHubWorkItemEffectAdapter = GitLabAIAttributionClickHouseAdapter{}
