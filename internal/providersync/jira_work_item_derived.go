package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Jira's canonical work-item job has nine schema-backed computed destinations.
// ai_attribution is included as an explicit evaluated-empty effect: the live
// Jira ProviderBatch has that field but no Jira AI producer populates it. This
// is distinct from GitLab, whose producer exists and needs MR fields that are
// absent from its six normalized facts.
var jiraWorkItemDerivedDestinations = []string{
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

type JiraEstimateCoverageMetricsDailyRow = githubEstimateCoverageMetricsDailyRow
type JiraInvestmentClassificationDailyRow = githubInvestmentClassificationDailyRow
type JiraInvestmentMetricsDailyRow = githubInvestmentMetricsDailyRow
type JiraIssueTypeMetricsDailyRow = githubIssueTypeMetricsDailyRow
type JiraWorkItemCycleTimePersistenceRow = githubWorkItemCycleTimePersistenceRow
type JiraWorkItemMetricsDailyRow = githubWorkItemMetricsDailyRow
type JiraWorkItemStateDurationDailyRow = githubWorkItemStateDurationDailyRow
type JiraWorkItemTeamAttributionRow = githubWorkItemTeamAttributionRow
type JiraWorkItemUserMetricsDailyRow = githubWorkItemUserMetricsDailyRow

// JiraWorkItemDerivedRows is the concrete compute result. Each field is the
// row type owned by its ClickHouse table; no string-keyed evidence envelope can
// substitute for a destination's schema.
type JiraWorkItemDerivedRows struct {
	EstimateCoverageMetricsDaily   []JiraEstimateCoverageMetricsDailyRow
	InvestmentClassificationsDaily []JiraInvestmentClassificationDailyRow
	InvestmentMetricsDaily         []JiraInvestmentMetricsDailyRow
	IssueTypeMetricsDaily          []JiraIssueTypeMetricsDailyRow
	WorkItemCycleTimes             []JiraWorkItemCycleTimePersistenceRow
	WorkItemMetricsDaily           []JiraWorkItemMetricsDailyRow
	WorkItemStateDurationsDaily    []JiraWorkItemStateDurationDailyRow
	WorkItemTeamAttributions       []JiraWorkItemTeamAttributionRow
	WorkItemUserMetricsDaily       []JiraWorkItemUserMetricsDailyRow
	Watermark                      *time.Time
}

func (rows JiraWorkItemDerivedRows) producedDestinations() []string {
	return append([]string(nil), jiraWorkItemDerivedDestinations...)
}

func jiraWorkItemRowsAsGitHub(rows jiraWorkItemRows) githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems:         rows.WorkItems,
		StatusTransitions: rows.Transitions,
		Dependencies:      rows.Dependencies,
		ReopenEvents:      rows.ReopenEvents,
		Interactions:      rows.Interactions,
		Sprints:           rows.Sprints,
	}
}

// jiraWorkItemsDeriver is injected at the provider route boundary. It is not a
// registry, constructor-selection, or activation seam.
type jiraWorkItemsDeriver interface {
	Derive(context.Context, Claim, jiraWorkItemRows, time.Time) (JiraWorkItemDerivedRows, error)
}

// JiraWorkItemDeriver owns the Jira compute boundary and reuses the
// provider-neutral arithmetic already differential-tested against Python.
type JiraWorkItemDeriver struct {
	Source               githubWorkItemDerivationContextSource
	statusMapping        *StatusMapping
	investmentClassifier *InvestmentClassifier
	observations         *workItemDerivationObservations
}

// StoredEdgeMergeObservation reports the stored-edge union this deriver has
// observed so far on its unit (CHAOS-3978).
func (deriver *JiraWorkItemDeriver) StoredEdgeMergeObservation() githubWorkItemStoredEdgeMergeObservation {
	if deriver == nil {
		return githubWorkItemStoredEdgeMergeObservation{}
	}
	return deriver.observations.storedEdgeMergeSnapshot()
}

// jiraWorkItemClickHouseDerivationContextSource is provider-local because the
// older shared source currently fences only GitHub/GitLab. It calls the same
// migrated ClickHouse loaders and keeps both lease checks without expanding a
// shared provider switch in this slice.
type jiraWorkItemClickHouseDerivationContextSource struct {
	delegate githubWorkItemClickHouseDerivationContextSource
}

func (source jiraWorkItemClickHouseDerivationContextSource) Load(
	ctx context.Context,
	claim Claim,
	request githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	delegate := source.delegate
	if ctx == nil || delegate.Conn == nil || delegate.Lease == nil ||
		claim.Validate() != nil || claim.Provider != "jira" ||
		claim.Dataset != "work-items" || request.AsOf.IsZero() {
		return githubWorkItemDerivationFacts{}, ErrInvalidConfiguration
	}
	if err := delegate.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	facts := githubWorkItemDerivationFacts{}
	var err error
	if facts.Teams, err = delegate.loadTeams(ctx, claim.OrgID); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Projects, err = delegate.loadProjects(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Repos, err = delegate.loadRepos(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.Members, facts.UntypedMembers, facts.ProviderUntypedMembers, err = delegate.loadMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.ProviderMembers, err = delegate.loadProviderMembers(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.ManualFallbacks, err = delegate.loadManualFallbacks(ctx, claim.OrgID, request.AsOf); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if facts.DonorItems, err = delegate.loadDonors(ctx, claim.OrgID, request); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	if err := delegate.Lease.Assert(ctx); err != nil {
		return githubWorkItemDerivationFacts{}, err
	}
	return facts, nil
}

// LoadStoredInheritableEdges keeps the Jira-local source on the same
// stored-edge contract as the shared one (CHAOS-3978). It fences the claim to
// Jira exactly as Load does, then delegates: the read itself is
// provider-neutral -- it is keyed on the SUBJECT items, and the whole point is
// that the edges it finds may have been minted by another provider's sync.
func (source jiraWorkItemClickHouseDerivationContextSource) LoadStoredInheritableEdges(
	ctx context.Context,
	claim Claim,
	sourceWorkItemIDs []string,
) ([]githubWorkItemDependencyRow, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "jira" ||
		claim.Dataset != "work-items" {
		return nil, ErrInvalidConfiguration
	}
	return source.delegate.LoadStoredInheritableEdges(ctx, claim, sourceWorkItemIDs)
}

func NewJiraWorkItemDeriver(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	statusMappingConfigPath string,
	investmentConfigPath string,
) (*JiraWorkItemDeriver, error) {
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
	return &JiraWorkItemDeriver{
		Source: jiraWorkItemClickHouseDerivationContextSource{delegate: githubWorkItemClickHouseDerivationContextSource{
			Conn: conn, Lease: lease,
		}},
		statusMapping: statusMapping, investmentClassifier: classifier,
		observations: newWorkItemDerivationObservations(),
	}, nil
}

// Derive mirrors job_work_items.py's one-context, per-UTC-day execution. The
// five historical Jira aliases retain their frozen claim identity; only the
// internal context-load call is canonicalized because the shared loader's
// contract predates alias-family routing.
func (deriver JiraWorkItemDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows jiraWorkItemRows,
	normalizedAt time.Time,
) (JiraWorkItemDerivedRows, error) {
	if ctx == nil || deriver.Source == nil || deriver.statusMapping == nil ||
		deriver.investmentClassifier == nil || claim.Validate() != nil ||
		claim.Provider != "jira" || !isWorkItemFamilyDataset(claim.Dataset) ||
		claim.BeforeAt == nil || normalizedAt.IsZero() {
		return JiraWorkItemDerivedRows{}, ErrInvalidConfiguration
	}
	days, err := githubWorkItemDerivedDays(claim, normalizedAt)
	if err != nil {
		return JiraWorkItemDerivedRows{}, err
	}
	canonicalClaim := claim
	canonicalClaim.Dataset = "work-items"
	canonicalCapability, ok := Capability("jira", "work-items")
	if !ok {
		return JiraWorkItemDerivedRows{}, ErrInvalidConfiguration
	}
	canonicalClaim.CostClass = canonicalCapability.CostClass
	githubRows := jiraWorkItemRowsAsGitHub(rows)
	contextFacts, err := loadWorkItemDerivationContextForProvider(
		ctx, "jira", canonicalClaim, githubRows, deriver.Source, normalizedAt,
	)
	if err != nil {
		return JiraWorkItemDerivedRows{}, err
	}
	deriver.observations.recordStoredEdgeMerge(contextFacts.storedEdgeMerge)
	result := JiraWorkItemDerivedRows{
		EstimateCoverageMetricsDaily:   []JiraEstimateCoverageMetricsDailyRow{},
		InvestmentClassificationsDaily: []JiraInvestmentClassificationDailyRow{},
		InvestmentMetricsDaily:         []JiraInvestmentMetricsDailyRow{},
		IssueTypeMetricsDaily:          []JiraIssueTypeMetricsDailyRow{},
		WorkItemCycleTimes:             []JiraWorkItemCycleTimePersistenceRow{},
		WorkItemMetricsDaily:           []JiraWorkItemMetricsDailyRow{},
		WorkItemStateDurationsDaily:    []JiraWorkItemStateDurationDailyRow{},
		WorkItemTeamAttributions:       []JiraWorkItemTeamAttributionRow{},
		WorkItemUserMetricsDaily:       []JiraWorkItemUserMetricsDailyRow{},
	}
	for _, day := range days {
		triplet, err := buildWorkItemMetricTripletForProvider(
			"jira", claim, githubRows, day, normalizedAt, contextFacts,
		)
		if err != nil {
			return JiraWorkItemDerivedRows{}, err
		}
		result.WorkItemMetricsDaily = append(result.WorkItemMetricsDaily, triplet.MetricsDaily...)
		result.WorkItemUserMetricsDaily = append(result.WorkItemUserMetricsDaily, triplet.UserMetricsDaily...)
		for _, cycle := range triplet.CycleTimes {
			result.WorkItemCycleTimes = append(result.WorkItemCycleTimes, cycle.persistenceRow())
		}

		surfaces, err := buildWorkItemDerivedSurfacesForProvider(
			"jira", claim, githubRows, day, normalizedAt, contextFacts,
		)
		if err != nil {
			return JiraWorkItemDerivedRows{}, err
		}
		result.EstimateCoverageMetricsDaily = append(result.EstimateCoverageMetricsDaily, surfaces.EstimateCoverage...)
		result.WorkItemTeamAttributions = append(result.WorkItemTeamAttributions, surfaces.TeamAttributions...)
		result.WorkItemStateDurationsDaily = append(result.WorkItemStateDurationsDaily, surfaces.StateDurations...)

		engine := GitHubWorkItemEngineDeriver{
			statusMapping: deriver.statusMapping, investmentClassifier: deriver.investmentClassifier,
		}
		engineRows, err := engine.deriveRowsForProvider(
			ctx, "jira", claim, githubRows, day, normalizedAt, contextFacts,
		)
		if err != nil {
			return JiraWorkItemDerivedRows{}, err
		}
		result.IssueTypeMetricsDaily = append(result.IssueTypeMetricsDaily, engineRows.IssueTypes...)
		result.InvestmentClassificationsDaily = append(result.InvestmentClassificationsDaily, engineRows.Classifications...)
		result.InvestmentMetricsDaily = append(result.InvestmentMetricsDaily, engineRows.InvestmentMetrics...)
	}
	watermark := claim.BeforeAt.UTC()
	result.Watermark = &watermark
	return result, nil
}

// JiraWorkItemDerivedEffectRows is the explicit ten-effect projection. There
// is intentionally no arbitrary AI row field; the builder below owns the sole
// legal Jira AI disposition, an empty slice.
type JiraWorkItemDerivedEffectRows struct {
	EstimateCoverageMetricsDaily   []JiraEstimateCoverageMetricsDailyRow
	InvestmentClassificationsDaily []JiraInvestmentClassificationDailyRow
	InvestmentMetricsDaily         []JiraInvestmentMetricsDailyRow
	IssueTypeMetricsDaily          []JiraIssueTypeMetricsDailyRow
	WorkItemCycleTimes             []JiraWorkItemCycleTimePersistenceRow
	WorkItemMetricsDaily           []JiraWorkItemMetricsDailyRow
	WorkItemStateDurationsDaily    []JiraWorkItemStateDurationDailyRow
	WorkItemTeamAttributions       []JiraWorkItemTeamAttributionRow
	WorkItemUserMetricsDaily       []JiraWorkItemUserMetricsDailyRow
}

func (rows JiraWorkItemDerivedRows) EffectRows() JiraWorkItemDerivedEffectRows {
	return JiraWorkItemDerivedEffectRows{
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

func BuildJiraWorkItemDerivedEffects(rows JiraWorkItemDerivedEffectRows) ([]EffectBatch, error) {
	effects := make([]EffectBatch, 0, len(jiraWorkItemDerivedDestinations))
	for _, destination := range jiraWorkItemDerivedDestinations {
		var effect EffectBatch
		var err error
		switch destination {
		case "ai_attribution":
			effect, err = BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{})
		case "estimate_coverage_metrics_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.EstimateCoverageMetricsDaily)
		case "investment_classifications_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.InvestmentClassificationsDaily)
		case "investment_metrics_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.InvestmentMetricsDaily)
		case "issue_type_metrics_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.IssueTypeMetricsDaily)
		case "work_item_cycle_times":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.WorkItemCycleTimes)
		case "work_item_metrics_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.WorkItemMetricsDaily)
		case "work_item_state_durations_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.WorkItemStateDurationsDaily)
		case "work_item_team_attributions":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.WorkItemTeamAttributions)
		case "work_item_user_metrics_daily":
			effect, err = buildJiraTypedDerivedEffect(destination, rows.WorkItemUserMetricsDaily)
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

func buildJiraTypedDerivedEffect[T any](destination string, rows []T) (EffectBatch, error) {
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

// jiraDerivedGitHubAdapter supplies Jira's frozen identity to the migrated
// schema-specific adapters. The evaluated-empty AI destination is deliberately
// a no-I/O adapter: write is a no-op and readback remains EffectAbsent.
type jiraDerivedGitHubAdapter struct {
	destination string
	delegate    JiraWorkItemEffectAdapter
}

func (adapter jiraDerivedGitHubAdapter) validate(
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if identity.Provider != "jira" || !isWorkItemFamilyDataset(identity.Dataset) ||
		identity.Destination != adapter.destination || effect.Destination != adapter.destination ||
		effect.Recovery != EffectReadbackRequired || identity.OrgID == "" ||
		identity.RowCount != len(effect.Rows) || identity.ContentDigest != effect.ContentDigest {
		return ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return ErrInvalidConfiguration
	}
	if adapter.destination == "ai_attribution" {
		if len(effect.Rows) != 0 {
			return ErrInvalidConfiguration
		}
		return nil
	}
	for _, raw := range effect.Rows {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return ErrInvalidConfiguration
		}
		if adapter.destination != "investment_metrics_daily" {
			var provider string
			providerRaw, ok := fields["provider"]
			if !ok || json.Unmarshal(providerRaw, &provider) != nil || provider != "jira" {
				return ErrInvalidConfiguration
			}
		}
		var orgID string
		orgRaw, ok := fields["org_id"]
		if !ok || json.Unmarshal(orgRaw, &orgID) != nil || orgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func (adapter jiraDerivedGitHubAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if err := adapter.validate(identity, effect); err != nil {
		return err
	}
	if adapter.destination == "ai_attribution" {
		return nil
	}
	if adapter.delegate == nil {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter jiraDerivedGitHubAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if err := adapter.validate(identity, effect); err != nil {
		return EffectConflict, err
	}
	if adapter.destination == "ai_attribution" {
		return EffectAbsent, nil
	}
	if adapter.delegate == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(ctx, identity, effect)
}

var _ JiraWorkItemEffectAdapter = jiraDerivedGitHubAdapter{}

// JiraWorkItemDerivedClickHouseEffects is the provider-local ten-destination
// dispatcher. Direct Jira facts and worklogs stay in their existing sinks.
type JiraWorkItemDerivedClickHouseEffects struct {
	Lease                          providerfoundation.LeaseGuard
	AIAttribution                  JiraWorkItemEffectAdapter
	EstimateCoverageMetricsDaily   JiraWorkItemEffectAdapter
	InvestmentClassificationsDaily JiraWorkItemEffectAdapter
	InvestmentMetricsDaily         JiraWorkItemEffectAdapter
	IssueTypeMetricsDaily          JiraWorkItemEffectAdapter
	WorkItemCycleTimes             JiraWorkItemEffectAdapter
	WorkItemMetricsDaily           JiraWorkItemEffectAdapter
	WorkItemStateDurationsDaily    JiraWorkItemEffectAdapter
	WorkItemTeamAttributions       JiraWorkItemEffectAdapter
	WorkItemUserMetricsDaily       JiraWorkItemEffectAdapter
}

func NewJiraWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (JiraWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return JiraWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	wrap := func(destination string, delegate JiraWorkItemEffectAdapter) JiraWorkItemEffectAdapter {
		return jiraDerivedGitHubAdapter{destination: destination, delegate: delegate}
	}
	sink := JiraWorkItemDerivedClickHouseEffects{
		Lease:                          lease,
		AIAttribution:                  wrap("ai_attribution", nil),
		EstimateCoverageMetricsDaily:   wrap("estimate_coverage_metrics_daily", GitHubEstimateCoverageClickHouseEffects{Conn: conn, Lease: lease}),
		InvestmentClassificationsDaily: wrap("investment_classifications_daily", GitHubInvestmentClassificationsClickHouseEffects{Conn: conn, Lease: lease}),
		InvestmentMetricsDaily:         wrap("investment_metrics_daily", GitHubInvestmentMetricsClickHouseEffects{Conn: conn, Lease: lease}),
		IssueTypeMetricsDaily:          wrap("issue_type_metrics_daily", GitHubIssueTypeMetricsClickHouseEffects{Conn: conn, Lease: lease}),
		WorkItemCycleTimes:             wrap("work_item_cycle_times", GitHubWorkItemCycleTimesClickHouseEffects{Conn: conn, Lease: lease}),
		WorkItemMetricsDaily:           wrap("work_item_metrics_daily", GitHubWorkItemMetricsDailyClickHouseEffects{Conn: conn, Lease: lease}),
		WorkItemStateDurationsDaily:    wrap("work_item_state_durations_daily", GitHubWorkItemStateDurationsClickHouseEffects{Conn: conn, Lease: lease}),
		WorkItemTeamAttributions:       wrap("work_item_team_attributions", GitHubWorkItemTeamAttributionsClickHouseEffects{Conn: conn, Lease: lease}),
		WorkItemUserMetricsDaily:       wrap("work_item_user_metrics_daily", GitHubWorkItemUserMetricsDailyClickHouseEffects{Conn: conn, Lease: lease}),
	}
	if len(sink.MissingDestinations()) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink JiraWorkItemDerivedClickHouseEffects) adapterForDestination(
	destination string,
) (JiraWorkItemEffectAdapter, bool) {
	switch destination {
	case "ai_attribution":
		return sink.AIAttribution, true
	case "estimate_coverage_metrics_daily":
		return sink.EstimateCoverageMetricsDaily, true
	case "investment_classifications_daily":
		return sink.InvestmentClassificationsDaily, true
	case "investment_metrics_daily":
		return sink.InvestmentMetricsDaily, true
	case "issue_type_metrics_daily":
		return sink.IssueTypeMetricsDaily, true
	case "work_item_cycle_times":
		return sink.WorkItemCycleTimes, true
	case "work_item_metrics_daily":
		return sink.WorkItemMetricsDaily, true
	case "work_item_state_durations_daily":
		return sink.WorkItemStateDurationsDaily, true
	case "work_item_team_attributions":
		return sink.WorkItemTeamAttributions, true
	case "work_item_user_metrics_daily":
		return sink.WorkItemUserMetricsDaily, true
	default:
		return nil, false
	}
}

func (sink JiraWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(jiraWorkItemDerivedDestinations))
	for _, destination := range jiraWorkItemDerivedDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink JiraWorkItemDerivedClickHouseEffects) resolve(
	claim Claim,
	effect EffectBatch,
) (JiraWorkItemEffectIdentity, JiraWorkItemEffectAdapter, error) {
	if claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) ||
		!slices.Contains(jiraWorkItemDerivedDestinations, effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) ||
		effect.PayloadBytes < 0 || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	identity := JiraWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink JiraWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink JiraWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
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

var _ EffectSink = JiraWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = JiraWorkItemDerivedClickHouseEffects{}

// JiraWorkItemCompositeClickHouseEffects is the provider-local dispatcher for
// the complete route batch: seven direct Jira effects (the canonical six plus
// worklogs) and ten derived effects. Activation remains outside this file.
type JiraWorkItemCompositeClickHouseEffects struct {
	Direct  JiraAtlassianClickHouseEffects
	Derived JiraWorkItemDerivedClickHouseEffects
}

func NewJiraWorkItemCompositeClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (JiraWorkItemCompositeClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return JiraWorkItemCompositeClickHouseEffects{}, ErrInvalidConfiguration
	}
	derived, err := NewJiraWorkItemDerivedClickHouseEffects(conn, lease)
	if err != nil {
		return JiraWorkItemCompositeClickHouseEffects{}, err
	}
	sink := JiraWorkItemCompositeClickHouseEffects{
		Direct: NewJiraAtlassianClickHouseEffects(conn, lease), Derived: derived,
	}
	if len(sink.MissingDestinations()) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink JiraWorkItemCompositeClickHouseEffects) MissingDestinations() []string {
	missing := sink.Direct.MissingDestinations()
	missing = append(missing, sink.Derived.MissingDestinations()...)
	return missing
}

func (sink JiraWorkItemCompositeClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	switch {
	case slices.Contains(jiraAtlassianEffectDestinations, effect.Destination):
		return sink.Direct.WriteEffect(ctx, claim, effect)
	case slices.Contains(jiraWorkItemDerivedDestinations, effect.Destination):
		return sink.Derived.WriteEffect(ctx, claim, effect)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink JiraWorkItemCompositeClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	switch {
	case slices.Contains(jiraAtlassianEffectDestinations, effect.Destination):
		return sink.Direct.InspectEffect(ctx, claim, effect)
	case slices.Contains(jiraWorkItemDerivedDestinations, effect.Destination):
		return sink.Derived.InspectEffect(ctx, claim, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ EffectSink = JiraWorkItemCompositeClickHouseEffects{}
var _ EffectReadback = JiraWorkItemCompositeClickHouseEffects{}
