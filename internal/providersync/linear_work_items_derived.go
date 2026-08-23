package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

// LinearWorkItemDerivedEffectRows is the provider-owned, typed projection for
// the ten destinations that the real Linear work-item producer can emit from
// its six normalized facts. AI attribution is derived only from explicit issue
// labels; title and description text are not attribution inputs.
type LinearWorkItemDerivedEffectRows struct {
	AIAttributions                 []LinearAIAttributionRow
	EstimateCoverageMetricsDaily   []LinearEstimateCoverageMetricsDailyRow
	InvestmentClassificationsDaily []LinearInvestmentClassificationDailyRow
	InvestmentMetricsDaily         []LinearInvestmentMetricsDailyRow
	IssueTypeMetricsDaily          []LinearIssueTypeMetricsDailyRow
	WorkItemCycleTimes             []LinearWorkItemCycleTimePersistenceRow
	WorkItemMetricsDaily           []LinearWorkItemMetricsDailyRow
	WorkItemStateDurationsDaily    []LinearWorkItemStateDurationDailyRow
	WorkItemTeamAttributions       []LinearWorkItemTeamAttributionRow
	WorkItemUserMetricsDaily       []LinearWorkItemUserMetricsDailyRow
}

var linearWorkItemDerivedEffectDestinations = []string{
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

func BuildLinearWorkItemDerivedEffects(
	rows LinearWorkItemDerivedEffectRows,
) ([]EffectBatch, error) {
	projections := map[string][]json.RawMessage{}
	var err error
	projections["ai_attribution"], err = effectRowsFromValues(rows.AIAttributions)
	if err != nil {
		return nil, err
	}
	projections["estimate_coverage_metrics_daily"], err = effectRowsFromValues(rows.EstimateCoverageMetricsDaily)
	if err != nil {
		return nil, err
	}
	projections["investment_classifications_daily"], err = effectRowsFromValues(rows.InvestmentClassificationsDaily)
	if err != nil {
		return nil, err
	}
	projections["investment_metrics_daily"], err = effectRowsFromValues(rows.InvestmentMetricsDaily)
	if err != nil {
		return nil, err
	}
	projections["issue_type_metrics_daily"], err = effectRowsFromValues(rows.IssueTypeMetricsDaily)
	if err != nil {
		return nil, err
	}
	projections["work_item_cycle_times"], err = effectRowsFromValues(rows.WorkItemCycleTimes)
	if err != nil {
		return nil, err
	}
	projections["work_item_metrics_daily"], err = effectRowsFromValues(rows.WorkItemMetricsDaily)
	if err != nil {
		return nil, err
	}
	projections["work_item_state_durations_daily"], err = effectRowsFromValues(rows.WorkItemStateDurationsDaily)
	if err != nil {
		return nil, err
	}
	projections["work_item_team_attributions"], err = effectRowsFromValues(rows.WorkItemTeamAttributions)
	if err != nil {
		return nil, err
	}
	projections["work_item_user_metrics_daily"], err = effectRowsFromValues(rows.WorkItemUserMetricsDaily)
	if err != nil {
		return nil, err
	}
	return buildLinearWorkItemDerivedEffectsFromMap(projections)
}

func buildLinearWorkItemDerivedEffectsFromMap(
	rows map[string][]json.RawMessage,
) ([]EffectBatch, error) {
	if len(rows) != len(linearWorkItemDerivedEffectDestinations) {
		return nil, ErrInvalidConfiguration
	}
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		if _, ok := rows[destination]; !ok {
			return nil, ErrInvalidConfiguration
		}
	}
	effects := make([]EffectBatch, 0, len(linearWorkItemDerivedEffectDestinations))
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		effect, err := BuildEffectBatch(
			destination, EffectReadbackRequired, rows[destination],
		)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

// These aliases share the exact schema-mirroring rows with the provider-neutral
// builders without copying a second schema that could drift from the real
// Python dataclasses or ClickHouse migrations.
type LinearEstimateCoverageMetricsDailyRow = githubEstimateCoverageMetricsDailyRow
type LinearAIAttributionRow = githubAIAttributionRow
type LinearInvestmentClassificationDailyRow = githubInvestmentClassificationDailyRow
type LinearInvestmentMetricsDailyRow = githubInvestmentMetricsDailyRow
type LinearIssueTypeMetricsDailyRow = githubIssueTypeMetricsDailyRow
type LinearWorkItemCycleTimePersistenceRow = githubWorkItemCycleTimePersistenceRow
type LinearWorkItemMetricsDailyRow = githubWorkItemMetricsDailyRow
type LinearWorkItemStateDurationDailyRow = githubWorkItemStateDurationDailyRow
type LinearWorkItemTeamAttributionRow = githubWorkItemTeamAttributionRow
type LinearWorkItemUserMetricsDailyRow = githubWorkItemUserMetricsDailyRow

// linearWorkItemRowsAsGitHub is a representation conversion only. The
// provider-neutral builders consume UUID repository ids because GitHub's
// normalized model does; Linear's normalizer intentionally leaves repo_id
// absent. A non-empty malformed value is refused rather than normalized to nil,
// which would silently move an item to a different attribution bucket.
func linearWorkItemRowsAsGitHub(rows linearWorkItemRows) (githubWorkItemRows, error) {
	result := githubWorkItemRows{
		WorkItems:         make([]githubWorkItemRow, 0, len(rows.WorkItems)),
		StatusTransitions: make([]githubWorkItemTransitionRow, 0, len(rows.StatusTransitions)),
		Dependencies:      append([]githubWorkItemDependencyRow(nil), rows.Dependencies...),
		ReopenEvents:      append([]githubWorkItemReopenRow(nil), rows.ReopenEvents...),
		Interactions:      append([]githubWorkItemInteractionRow(nil), rows.Interactions...),
		Sprints:           append([]githubSprintRow(nil), rows.Sprints...),
	}
	for _, row := range rows.WorkItems {
		var repoID *uuid.UUID
		if row.RepoID != nil && strings.TrimSpace(*row.RepoID) != "" {
			parsed, err := uuid.Parse(strings.TrimSpace(*row.RepoID))
			if err != nil {
				return githubWorkItemRows{}, ErrInvalidConfiguration
			}
			repoID = &parsed
		}
		result.WorkItems = append(result.WorkItems, githubWorkItemRow{
			WorkItemID: row.WorkItemID, Provider: row.Provider, Title: row.Title,
			Type: row.Type, Status: row.Status, StatusRaw: row.StatusRaw,
			Description: row.Description, RepoID: repoID,
			NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
			ProjectID: row.ProjectID, ProjectName: row.ProjectName,
			Assignees: append([]string(nil), row.Assignees...), Reporter: row.Reporter,
			CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt, StartedAt: row.StartedAt,
			CompletedAt: row.CompletedAt, ClosedAt: row.ClosedAt,
			Labels: append([]string(nil), row.Labels...), StoryPoints: row.StoryPoints,
			SprintID: row.SprintID, SprintName: row.SprintName, ParentID: row.ParentID,
			EpicID: row.EpicID, URL: row.URL, PriorityRaw: row.PriorityRaw,
			ServiceClass: row.ServiceClass, DueAt: row.DueAt, OrgID: row.OrgID,
			LastSynced: row.LastSynced,
		})
	}
	for _, row := range rows.StatusTransitions {
		result.StatusTransitions = append(result.StatusTransitions, githubWorkItemTransitionRow{
			WorkItemID: row.WorkItemID, Provider: row.Provider, OccurredAt: row.OccurredAt,
			FromStatusRaw: row.FromStatusRaw, ToStatusRaw: row.ToStatusRaw,
			FromStatus: row.FromStatus, ToStatus: row.ToStatus, Actor: row.Actor,
			OrgID: row.OrgID, LastSynced: row.LastSynced,
		})
	}
	return result, nil
}

// LinearWorkItemDeriver runs the same production-derived functions as the
// GitHub deriver, with Linear's normalized rows and claim namespace. It is a
// provider-owned seam and does not register or activate a route.
type LinearWorkItemDeriver struct {
	Source       githubWorkItemDerivationContextSource
	engine       githubWorkItemEngineDeriver
	observations *workItemDerivationObservations
}

// StoredEdgeMergeObservation reports the stored-edge union this deriver has
// observed so far on its unit (CHAOS-3978).
func (deriver *LinearWorkItemDeriver) StoredEdgeMergeObservation() githubWorkItemStoredEdgeMergeObservation {
	if deriver == nil {
		return githubWorkItemStoredEdgeMergeObservation{}
	}
	return deriver.observations.storedEdgeMergeSnapshot()
}

type linearWorkItemEngineDeriver struct {
	engine *GitHubWorkItemEngineDeriver
}

func (adapter linearWorkItemEngineDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	if adapter.engine == nil {
		return nil, ErrInvalidConfiguration
	}
	return adapter.engine.deriveForProvider(
		ctx, "linear", claim, rows, day, computedAt, derived,
	)
}

func NewLinearWorkItemDeriver(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	statusMappingConfigPath string,
	investmentConfigPath string,
) (*LinearWorkItemDeriver, error) {
	if conn == nil || lease == nil || strings.TrimSpace(statusMappingConfigPath) == "" ||
		strings.TrimSpace(investmentConfigPath) == "" {
		return nil, ErrInvalidConfiguration
	}
	statusMapping, err := LoadStatusMapping(statusMappingConfigPath)
	if err != nil {
		return nil, err
	}
	investmentClassifier, err := NewInvestmentClassifier(investmentConfigPath)
	if err != nil {
		return nil, err
	}
	engine, err := NewGitHubWorkItemEngineDeriver(statusMapping, investmentClassifier)
	if err != nil {
		return nil, err
	}
	return &LinearWorkItemDeriver{
		Source:       githubWorkItemClickHouseDerivationContextSource{Conn: conn, Lease: lease},
		engine:       linearWorkItemEngineDeriver{engine: engine},
		observations: newWorkItemDerivationObservations(),
	}, nil
}

func (deriver LinearWorkItemDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows linearWorkItemRows,
	normalizedAt time.Time,
) (map[string][]json.RawMessage, error) {
	if ctx == nil || deriver.Source == nil || claim.Validate() != nil ||
		claim.Provider != "linear" || claim.Dataset != "work-items" ||
		normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	githubRows, err := linearWorkItemRowsAsGitHub(rows)
	if err != nil {
		return nil, err
	}
	days, err := githubWorkItemDerivedDays(claim, normalizedAt)
	if err != nil {
		return nil, err
	}
	derivationContext, err := loadWorkItemDerivationContextForProvider(
		ctx, "linear", claim, githubRows, deriver.Source, normalizedAt,
	)
	if err != nil {
		return nil, err
	}
	deriver.observations.recordStoredEdgeMerge(derivationContext.storedEdgeMerge)
	aiAttributions, err := normalizeLinearWorkItemAIAttributions(claim, rows, normalizedAt)
	if err != nil {
		return nil, err
	}
	aiRows, err := effectRowsFromValues(aiAttributions)
	if err != nil {
		return nil, err
	}
	derived := map[string][]json.RawMessage{"ai_attribution": aiRows}
	for _, destination := range githubWorkItemDerivedOwnedDestinations {
		derived[destination] = []json.RawMessage{}
	}
	for _, day := range days {
		triplet, err := buildWorkItemMetricTripletForProvider(
			"linear", claim, githubRows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		tripletRows, err := triplet.derivedRows()
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeDerivedRows(derived, tripletRows); err != nil {
			return nil, err
		}
		surfaces, err := buildWorkItemDerivedSurfacesForProvider(
			"linear", claim, githubRows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		surfaceRows, err := surfaces.derivedRows()
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeDerivedRows(derived, surfaceRows); err != nil {
			return nil, err
		}
		if deriver.engine == nil {
			continue
		}
		engineRows, err := deriver.engine.Derive(
			ctx, claim, githubRows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeEngineRows(derived, engineRows); err != nil {
			return nil, err
		}
	}
	missing := make([]string, 0)
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		if _, ok := derived[destination]; !ok {
			missing = append(missing, destination)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf(
			"%w: %s", ErrGitHubWorkItemsDerivationsUnavailable, strings.Join(missing, ", "),
		)
	}
	return derived, nil
}

var linearAIAttributionLabelKinds = map[string]string{
	"ai-assisted": "ai_assisted", "agent-created": "agent_created",
	"ai-review": "ai_review", "copilot": "ai_assisted",
	"claude-code": "ai_assisted", "codex": "ai_assisted",
	"cursor": "ai_assisted", "windsurf": "ai_assisted",
}

func normalizeLinearWorkItemAIAttributions(
	claim Claim,
	rows linearWorkItemRows,
	normalizedAt time.Time,
) ([]githubAIAttributionRow, error) {
	if claim.Validate() != nil || claim.Provider != "linear" ||
		claim.Dataset != "work-items" || normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	tenant, err := uuid.Parse(claim.OrgID)
	if err != nil {
		return nil, providerfoundation.ErrInvalidScope
	}
	result := make([]githubAIAttributionRow, 0)
	for _, item := range rows.WorkItems {
		if item.Provider != "linear" || item.OrgID != claim.OrgID ||
			item.WorkItemID == "" || item.CreatedAt.IsZero() || item.Labels == nil {
			return nil, providerfoundation.ErrInvalidScope
		}
		for index, label := range item.Labels {
			kind, matched := linearAIAttributionLabelKinds[strings.ToLower(strings.TrimSpace(label))]
			if !matched {
				continue
			}
			evidence := map[string]any{"label": label}
			encodedEvidence, marshalErr := json.Marshal(evidence)
			if marshalErr != nil {
				return nil, providerfoundation.ErrNormalizationInvalid
			}
			identity := strings.Join([]string{
				claim.OrgID, item.WorkItemID, "issue_label", fmt.Sprint(index), string(encodedEvidence),
			}, "|")
			result = append(result, githubAIAttributionRow{
				RecordID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)),
				OrgID:    tenant, Provider: "linear", SubjectType: "issue",
				SubjectID: item.WorkItemID, RepoID: nil, Kind: kind,
				Source: "issue_label", Confidence: 0.95, Evidence: evidence,
				ObservedAt: item.CreatedAt.UTC(), IngestedAt: normalizedAt.UTC(),
			})
		}
	}
	return result, nil
}

// linearDerivedGitHubAdapter is a narrow provider adapter around the existing
// ClickHouse implementations. The underlying adapters retain their exact SQL,
// coercion, readback, and lease behavior; this wrapper supplies the Linear
// identity and refuses a row that crosses the frozen tenant/provider fence.
type linearDerivedGitHubAdapter struct {
	destination string
	delegate    GitHubWorkItemEffectAdapter
}

func (adapter linearDerivedGitHubAdapter) validate(
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if identity.Provider != "linear" || identity.Dataset != "work-items" ||
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
		if len(effect.Rows) == 0 {
			return nil
		}
		tenant, parseErr := uuid.Parse(identity.OrgID)
		if parseErr != nil {
			return ErrInvalidConfiguration
		}
		for _, raw := range effect.Rows {
			var row githubAIAttributionRow
			if json.Unmarshal(raw, &row) != nil || row.RecordID == uuid.Nil ||
				row.OrgID != tenant || row.Provider != "linear" ||
				row.SubjectType != "issue" || row.SubjectID == "" || row.RepoID != nil ||
				row.Kind == "" || row.Source != "issue_label" || row.Confidence != 0.95 ||
				row.Evidence == nil || row.ObservedAt.IsZero() || row.IngestedAt.IsZero() {
				return ErrInvalidConfiguration
			}
		}
		return nil
	}
	for _, raw := range effect.Rows {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return ErrInvalidConfiguration
		}
		if adapter.destination != "investment_metrics_daily" {
			providerRaw, ok := fields["provider"]
			if !ok {
				return ErrInvalidConfiguration
			}
			var provider string
			if err := json.Unmarshal(providerRaw, &provider); err != nil || provider != "linear" {
				return ErrInvalidConfiguration
			}
		}
		orgRaw, ok := fields["org_id"]
		if !ok {
			return ErrInvalidConfiguration
		}
		var orgID string
		if err := json.Unmarshal(orgRaw, &orgID); err != nil || orgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func (adapter linearDerivedGitHubAdapter) WriteLinearWorkItemEffect(
	ctx context.Context,
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if err := adapter.validate(identity, effect); err != nil {
		return err
	}
	if adapter.destination == "ai_attribution" && len(effect.Rows) == 0 {
		return nil
	}
	if adapter.delegate == nil {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(
		ctx, linearRawEffectIdentity(identity), effect,
	)
}

func (adapter linearDerivedGitHubAdapter) InspectLinearWorkItemEffect(
	ctx context.Context,
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if err := adapter.validate(identity, effect); err != nil {
		return EffectConflict, err
	}
	if adapter.destination == "ai_attribution" && len(effect.Rows) == 0 {
		return EffectAbsent, nil
	}
	if adapter.delegate == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(
		ctx, linearRawEffectIdentity(identity), effect,
	)
}

var _ LinearWorkItemEffectAdapter = linearDerivedGitHubAdapter{}

// LinearWorkItemDerivedClickHouseEffects is the ten-destination derived
// dispatcher. It is intentionally separate from the six-fact dispatcher until
// the Linear activation layer is ready to own the complete sixteen-destination
// unit.
type LinearWorkItemDerivedClickHouseEffects struct {
	Lease                          providerfoundation.LeaseGuard
	AIAttribution                  LinearWorkItemEffectAdapter
	EstimateCoverageMetricsDaily   LinearWorkItemEffectAdapter
	InvestmentClassificationsDaily LinearWorkItemEffectAdapter
	InvestmentMetricsDaily         LinearWorkItemEffectAdapter
	IssueTypeMetricsDaily          LinearWorkItemEffectAdapter
	WorkItemCycleTimes             LinearWorkItemEffectAdapter
	WorkItemMetricsDaily           LinearWorkItemEffectAdapter
	WorkItemStateDurationsDaily    LinearWorkItemEffectAdapter
	WorkItemTeamAttributions       LinearWorkItemEffectAdapter
	WorkItemUserMetricsDaily       LinearWorkItemEffectAdapter
}

func NewLinearWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (LinearWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return LinearWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	wrap := func(destination string, delegate GitHubWorkItemEffectAdapter) LinearWorkItemEffectAdapter {
		return linearDerivedGitHubAdapter{destination: destination, delegate: delegate}
	}
	sink := LinearWorkItemDerivedClickHouseEffects{
		Lease:                          lease,
		AIAttribution:                  wrap("ai_attribution", GitHubAIAttributionClickHouseAdapter{Conn: conn}),
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
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf("%w: %s", ErrLinearWorkItemDerivedSinkIncomplete, strings.Join(missing, ", "))
	}
	return sink, nil
}

var ErrLinearWorkItemDerivedSinkIncomplete = fmt.Errorf(
	"linear derived work-item clickhouse sink is missing destination adapters",
)

func (sink LinearWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink LinearWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectLinearWorkItemEffect(ctx, identity, effect)
	if err != nil {
		return EffectConflict, err
	}
	switch inspection {
	case EffectExact, EffectAbsent, EffectConflict:
		return inspection, nil
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink LinearWorkItemDerivedClickHouseEffects) resolve(
	claim Claim, effect EffectBatch,
) (LinearWorkItemEffectIdentity, LinearWorkItemEffectAdapter, error) {
	identity, err := newLinearWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || !sink.complete() {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func newLinearWorkItemDerivedEffectIdentity(
	claim Claim, effect EffectBatch,
) (LinearWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "linear" || claim.Dataset != "work-items" ||
		!linearWorkItemDerivedDestination(effect.Destination) || effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := LinearWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

func linearWorkItemDerivedDestination(destination string) bool {
	for _, candidate := range linearWorkItemDerivedEffectDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

func (sink LinearWorkItemDerivedClickHouseEffects) adapterForDestination(
	destination string,
) (LinearWorkItemEffectAdapter, bool) {
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

func (sink LinearWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(linearWorkItemDerivedEffectDestinations))
	for _, destination := range linearWorkItemDerivedEffectDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink LinearWorkItemDerivedClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

var _ EffectSink = LinearWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = LinearWorkItemDerivedClickHouseEffects{}
