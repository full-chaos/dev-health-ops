package providersync

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
)

// Exported aliases keep the provider's normalized rows concrete at the
// lifecycle boundary without introducing a second row definition. The
// underlying types are the same structs used by the ClickHouse adapters.
type LinearWorkItemRow = linearWorkItemRow
type LinearWorkItemTransitionRow = linearWorkItemTransitionRow
type LinearWorkItemDependencyRow = linearWorkItemDependencyRow
type LinearWorkItemReopenRow = linearWorkItemReopenRow
type LinearWorkItemInteractionRow = linearWorkItemInteractionRow
type LinearSprintRow = linearSprintRow

// LinearWorkItemsRows is the complete normalized producer output. Each field
// maps to one of the six migrated ClickHouse destination projections.
type LinearWorkItemsRows struct {
	WorkItems         []LinearWorkItemRow
	StatusTransitions []LinearWorkItemTransitionRow
	Dependencies      []LinearWorkItemDependencyRow
	ReopenEvents      []LinearWorkItemReopenRow
	Interactions      []LinearWorkItemInteractionRow
	Sprints           []LinearSprintRow
}

type LinearWorkItemCounts struct {
	WorkItems         int `json:"work_items"`
	StatusTransitions int `json:"status_transitions"`
	Dependencies      int `json:"dependencies"`
	ReopenEvents      int `json:"reopen_events"`
	Interactions      int `json:"interactions"`
	Sprints           int `json:"sprints"`
}

func (rows LinearWorkItemsRows) Counts() LinearWorkItemCounts {
	return LinearWorkItemCounts{
		WorkItems: len(rows.WorkItems), StatusTransitions: len(rows.StatusTransitions),
		Dependencies: len(rows.Dependencies), ReopenEvents: len(rows.ReopenEvents),
		Interactions: len(rows.Interactions), Sprints: len(rows.Sprints),
	}
}

func (rows LinearWorkItemsRows) NonEmpty() bool {
	counts := rows.Counts()
	return counts.WorkItems > 0 || counts.StatusTransitions > 0 ||
		counts.Dependencies > 0 || counts.ReopenEvents > 0 ||
		counts.Interactions > 0 || counts.Sprints > 0
}

type LinearWorkItemsFailureStage string

const (
	LinearWorkItemsFailureNone      LinearWorkItemsFailureStage = ""
	LinearWorkItemsFailureFetch     LinearWorkItemsFailureStage = "fetch"
	LinearWorkItemsFailureNormalize LinearWorkItemsFailureStage = "normalize"
	LinearWorkItemsFailureEffects   LinearWorkItemsFailureStage = "effects"
	LinearWorkItemsFailureReadback  LinearWorkItemsFailureStage = "readback"
	LinearWorkItemsFailureComplete  LinearWorkItemsFailureStage = "complete"
)

type LinearWorkItemsFailureState struct {
	Stage     LinearWorkItemsFailureStage `json:"stage"`
	Code      string                      `json:"code"`
	Retryable bool                        `json:"retryable"`
	Cause     string                      `json:"cause"`
}

// LinearWorkItemsRouteResult is the typed counterpart to the legacy generic
// CompleteRouteBatch.Result map. New lifecycle callers consume this value;
// the map remains populated only for compatibility with the pre-existing
// CompleteRouteHandler interface.
type LinearWorkItemsRouteResult struct {
	Rows     LinearWorkItemsRows
	Counts   LinearWorkItemCounts
	NonEmpty bool
	Evidence FetchEvidence
	Failure  LinearWorkItemsFailureState
}

type LinearWorkItemsRouteBatch struct {
	Effects   []EffectBatch
	Watermark *time.Time
	Evidence  FetchEvidence
	Result    LinearWorkItemsRouteResult
}

func linearWorkItemsFailure(stage LinearWorkItemsFailureStage, err error) LinearWorkItemsFailureState {
	if err == nil {
		return LinearWorkItemsFailureState{}
	}
	failure := LinearWorkItemsFailureState{
		Stage: stage, Code: "provider_error", Cause: err.Error(),
	}
	var providerErr *providerfoundation.ProviderError
	if errors.As(err, &providerErr) {
		failure.Code = string(providerErr.Class)
		failure.Retryable = providerErr.Retryable()
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		failure.Code, failure.Retryable = "canceled", true
	case errors.Is(err, providerfoundation.ErrLeaseLost), errors.Is(err, ErrLeaseLost):
		failure.Code, failure.Retryable = "lease_lost", true
	case errors.Is(err, ErrPaginationCapExceeded):
		failure.Code = "pagination_cap"
	case errors.Is(err, providerfoundation.ErrPaginationInvalid):
		failure.Code = "pagination_invalid"
	case errors.Is(err, providerfoundation.ErrGraphQLComplexity):
		failure.Code = "graphql_complexity"
	case errors.Is(err, providerfoundation.ErrGraphQLResponse):
		failure.Code = "graphql_response"
	case errors.Is(err, providerfoundation.ErrNormalizationInvalid):
		failure.Stage, failure.Code = LinearWorkItemsFailureNormalize, "normalization_invalid"
	case errors.Is(err, ErrEffectRecoveryAmbiguous), errors.Is(err, ErrEffectLedgerConflict):
		failure.Code = "effect_recovery_ambiguous"
	}
	return failure
}

func decodeLinearWorkItemsRouteBatch(
	batch CompleteRouteBatch,
) (LinearWorkItemsRows, error) {
	var rows LinearWorkItemsRows
	seen := make(map[string]struct{}, len(batch.Effects))
	for _, effect := range batch.Effects {
		if _, duplicate := seen[effect.Destination]; duplicate {
			return LinearWorkItemsRows{}, ErrInvalidConfiguration
		}
		seen[effect.Destination] = struct{}{}
		switch effect.Destination {
		case "work_items":
			decoded, err := decodeEffectRows[LinearWorkItemRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.WorkItems = decoded
		case "work_item_transitions":
			decoded, err := decodeEffectRows[LinearWorkItemTransitionRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.StatusTransitions = decoded
		case "work_item_dependencies":
			decoded, err := decodeEffectRows[LinearWorkItemDependencyRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.Dependencies = decoded
		case "work_item_reopen_events":
			decoded, err := decodeEffectRows[LinearWorkItemReopenRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.ReopenEvents = decoded
		case "work_item_interactions":
			decoded, err := decodeEffectRows[LinearWorkItemInteractionRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.Interactions = decoded
		case "sprints":
			decoded, err := decodeEffectRows[LinearSprintRow](effect)
			if err != nil {
				return LinearWorkItemsRows{}, err
			}
			rows.Sprints = decoded
		case "project_membership_transitions", "projects":
			// CHAOS-4193: not part of the six-destination Python-mirrored
			// family this typed lifecycle/alias-readback boundary exists
			// for -- the five legacy aliases (work-items/-labels/-projects/
			// -history/-comments) predate project membership and none of
			// them own it. Accepted (not the exhaustiveness default) so this
			// decoder does not reject a route batch for carrying a
			// destination outside its own scope; deliberately not decoded
			// into LinearWorkItemsRows, which stays scoped to the six.
		default:
			return LinearWorkItemsRows{}, ErrInvalidConfiguration
		}
	}
	if len(seen) != len(linearWorkItemEffectDestinations) {
		return LinearWorkItemsRows{}, ErrInvalidConfiguration
	}
	return rows, nil
}

// CollectTyped executes the same real provider route as Collect and exposes
// only concrete normalized rows/evidence/failure state to lifecycle callers.
func (handler LinearWorkItemsRouteHandler) CollectTyped(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (LinearWorkItemsRouteBatch, error) {
	batch, err := handler.Collect(ctx, claim, credential, client, normalizedAt)
	if err != nil {
		return LinearWorkItemsRouteBatch{
			Result: LinearWorkItemsRouteResult{
				Failure: linearWorkItemsFailure(LinearWorkItemsFailureFetch, err),
			},
		}, err
	}
	rows, err := decodeLinearWorkItemsRouteBatch(batch)
	if err != nil {
		return LinearWorkItemsRouteBatch{
			Effects: batch.Effects, Watermark: batch.Watermark, Evidence: batch.Evidence,
			Result: LinearWorkItemsRouteResult{
				Failure: linearWorkItemsFailure(LinearWorkItemsFailureNormalize, err),
			},
		}, err
	}
	return LinearWorkItemsRouteBatch{
		Effects: batch.Effects, Watermark: batch.Watermark, Evidence: batch.Evidence,
		Result: LinearWorkItemsRouteResult{
			Rows: rows, Counts: rows.Counts(), NonEmpty: rows.NonEmpty(),
			Evidence: batch.Evidence,
		},
	}, nil
}

type LinearWorkItemAliasReadbackState string

const (
	LinearAliasReadbackExact    LinearWorkItemAliasReadbackState = "exact"
	LinearAliasReadbackEmpty    LinearWorkItemAliasReadbackState = "empty"
	LinearAliasReadbackConflict LinearWorkItemAliasReadbackState = "conflict"
)

type LinearWorkItemEffectAudit struct {
	Destination   string                           `json:"destination"`
	Rows          int                              `json:"rows"`
	ContentDigest string                           `json:"content_digest"`
	Readback      LinearWorkItemAliasReadbackState `json:"readback"`
}

type LinearWorkItemAliasAudit struct {
	Dataset            string                           `json:"dataset"`
	Enabled            bool                             `json:"enabled"`
	Rows               int                              `json:"rows"`
	EffectDestinations []string                         `json:"effect_destinations"`
	Readback           LinearWorkItemAliasReadbackState `json:"readback"`
	Complete           bool                             `json:"complete"`
}

type LinearWorkItemsCompletionResult struct {
	Provider   string                      `json:"provider"`
	Dataset    string                      `json:"dataset"`
	Generation string                      `json:"generation"`
	Status     string                      `json:"status"`
	NonEmpty   bool                        `json:"non_empty"`
	Counts     LinearWorkItemCounts        `json:"counts"`
	Effects    []LinearWorkItemEffectAudit `json:"effects"`
	Aliases    [5]LinearWorkItemAliasAudit `json:"aliases"`
	Evidence   FetchEvidence               `json:"evidence"`
	Watermark  time.Time                   `json:"watermark"`
	Failure    LinearWorkItemsFailureState `json:"failure"`
}

func linearAliasDestinations(dataset string) []string {
	switch dataset {
	case "work-items":
		return []string{"work_items"}
	case "work-item-labels":
		return []string{"work_items"}
	case "work-item-projects":
		return []string{"work_items", "sprints"}
	case "work-item-history":
		return []string{"work_item_transitions", "work_item_reopen_events"}
	case "work-item-comments":
		return []string{"work_item_interactions"}
	default:
		return nil
	}
}

func linearAliasRows(dataset string, counts LinearWorkItemCounts) int {
	switch dataset {
	case "work-items", "work-item-labels":
		return counts.WorkItems
	case "work-item-projects":
		return counts.WorkItems + counts.Sprints
	case "work-item-history":
		return counts.StatusTransitions + counts.ReopenEvents
	case "work-item-comments":
		return counts.Interactions
	default:
		return 0
	}
}

func linearEffectReadbackState(inspection EffectInspection, rows int) LinearWorkItemAliasReadbackState {
	if inspection == EffectExact {
		return LinearAliasReadbackExact
	}
	if inspection == EffectAbsent && rows == 0 {
		return LinearAliasReadbackEmpty
	}
	return LinearAliasReadbackConflict
}

func buildLinearWorkItemsCompletionResult(
	claim Claim,
	route LinearWorkItemsRouteBatch,
	audits []LinearWorkItemEffectAudit,
	watermark time.Time,
) (LinearWorkItemsCompletionResult, error) {
	if claim.Validate() != nil || claim.Provider != "linear" || claim.Dataset != "work-items" ||
		route.Result.Failure.Stage != LinearWorkItemsFailureNone || !route.Result.NonEmpty || watermark.IsZero() {
		return LinearWorkItemsCompletionResult{}, ErrInvalidConfiguration
	}
	result := LinearWorkItemsCompletionResult{
		Provider: claim.Provider, Dataset: claim.Dataset, Generation: claim.GenerationKey(),
		Status: "success", NonEmpty: route.Result.NonEmpty, Counts: route.Result.Counts,
		Effects: audits, Evidence: route.Result.Evidence, Watermark: watermark.UTC(),
	}
	datasets := workitemcontract.FamilyDatasets()
	if len(datasets) != len(result.Aliases) {
		return LinearWorkItemsCompletionResult{}, ErrInvalidConfiguration
	}
	for index, dataset := range datasets {
		destinations := linearAliasDestinations(dataset)
		readback := LinearAliasReadbackExact
		for _, destination := range destinations {
			found := false
			for _, audit := range audits {
				if audit.Destination == destination {
					found = true
					if audit.Readback != LinearAliasReadbackExact && audit.Readback != LinearAliasReadbackEmpty {
						readback = LinearAliasReadbackConflict
					}
					break
				}
			}
			if !found {
				return LinearWorkItemsCompletionResult{}, ErrInvalidConfiguration
			}
		}
		result.Aliases[index] = LinearWorkItemAliasAudit{
			Dataset: dataset, Enabled: true, Rows: linearAliasRows(dataset, route.Result.Counts),
			EffectDestinations: destinations, Readback: readback,
			Complete: readback == LinearAliasReadbackExact || readback == LinearAliasReadbackEmpty,
		}
	}
	if err := ValidateLinearWorkItemsCompletion(claim, result); err != nil {
		return LinearWorkItemsCompletionResult{}, err
	}
	return result, nil
}

func ValidateLinearWorkItemsCompletion(
	claim Claim,
	result LinearWorkItemsCompletionResult,
) error {
	if claim.Validate() != nil || claim.Provider != "linear" || claim.Dataset != "work-items" ||
		result.Provider != claim.Provider || result.Dataset != claim.Dataset ||
		result.Generation != claim.GenerationKey() || result.Status != "success" ||
		!result.NonEmpty || result.Watermark.IsZero() || result.Failure.Stage != LinearWorkItemsFailureNone {
		return ErrInvalidConfiguration
	}
	datasets := workitemcontract.FamilyDatasets()
	if len(datasets) != len(result.Aliases) {
		return ErrInvalidConfiguration
	}
	knownFlags := make(map[string]struct{}, len(datasets))
	for _, dataset := range datasets {
		knownFlags[workItemFamilyFlagForDataset(dataset)] = struct{}{}
	}
	for flag := range claim.ProcessorFlags {
		if strings.HasPrefix(flag, workItemFamilyFlagPrefix) {
			if _, known := knownFlags[flag]; !known {
				return ErrInvalidConfiguration
			}
		}
	}
	for index, dataset := range datasets {
		alias := result.Aliases[index]
		expectedDestinations := linearAliasDestinations(dataset)
		if alias.Dataset != dataset || !alias.Enabled || !alias.Complete ||
			(alias.Readback != LinearAliasReadbackExact && alias.Readback != LinearAliasReadbackEmpty) ||
			!slices.Equal(alias.EffectDestinations, expectedDestinations) {
			return ErrInvalidConfiguration
		}
		if !strings.EqualFold(workItemFamilyFlagForDataset(dataset), workItemFamilyFlagForDataset(alias.Dataset)) ||
			!claim.ProcessorFlags[workItemFamilyFlagForDataset(dataset)] {
			return ErrInvalidConfiguration
		}
	}
	if len(result.Effects) != len(linearWorkItemEffectDestinations) {
		return ErrInvalidConfiguration
	}
	seenDestinations := make(map[string]struct{}, len(result.Effects))
	for _, audit := range result.Effects {
		if !linearWorkItemDestination(audit.Destination) || audit.ContentDigest == "" ||
			(audit.Readback != LinearAliasReadbackExact && audit.Readback != LinearAliasReadbackEmpty) {
			return ErrInvalidConfiguration
		}
		if _, duplicate := seenDestinations[audit.Destination]; duplicate {
			return ErrInvalidConfiguration
		}
		seenDestinations[audit.Destination] = struct{}{}
	}
	if len(seenDestinations) != len(linearWorkItemEffectDestinations) {
		return ErrInvalidConfiguration
	}
	return nil
}

// LinearWorkItemsLifecycle commits the typed route manifest through the
// existing lease-fenced effect ledger/sink and performs exact readback for
// every migrated destination before producing a completion result.
type LinearWorkItemsLifecycle struct {
	Committer EffectCommitter
}

func (lifecycle LinearWorkItemsLifecycle) Commit(
	ctx context.Context,
	claim Claim,
	route LinearWorkItemsRouteBatch,
	normalizedAt time.Time,
) (LinearWorkItemsCompletionResult, EffectCommitResult, error) {
	if ctx == nil || lifecycle.Committer.Readback == nil || !route.Result.NonEmpty ||
		route.Result.Failure.Stage != LinearWorkItemsFailureNone {
		return LinearWorkItemsCompletionResult{}, EffectCommitResult{}, ErrInvalidConfiguration
	}
	commitResult, err := lifecycle.Committer.Commit(ctx, claim, route.Effects, normalizedAt)
	if err != nil {
		return LinearWorkItemsCompletionResult{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Generation: claim.GenerationKey(), Status: "failed",
			Failure: linearWorkItemsFailure(LinearWorkItemsFailureEffects, err),
		}, commitResult, err
	}
	audits := make([]LinearWorkItemEffectAudit, 0, len(route.Effects))
	for _, effect := range route.Effects {
		inspection, inspectErr := lifecycle.Committer.Readback.InspectEffect(ctx, claim, effect)
		if inspectErr != nil {
			return LinearWorkItemsCompletionResult{
				Provider: claim.Provider, Dataset: claim.Dataset,
				Generation: claim.GenerationKey(), Status: "failed",
				Failure: linearWorkItemsFailure(LinearWorkItemsFailureReadback, inspectErr),
			}, commitResult, inspectErr
		}
		audits = append(audits, LinearWorkItemEffectAudit{
			Destination: effect.Destination, Rows: len(effect.Rows),
			ContentDigest: effect.ContentDigest,
			Readback:      linearEffectReadbackState(inspection, len(effect.Rows)),
		})
	}
	watermark := route.Watermark
	if watermark == nil {
		return LinearWorkItemsCompletionResult{}, commitResult, ErrInvalidConfiguration
	}
	result, err := buildLinearWorkItemsCompletionResult(claim, route, audits, *watermark)
	if err != nil {
		return LinearWorkItemsCompletionResult{}, commitResult, err
	}
	return result, commitResult, nil
}
