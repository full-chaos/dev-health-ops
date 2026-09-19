package providersync

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
)

// preparedRouteInsertStatements maps every destination a prepared-recovery
// route writes to the one INSERT its sink runs. A row a prepared route commits
// keeps only the keys that INSERT names (plus the destination's sink-read
// keys), so the effect ledger, the sink, the readback and the stored snapshot
// all see the same projected rows, and nothing the sink would not write is at
// rest in a snapshot.
var preparedRouteInsertStatements = map[string]string{
	"deployments":                      deploymentsInsert,
	"git_pull_requests":                pullRequestInsert,
	"git_pull_request_reviews":         pullRequestReviewInsert,
	"ai_attribution":                   gitHubAIAttributionInsert,
	"estimate_coverage_metrics_daily":  gitHubEstimateCoverageInsert,
	"investment_classifications_daily": gitHubInvestmentClassificationsInsert,
	"investment_metrics_daily":         gitHubInvestmentMetricsInsert,
	"issue_type_metrics_daily":         gitHubIssueTypeMetricsInsert,
	"sprints":                          gitHubSprintsInsert,
	"work_item_cycle_times":            gitHubWorkItemCycleTimesInsert,
	"work_item_dependencies":           gitHubWorkItemDependenciesInsert,
	"work_item_interactions":           gitHubWorkItemInteractionsInsert,
	"work_item_metrics_daily":          gitHubWorkItemMetricsDailyInsert,
	"work_item_reopen_events":          gitHubWorkItemReopenEventsInsert,
	"work_item_state_durations_daily":  gitHubWorkItemStateDurationsInsert,
	"work_item_team_attributions":      gitHubWorkItemTeamAttributionsInsert,
	"work_item_transitions":            gitHubWorkItemTransitionsInsert,
	"work_item_user_metrics_daily":     gitHubWorkItemUserMetricsDailyInsert,
	"project_membership_transitions":   projectmembership.TransitionsInsert,
	"projects":                         projectmembership.ProjectsInsert,
	"work_items":                       gitHubWorkItemsInsert,
}

// preparedRouteSinkReadKeys are row keys a sink reads but does not INSERT:
// route-computed signals (a failed-lookup flag that arms a carry-forward
// guard, a resolution layer or ownership outcome counted as a metric). Each
// is a bool, an int, or a string whose value is in a closed set named here;
// a row whose signal value is outside its kind or set is refused.
var preparedRouteSinkReadKeys = map[string]map[string]sinkReadSignal{
	"deployments": {
		"lifecycle_lookup_failed":    {kind: sinkReadBool},
		"pull_request_lookup_failed": {kind: sinkReadBool},
	},
	"git_pull_requests": {"reviews_lookup_failed": {kind: sinkReadBool}},
	"work_item_team_attributions": {
		"priority":         {kind: sinkReadInt},
		"ownership_reason": {kind: sinkReadEnum, values: teamAttributionOwnershipReasons},
	},
}

// teamAttributionOwnershipReasons is every value a team-attribution row's
// ownership_reason takes: empty for a row that passed no membership gate,
// teamattribution.GithubWorkItemDerivationOwnershipCheckedLabel's labels otherwise.
var teamAttributionOwnershipReasons = map[string]bool{
	"":      true,
	"owned": true,
	teamattribution.MembershipOwnershipReasonNotOwned:         true,
	teamattribution.MembershipOwnershipReasonUnknown:          true,
	teamattribution.MembershipOwnershipReasonTeamNullCarrying: true,
}

type sinkReadKind int

const (
	sinkReadBool sinkReadKind = iota + 1
	sinkReadInt
	sinkReadEnum
)

type sinkReadSignal struct {
	kind   sinkReadKind
	values map[string]bool
}

// admits reports whether a raw JSON value is of the signal's kind and, for an
// enum, in its closed set. JSON null is of no kind: it would decode as the
// zero value and disarm the guard or metric the signal drives.
func (signal sinkReadSignal) admits(raw json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	switch signal.kind {
	case sinkReadBool:
		var value bool
		return json.Unmarshal(raw, &value) == nil
	case sinkReadInt:
		var value int64
		return json.Unmarshal(raw, &value) == nil
	case sinkReadEnum:
		var value string
		return json.Unmarshal(raw, &value) == nil && signal.values[value]
	default:
		return false
	}
}

// insertStatementColumns returns the column list of an
// `INSERT INTO table (a, b, ...)` statement.
func insertStatementColumns(statement string) []string {
	open := strings.Index(statement, "(")
	closing := strings.LastIndex(statement, ")")
	if open < 0 || closing <= open {
		return nil
	}
	var columns []string
	for _, column := range strings.Split(statement[open+1:closing], ",") {
		if column = strings.TrimSpace(column); column != "" {
			columns = append(columns, column)
		}
	}
	return columns
}

// preparedRouteColumns returns the columns a destination's sink INSERTs.
func preparedRouteColumns(destination string) (map[string]bool, bool) {
	statement, ok := preparedRouteInsertStatements[destination]
	if !ok || len(insertStatementColumns(statement)) == 0 {
		return nil, false
	}
	return projectionKeys(statement), true
}

// projectionKeys returns the column names of an INSERT statement.
func projectionKeys(statement string) map[string]bool {
	columns := insertStatementColumns(statement)
	keys := make(map[string]bool, len(columns))
	for _, column := range columns {
		keys[column] = true
	}
	return keys
}

// projectedRow keeps a row's INSERT columns and its admitted sink-read
// signals. It refuses a signal value outside its kind or closed set.
func projectedRow(destination string, columns map[string]bool, row json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(row, &object) != nil || object == nil {
		return nil, ErrEffectRecoveryUnsafe
	}
	signals := preparedRouteSinkReadKeys[destination]
	projected := make(map[string]json.RawMessage, len(object))
	if len(object) == 0 {
		return projected, nil
	}
	for key, value := range object {
		if columns[key] {
			projected[key] = value
			continue
		}
		signal, ok := signals[key]
		if !ok {
			continue
		}
		if !signal.admits(value) {
			slog.Warn(
				"provider_sync.prepared_projection_signal_refused",
				"destination", destination, "key", key,
			)
			return nil, ErrEffectRecoveryUnsafe
		}
		projected[key] = value
	}
	return projected, nil
}

// projectPreparedRouteEffects rebuilds each effect from its rows' projection
// onto the keys its destination's sink writes or reads. A destination with no
// known INSERT refuses: a prepared route never commits a row it cannot project.
func projectPreparedRouteEffects(effects []EffectBatch) ([]EffectBatch, error) {
	projected := make([]EffectBatch, 0, len(effects))
	for _, effect := range effects {
		columns, ok := preparedRouteColumns(effect.Destination)
		if !ok {
			return nil, ErrEffectRecoveryUnsafe
		}
		rows := make([]json.RawMessage, 0, len(effect.Rows))
		for _, row := range effect.Rows {
			object, err := projectedRow(effect.Destination, columns, row)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(object)
			if err != nil {
				return nil, ErrEffectRecoveryUnsafe
			}
			rows = append(rows, encoded)
		}
		rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, rows)
		if err != nil {
			return nil, err
		}
		rebuilt.MembershipRejections = effect.MembershipRejections
		projected = append(projected, rebuilt)
	}
	return projected, nil
}

// preparedRouteRowsAreProjected reports whether every row of an effect whose
// destination has a known INSERT is its own projection: INSERT columns and
// admitted sink-read signals only. A destination with no known INSERT never
// reaches a snapshot from a commit (projectPreparedRouteEffects refuses it);
// the manifest check owns it.
func preparedRouteRowsAreProjected(effects []EffectBatch) bool {
	for _, effect := range effects {
		columns, ok := preparedRouteColumns(effect.Destination)
		if !ok {
			continue
		}
		for _, row := range effect.Rows {
			var object map[string]json.RawMessage
			if json.Unmarshal(row, &object) != nil {
				return false
			}
			projected, err := projectedRow(effect.Destination, columns, row)
			if err != nil || len(projected) != len(object) {
				return false
			}
		}
	}
	return true
}

// preparedRouteEffectsForCommit returns the effects a prepared route commits.
// They are the projected rows, except when a ledger written without a
// snapshot records the unprojected rows' digests: that unit finishes on the
// rows its ledger was written with. When no key is dropped the two forms are
// the same rows.
func preparedRouteEffectsForCommit(
	claim Claim,
	effects []EffectBatch,
	recovered *EffectLedgerState,
) ([]EffectBatch, error) {
	projected, err := projectPreparedRouteEffects(effects)
	if err != nil {
		return nil, err
	}
	if recovered == nil || !effectDigestsMatchLedger(effects, *recovered) {
		return projected, nil
	}
	slog.Warn(
		"provider_sync.prepared_projection_ledger_unprojected",
		"provider", claim.Provider, "dataset", claim.Dataset,
		"unit", claim.ID, "generation", claim.GenerationKey(),
		"persisted_effects", len(recovered.Effects),
	)
	return effects, nil
}

func effectDigestsMatchLedger(effects []EffectBatch, ledger EffectLedgerState) bool {
	if len(effects) != len(ledger.Effects) {
		return false
	}
	digests := make(map[string]string, len(effects))
	for _, effect := range effects {
		digests[effect.Destination] = effect.ContentDigest
	}
	for _, entry := range ledger.Effects {
		if digest, ok := digests[entry.Destination]; !ok || digest != entry.ContentDigest {
			return false
		}
	}
	return true
}
