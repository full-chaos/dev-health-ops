// CHAOS-5523: the Go port of
// dev_health_ops.api.graphql.resolvers.feature_flags.resolve_feature_flag_events
// (ops/src/dev_health_ops/api/graphql/resolvers/feature_flags.py), the
// featureFlags sibling deferred out of the Wave 1 canary
// (cmd/query-api/README.md's former "not here yet" bullet, removed by
// this change). Lives in this package -- not a new one -- because it
// shares featureFlags's org-scoping convention, its FEATURE_FLAG_LIMIT_MAX
// clamp (clampLimit, unchanged), and its missing-ClickHouse-table
// degraded-not-error convention (isMissingTable, unchanged) verbatim.
//
// Ported deliberately verbatim: same WHERE-clause construction (org_id
// always, flag_key/environment only when the caller supplies them), same
// ORDER BY event_ts ASC, same LIMIT clamp, and the same missing-table
// (ClickHouse code 60 / UNKNOWN_TABLE naming "feature_flag_event")
// degraded-result path instead of raising. One deliberate divergence from
// featureFlags's own Resolve, faithfully mirroring Python's actual
// resolve_feature_flag_events rather than "fixing" it: the count query
// reuses the SAME where_clauses as the row query but NOT the limit -- this
// is a TRUE total count, not a "did we truncate" heuristic, and there is
// no 5000/N-row cap on this operation.
//
// feature_flag_event's columns (org_id, event_type, flag_key,
// environment, repo_id, actor_type, prev_state, next_state, event_ts,
// ingested_at, source_event_id, dedupe_key) are all plain non-nullable
// String/DateTime64 in the live schema (confirmed via `DESCRIBE TABLE
// feature_flag_event` against the local compose ClickHouse before writing
// this file) -- so the six string fields scan directly into Go string,
// the same convention Resolve's FeatureFlagItem scan already uses for
// provider/flagKey/projectKey/flagType, not a defensive *string dance
// Python's `row.get(x) or ""` guard would otherwise suggest.
package featureflags

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// EventNotMaterializedReason mirrors Python's
// FEATURE_FLAG_EVENT_NOT_MATERIALIZED -- the degraded-result reason
// surfaced when the feature_flag_event ClickHouse table does not exist
// yet for this environment, instead of the request failing.
const EventNotMaterializedReason = "FEATURE_FLAG_EVENT_NOT_MATERIALIZED"

func degradedEventsResult(reason string) *model.FeatureFlagEventsResult {
	r := reason
	return &model.FeatureFlagEventsResult{
		Events:         []model.FeatureFlagEventItem{},
		TotalCount:     0,
		DegradedReason: &r,
	}
}

// eventWhere is the shared WHERE-clause + bindings builder for both the
// row query and the count query -- mirrors resolve_feature_flag_events
// building where_clauses/params once and reusing them for both
// statements, the same discipline flagWhere already follows for
// featureFlags.
func eventWhere(orgID string, flagKey, environment *string) (string, []clickhouse.Binding) {
	clause := "org_id = {org_id:String}"
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if flagKey != nil {
		clause += " AND flag_key = {flag_key:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "flag_key", Value: *flagKey})
	}
	if environment != nil {
		clause += " AND environment = {environment:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "environment", Value: *environment})
	}
	return clause, bindings
}

// ResolveEvents ports resolve_feature_flag_events. limit is clamped
// internally via clampLimit (the same FEATURE_FLAG_LIMIT_MAX=1000 bound
// featureFlags uses) -- callers must not pre-clamp. The count query
// deliberately does NOT receive the clamped limit binding: it counts
// every row matching the WHERE clause, not just the ones the row query
// returns.
func ResolveEvents(ctx context.Context, client QueryClient, orgID string, flagKey, environment *string, limit int) (*model.FeatureFlagEventsResult, error) {
	if client == nil {
		return nil, errors.New("featureflags: clickhouse client is required")
	}

	whereClause, bindings := eventWhere(orgID, flagKey, environment)

	rowBindings := append(append([]clickhouse.Binding{}, bindings...), clickhouse.Binding{Name: "limit", Value: clampLimit(limit)})

	rowQuery := `SELECT
    flag_key,
    event_type,
    prev_state,
    next_state,
    actor_type,
    environment,
    event_ts
FROM feature_flag_event
WHERE ` + whereClause + `
ORDER BY event_ts ASC
LIMIT {limit:UInt64}`

	countQuery := `SELECT count() AS total
FROM feature_flag_event
WHERE ` + whereClause

	rows, err := client.Query(ctx, rowQuery, rowBindings)
	if err != nil {
		if isMissingFeatureFlagEventTable(err) {
			return degradedEventsResult(EventNotMaterializedReason), nil
		}
		return nil, fmt.Errorf("featureflags: events query: %w", err)
	}
	defer rows.Close()

	// Non-nil even with zero rows: the schema declares
	// events: [FeatureFlagEventItem!]! (non-null list) -- same
	// initialize-explicitly discipline Resolve's flags slice follows.
	events := []model.FeatureFlagEventItem{}
	for rows.Next() {
		var eventFlagKey, eventType, prevState, nextState, actorType, eventEnvironment string
		var eventTs time.Time
		if scanErr := rows.Scan(&eventFlagKey, &eventType, &prevState, &nextState, &actorType, &eventEnvironment, &eventTs); scanErr != nil {
			return nil, fmt.Errorf("featureflags: events scan: %w", scanErr)
		}
		events = append(events, model.FeatureFlagEventItem{
			FlagKey:     eventFlagKey,
			EventType:   eventType,
			PrevState:   prevState,
			NextState:   nextState,
			ActorType:   actorType,
			Environment: eventEnvironment,
			EventTs:     isoformatUTC(eventTs),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("featureflags: events rows: %w", err)
	}

	// The count query reuses `bindings` (org_id [+flag_key][+environment]
	// only) -- deliberately NOT `rowBindings`, which carries the clamped
	// limit. See this file's package doc comment: this is a true total
	// count, not a truncation heuristic.
	countRows, err := client.Query(ctx, countQuery, bindings)
	if err != nil {
		if isMissingFeatureFlagEventTable(err) {
			return degradedEventsResult(EventNotMaterializedReason), nil
		}
		return nil, fmt.Errorf("featureflags: events count query: %w", err)
	}
	defer countRows.Close()

	// count() returns UInt64; scan into uint64 first, same
	// driver-type-mismatch avoidance Resolve's own count-scan comment
	// documents for TotalCount.
	var total uint64
	if countRows.Next() {
		if scanErr := countRows.Scan(&total); scanErr != nil {
			return nil, fmt.Errorf("featureflags: events count scan: %w", scanErr)
		}
	}
	if err := countRows.Err(); err != nil {
		return nil, fmt.Errorf("featureflags: events count rows: %w", err)
	}

	return &model.FeatureFlagEventsResult{
		Events:     events,
		TotalCount: int(total),
	}, nil
}
