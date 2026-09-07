// Package capacityforecast is the Go port of
// dev_health_ops.api.graphql.resolvers.capacity -- both of its resolvers
// (CHAOS-5349, the query-api half of the capacity/forecast cutover).
//
// The two operations share a table name and almost nothing else:
//
//   - capacityForecasts is a READ. It selects rows the worker already wrote to
//     `capacity_forecasts` and wraps them in a Relay-shaped connection. No
//     computation happens.
//
//   - capacityForecast is a COMPUTE. It loads throughput history and a backlog
//     from work_item_metrics_daily and runs the Monte Carlo forecast at query
//     time, writing nothing.
//
// # The kernel is not reimplemented here
//
// The Monte Carlo itself is internal/jobs/metrics/numerical.ForecastCapacity,
// which the native worker executor (CUT-20 R2) already ports and which is
// pinned against live Python by tests/fixtures/capacity_forecast_golden.json
// and cpython_random_golden.json. Reusing it is the whole point: two ports of
// one stochastic function could not be compared to each other OR to Python, and
// the second would drift silently the moment either side changed.
//
// # The seed: a declared divergence
//
// Python's resolver never passes `seed`, so `random.seed()` is never called and
// the resolver runs on CPython's OS-entropy-seeded Mersenne Twister -- a
// different stream on every request, and therefore a different forecast for the
// same inputs. numerical.ForecastCapacity, built for the WORKER path, requires
// a seed because the worker's rows must be reproducible.
//
// This port draws a fresh seed from crypto/rand per request. That preserves the
// algorithm and the distribution exactly (the draws still come from cpyrandom's
// CPython-compatible generator, not Go's own) while reproducing Python's
// per-request non-determinism. It is NOT bit-comparable with a Python response
// -- neither is a second Python response -- which is why the golden fixtures
// pin seeded behaviour and the resolver tests assert distributional and
// structural properties instead.
//
// # Behaviours reproduced deliberately
//
//  1. THE ORG ARGUMENT IS PARSED AND NEVER TRUSTED. Both Python resolvers take
//     an `org_id` GraphQL argument, ignore it, and scope off
//     require_org_id(context). The caller passes the authorized org here.
//
//  2. THE ITEM TARGET FALLS BACK ON A FALSY CHECK. `items = target_items if
//     target_items else backlog`, so a target of 0 falls back to the backlog
//     rather than forecasting zero items. A nil-check port disagrees on exactly
//     that one input.
//
//  3. TWO EMPTY ANSWERS ARE null, NOT AN ERROR. No throughput history, or a
//     non-positive item target, each log and return null. Turning either into
//     an error would convert a tolerated empty into a failed query.
//
//  4. capacityForecasts READS WITHOUT FINAL, over a ReplacingMergeTree. That is
//     Python's query and it is reproduced rather than improved on -- note the
//     table's ORDER BY is (forecast_id) and every row carries a fresh uuid4, so
//     nothing ever collapses and FINAL would change no result while changing
//     the cost. The SIBLING path in this same package DOES use FINAL, because
//     work_item_metrics_daily genuinely replaces rows.
//
//  5. THE CONNECTION IS RELAY-SHAPED BUT NOT PAGINATED. The cursor is the
//     forecast_id verbatim -- not base64, not an offset -- totalCount is the
//     PAGE length rather than a table count, and hasPreviousPage is hardcoded
//     false. There is no `after`/`before` argument, so no cursor is ever
//     consumed. Encoding the cursor or counting the table would each be a wire
//     change dressed as a fix.
package capacityforecast

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/vektah/gqlparser/v2/gqlerror"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/numerical"
)

// QueryClient is the read-only ClickHouse query boundary this package needs --
// the same single-method shape the other operation packages declare, redeclared
// locally per their documented convention.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// Defaults mirroring CapacityForecastInput's own, applied when the whole input
// object is absent.
//
// The SDL declares these too (`historyDays: Int! = 90`), so gqlgen fills them
// for a PARTIAL input -- these constants only cover `input: null`, which is the
// case Python handles with its own inline `if input else` chain.
const (
	defaultHistoryDays = 90
	defaultSimulations = 10000
)

// randomSeed draws the per-request Monte Carlo seed. See the package doc for
// why this is a fresh draw rather than a fixed value.
//
// Bounded to the positive int64 range because cpyrandom.New takes an int64 and
// CPython's own random.seed(negative) folds through abs() -- a negative draw
// would therefore alias onto its positive twin, halving the effective seed
// space for no reason.
func randomSeed() (int64, error) {
	value, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		// Fails the request rather than substituting a constant. A fixed
		// fallback seed would make every forecast on a broken-entropy host
		// identical while still looking like a Monte Carlo.
		return 0, fmt.Errorf("capacityForecast: draw simulation seed: %w", err)
	}
	return value.Int64(), nil
}

// ResolveForecast ports resolve_capacity_forecast.
//
// Returns (nil, nil) for the two tolerated empties -- no history, or no
// positive item target -- exactly as Python returns None for them.
func ResolveForecast(
	ctx context.Context,
	client QueryClient,
	orgID string,
	input *model.CapacityForecastInput,
	now time.Time,
) (*model.CapacityForecast, error) {
	started := time.Now()

	var teamID, workScopeID *string
	var targetItems *int
	var targetDate *graphqldate.Date
	historyDays, simulations := defaultHistoryDays, defaultSimulations
	if input != nil {
		teamID, workScopeID = input.TeamID, input.WorkScopeID
		targetItems, targetDate = input.TargetItems, input.TargetDate
		historyDays, simulations = input.HistoryDays, input.Simulations
	}

	// CHAOS-5349 r1 P1. `simulations` is `Int! = 10000` in the SDL: non-null,
	// but nothing in GraphQL constrains it below, so a caller can send -1. That
	// used to reach the Monte Carlo kernel's make() and panic the resolver
	// ("makeslice: cap out of range") -- a 500 with no useful body.
	//
	// Python does not panic; it answers p50=p85=p95=0, because `range(-1)` is
	// empty. That is NOT a contract worth preserving. A forecast built from
	// zero simulations is not a conservative estimate, it is a fabricated one,
	// and returning it as though it were an answer is worse than refusing the
	// request. DECLARED DIVERGENCE -- see the PR body.
	//
	// Rejected BEFORE any query, so a nonsensical request costs the org nothing,
	// and the field and the offending value are both named so the caller can
	// see what to change. There is deliberately no UPPER bound: neither Python
	// nor the worker path enforces one, and inventing a ceiling here would
	// silently truncate a request that works today.
	if simulations < 1 {
		slog.WarnContext(ctx, "query_api.capacity_forecast.invalid_input",
			"org_id", orgID,
			"field", "simulations",
			"value", simulations,
			"reason", "simulations must be at least 1; a zero-simulation Monte Carlo has no output to take percentiles of",
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return nil, &gqlerror.Error{
			Message: fmt.Sprintf(
				"capacityForecast: simulations must be at least 1, got %d", simulations),
			Extensions: map[string]any{
				"code":  "BAD_USER_INPUT",
				"field": "simulations",
			},
		}
	}

	history, err := loadThroughput(ctx, client, orgID, teamID, workScopeID, historyDays, now)
	if err != nil {
		return nil, err
	}
	if len(history) == 0 {
		// Python logs a warning and returns None. The reason belongs in the log
		// or a null response is indistinguishable from a broken predicate.
		slog.WarnContext(ctx, "query_api.capacity_forecast.empty",
			"org_id", orgID,
			"team_id", stringOrOrgWide(teamID),
			"work_scope_id", stringOrOrgWide(workScopeID),
			"history_days", historyDays,
			"reason", "no throughput history in the requested window",
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return nil, nil
	}

	backlog, err := loadBacklog(ctx, client, orgID, teamID, workScopeID)
	if err != nil {
		return nil, err
	}

	items := resolveTargetItems(targetItems, backlog)
	if items <= 0 {
		slog.WarnContext(ctx, "query_api.capacity_forecast.empty",
			"org_id", orgID,
			"team_id", stringOrOrgWide(teamID),
			"work_scope_id", stringOrOrgWide(workScopeID),
			"history_days", historyDays,
			"backlog_size", backlog,
			"reason", "no positive item target: the request supplied none and the backlog is empty",
			"duration_ms", time.Since(started).Milliseconds(),
		)
		return nil, nil
	}

	seed, err := randomSeed()
	if err != nil {
		return nil, err
	}

	request := numerical.ForecastRequest{
		History: numerical.Throughput{
			DailyThroughputs: history,
			// Python's days_of_history counts SAMPLES, and the query returns one
			// row per day, so the two are the same number here.
			DaysOfHistory: len(history),
		},
		TargetItems: &items,
		BacklogSize: backlog,
		Simulations: simulations,
		Seed:        seed,
	}
	if targetDate != nil {
		moment := targetDate.Time().UTC()
		request.TargetDate = &moment
	}

	result, err := numerical.ForecastCapacity(request, now)
	if err != nil {
		return nil, fmt.Errorf("capacityForecast: %w", err)
	}

	slog.InfoContext(ctx, "query_api.capacity_forecast.served",
		"org_id", orgID,
		"team_id", stringOrOrgWide(teamID),
		"work_scope_id", stringOrOrgWide(workScopeID),
		"history_days", historyDays,
		"history_samples", len(history),
		"backlog_size", backlog,
		"target_items", items,
		"simulations", simulations,
		"insufficient_history", result.InsufficientHistory,
		"high_variance", result.HighVariance,
		"duration_ms", time.Since(started).Milliseconds(),
	)

	return forecastToModel(now, teamID, workScopeID, backlog, items, targetDate, result), nil
}

// resolveTargetItems ports `items = target_items if target_items else backlog`.
//
// Python's check is FALSY, not None-aware, so a request carrying targetItems=0
// falls back to the backlog rather than forecasting zero items. Extracted so
// the test exercises THIS function rather than restating the rule -- a test
// that re-implements the logic it checks passes whatever production does. Same
// shape, and the same reasoning, as the worker executor's own
// resolveTargetItems.
func resolveTargetItems(targetItems *int, backlog int) int {
	if targetItems != nil && *targetItems != 0 {
		return *targetItems
	}
	return backlog
}

// forecastToModel maps a kernel result onto the GraphQL type, mirroring
// _result_to_forecast field for field.
func forecastToModel(
	computedAt time.Time,
	teamID, workScopeID *string,
	backlog, targetItems int,
	targetDate *graphqldate.Date,
	result numerical.ForecastResult,
) *model.CapacityForecast {
	items := targetItems
	return &model.CapacityForecast{
		// uuid4 per call on both sides, and never compared.
		ForecastID: newForecastID(),
		// str(datetime), NOT isoformat(): Python renders this field with str(),
		// which uses a SPACE separator, while the throughputForecast resolver
		// next door uses isoformat() and gets a "T". The two really do differ,
		// and a client parsing both sees both shapes.
		ComputedAt:          strDatetimeUTC(computedAt),
		TeamID:              teamID,
		WorkScopeID:         workScopeID,
		BacklogSize:         backlog,
		TargetItems:         &items,
		TargetDate:          targetDate,
		P50Date:             dateOrNil(result.P50Date),
		P85Date:             dateOrNil(result.P85Date),
		P95Date:             dateOrNil(result.P95Date),
		P50Days:             result.P50Days,
		P85Days:             result.P85Days,
		P95Days:             result.P95Days,
		P50Items:            result.P50Items,
		P85Items:            result.P85Items,
		P95Items:            result.P95Items,
		ThroughputMean:      result.ThroughputMean,
		ThroughputStddev:    result.ThroughputStddev,
		HistoryDays:         result.HistoryDays,
		InsufficientHistory: result.InsufficientHistory,
		HighVariance:        result.HighVariance,
	}
}

func dateOrNil(moment *time.Time) *graphqldate.Date {
	if moment == nil {
		return nil
	}
	date := graphqldate.New(*moment)
	return &date
}

// stringOrOrgWide renders an optional scope for a log line, so an absent scope
// reads as the org-wide forecast it is rather than as an empty field.
func stringOrOrgWide(value *string) string {
	if value == nil || *value == "" {
		return "org-wide"
	}
	return *value
}

// strDatetimeUTC reproduces Python's str(datetime) for a UTC-aware datetime.
//
// Not isoformat(): str() separates the date and time with a SPACE where
// isoformat() uses "T". Both appear in this cutover -- capacity uses str(),
// throughputForecast uses isoformat() -- and collapsing them to one shape would
// silently change one of the two wire formats a client already parses.
//
// The fraction is six digits when the microsecond is non-zero and absent when
// it is zero, which is Python's rule and not one any Go layout expresses
// (".999999" trims trailing zeros and would render 123000 as ".123").
func strDatetimeUTC(moment time.Time) string {
	utc := moment.UTC()
	rendered := utc.Format("2006-01-02 15:04:05")
	if microseconds := utc.Nanosecond() / 1000; microseconds != 0 {
		rendered += fmt.Sprintf(".%06d", microseconds)
	}
	return rendered + "+00:00"
}

// ResolveForecasts ports resolve_capacity_forecasts.
func ResolveForecasts(
	ctx context.Context,
	client QueryClient,
	orgID string,
	filters *model.CapacityForecastFilterInput,
) (*model.CapacityForecastConnection, error) {
	started := time.Now()

	limit := 10
	if filters != nil {
		limit = filters.Limit
	}

	conditions := []string{"org_id = {org_id:String}"}
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if filters != nil {
		// Every one of these is a FALSY check in Python (`if filters.team_id:`),
		// so an explicitly empty string is "no filter" rather than "match the
		// empty string" -- which for a Nullable(String) column would match
		// nothing at all.
		if filters.TeamID != nil && *filters.TeamID != "" {
			conditions = append(conditions, "team_id = {team_id:String}")
			bindings = append(bindings, clickhouse.Binding{Name: "team_id", Value: *filters.TeamID})
		}
		if filters.WorkScopeID != nil && *filters.WorkScopeID != "" {
			conditions = append(conditions, "work_scope_id = {work_scope_id:String}")
			bindings = append(bindings, clickhouse.Binding{Name: "work_scope_id", Value: *filters.WorkScopeID})
		}
		if filters.FromDate != nil {
			conditions = append(conditions, "toDate(computed_at) >= {from_date:Date}")
			bindings = append(bindings, clickhouse.Binding{Name: "from_date", Value: filters.FromDate.String()})
		}
		if filters.ToDate != nil {
			conditions = append(conditions, "toDate(computed_at) <= {to_date:Date}")
			bindings = append(bindings, clickhouse.Binding{Name: "to_date", Value: filters.ToDate.String()})
		}
	}

	// Python binds LIMIT as a pyformat parameter through a different client.
	// Server-side bindings here are strings, and ClickHouse will not accept a
	// bound value in a LIMIT clause, so the already-integer limit is formatted
	// in. There is deliberately NO clamp: Python has none, and adding one would
	// silently truncate a page a client asks for today.
	query := fmt.Sprintf(`
        SELECT
            forecast_id,
            computed_at,
            team_id,
            work_scope_id,
            backlog_size,
            target_items,
            target_date,
            p50_date,
            p85_date,
            p95_date,
            p50_days,
            p85_days,
            p95_days,
            p50_items,
            p85_items,
            p95_items,
            throughput_mean,
            throughput_stddev,
            history_days,
            insufficient_history,
            high_variance
        FROM capacity_forecasts
        WHERE %s
        ORDER BY computed_at DESC
        LIMIT %d
    `, strings.Join(conditions, " AND "), limit)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("capacityForecasts: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// Non-nil and empty, never nil: `edges` is a non-null list on the wire, and
	// a nil slice would marshal as JSON null and break a client that iterates
	// it.
	edges := make([]model.CapacityForecastEdge, 0, 16)
	for rows.Next() {
		forecast, scanErr := scanForecastRow(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		edges = append(edges, model.CapacityForecastEdge{
			Node: forecast,
			// The cursor is the forecast_id VERBATIM -- see this package's doc
			// comment, item 5.
			Cursor: forecast.ForecastID,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("capacityForecasts: rows: %w", err)
	}

	pageInfo := &model.PageInfo{
		// "A full page probably means there is more", which is what Python
		// computes. It is a guess, not a count -- a result of exactly `limit`
		// rows with nothing beyond it reports true.
		HasNextPage: len(edges) == limit,
		// Hardcoded false, because nothing here can page backwards: there is no
		// `before` argument to have come from.
		HasPreviousPage: false,
	}
	if len(edges) > 0 {
		start, end := edges[0].Cursor, edges[len(edges)-1].Cursor
		pageInfo.StartCursor, pageInfo.EndCursor = &start, &end
	}

	if len(edges) == 0 {
		// Python does NOT log here. Added because a silently-empty result is
		// exactly what an operator cannot diagnose after the fact, and this
		// connection has four independent filters any one of which can empty it.
		slog.WarnContext(ctx, "query_api.capacity_forecasts.empty",
			"org_id", orgID,
			"filters", filterLabel(filters),
			"limit", limit,
			"reason", "no persisted capacity_forecasts rows matched",
			"duration_ms", time.Since(started).Milliseconds(),
		)
	}

	slog.InfoContext(ctx, "query_api.capacity_forecasts.served",
		"org_id", orgID,
		"filters", filterLabel(filters),
		"limit", limit,
		"rows", len(edges),
		"duration_ms", time.Since(started).Milliseconds(),
	)

	return &model.CapacityForecastConnection{
		Edges:    edges,
		PageInfo: pageInfo,
		// The PAGE length, not a table count. Renaming it would be honest and
		// would also change what every existing client displays.
		TotalCount: len(edges),
	}, nil
}

// filterLabel renders the active filters for a log line.
func filterLabel(filters *model.CapacityForecastFilterInput) string {
	if filters == nil {
		return "none"
	}
	var parts []string
	if filters.TeamID != nil && *filters.TeamID != "" {
		parts = append(parts, "team="+*filters.TeamID)
	}
	if filters.WorkScopeID != nil && *filters.WorkScopeID != "" {
		parts = append(parts, "scope="+*filters.WorkScopeID)
	}
	if filters.FromDate != nil {
		parts = append(parts, "from="+filters.FromDate.String())
	}
	if filters.ToDate != nil {
		parts = append(parts, "to="+filters.ToDate.String())
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, " ")
}
