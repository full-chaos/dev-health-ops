package investmentexplain

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The two writes at the end of an explain request (the explanation cache row and the LLM token-usage row) are
// best-effort: a failed write never fails the request. A cache that stops persisting is then invisible, every request
// recomputes and spends LLM budget while the route answers 200, so each failure is logged (error class and Go type,
// never the error text) and counted.
const (
	writeCache      = "cache"
	writeTokenUsage = "token_usage"

	writeFailureEvent  = "query_api.investment_mix_explain.write_failed"
	writeFailureMetric = "devhealth_query_api_investment_explain_write_failed_total"
)

func recordWriteFailure(ctx context.Context, write, orgID string, err error) {
	if err == nil {
		return
	}
	slog.WarnContext(ctx, writeFailureEvent,
		"org_id", orgID, "write", write,
		"error_class", logging.ErrorClass(err), "error_type", logging.ErrorType(err))
	counter, counterErr := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/investmentexplain").Int64Counter(
		writeFailureMetric,
		metric.WithDescription("Failed best-effort writes of the investment explain cache row or the LLM token-usage row, by write"),
	)
	if counterErr != nil {
		return
	}
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("write", write)))
}
