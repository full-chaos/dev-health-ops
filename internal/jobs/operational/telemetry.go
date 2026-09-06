package operational

// CHAOS-5320 deleted the Python HTTP bridge (HTTPDispatcher.DispatchWebhook,
// /api/internal/worker-operational/webhook, process_webhook_event) -- the
// fallback path WebhookHandler.Work used for every event type PR1/PR2/PR3's
// native routing didn't cover. With no fallback left, an unhandled event
// type has nowhere to go but an EXPLICIT ignore: this file is that ignore's
// telemetry, so it never degrades into a silent drop (root AGENTS.md
// standing order: no new decision path without its telemetry, same PR).

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var ignoredWebhookEventCounter = mustOperationalCounter(
	"devhealth_operational_webhook_ignored_total",
	"webhook deliveries with no native handler, explicitly ignored rather than dispatched (CHAOS-5320 removed the HTTP bridge fallback), by provider/event_type",
)

func mustOperationalCounter(name, description string) metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/jobs/operational")
	counter, err := meter.Int64Counter(name, metric.WithDescription(description))
	if err != nil {
		// Same otel guarantee cmd/query-api/internal/analytics/telemetry.go
		// relies on: Int64Counter never returns a nil counter even on
		// error, so a broken meter provider must not panic the worker.
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// recordIgnoredWebhookEvent is a package var, not a plain func, so a test
// can observe that an ignore actually reported itself -- same reasoning as
// analytics/telemetry.go's recordDegradation.
var recordIgnoredWebhookEvent = defaultRecordIgnoredWebhookEvent

func defaultRecordIgnoredWebhookEvent(ctx context.Context, provider, eventType, deliveryID string) {
	ignoredWebhookEventCounter.Add(ctx, 1, metric.WithAttributes(
		attribute.String("provider", provider),
		attribute.String("event_type", eventType),
	))
	slog.InfoContext(ctx, "webhook event has no native handler, ignored",
		"provider", provider, "event_type", eventType, "delivery_id", deliveryID)
}
