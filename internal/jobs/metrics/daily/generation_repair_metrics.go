package daily

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// GenerationRepairFailuresCounter counts generation-repair passes that
	// failed on a store call, by step.
	GenerationRepairFailuresCounter = "dev_health_generation_repair_failures_total"
	// GenerationRepairPartialCounter counts passes that ended at their budget
	// with a resumable partial result (expected, not a failure).
	GenerationRepairPartialCounter = "dev_health_generation_repair_partial_total"
)

func generationRepairMeter() metric.Meter {
	return otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily")
}

func recordGenerationRepairFailure(ctx context.Context, step string) {
	counter, err := generationRepairMeter().Int64Counter(GenerationRepairFailuresCounter,
		metric.WithDescription("generation-repair passes that failed on a store call, by step"))
	if err != nil {
		return
	}
	counter.Add(context.WithoutCancel(ctx), 1, metric.WithAttributes(attribute.String("step", step)))
}

func recordGenerationRepairPartial(ctx context.Context) {
	counter, err := generationRepairMeter().Int64Counter(GenerationRepairPartialCounter,
		metric.WithDescription("generation-repair passes that ended at their budget with a resumable partial result"))
	if err != nil {
		return
	}
	counter.Add(context.WithoutCancel(ctx), 1)
}
