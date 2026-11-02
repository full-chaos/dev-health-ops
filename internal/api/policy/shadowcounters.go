package policy

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
)

const (
	shadowMismatchMetric = "dev_health_api_shadow_mismatch_total"
	shadowResultMetric   = "dev_health_api_shadow_results_total"
)

type shadowMismatchAdder func(ctx context.Context, namespace, caller, class string)

type shadowResultAdder func(ctx context.Context, result string)

var (
	shadowMismatchAdd shadowMismatchAdder
	shadowResultAdd   shadowResultAdder
)

func mismatchAttributes(namespace, caller, class string) []attribute.KeyValue { return nil }

func resultAttributes(result string) []attribute.KeyValue { return nil }
