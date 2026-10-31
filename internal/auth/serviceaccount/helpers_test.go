package serviceaccount

import (
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

func nopLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func nopCounter(t *testing.T) metric.Int64Counter {
	t.Helper()
	c, err := metricnoop.NewMeterProvider().Meter("test").Int64Counter("x")
	if err != nil {
		t.Fatal(err)
	}
	return c
}
