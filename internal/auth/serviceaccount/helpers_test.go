package serviceaccount

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/audit"
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

func allow(context.Context, audit.TxOps, Locked) error { return nil }

func okActor() Actor { return Actor{Authorize: allow} }

func okActorFor(id *uuid.UUID) Actor { return Actor{PrincipalID: id, Authorize: allow} }
