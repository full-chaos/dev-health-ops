package org

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/metric/noop"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func newTestStore(commit func(context.Context, audit.Mutation) error) *Store {
	c, _ := noop.NewMeterProvider().Meter("test").Int64Counter("x")
	return &Store{commit: commit, logger: slog.Default(), mutations: c}
}
