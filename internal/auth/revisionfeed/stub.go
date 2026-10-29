// Package revisionfeed is the consumer side of the auth outbox feed. This file is a stub: every function does nothing.
package revisionfeed

import (
	"context"
	"errors"
	"log/slog"

	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

var (
	ErrKeyDisagrees          = errors.New("revisionfeed: key disagrees")
	ErrNotAProvisioningEvent = errors.New("revisionfeed: not a provisioning event")
)

type Invalidator interface {
	Invalidate(ctx context.Context, aggregateType, aggregateID string, revision int64)
	InvalidateAll(ctx context.Context)
}

type Config struct {
	Invalidator Invalidator
	Logger      *slog.Logger
	Meter       metric.Meter
}

type Watermarks struct{}

func (w *Watermarks) Latest(aggregateType, aggregateID string) (int64, bool) { return 0, false }

type Consumer struct{}

func NewConsumer(Config) (*Consumer, error) { return &Consumer{}, nil }

func (c *Consumer) Handle(context.Context, contracts.ProvisioningEvent) contracts.Disposition {
	return ""
}

func (c *Consumer) HandleOutbox(context.Context, audit.Event) (contracts.Disposition, error) {
	return "", nil
}

func (c *Consumer) Resubscribed(context.Context) {}

func (c *Consumer) Watermarks() *Watermarks { return &Watermarks{} }

func FromOutbox(audit.Event) (contracts.ProvisioningEvent, error) {
	return contracts.ProvisioningEvent{}, nil
}
