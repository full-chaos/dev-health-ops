package org

import (
	"context"
	"errors"
	"log/slog"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

type CreateInput struct {
	Name string
	Slug string
}

type Store struct {
	commit    func(context.Context, audit.Mutation) error
	logger    *slog.Logger
	mutations metric.Int64Counter
}

func (s *Store) Create(ctx context.Context, in CreateInput, a Actor) (uuid.UUID, error) {
	return uuid.Nil, errors.New("org: not implemented")
}

func (s *Store) Rename(ctx context.Context, id uuid.UUID, name string, ifRevision *int64, a Actor) error {
	return errors.New("org: not implemented")
}

func (s *Store) Suspend(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor) error {
	return errors.New("org: not implemented")
}

func (s *Store) Reactivate(ctx context.Context, id uuid.UUID, ifRevision *int64, a Actor) error {
	return errors.New("org: not implemented")
}
