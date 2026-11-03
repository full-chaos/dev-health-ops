// Package reconcilejob runs the External Push projection reconcile (CHAOS-10897).
package reconcilejob

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/customerpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushreconcile"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	PageTimeout = 5 * time.Second
	RunBudget   = 5 * time.Minute
)

type Pass struct {
	Tokens    int
	Projected int
	Findings  []pushreconcile.Finding
}

type PassFunc func(ctx context.Context) (Pass, error)

type Report struct {
	Passes    int
	Tokens    int
	Projected int
	Drift     []pushreconcile.Finding
	Transient int
}

func (r Report) Counts() map[pushreconcile.Class]int { return nil }

func CountByClass([]pushreconcile.Finding) map[pushreconcile.Class]int { return nil }

func Run(context.Context, PassFunc, time.Duration, func(context.Context, time.Duration) error) (Report, error) {
	return Report{}, nil
}

func Sleep(context.Context, time.Duration) error { return nil }

func NewPass(customerpush.TxBeginner, authschema.ValidatedIdentifier, int, func() time.Time) PassFunc {
	return func(context.Context) (Pass, error) { return Pass{}, nil }
}
