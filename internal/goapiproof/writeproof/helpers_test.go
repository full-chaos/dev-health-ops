package writeproof

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

type noSeeder struct{}

func (noSeeder) Seed(context.Context, goapiproof.Querier, string, RunTag) error     { return nil }
func (noSeeder) Teardown(context.Context, goapiproof.Querier, string, RunTag) error { return nil }
