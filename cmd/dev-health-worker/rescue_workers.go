package main

import (
	"github.com/full-chaos/dev-health-ops/internal/jobrescue"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/riverqueue/river"
)

func registerRescueCoverage(
	workers *river.Workers,
	registry *jobruntime.Registry,
	handlers []jobruntime.HandlerSpec,
	extraOwnedKinds ...string,
) error {
	owned := make([]string, 0, len(handlers)+len(extraOwnedKinds))
	for _, handler := range handlers {
		owned = append(owned, handler.Kind)
	}
	owned = append(owned, extraOwnedKinds...)
	_, err := jobrescue.RegisterMissingWorkers(workers, registry, owned)
	return err
}
