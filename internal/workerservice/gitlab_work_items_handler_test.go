package workerservice

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-8777: the GitLab nested-list bound (100 pages) is the route's own
// default. The handler the WORKER builds must leave every page limit unset, so
// that default is what runs; a value set here would only be a second place to
// get the ceiling wrong. The route clamps a configured NestedMaxPages to the
// ceiling (providersync TestGitLabWorkItemsRouteConfiguredNestedBoundIsClampedToTheCeiling),
// so such a value would be harmless, but it must not appear unnoticed.
func TestGitLabWorkItemsRouteHandlerBuiltForTheWorkerSetsNoPageLimits(t *testing.T) {
	t.Parallel()
	mapping := &providersync.StatusMapping{}
	handler := newGitLabWorkItemsRouteHandler(mapping, nil)
	if handler.PerPage != 0 || handler.MaxPages != 0 || handler.NestedMaxPages != 0 {
		t.Fatalf("worker handler sets page limits: per_page=%d max_pages=%d nested_max_pages=%d",
			handler.PerPage, handler.MaxPages, handler.NestedMaxPages)
	}
	if handler.StatusMapping != mapping {
		t.Fatal("worker handler does not carry the status mapping it was given")
	}
}
