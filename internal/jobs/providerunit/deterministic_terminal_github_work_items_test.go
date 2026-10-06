package providerunit

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-8777 r1 P1-3: a GitHub work-items unit blocked by a page-bound refusal
// is a deterministic failure. The route returns it as a GitHubWorkItemsRouteError
// whose cause is built by GitHubWorkItemsBlockingIncompleteCause (the same
// function the route calls); the REAL classifier must record it as terminal
// pagination_incomplete, not leave it to retry to the attempt limit.
func TestDeterministicTerminalCategoryGitHubWorkItemsPageBound(t *testing.T) {
	for _, test := range []struct {
		name       string
		incomplete []providersync.GitHubWorkItemsIncomplete
		want       string
		terminal   bool
	}{
		{"item page bound", []providersync.GitHubWorkItemsIncomplete{{Component: "pr_social", Cause: "item_page_bound"}}, PaginationIncompleteCategory, true},
		{"shared request budget", []providersync.GitHubWorkItemsIncomplete{{Component: "pr_social", Cause: "pagination_cap"}}, PaginationIncompleteCategory, true},
		{"bound next to an optional entry", []providersync.GitHubWorkItemsIncomplete{
			{Component: "milestones", Cause: "bad_gateway"}, {Component: "pr_social", Cause: "item_page_bound"},
		}, PaginationIncompleteCategory, true},
		// A defect of our own traversal is not a bound: it keeps retrying.
		{"invalid pagination", []providersync.GitHubWorkItemsIncomplete{{Component: "pr_social", Cause: "invalid_pagination"}}, "", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := &providersync.GitHubWorkItemsRouteError{
				Cause:      providersync.GitHubWorkItemsBlockingIncompleteCause(test.incomplete),
				Incomplete: test.incomplete,
			}
			category, terminal := deterministicTerminalCategory(err)
			if category != test.want || terminal != test.terminal {
				t.Fatalf("classified as (%q, %v), want (%q, %v)", category, terminal, test.want, test.terminal)
			}
		})
	}
}
