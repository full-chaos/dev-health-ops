package providerunit

import (
	"errors"
	"fmt"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// The signature contract of deterministicTerminalCategory, pinned as a table so
// that adding, removing or renaming a mapping is a deliberate edit to this list
// rather than a silent change in how a whole class of units terminalizes.
//
// Both directions matter. A missing entry burns every retry and then records
// the generic exhausted category, hiding the real cause; a spurious entry
// terminalizes a fault a retry would have cleared.
func TestDeterministicTerminalCategoryContract(t *testing.T) {
	deterministic := []struct {
		name     string
		err      error
		category string
	}{
		{
			name:     "provider dataset unavailable",
			err:      providersync.ErrProviderDatasetUnavailable,
			category: ProviderDatasetUnavailableCategory,
		},
		{
			name:     "repository identity ambiguous",
			err:      providersync.ErrRepositoryIdentityAmbiguous,
			category: RepositoryIdentityCategory,
		},
		{
			// Reached when a readback finds a stored row that disagrees with
			// the replayed effect, when the effect is recovery-blocked, or when
			// the committer has no readback. All three are decided by already
			// persisted state, so every later attempt reaches the same verdict.
			name:     "effect recovery ambiguous",
			err:      providersync.ErrEffectRecoveryAmbiguous,
			category: EffectRecoveryAmbiguousCategory,
		},
		{
			// Deterministic given the provider's current state: a repo with
			// more in-window rows than the cap refuses identically on every
			// attempt, so retrying only re-fetched up to 100 list pages and
			// 10k detail GETs five times over (CHAOS-3871).
			name:     "pagination cap refusal",
			err:      providersync.ErrPaginationCapExceeded,
			category: PaginationIncompleteCategory,
		},
		{
			// Already non-retryable at the HTTP layer; the unit handler was
			// still spending four more executions against a dead credential.
			name:     "authentication",
			err:      &providerfoundation.ProviderError{Class: providerfoundation.ErrorAuthentication, StatusCode: 401},
			category: AuthCategory,
		},
		{
			name:     "not found",
			err:      &providerfoundation.ProviderError{Class: providerfoundation.ErrorNotFound, StatusCode: 404},
			category: NotFoundCategory,
		},
	}
	for _, testCase := range deterministic {
		t.Run(testCase.name, func(t *testing.T) {
			category, ok := deterministicTerminalCategory(testCase.err)
			if !ok || category != testCase.category {
				t.Fatalf("bare error: got (%q, %v), want (%q, true)",
					category, ok, testCase.category)
			}
			// Executors return wrapped errors, so matching MUST go through
			// errors.Is. An equality check would pass the bare case above and
			// silently fail in production.
			wrapped := fmt.Errorf("executing route: %w", testCase.err)
			category, ok = deterministicTerminalCategory(wrapped)
			if !ok || category != testCase.category {
				t.Fatalf("wrapped error: got (%q, %v), want (%q, true)",
					category, ok, testCase.category)
			}
		})
	}

	// The categories must stay distinguishable from each other and from the
	// exhausted fallback: collapsing two of them would make the alert that
	// fires on one of them fire on both.
	seen := map[string]string{}
	for _, testCase := range deterministic {
		if previous, duplicate := seen[testCase.category]; duplicate {
			t.Errorf("category %q is shared by %q and %q",
				testCase.category, previous, testCase.name)
		}
		seen[testCase.category] = testCase.name
	}
	for category := range seen {
		if category == "provider_unit_exhausted" ||
			category == GitHubFilesInventoryFailureCategory ||
			category == RouteReconciliationCategory {
			t.Errorf("deterministic category %q collides with a non-deterministic one", category)
		}
	}

	// Anything not listed must keep the ordinary bounded-retry path. A mapper
	// that returned true for an arbitrary error would terminalize transient
	// faults, which is the failure this half of the contract exists to catch.
	for _, err := range []error{
		errors.New("connection reset by peer"),
		providersync.ErrInvalidConfiguration,
		fmt.Errorf("wrapped: %w", errors.New("timeout")),
		// A rate limit is deferred by its own branch upstream of this mapper
		// and must never be terminalized here, and a transient provider fault
		// still deserves its bounded retries.
		&providerfoundation.ProviderError{Class: providerfoundation.ErrorRateLimited, StatusCode: 429},
		&providerfoundation.ProviderError{Class: providerfoundation.ErrorTransient, StatusCode: 503},
	} {
		if category, ok := deterministicTerminalCategory(err); ok {
			t.Errorf("deterministicTerminalCategory(%v) = (%q, true), want retryable",
				err, category)
		}
	}
}
