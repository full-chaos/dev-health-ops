package investmentexplain

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// An org on its own model whose model cannot be built never falls back to the platform environment.
func TestNewProviderForOrgDoesNotFallBackToThePlatformForAnOwnModelOrg(t *testing.T) {
	ctx := WithOwnModelOnly(context.Background())
	_, err := newProviderForOrg(ctx, categorize.ProviderKindMock, "org-1", "", nil, nil)
	if !errors.Is(err, ErrOwnModelUnusable) {
		t.Fatalf("err = %v, want ErrOwnModelUnusable", err)
	}
	provider, err := newProviderForOrg(context.Background(), categorize.ProviderKindMock, "org-1", "", nil, nil)
	if err != nil || provider == nil {
		t.Fatalf("positive control: platform path without the marker built nothing: %v", err)
	}
}
