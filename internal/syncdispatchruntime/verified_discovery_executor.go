package syncdispatchruntime

import (
	"context"
	"fmt"
)

// VerifiedDiscoveryExecutor decorates another DiscoveryExecutor (in
// practice, BridgeDiscoveryExecutor) with the ClickHouse readback
// verification step -- ports run_sync_reference_discovery's sequential
// run_team_autoimport_strict-then-_verify_reference_readback call, as a
// composed decorator rather than folding the verification directly into
// BridgeDiscoveryExecutor, so the populate call and the verify step stay
// independently testable.
//
// provider comes from the inner executor's own summary["provider"] field
// (run_team_autoimport_strict's Python return always sets this key) rather
// than a second database round trip -- the populate call and the readback
// check must agree on which provider's tables to look at, and re-deriving
// it separately would risk the two disagreeing.
type VerifiedDiscoveryExecutor struct {
	inner    DiscoveryExecutor
	verifier *ReferenceReadbackVerifier
}

func NewVerifiedDiscoveryExecutor(inner DiscoveryExecutor, verifier *ReferenceReadbackVerifier) (*VerifiedDiscoveryExecutor, error) {
	if inner == nil || verifier == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	return &VerifiedDiscoveryExecutor{inner: inner, verifier: verifier}, nil
}

func (executor *VerifiedDiscoveryExecutor) Discover(ctx context.Context, orgID, runID string) (map[string]any, error) {
	if executor == nil || executor.inner == nil || executor.verifier == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	summary, err := executor.inner.Discover(ctx, orgID, runID)
	if err != nil {
		return nil, err
	}
	provider, ok := summary["provider"].(string)
	if !ok || provider == "" {
		return nil, fmt.Errorf("%w: populate summary carried no provider to verify readback against", ErrReferenceDiscoveryUnavailable)
	}
	if err := executor.verifier.Verify(ctx, orgID, provider, summary); err != nil {
		return nil, err
	}
	return summary, nil
}
