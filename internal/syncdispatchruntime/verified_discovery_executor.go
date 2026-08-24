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
// The provider used for readback is the caller-supplied AUTHORITATIVE
// value (resolveAuthoritativeProvider), never the populate response's own
// echoed "provider" field (CHAOS-4175 round 2): Python verifies with
// context["provider"] from the integrations table and never trusts the
// echo, so trusting the bridge's self-reported value here would be a
// parity downgrade, not a shortcut. Under correct operation the two values
// are always identical by construction (run_team_autoimport_strict just
// re-normalizes whatever provider it was given), so this doubles as a
// defense-in-depth check: Verify's own cross-check below fails closed if
// they ever disagree, which only happens if the bridge response is broken
// or lying.
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

func (executor *VerifiedDiscoveryExecutor) Discover(ctx context.Context, orgID, runID, provider string) (map[string]any, error) {
	if executor == nil || executor.inner == nil || executor.verifier == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	summary, err := executor.inner.Discover(ctx, orgID, runID, provider)
	if err != nil {
		return nil, err
	}
	echoedProvider, _ := summary["provider"].(string)
	if echoedProvider != provider {
		return nil, fmt.Errorf("%w: authoritative=%q echoed=%q", ErrDiscoveryProviderMismatch, provider, echoedProvider)
	}
	if err := executor.verifier.Verify(ctx, orgID, provider, summary); err != nil {
		return nil, err
	}
	return summary, nil
}
