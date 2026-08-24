package syncdispatchruntime

import "context"

// referenceDiscoveryPopulator is the narrow surface BridgeDiscoveryExecutor
// needs from *HTTPBridge -- an interface so this type is red-first testable
// against a fake instead of a real HTTP server.
type referenceDiscoveryPopulator interface {
	PopulateReferenceDiscovery(ctx context.Context, orgID, runID string) (map[string]any, error)
}

// BridgeDiscoveryExecutor implements DiscoveryExecutor by calling the
// narrow, identifiers-only /reference-discovery-populate bridge endpoint
// (CHAOS-4175, ruling widened 2026-08-24): organization_id/sync_run_id in,
// the populator's summary dict out, no credential material either
// direction. It does NOT yet verify ClickHouse readback -- that is its own
// later step, per the agreed build order (bridge endpoint + Discover
// implementation, then ClickHouse readback).
type BridgeDiscoveryExecutor struct {
	populator referenceDiscoveryPopulator
}

// NewBridgeDiscoveryExecutor wraps an *HTTPBridge. Takes the concrete type
// (not the referenceDiscoveryPopulator interface) at the construction
// boundary so a caller cannot accidentally wire in something that isn't
// really the authenticated compatibility bridge; the interface exists for
// this package's own tests.
func NewBridgeDiscoveryExecutor(bridge *HTTPBridge) (*BridgeDiscoveryExecutor, error) {
	if bridge == nil {
		return nil, ErrInvalidBridge
	}
	return &BridgeDiscoveryExecutor{populator: bridge}, nil
}

// Discover ignores the caller-resolved authoritative provider: the wire
// contract stays identifiers-only (organization_id/sync_run_id), matching
// the narrow bridge endpoint's request shape exactly -- Python resolves its
// own provider independently on its side of the boundary. The authoritative
// provider exists for the CALLER (VerifiedDiscoveryExecutor) to check the
// response against, not for this call to send.
func (executor *BridgeDiscoveryExecutor) Discover(ctx context.Context, orgID, runID, _ string) (map[string]any, error) {
	if executor == nil || executor.populator == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	return executor.populator.PopulateReferenceDiscovery(ctx, orgID, runID)
}
