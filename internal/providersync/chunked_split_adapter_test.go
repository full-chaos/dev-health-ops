package providersync

// splitUnderTest is the one call the gate and the margin cells make into the product split: the claim is the unit's claim.
func splitUnderTest(claim Claim, batch CompleteRouteBatch, policy ChunkPolicy) ([]PreparedProviderChunk, error) {
	return splitCompleteRouteBatch(&claim, batch, ShadowComparison{}, policy)
}
