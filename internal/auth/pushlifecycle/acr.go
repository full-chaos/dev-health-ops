package pushlifecycle

import "github.com/full-chaos/dev-health-ops/internal/auth/contracts"

const AcrSourceID = "acr"

func AcrFingerprint(string) string { return "" }

func FromAcr(contracts.AcrCredentialLifecycleEvent) contracts.ExternalPushLifecycleEvent {
	return contracts.ExternalPushLifecycleEvent{}
}
