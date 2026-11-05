package identity

import (
	"crypto/sha256"
	"encoding/hex"
)

// projectedProtocols are the stored auth_provider values a log line may name; any other value is "other".
var projectedProtocols = map[string]bool{"oidc": true, "saml": true, "github": true, "gitlab": true, "google": true}

// projectedLogAttrs are the attributes of the "legacy external identity projected" Info line. An SSO key adds
// provider_ref, the first 12 hex of sha256 of the provider row id: the full id stays in the audit event.
func projectedLogAttrs(key ExternalKey, protocol string) []any {
	switch {
	case protocol == "":
		protocol = "unknown"
	case !projectedProtocols[protocol]:
		protocol = "other"
	}
	attrs := []any{"provider", providerLabel(key.Provider)}
	if key.Provider.IsSSO() {
		sum := sha256.Sum256([]byte(key.Provider))
		attrs = append(attrs, "provider_ref", hex.EncodeToString(sum[:])[:12])
	}
	return append(attrs, "protocol", protocol)
}
