package contracts

import (
	"encoding/json"
	"fmt"
)

// DiscoverySurface names the discovery.v1 wire contract.
const DiscoverySurface = "discovery.v1"

// DiscoveryJWKSPath is the path directly beneath the issuer where its jwks.v1
// document is served (TRD section 15).
const DiscoveryJWKSPath = "/.well-known/jwks.json"

// PlatformTokenKeyUse is the dev_health_key_use of the platform access and
// workload token issuer. TRD section 12 keeps ACR Web-assertion keys on a
// separate issuer and key use.
const PlatformTokenKeyUse = "platform_token"

// Discovery is one discovery.v1 document, validated and bound to a trusted
// issuer.
type Discovery struct {
	Issuer                           string   `json:"issuer"`
	JWKSURI                          string   `json:"jwks_uri"`
	IDTokenSigningAlgValuesSupported []string `json:"id_token_signing_alg_values_supported"`
	KeyUse                           string   `json:"dev_health_key_use"`
}

// ParseDiscovery validates a discovery document fetched for trustedIssuer.
//
// The two checks the schema cannot make live here, and trustedIssuer is a
// required parameter so a caller cannot skip them: JSON Schema compares nothing
// across members and knows nothing of configuration.
//
//   - the document's issuer must equal trustedIssuer byte for byte (OpenID
//     Connect Discovery section 4.3). A well-formed document from another
//     issuer is the wrong-issuer attack, not a valid one.
//   - jwks_uri must equal issuer + DiscoveryJWKSPath. Equality, not a prefix or
//     host test: a sibling path or another port on the same host is another
//     tenant's key set.
//
// Duplicate members are refused before decoding for the reason given on
// refuseDuplicateMembers.
func ParseDiscovery(root string, raw []byte, trustedIssuer string) (Discovery, error) {
	if err := refuseDuplicateMembers(raw); err != nil {
		return Discovery{}, fmt.Errorf("%s: %w", DiscoverySurface, err)
	}

	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		return Discovery{}, fmt.Errorf("%s: document is not valid JSON: %w", DiscoverySurface, err)
	}
	if err := Validate(root, DiscoverySurface, document); err != nil {
		return Discovery{}, err
	}

	var discovery Discovery
	if err := json.Unmarshal(raw, &discovery); err != nil {
		return Discovery{}, fmt.Errorf("%s: decoding validated document: %w", DiscoverySurface, err)
	}

	if discovery.Issuer != trustedIssuer {
		return Discovery{}, fmt.Errorf(
			"%s: document issuer %q is not the trusted issuer %q; refusing rather than "+
				"following a jwks_uri that a different issuer published",
			DiscoverySurface, discovery.Issuer, trustedIssuer)
	}
	if want := discovery.Issuer + DiscoveryJWKSPath; discovery.JWKSURI != want {
		return Discovery{}, fmt.Errorf(
			"%s: jwks_uri %q is not %q; the issuer disagrees with its own endpoint URL",
			DiscoverySurface, discovery.JWKSURI, want)
	}
	return discovery, nil
}
