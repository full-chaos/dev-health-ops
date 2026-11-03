package authv1

import _ "embed"

// AuthServiceEndpointProfiles is endpoint-profiles.auth-service.json's exact
// byte content.
//
//go:embed endpoint-profiles.auth-service.json
var AuthServiceEndpointProfiles []byte
