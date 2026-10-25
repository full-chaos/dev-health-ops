package authv1

import _ "embed"

// GoAPIEndpointProfiles is endpoint-profiles.go-api.json's exact byte content:
// the go-api endpoint-profile inventory.
//
//go:embed endpoint-profiles.go-api.json
var GoAPIEndpointProfiles []byte
