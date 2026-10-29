package authv1

import _ "embed"

// QueryAPIEndpointProfiles is endpoint-profiles.query-api.json's exact byte
// content: the query-api endpoint-profile inventory.
//
//go:embed endpoint-profiles.query-api.json
var QueryAPIEndpointProfiles []byte
