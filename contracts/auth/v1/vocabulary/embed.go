// Package vocabulary embeds the Auth Control Plane vocabulary files into any
// Go binary that imports it; go:embed cannot reach outside the directory that
// holds the declaring .go file.
package vocabulary

import _ "embed"

// ActionsV1 is actions.v1.json's exact byte content.
//
//go:embed actions.v1.json
var ActionsV1 []byte

// RolesV1 is roles.v1.json's exact byte content.
//
//go:embed roles.v1.json
var RolesV1 []byte
