// Package authv1 embeds the Auth Control Plane credential-classes contract and
// the OAuth client registry into any Go binary that imports it; go:embed cannot
// reach outside the directory that holds the declaring .go file.
package authv1

import _ "embed"

// CredentialClasses is credential-classes.json's exact byte content.
//
//go:embed credential-classes.json
var CredentialClasses []byte

// OAuthClients is oauth-clients.v1.json's exact byte content.
//
//go:embed oauth-clients.v1.json
var OAuthClients []byte

// AcrCredentialLifecycleEventSchema is the acr-credential-lifecycle-event.v1
// schema's exact byte content: the acr events route validates with it, and the
// service image holds no repository to read it from.
//
//go:embed jsonschema/acr-credential-lifecycle-event.v1.schema.json
var AcrCredentialLifecycleEventSchema []byte
