// Package authv1 embeds the Auth Control Plane credential-classes contract into
// any Go binary that imports it; go:embed cannot reach outside the directory
// that holds the declaring .go file.
package authv1

import _ "embed"

// CredentialClasses is credential-classes.json's exact byte content.
//
//go:embed credential-classes.json
var CredentialClasses []byte
