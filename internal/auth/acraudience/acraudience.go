// Package acraudience names the two ACR user access token audiences
// (CHAOS-12248) and the refusal every platform token verifier and minter
// returns for them: no ops platform token (an HS256 edge token, an
// effective-principal envelope, a session token) is minted or accepted with an
// ACR audience (ACP-ADR-13). It imports nothing from ops, so every token
// package can use it without an import cycle. contracts.ACRAPIAudience and
// contracts.ACRMCPAudience equal these constants (pinned by a contracts test).
package acraudience

import "fmt"

// API and MCP are the audiences acr-api and acr-mcp accept, each its own only.
const (
	API = "acr-api"
	MCP = "acr-mcp"
)

// Is reports whether audience is an ACR audience.
func Is(audience string) bool { return audience == API || audience == MCP }

// Refuse is nil for any audience but an ACR one, and for an ACR audience the
// error owner (the refusing package) returns.
func Refuse(owner, audience string) error {
	if Is(audience) {
		return fmt.Errorf("%s: audience %s is an ACR audience; no platform token carries one (ACP-ADR-13)", owner, audience)
	}
	return nil
}
