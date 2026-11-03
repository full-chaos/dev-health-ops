package oauthgrant

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// Client is one row of the static client registry (oauth-clients.v1). It is the
// trusted source of a client's class and first-party status: nothing a client
// supplies can set either (ACP-ADR-13 decision 7).
type Client struct {
	ID         string   `json:"client_id"`
	Name       string   `json:"client_name"`
	Class      string   `json:"client_class"`
	FirstParty bool     `json:"first_party"`
	Audiences  []string `json:"audiences"`
	// RedirectPath is a first-party row's redirect, a path resolved against
	// AUTH_OAUTH_WEB_ORIGIN.
	RedirectPath string `json:"redirect_path,omitempty"`
	// RedirectURIs are a fixed-URI row's exact redirects.
	RedirectURIs []string `json:"redirect_uris,omitempty"`
}

// Registry is the parsed client registry.
type Registry struct {
	clients map[string]Client
}

// LoadRegistry parses the embedded registry. Its schema is enforced by the contract
// tests, on the same bytes; this parse refuses what a schema cannot: a repeated
// client_id.
func LoadRegistry() (Registry, error) {
	return ParseRegistry(authv1.OAuthClients)
}

// ParseRegistry parses a registry document. LoadRegistry is the production caller.
func ParseRegistry(raw []byte) (Registry, error) {
	var doc struct {
		SchemaVersion string   `json:"schema_version"`
		Clients       []Client `json:"clients"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&doc); err != nil {
		return Registry{}, fmt.Errorf("oauthgrant: the client registry: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Registry{}, errors.New("oauthgrant: the client registry has trailing data")
	}
	if doc.SchemaVersion != "oauth-clients.v1" {
		return Registry{}, fmt.Errorf("oauthgrant: the client registry is %q", doc.SchemaVersion)
	}
	clients := make(map[string]Client, len(doc.Clients))
	for _, c := range doc.Clients {
		if _, repeated := clients[c.ID]; repeated {
			return Registry{}, fmt.Errorf("oauthgrant: client %q is listed twice", c.ID)
		}
		clients[c.ID] = c
	}
	return Registry{clients: clients}, nil
}

// Client returns one row.
func (r Registry) Client(id string) (Client, bool) {
	c, ok := r.clients[id]
	return c, ok
}
