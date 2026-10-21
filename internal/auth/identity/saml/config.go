// Package saml is the auth-service SAML 2.0 service provider: the SP metadata,
// the SP-initiated AuthnRequest, and the assertion consumer that turns a
// verified assertion into a sign-in.
//
// Signature, issuer, audience, recipient, destination, time, request and
// replay checks are made here explicitly, and github.com/crewjam/saml is run
// afterwards as a second check. Claims are read only from the XML that the
// signature verification returned.
//
// A SAML identity is keyed by (issuer = IdP entity ID, provider = the
// sso_providers row id, subject = NameID with its Format and SPNameQualifier).
// An email attribute never selects an account.
package saml

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
)

// DefaultNameIDFormat is the NameIDPolicy format when the provider config
// names none.
const DefaultNameIDFormat = "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"

// ErrConfig reports a provider config that cannot be used.
var ErrConfig = errors.New("saml: invalid provider config")

// Config is the SAML part of an sso_providers row's config column.
type Config struct {
	EntityID         string
	SSOURL           string
	Certificate      string
	SPEntityID       string
	SPACSURL         string
	NameIDFormat     string
	AttributeMapping map[string]string
}

// Provider is one sso_providers row with protocol "saml".
type Provider struct {
	ID                uuid.UUID
	OrgID             uuid.UUID
	Protocol          string
	Status            string
	AllowIdPInitiated bool
	AutoProvision     bool
	Config            Config
	AllowedDomains    []string
}

// ParseConfig reads the config JSON object. A missing or null column is an
// empty config; any other non-object value, or a non-string attribute mapping
// value, is ErrConfig. sp_entity_id must be an absolute URI and sp_acs_url an
// https (or loopback http) URL when they are set.
func ParseConfig(raw []byte) (Config, error) {
	fields := map[string]json.RawMessage{}
	trimmed := strings.TrimSpace(string(raw))
	if trimmed != "" && trimmed != "null" {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return Config{}, fmt.Errorf("%w: config is not a JSON object", ErrConfig)
		}
	}
	cfg := Config{
		EntityID:    jsonString(fields["entity_id"]),
		SSOURL:      jsonString(fields["sso_url"]),
		Certificate: jsonString(fields["certificate"]),
		SPEntityID:  jsonString(fields["sp_entity_id"]),
		SPACSURL:    jsonString(fields["sp_acs_url"]),
	}
	cfg.NameIDFormat = jsonString(fields["name_id_format"])
	if cfg.NameIDFormat == "" {
		cfg.NameIDFormat = DefaultNameIDFormat
	}
	cfg.AttributeMapping = map[string]string{}
	if rawMapping, ok := fields["attribute_mapping"]; ok && string(rawMapping) != "null" {
		var mapping map[string]json.RawMessage
		if err := json.Unmarshal(rawMapping, &mapping); err != nil {
			return Config{}, fmt.Errorf("%w: attribute_mapping is not an object", ErrConfig)
		}
		for k, v := range mapping {
			var s string
			if err := json.Unmarshal(v, &s); err != nil {
				return Config{}, fmt.Errorf("%w: attribute_mapping value is not a string", ErrConfig)
			}
			cfg.AttributeMapping[k] = s
		}
	}
	if cfg.SPEntityID != "" {
		if u, err := url.Parse(cfg.SPEntityID); err != nil || u.Scheme == "" {
			return Config{}, fmt.Errorf("%w: sp_entity_id must be an absolute URI", ErrConfig)
		}
	}
	if cfg.SPACSURL != "" && !httpsOrLoopback(cfg.SPACSURL) {
		return Config{}, fmt.Errorf("%w: sp_acs_url must be https", ErrConfig)
	}
	return cfg, nil
}

// ParseAllowedDomains reads the allowed_domains JSON column: null or absent is
// no restriction, a list keeps its string items and skips null items, and
// anything else is ErrConfig.
func ParseAllowedDomains(raw []byte) ([]string, error) {
	trimmed := strings.TrimSpace(string(raw))
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, fmt.Errorf("%w: allowed_domains is not a list", ErrConfig)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if string(item) == "null" {
			continue
		}
		var s string
		if err := json.Unmarshal(item, &s); err != nil {
			return nil, fmt.Errorf("%w: allowed_domains item is not a string", ErrConfig)
		}
		out = append(out, s)
	}
	return out, nil
}

func jsonString(raw json.RawMessage) string {
	var s string
	if len(raw) == 0 || json.Unmarshal(raw, &s) != nil {
		return ""
	}
	return s
}

func httpsOrLoopback(candidate string) bool {
	u, err := url.Parse(candidate)
	if err != nil || u.User != nil {
		return false
	}
	switch {
	case strings.EqualFold(u.Scheme, "https") && u.Host != "":
		return true
	case strings.EqualFold(u.Scheme, "http"):
		switch u.Hostname() {
		case "localhost", "127.0.0.1", "::1":
			return true
		}
	}
	return false
}

// SPEntityID is the configured SP entity ID, else
// <base>/saml/<provider id>/metadata.
func (p Provider) SPEntityID(base string) string {
	if p.Config.SPEntityID != "" {
		return p.Config.SPEntityID
	}
	return base + "/saml/" + p.ID.String() + "/metadata"
}

// ACSURL is the configured assertion consumer URL, else
// <base>/saml/<provider id>/acs.
func (p Provider) ACSURL(base string) string {
	if p.Config.SPACSURL != "" {
		return p.Config.SPACSURL
	}
	return base + "/saml/" + p.ID.String() + "/acs"
}

// DomainAllowed reports whether email's domain is in the provider's allowed
// list, compared through normalizeDomain. An empty list allows every domain.
func (p Provider) DomainAllowed(email string) bool {
	if len(p.AllowedDomains) == 0 {
		return true
	}
	at := strings.LastIndex(email, "@")
	if at < 0 {
		return false
	}
	domain := normalizeDomain(email[at+1:])
	for _, allowed := range p.AllowedDomains {
		if normalizeDomain(allowed) == domain {
			return true
		}
	}
	return false
}

// normalizeDomain is the one form every email domain compare in this package
// uses: trimmed and lower case.
func normalizeDomain(d string) string { return strings.ToLower(strings.TrimSpace(d)) }

// ValidateACSOverride refuses an sp_acs_url override that is not this row's ACS
// URL: its path must end with /saml/{row id}/acs. A proxy prefix in front of
// that is allowed. The suffix must hold both decoded and as written: an escaped
// separator does not match the served route.
func ValidateACSOverride(cfg Config, rowID uuid.UUID) error {
	if cfg.SPACSURL == "" {
		return nil
	}
	u, err := url.Parse(cfg.SPACSURL)
	if err != nil || !strings.HasSuffix(u.Path, acsPathSuffix(rowID)) || !strings.HasSuffix(u.EscapedPath(), acsPathSuffix(rowID)) {
		return fmt.Errorf("%w: sp_acs_url must end with /saml/{provider id}/acs of this provider", ErrConfig)
	}
	return nil
}

func acsPathSuffix(rowID uuid.UUID) string { return "/saml/" + rowID.String() + "/acs" }
