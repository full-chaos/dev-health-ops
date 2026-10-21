package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Provider is one public.sso_providers row, as the flow reads it. The table
// stays a go-api aggregate: this package only reads it.
type Provider struct {
	ID               uuid.UUID
	OrgID            uuid.UUID
	Protocol         string
	Status           string
	AutoProvision    bool
	Config           ProviderConfig // an OIDC row
	OAuth            OAuthConfig    // an OAuth row
	AllowedDomains   []string
	EncryptedSecrets string // the raw JSON column; "" when NULL
}

// ProviderConfig is the OIDC part of sso_providers.config.
type ProviderConfig struct {
	ClientID, Issuer, AuthorizationEndpoint, TokenEndpoint, UserinfoEndpoint, JWKSURI string
	Scopes                                                                            []string
	ClaimMapping                                                                      map[string]string
}

var defaultScopes = []string{"openid", "profile", "email"}

var errProviderShape = errors.New("oidc: stored provider column has the wrong JSON shape")

// ProviderSource reads one provider row in one statement, its stored columns
// undecoded, so a request checks and decodes one version of the row. A
// missing row is ErrProviderNotFound.
//
// DefaultProviderRow picks an organization's active row of one protocol: the
// default row first, then the oldest.
type ProviderSource interface {
	ProviderRow(ctx context.Context, id uuid.UUID) (StoredProvider, error)
	DefaultProviderRow(ctx context.Context, orgID uuid.UUID, protocol string) (StoredProvider, error)
}

// StoredProvider is one public.sso_providers row as read: the head the
// provider checks use, and the stored JSON columns as text, decoded only by
// decodeProvider after the checks pass. A NULL column is nil.
type StoredProvider struct {
	ID, OrgID                        uuid.UUID
	Protocol, Status                 string
	AutoProvision                    bool
	Config                           string
	AllowedDomains, EncryptedSecrets *string
}

// ErrProviderNotFound reports an unknown provider id.
var ErrProviderNotFound = errors.New("oidc: provider not found")

// ErrProviderConfig reports a stored provider row this flow cannot read.
var ErrProviderConfig = errors.New("oidc: stored provider configuration is malformed")

// PGProviders reads public.sso_providers. The runtime role needs column SELECT
// on the columns below and nothing else.
type PGProviders struct{ Pool *pgxpool.Pool }

func (p PGProviders) ProviderRow(ctx context.Context, id uuid.UUID) (StoredProvider, error) {
	return scanStoredProvider(p.Pool.QueryRow(ctx, `SELECT id, org_id, protocol, status, auto_provision_users, config::text,
	allowed_domains::text, encrypted_secrets::text
FROM public.sso_providers WHERE id = $1`, id))
}

func (p PGProviders) DefaultProviderRow(ctx context.Context, orgID uuid.UUID, protocol string) (StoredProvider, error) {
	return scanStoredProvider(p.Pool.QueryRow(ctx, `SELECT id, org_id, protocol, status, auto_provision_users, config::text,
	allowed_domains::text, encrypted_secrets::text
FROM public.sso_providers
WHERE org_id = $1 AND protocol = $2 AND status = 'active'
ORDER BY is_default DESC, created_at, id LIMIT 1`, orgID, protocol))
}

func scanStoredProvider(r pgx.Row) (StoredProvider, error) {
	var row StoredProvider
	err := r.Scan(&row.ID, &row.OrgID, &row.Protocol, &row.Status, &row.AutoProvision, &row.Config,
		&row.AllowedDomains, &row.EncryptedSecrets)
	if errors.Is(err, pgx.ErrNoRows) {
		return StoredProvider{}, ErrProviderNotFound
	}
	if err != nil {
		return StoredProvider{}, err
	}
	return row, nil
}

// decodeProvider decodes a stored row's JSON columns; a column it cannot read
// is ErrProviderConfig.
func decodeProvider(stored StoredProvider) (Provider, error) {
	row := Provider{ID: stored.ID, OrgID: stored.OrgID, Protocol: stored.Protocol, Status: stored.Status,
		AutoProvision: stored.AutoProvision}
	var err error
	if _, oauth := oauthProviderOf(row.Protocol); oauth {
		if row.OAuth, err = decodeOAuthConfig(stored.Config); err == nil {
			err = checkOAuthBase(row.Protocol, row.OAuth)
		}
	} else {
		row.Config, err = decodeProviderConfig(stored.Config)
	}
	if err != nil {
		return Provider{}, fmt.Errorf("%w: config: %w", ErrProviderConfig, err)
	}
	if stored.AllowedDomains != nil {
		if row.AllowedDomains, err = decodeAllowedDomains(*stored.AllowedDomains); err != nil {
			return Provider{}, fmt.Errorf("%w: allowed_domains: %w", ErrProviderConfig, err)
		}
	}
	if stored.EncryptedSecrets != nil {
		row.EncryptedSecrets = *stored.EncryptedSecrets
	}
	return row, nil
}

// decodeProviderConfig is go-api decodeOIDCConfig: an absent or null config is
// empty; a present config that is not an object, or a key with the wrong type,
// is a shape error.
func decodeProviderConfig(raw string) (ProviderConfig, error) {
	var object map[string]any
	if err := decodeJSONObject(raw, &object); err != nil {
		return ProviderConfig{}, err
	}
	cfg := ProviderConfig{
		ClientID: objString(object, "client_id"), Issuer: objString(object, "issuer"),
		AuthorizationEndpoint: objString(object, "authorization_endpoint"),
		TokenEndpoint:         objString(object, "token_endpoint"),
		UserinfoEndpoint:      objString(object, "userinfo_endpoint"),
		JWKSURI:               objString(object, "jwks_uri"),
		Scopes:                defaultScopes,
		ClaimMapping:          map[string]string{},
	}
	if value, ok := object["scopes"]; ok {
		items, ok := value.([]any)
		if !ok {
			return ProviderConfig{}, fmt.Errorf("%w: scopes", errProviderShape)
		}
		cfg.Scopes = make([]string, 0, len(items))
		for _, item := range items {
			text, ok := item.(string)
			if !ok {
				return ProviderConfig{}, fmt.Errorf("%w: scopes item", errProviderShape)
			}
			cfg.Scopes = append(cfg.Scopes, text)
		}
	}
	if value, ok := object["claim_mapping"]; ok {
		nested, ok := value.(map[string]any)
		if !ok {
			return ProviderConfig{}, fmt.Errorf("%w: claim_mapping", errProviderShape)
		}
		for k, v := range nested {
			text, ok := v.(string)
			if !ok {
				return ProviderConfig{}, fmt.Errorf("%w: claim_mapping value", errProviderShape)
			}
			cfg.ClaimMapping[k] = text
		}
	}
	return cfg, nil
}

func decodeJSONObject(raw string, out *map[string]any) error {
	if strings.TrimSpace(raw) == "" || strings.TrimSpace(raw) == "null" {
		*out = map[string]any{}
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return err
	}
	switch value := decoded.(type) {
	case nil:
		*out = map[string]any{}
	case map[string]any:
		*out = value
	default:
		return fmt.Errorf("%w: config", errProviderShape)
	}
	return nil
}

func objString(object map[string]any, key string) string {
	text, _ := object[key].(string)
	return text
}

// decodeAllowedDomains is go-api decodeAllowedDomains: a list of strings, null
// items skipped.
func decodeAllowedDomains(raw string) ([]string, error) {
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, err
	}
	if decoded == nil {
		return nil, nil
	}
	items, ok := decoded.([]any)
	if !ok {
		return nil, fmt.Errorf("%w: allowed_domains", errProviderShape)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		switch value := item.(type) {
		case string:
			out = append(out, value)
		case nil:
		default:
			return nil, fmt.Errorf("%w: allowed_domains item", errProviderShape)
		}
	}
	return out, nil
}

// SecretDecrypter decrypts sso_providers.encrypted_secrets values.
type SecretDecrypter interface {
	Decrypt(stored string) (string, error)
}

// clientSecret is go-api decryptProviderSecret: decrypt the stored value, and
// fall back to it as stored when no decrypter is configured or decryption
// fails (a legacy plaintext value).
func clientSecret(dec SecretDecrypter, raw string) string {
	var object map[string]any
	if err := decodeJSONObject(raw, &object); err != nil {
		return ""
	}
	text, _ := object["client_secret"].(string)
	if text == "" || dec == nil {
		return text
	}
	plain, err := dec.Decrypt(text)
	if err != nil {
		return text
	}
	return plain
}
