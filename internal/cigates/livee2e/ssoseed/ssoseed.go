// Package ssoseed is a TEST-ONLY stand-in that seeds one SSO provider row into the live e2e auth stack. go-api has no
// working provider create route (CHAOS-13636: the CRUD is a fail-loud stub), so the live SSO sign-in journeys cannot create
// a provider through the product. This package is deleted when CHAOS-13636 lands and the setup switches to the real routes.
//
// It is linked only by the cigate binary (cmd/cigate), never by a production command: a census cell in
// internal/cigates/livee2e lists every command of ./cmd/... and proves it. Its row type mirrors the production row
// (internal/api/sso providerRow) field for field, and a drift cell in internal/api/sso compares it with the production
// column lists and field types, so a schema change turns that cell red.
package ssoseed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// Provider mirrors internal/api/sso providerRow (field names, order and types) plus EncryptedSecrets, and tags each field
// with its sso_providers column. The column order is the order of the production oidcProviderColumns.
type Provider struct {
	ID                 string     `col:"id"`
	OrgID              string     `col:"org_id"`
	Name               string     `col:"name"`
	Protocol           string     `col:"protocol"`
	Status             string     `col:"status"`
	DefaultRole        string     `col:"default_role"`
	IsDefault          bool       `col:"is_default"`
	AllowIdpInitiated  bool       `col:"allow_idp_initiated"`
	AutoProvision      bool       `col:"auto_provision_users"`
	Config             *string    `col:"config"`
	AllowedDomains     *string    `col:"allowed_domains"`
	LastMetadataSyncAt *time.Time `col:"last_metadata_sync_at"`
	LastLoginAt        *time.Time `col:"last_login_at"`
	LastErrorAt        *time.Time `col:"last_error_at"`
	LastError          *string    `col:"last_error"`
	CreatedAt          time.Time  `col:"created_at"`
	UpdatedAt          time.Time  `col:"updated_at"`
	EncryptedSecrets   *string    `col:"encrypted_secrets"`
}

// Columns is the sso_providers column of every Provider field, in field order (the production oidcProviderColumns order).
func Columns() []string {
	var out []string
	typ := reflect.TypeOf(Provider{})
	for i := 0; i < typ.NumField(); i++ {
		out = append(out, typ.Field(i).Tag.Get("col"))
	}
	return out
}

// ValidProtocol reports the two protocols this stand-in seeds; anything else is refused.
func ValidProtocol(protocol string) bool { return protocol == "oidc" || protocol == "saml" }

// Execer is the part of a pgx pool or connection the insert needs.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Querier is the part of a pgx pool or connection the org lookup needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const insertSQL = `INSERT INTO sso_providers
	(id, org_id, name, protocol, status, default_role, is_default, allow_idp_initiated, auto_provision_users,
	 config, allowed_domains, created_at, updated_at, encrypted_secrets)
VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, $9, $10::json, $11::json, $12, $13, $14::json)`

// Insert writes p as one sso_providers row. last_* columns stay NULL.
func Insert(ctx context.Context, db Execer, p Provider) error {
	if !ValidProtocol(p.Protocol) {
		return fmt.Errorf("ssoseed: unknown protocol %q (want oidc or saml)", p.Protocol)
	}
	if _, err := uuid.Parse(p.ID); err != nil {
		return errors.New("ssoseed: id is not a uuid")
	}
	if _, err := uuid.Parse(p.OrgID); err != nil {
		return errors.New("ssoseed: org id is not a uuid")
	}
	if _, err := db.Exec(ctx, insertSQL, p.ID, p.OrgID, p.Name, p.Protocol, p.Status, p.DefaultRole, p.IsDefault,
		p.AllowIdpInitiated, p.AutoProvision, p.Config, p.AllowedDomains, p.CreatedAt, p.UpdatedAt, p.EncryptedSecrets); err != nil {
		return fmt.Errorf("ssoseed: insert: %w", err)
	}
	return nil
}

// OrgIDForEmail is the organization the user with that e-mail owns or administers (the earliest membership).
func OrgIDForEmail(ctx context.Context, db Querier, email string) (string, error) {
	var org string
	err := db.QueryRow(ctx, `SELECT m.org_id::text FROM memberships m JOIN users u ON u.id = m.user_id
WHERE lower(u.email) = lower($1) AND m.role IN ('owner', 'admin') ORDER BY m.joined_at, m.id LIMIT 1`, email).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", errors.New("ssoseed: no organization is owned or administered by that e-mail")
	}
	if err != nil {
		return "", fmt.Errorf("ssoseed: org lookup: %w", err)
	}
	return org, nil
}

// Seal encrypts the client secret with the production cipher (providerfoundation.FernetDecryptor, the one go-api builds from
// SETTINGS_ENCRYPTION_KEY) and wraps it as the stored encrypted_secrets object {"client_secret": <sealed>}.
func Seal(key, salt, clientSecret string) (string, error) {
	cipher, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		return "", errors.New("ssoseed: the settings encryption key is not usable")
	}
	sealed, err := cipher.Encrypt([]byte(clientSecret))
	if err != nil {
		return "", errors.New("ssoseed: the client secret could not be sealed")
	}
	raw, err := json.Marshal(map[string]string{"client_secret": sealed.Reveal()})
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// OIDCConfig is the sso_providers.config of an OIDC provider, with the keys the production decoder reads.
func OIDCConfig(issuer, clientID, authorizationEndpoint, tokenEndpoint, userinfoEndpoint, jwksURI string) (string, error) {
	return configJSON(map[string]any{
		"client_id": clientID, "issuer": issuer, "authorization_endpoint": authorizationEndpoint,
		"token_endpoint": tokenEndpoint, "userinfo_endpoint": userinfoEndpoint, "jwks_uri": jwksURI,
	})
}

// SAMLConfig is the sso_providers.config of a SAML provider, with the keys the production decoder reads. The decoder accepts an
// sp_acs_url only when its path is /api/v1/auth/saml/<provider id>/acs, so the ACS URL is built from the provider id and a
// base (scheme and host). spEntityID and acsBase are optional: empty ones are left out and the production defaults apply.
func SAMLConfig(providerID, entityID, ssoURL, certificatePEM, spEntityID, acsBase string) (string, error) {
	values := map[string]any{"entity_id": entityID, "sso_url": ssoURL, "certificate": certificatePEM}
	if spEntityID != "" {
		values["sp_entity_id"] = spEntityID
	}
	if acsBase != "" {
		if _, err := uuid.Parse(providerID); err != nil {
			return "", errors.New("ssoseed: provider id is not a uuid")
		}
		values["sp_acs_url"] = strings.TrimRight(acsBase, "/") + "/api/v1/auth/saml/" + providerID + "/acs"
	}
	return configJSON(values)
}

func configJSON(values map[string]any) (string, error) {
	for key, value := range values {
		if text, ok := value.(string); ok && text == "" {
			return "", fmt.Errorf("ssoseed: config value %s is empty", key)
		}
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
