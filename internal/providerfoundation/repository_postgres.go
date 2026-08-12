package providerfoundation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresCredentialRepository reads the existing Python-owned
// integration_credentials table. It only returns ciphertext; decrypting is
// deliberately kept at the claimed worker boundary in CredentialResolver.
type PostgresCredentialRepository struct{ Pool *pgxpool.Pool }

func (r PostgresCredentialRepository) ResolveEncrypted(ctx context.Context, scope TenantScope) (EncryptedCredential, error) {
	if r.Pool == nil {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	if err := scope.Validate(); err != nil {
		return EncryptedCredential{}, err
	}
	scope = scope.normalized()
	query := `SELECT id::text, provider, name, is_active, credentials_encrypted, COALESCE(config::text, '{}')
FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND is_active = TRUE`
	args := []any{scope.OrgID, scope.Provider}
	if scope.CredentialID != "" {
		query += " AND id = $3::uuid"
		args = append(args, scope.CredentialID)
	} else if scope.CredentialName != "" {
		query += " AND name = $3"
		args = append(args, scope.CredentialName)
	} else {
		query += " ORDER BY CASE WHEN name = 'default' THEN 0 ELSE 1 END, name LIMIT 2"
	}
	rows, err := r.Pool.Query(ctx, query, args...)
	if err != nil {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	defer rows.Close()
	var matches []EncryptedCredential
	for rows.Next() {
		var record EncryptedCredential
		var cipherText string
		var configJSON []byte
		if err := rows.Scan(&record.ID, &record.Provider, &record.Name, &record.Active, &cipherText, &configJSON); err != nil {
			return EncryptedCredential{}, ErrCredentialNotFound
		}
		if !record.Active || cipherText == "" {
			continue
		}
		record.Ciphertext = secrets.NewValue(cipherText)
		record.Config = map[string]string{}
		if err := decodeConfig(configJSON, record.Config); err != nil {
			return EncryptedCredential{}, ErrCredentialInvalid
		}
		matches = append(matches, record)
	}
	if err := rows.Err(); err != nil || len(matches) == 0 {
		return EncryptedCredential{}, ErrCredentialNotFound
	}
	if scope.CredentialID == "" && scope.CredentialName == "" {
		for _, match := range matches {
			if match.Name == "default" {
				return match, nil
			}
		}
		if len(matches) != 1 {
			return EncryptedCredential{}, ErrCredentialInvalid
		}
	}
	return matches[0], nil
}

func decodeConfig(raw []byte, target map[string]string) error {
	// Config is intentionally non-secret. Avoiding a generic map in the public
	// credential keeps accidental value logging less likely in provider code.
	var values map[string]any
	if err := json.Unmarshal(raw, &values); err != nil {
		return err
	}
	for key, value := range values {
		if text, ok := value.(string); ok {
			target[key] = text
		}
	}
	return nil
}

// PostgresPagerDutyOAuthTokenRepository reads and rotates only the encrypted
// OAuth token row referenced by a tokenless integration descriptor.
type PostgresPagerDutyOAuthTokenRepository struct{ Pool *pgxpool.Pool }

func (r PostgresPagerDutyOAuthTokenRepository) Load(
	ctx context.Context,
	orgID string,
	credentialName string,
) (PagerDutyOAuthTokenRecord, error) {
	if r.Pool == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(credentialName) == "" {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialInvalid
	}
	var (
		ciphertext string
		version    int
		bindingID  *string
	)
	err := r.Pool.QueryRow(ctx, `
SELECT token_encrypted, version, binding_id
FROM provider_oauth_credentials
WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`,
		orgID, credentialName,
	).Scan(&ciphertext, &version, &bindingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialNotFound
	}
	if err != nil {
		return PagerDutyOAuthTokenRecord{}, &ProviderError{Class: ErrorTransient}
	}
	if bindingID == nil || strings.TrimSpace(*bindingID) == "" ||
		strings.TrimSpace(ciphertext) == "" || version < 1 {
		return PagerDutyOAuthTokenRecord{}, ErrCredentialInvalid
	}
	return PagerDutyOAuthTokenRecord{
		Ciphertext: secrets.NewValue(ciphertext), Version: version,
		BindingID: *bindingID,
	}, nil
}

func (r PostgresPagerDutyOAuthTokenRepository) Rotate(
	ctx context.Context,
	orgID string,
	credentialName string,
	rotation PagerDutyOAuthTokenRotation,
) (bool, error) {
	if r.Pool == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(credentialName) == "" ||
		rotation.ExpectedVersion < 1 ||
		strings.TrimSpace(rotation.ExpectedBindingID) == "" ||
		!rotation.Ciphertext.Configured() || rotation.ExpiresAt.IsZero() {
		return false, ErrCredentialInvalid
	}
	scopes, err := json.Marshal(normalizedPagerDutyScopes(rotation.GrantedScopes))
	if err != nil {
		return false, ErrCredentialInvalid
	}
	var version int
	err = r.Pool.QueryRow(ctx, `
UPDATE provider_oauth_credentials
SET token_encrypted = $1,
    version = version + 1,
    expires_at = $2,
    granted_scopes = $3::json,
    has_refresh_token = $4,
    updated_at = $5
WHERE org_id = $6
  AND provider = 'pagerduty'
  AND credential_name = $7
  AND version = $8
  AND binding_id = $9
RETURNING version`,
		rotation.Ciphertext.Reveal(), rotation.ExpiresAt, string(scopes),
		rotation.HasRefreshToken, time.Now().UTC(), orgID, credentialName,
		rotation.ExpectedVersion, rotation.ExpectedBindingID,
	).Scan(&version)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, &ProviderError{Class: ErrorTransient}
	}
	return version == rotation.ExpectedVersion+1, nil
}

var _ CredentialRepository = PostgresCredentialRepository{}
var _ PagerDutyOAuthTokenRepository = PostgresPagerDutyOAuthTokenRepository{}
