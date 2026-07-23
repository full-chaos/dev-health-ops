package providerfoundation

import (
	"context"
	"encoding/json"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
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

var _ CredentialRepository = PostgresCredentialRepository{}
