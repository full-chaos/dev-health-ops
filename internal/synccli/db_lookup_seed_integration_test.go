//go:build integration

package synccli

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// startDBLookupPostgres is a PostgreSQL at the head baseline, migrated by the Go migrator (the frozen test has
// no Python), and the base URL the scenarios' URL forms derive from.
func startDBLookupPostgres(ctx context.Context, t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	applyRealSchema(t, ctx, pool)
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsed.RawQuery, parsed.Scheme = "", "postgresql+asyncpg"
	return pool, parsed.String()
}

// seedDBLookup rewrites organizations and integration_credentials the way the Python oracle's seed did: the
// organizations as given, every payload encrypted under the credential's own SETTINGS_ENCRYPTION_KEY/_SALT
// (the v1 PBKDF2 ciphertext, or the legacy SHA-256 derived one).
func seedDBLookup(ctx context.Context, t *testing.T, pool *pgxpool.Pool, scenario dbScenario) {
	t.Helper()
	if _, err := pool.Exec(ctx, "DELETE FROM integration_credentials"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM organizations"); err != nil {
		t.Fatal(err)
	}
	for _, org := range scenario.Orgs {
		created, err := time.Parse(time.RFC3339, org.CreatedAt)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
			VALUES ($1, $2, $2, 'community', 'manual', true, $3, $3)`, org.ID, org.Slug, created); err != nil {
			t.Fatal(err)
		}
	}
	for _, cred := range scenario.Creds {
		var ciphertext *string
		if cred.Raw != nil {
			ciphertext = cred.Raw
		}
		if cred.Payload != nil {
			sealed := sealDBLookupPayload(t, cred)
			ciphertext = &sealed
		}
		provider, name, active := cred.Provider, cred.Name, true
		if provider == "" {
			provider = "github"
		}
		if name == "" {
			name = "default"
		}
		if cred.Active != nil {
			active = *cred.Active
		}
		if _, err := pool.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, '{}'::jsonb, now(), now())`, uuid.New(), cred.OrgID, provider, name, active, ciphertext); err != nil {
			t.Fatal(err)
		}
	}
}

func sealDBLookupPayload(t *testing.T, cred dbCred) string {
	t.Helper()
	key, salt := cred.Env["SETTINGS_ENCRYPTION_KEY"], cred.Env["SETTINGS_ENCRYPTION_SALT"]
	if cred.Legacy {
		return legacyFernetSeal(t, key, []byte(*cred.Payload))
	}
	decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), salt)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := decryptor.Encrypt([]byte(*cred.Payload))
	if err != nil {
		t.Fatal(err)
	}
	return sealed.Reveal()
}

// legacyFernetSeal is Fernet(base64url(sha256(key))).encrypt(payload): the ciphertext the legacy path wrote.
func legacyFernetSeal(t *testing.T, key string, plaintext []byte) string {
	t.Helper()
	digest := sha256.Sum256([]byte(key))
	signingKey, encryptionKey := digest[:16], digest[16:]
	iv := make([]byte, aes.BlockSize)
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	padding := aes.BlockSize - len(plaintext)%aes.BlockSize
	padded := append(append([]byte{}, plaintext...), make([]byte, padding)...)
	for i := len(plaintext); i < len(padded); i++ {
		padded[i] = byte(padding)
	}
	block, err := aes.NewCipher(encryptionKey)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(padded))
	cipher.NewCBCEncrypter(block, iv).CryptBlocks(encrypted, padded)
	token := []byte{0x80}
	timestamp := make([]byte, 8)
	binary.BigEndian.PutUint64(timestamp, uint64(time.Now().Unix()))
	token = append(append(append(token, timestamp...), iv...), encrypted...)
	mac := hmac.New(sha256.New, signingKey)
	mac.Write(token)
	return base64.URLEncoding.EncodeToString(mac.Sum(token))
}
