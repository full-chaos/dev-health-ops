package admin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// `service-credentials create|list|rotate|revoke`: a port of
// src/dev_health_ops/service_credentials.py and the InternalServiceCredential
// model (models/internal_service_credential.py), over the same
// internal_service_credentials table the acr and worker-operator bearer checks
// read. The token is shown once, on stdout; the table keeps its SHA-256 and its
// first 16 characters.

// The internal services a credential can be issued to, and the scopes each may hold.
const (
	ServiceACR            = "acr"
	ServiceWorkerOperator = "worker-operator"

	acrTokenPrefix            = "svc_acr_"
	workerOperatorTokenPrefix = "svc_worker_"

	// MaxOverlapSeconds is how long a rotated credential may stay valid beside its replacement.
	MaxOverlapSeconds = 3600
)

var serviceScopes = map[string][]string{
	ServiceACR:            {"entitlements:read"},
	ServiceWorkerOperator: {"workers:read", "workers:operate"},
}

// ServiceNames are the services `--service` accepts, sorted as argparse's choices are.
func ServiceNames() []string {
	names := make([]string, 0, len(serviceScopes))
	for name := range serviceScopes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ServiceCredentialError is a refusal Python raised as a ValueError: the verb
// prints a traceback and exits 1 with nothing on stdout, so this is reported on
// stderr, never as the "Error: ..." line of the admin verbs.
type ServiceCredentialError struct{ Message string }

func (err *ServiceCredentialError) Error() string { return err.Message }

func credentialRefusal(format string, args ...any) error {
	return &ServiceCredentialError{Message: fmt.Sprintf(format, args...)}
}

// ServiceCredentialScopes is _scopes: sorted(set(raw)), each of which the service may hold.
func ServiceCredentialScopes(service string, raw []string) ([]string, error) {
	seen := map[string]bool{}
	scopes := []string{}
	for _, scope := range raw {
		if !seen[scope] {
			seen[scope] = true
			scopes = append(scopes, scope)
		}
	}
	sort.Strings(scopes) // Python sorts code points; ASCII scope names sort the same, and any other one is refused below
	allowed := map[string]bool{}
	for _, scope := range serviceScopes[service] {
		allowed[scope] = true
	}
	if len(scopes) == 0 {
		return nil, credentialRefusal("unsupported internal service credential scope")
	}
	for _, scope := range scopes {
		if !allowed[scope] {
			return nil, credentialRefusal("unsupported internal service credential scope")
		}
	}
	return scopes, nil
}

// ParseServiceCredentialExpiry is _parse_expiry: nil for no --expires-at; otherwise a
// datetime.fromisoformat value that carries a timezone and lies after now.
func ParseServiceCredentialExpiry(raw *string, now time.Time) (*time.Time, error) {
	if raw == nil {
		return nil, nil
	}
	value, ok := pytime.FromISOFormat(*raw)
	if !ok {
		return nil, credentialRefusal("Invalid isoformat string: %s", pythonparity.StrRepr(*raw))
	}
	if !value.Aware {
		return nil, credentialRefusal("--expires-at must include a timezone")
	}
	expiry := value.Time.UTC()
	if !expiry.After(now) {
		return nil, credentialRefusal("--expires-at must be in the future")
	}
	return &expiry, nil
}

// ParseServiceCredentialCreator is _parse_creator: uuid.UUID(raw) if raw else None.
func ParseServiceCredentialCreator(raw string) (*uuid.UUID, error) {
	if raw == "" {
		return nil, nil
	}
	parsed, err := pythonparity.ParseUUID(raw)
	if err != nil {
		return nil, credentialRefusal("badly formed hexadecimal UUID string")
	}
	return &parsed, nil
}

// ParseServiceCredentialID is uuid.UUID(credential_id).
func ParseServiceCredentialID(raw string) (uuid.UUID, error) {
	parsed, err := pythonparity.ParseUUID(raw)
	if err != nil {
		return uuid.Nil, credentialRefusal("badly formed hexadecimal UUID string")
	}
	return parsed, nil
}

// serviceCredentialRandom is where token bytes come from; a test replaces it.
var serviceCredentialRandom io.Reader = rand.Reader

// generateServiceToken is generate_internal_service_token: the service's prefix and
// secrets.token_urlsafe(32), which is 32 random bytes as unpadded base64url (43 characters).
func generateServiceToken(service string) (string, error) {
	random := make([]byte, 32)
	if _, err := io.ReadFull(serviceCredentialRandom, random); err != nil {
		return "", err
	}
	prefix := acrTokenPrefix
	if service == ServiceWorkerOperator {
		prefix = workerOperatorTokenPrefix
	}
	return prefix + base64.RawURLEncoding.EncodeToString(random), nil
}

// HashServiceToken is hash_internal_service_token: the SHA-256 of the token in hex.
func HashServiceToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func (o Operator) credentialNow() time.Time {
	now := time.Now
	if o.Now != nil {
		now = o.Now
	}
	return now().UTC().Truncate(time.Microsecond) // Python datetimes carry microseconds
}

// ServiceCredentialSpec is what a new credential is issued with, already validated.
type ServiceCredentialSpec struct {
	Service   string
	Scopes    []string
	CreatedBy *uuid.UUID
	ExpiresAt *time.Time
}

func scopesJSON(scopes []string) (string, error) {
	items := make([]any, len(scopes))
	for i, scope := range scopes {
		items[i] = scope
	}
	encoded, err := pythonparity.MarshalPythonJSONSorted(items)
	return string(encoded), err
}

// insertCredential is InternalServiceCredential.issue + session.add: a new row and the token to show.
func insertCredential(ctx context.Context, tx pgx.Tx, spec ServiceCredentialSpec, now time.Time) (string, error) {
	token, err := generateServiceToken(spec.Service)
	if err != nil {
		return "", err
	}
	scopes, err := scopesJSON(spec.Scopes)
	if err != nil {
		return "", err
	}
	var createdBy any
	if spec.CreatedBy != nil {
		createdBy = *spec.CreatedBy
	}
	var expiresAt any
	if spec.ExpiresAt != nil {
		expiresAt = *spec.ExpiresAt
	}
	if _, err := tx.Exec(ctx, `INSERT INTO internal_service_credentials
		(id, service_name, token_hash, token_prefix, scopes, created_by_user_id, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5::json, $6, $7, $8)`,
		uuid.New(), spec.Service, HashServiceToken(token), token[:16], scopes, createdBy, expiresAt, now); err != nil {
		return "", err
	}
	return token, nil
}

// IssueServiceCredential is `service-credentials create`: it returns the token, which is not stored.
func (o Operator) IssueServiceCredential(ctx context.Context, spec ServiceCredentialSpec) (string, error) {
	tx, err := o.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	token, err := insertCredential(ctx, tx, spec, o.credentialNow())
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}

// ServiceCredentialMetadata is public_metadata(): what `list` prints, never a secret.
type ServiceCredentialMetadata struct {
	ID          string
	ServiceName string
	TokenPrefix string
	Scopes      []string
	ExpiresAt   *time.Time
	RevokedAt   *time.Time
	LastUsedAt  *time.Time
}

// ListServiceCredentials is `service-credentials list`: the service's credentials in creation order.
func (o Operator) ListServiceCredentials(ctx context.Context, service string) ([]ServiceCredentialMetadata, error) {
	rows, err := o.Pool.Query(ctx, `SELECT id::text, service_name, token_prefix, scopes::text, expires_at, revoked_at, last_used_at
		FROM internal_service_credentials WHERE service_name = $1 ORDER BY created_at`, service)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []ServiceCredentialMetadata{}
	for rows.Next() {
		var item ServiceCredentialMetadata
		var scopes string
		if err := rows.Scan(&item.ID, &item.ServiceName, &item.TokenPrefix, &scopes, &item.ExpiresAt, &item.RevokedAt, &item.LastUsedAt); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(scopes), &item.Scopes); err != nil {
			return nil, fmt.Errorf("scopes of credential %s: %w", item.ID, err)
		}
		list = append(list, item)
	}
	return list, rows.Err()
}

// Document is the value json.dumps(public_metadata(), sort_keys=True) writes for one credential.
func (item ServiceCredentialMetadata) Document() map[string]any {
	stamp := func(at *time.Time) any {
		if at == nil {
			return nil
		}
		return pytime.ISOFormat(*at)
	}
	scopes := make([]any, len(item.Scopes))
	for i, scope := range item.Scopes {
		scopes[i] = scope
	}
	return map[string]any{
		"id": item.ID, "service_name": item.ServiceName, "token_prefix": item.TokenPrefix, "scopes": scopes,
		"expires_at": stamp(item.ExpiresAt), "revoked_at": stamp(item.RevokedAt), "last_used_at": stamp(item.LastUsedAt),
	}
}

// RotateServiceCredential is `service-credentials rotate`: the credential stays valid for
// overlapSeconds more (from now, however long it had left), and a replacement is issued.
func (o Operator) RotateServiceCredential(ctx context.Context, id uuid.UUID, overlapSeconds int, spec ServiceCredentialSpec) (string, error) {
	tx, err := o.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	// FOR UPDATE: Python read the row without a lock, so two rotations could both pass the
	// validity check; the lock only orders them.
	var service string
	var revokedAt, expiresAt *time.Time
	err = tx.QueryRow(ctx, `SELECT service_name, revoked_at, expires_at FROM internal_service_credentials WHERE id = $1 FOR UPDATE`, id).
		Scan(&service, &revokedAt, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && service != spec.Service) {
		return "", credentialRefusal("service credential not found")
	}
	if err != nil {
		return "", err
	}
	now := o.credentialNow()
	if revokedAt != nil || (expiresAt != nil && now.After(*expiresAt)) {
		return "", credentialRefusal("service credential is not active")
	}
	if _, err := tx.Exec(ctx, `UPDATE internal_service_credentials SET expires_at = $2 WHERE id = $1`, id, now.Add(time.Duration(overlapSeconds)*time.Second)); err != nil {
		return "", err
	}
	token, err := insertCredential(ctx, tx, spec, o.credentialNow())
	if err != nil {
		return "", err
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return token, nil
}

// RevokeServiceCredential is `service-credentials revoke`: revoked_at is set to now, also on a
// credential that was already revoked, as Python does.
func (o Operator) RevokeServiceCredential(ctx context.Context, id uuid.UUID) error {
	tag, err := o.Pool.Exec(ctx, `UPDATE internal_service_credentials SET revoked_at = $2 WHERE id = $1`, id, o.credentialNow())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return credentialRefusal("service credential not found")
	}
	return nil
}
