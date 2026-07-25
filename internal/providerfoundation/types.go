// Package providerfoundation owns the shared Go-side provider boundary.
//
// It deliberately contains no provider dataset implementations.  Dataset
// workers construct one of the explicit clients in this package after they
// have claimed a sync unit; they then normalize provider responses and write
// them through a sink.  Keeping this boundary small prevents a provider
// credential, raw response, or queue payload from leaking across tenants.
package providerfoundation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

var (
	ErrInvalidScope         = errors.New("invalid provider tenant scope")
	ErrCredentialNotFound   = errors.New("provider credential not found")
	ErrCredentialInactive   = errors.New("provider credential inactive")
	ErrCredentialInvalid    = errors.New("provider credential is invalid")
	ErrLeaseLost            = errors.New("provider lease is no longer valid")
	ErrBudgetUnavailable    = errors.New("provider budget unavailable")
	ErrSinkDuplicate        = errors.New("provider sink duplicate has different content")
	ErrSinkGenerationUnsafe = errors.New("provider sink generation is not safely deduplicated")
	ErrSinkReplayConflict   = errors.New("provider sink generation replay has different content")
	ErrNormalizationInvalid = errors.New("provider normalized record is invalid")
)

// TenantScope is derived from a claimed sync unit, never from a provider
// response or an untrusted request body. CredentialID is optional only while
// resolving the legacy default/single-active credential fallback.
type TenantScope struct {
	OrgID          string
	Provider       string
	IntegrationID  string
	CredentialID   string
	CredentialName string
}

func (s TenantScope) Validate() error {
	if strings.TrimSpace(s.OrgID) == "" || strings.TrimSpace(s.Provider) == "" || strings.TrimSpace(s.IntegrationID) == "" {
		return ErrInvalidScope
	}
	return nil
}

func (s TenantScope) normalized() TenantScope {
	s.Provider = strings.ToLower(strings.TrimSpace(s.Provider))
	s.CredentialName = strings.TrimSpace(s.CredentialName)
	return s
}

// LeaseGuard makes claim validation explicit at every security boundary. A
// sync worker must check it before decryption, before a network request, and
// before a sink write; the concrete sync-unit service supplies the CAS-backed
// implementation in CHAOS-3045/3046.
type LeaseGuard interface{ Assert(context.Context) error }

type LeaseGuardFunc func(context.Context) error

func (f LeaseGuardFunc) Assert(ctx context.Context) error {
	if f == nil {
		return ErrLeaseLost
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := f(ctx); err != nil {
		return ErrLeaseLost
	}
	return nil
}

// Credential carries secret material in secrets.Value. Its fields are
// intentionally private; callers can only reveal a named secret at the
// concrete HTTP-auth boundary and cannot accidentally serialize the map.
type Credential struct {
	Provider string
	ID       string
	Name     string
	Config   map[string]string
	fields   map[string]secrets.Value
}

func (c Credential) Secret(name string) (secrets.Value, bool) {
	v, ok := c.fields[name]
	return v, ok
}

// WithEphemeralSecret returns a copy augmented with a short-lived secret from
// an explicit token repository. It lets OAuth hydration remain outside the
// process environment without mutating the persisted credential descriptor.
func (c Credential) WithEphemeralSecret(name string, value secrets.Value) (Credential, error) {
	if strings.TrimSpace(name) == "" || !value.Configured() {
		return Credential{}, ErrCredentialInvalid
	}
	fields := make(map[string]secrets.Value, len(c.fields)+1)
	for key, existing := range c.fields {
		fields[key] = existing
	}
	fields[name] = value
	c.fields = fields
	return c, nil
}

func (c Credential) SafeAttributes() map[string]any {
	return map[string]any{
		"provider": c.Provider, "credential_id_configured": c.ID != "",
		"credential_name_configured": c.Name != "", "credential_field_count": len(c.fields),
	}
}

// EncryptedCredential is the only representation a repository may return.
// Ciphertext is never included in error text, metrics, or an envelope.
type EncryptedCredential struct {
	ID         string
	Provider   string
	Name       string
	Active     bool
	Ciphertext secrets.Value
	Config     map[string]string
}

type CredentialRepository interface {
	ResolveEncrypted(context.Context, TenantScope) (EncryptedCredential, error)
}

type CredentialDecryptor interface {
	Decrypt(secrets.Value) ([]byte, error)
}

// CredentialResolver has no environment dependency. The encryption key is
// supplied once by process construction using the existing secret loader.
type CredentialResolver struct {
	Repository CredentialRepository
	Decryptor  CredentialDecryptor
}

func (r CredentialResolver) Resolve(ctx context.Context, lease LeaseGuard, scope TenantScope) (Credential, error) {
	if r.Repository == nil || r.Decryptor == nil || lease == nil {
		return Credential{}, ErrCredentialInvalid
	}
	scope = scope.normalized()
	if err := scope.Validate(); err != nil {
		return Credential{}, err
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	record, err := r.Repository.ResolveEncrypted(ctx, scope)
	if err != nil {
		return Credential{}, err
	}
	if !record.Active {
		return Credential{}, ErrCredentialInactive
	}
	if record.Provider != scope.Provider || !record.Ciphertext.Configured() ||
		(scope.CredentialID != "" && record.ID != scope.CredentialID) {
		return Credential{}, ErrCredentialInvalid
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	plain, err := r.Decryptor.Decrypt(record.Ciphertext)
	if err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	credential, err := decodeCredential(record, plain)
	if err != nil {
		return Credential{}, err
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	return credential, nil
}

// ErrorClass is stable across providers and is safe for logs/metrics.
type ErrorClass string

const (
	ErrorAuthentication ErrorClass = "authentication"
	ErrorNotFound       ErrorClass = "not_found"
	ErrorConflict       ErrorClass = "conflict"
	ErrorRateLimited    ErrorClass = "rate_limited"
	ErrorTransient      ErrorClass = "transient"
	ErrorCancelled      ErrorClass = "cancelled"
	ErrorPermanent      ErrorClass = "permanent"
)

type ProviderError struct {
	Class      ErrorClass
	StatusCode int
	RetryAfter time.Duration
}

func (e *ProviderError) Error() string { return fmt.Sprintf("provider request failed: %s", e.Class) }

func (e *ProviderError) Retryable() bool {
	return e.Class == ErrorTransient || e.Class == ErrorRateLimited
}

// NormalizedEnvelope is the sink-ready, provider-independent result. Raw
// response bytes are intentionally omitted. DedupeKey is stable across retry
// and source identity; Provenance lets analytics consumers trace every value.
type NormalizedEnvelope struct {
	SchemaVersion string            `json:"schema_version"`
	Provider      string            `json:"provider"`
	OrgID         string            `json:"org_id"`
	IntegrationID string            `json:"integration_id"`
	EntityType    string            `json:"entity_type"`
	SourceID      string            `json:"source_id"`
	DedupeKey     string            `json:"dedupe_key"`
	ObservedAt    time.Time         `json:"observed_at"`
	Provenance    Provenance        `json:"provenance"`
	Attributes    map[string]string `json:"attributes"`
}

type Provenance struct {
	Source     string `json:"source"`
	Confidence string `json:"confidence"`
	EvidenceID string `json:"evidence_id,omitempty"`
}

func (e NormalizedEnvelope) Validate() error {
	if e.SchemaVersion != "v1" || e.Provider == "" || e.OrgID == "" || e.IntegrationID == "" || e.EntityType == "" || e.SourceID == "" || e.DedupeKey == "" || e.ObservedAt.IsZero() || e.Provenance.Source == "" || e.Provenance.Confidence == "" {
		return errors.New("invalid normalized provider envelope")
	}
	return nil
}
