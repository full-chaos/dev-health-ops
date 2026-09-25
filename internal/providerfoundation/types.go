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
	"log/slog"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

var (
	ErrInvalidScope       = errors.New("invalid provider tenant scope")
	ErrCredentialNotFound = errors.New("provider credential not found")
	ErrCredentialInactive = errors.New("provider credential inactive")
	ErrCredentialInvalid  = errors.New("provider credential is invalid")
	ErrLeaseLost          = errors.New("provider lease is no longer valid")
	// ErrBudgetContended means the shared request reservation store is healthy,
	// but every slot in this provider/org/host/cost bucket is in use. Callers
	// must defer without spending their failure-attempt budget.
	ErrBudgetContended      = errors.New("provider budget contended")
	ErrBudgetUnavailable    = errors.New("provider budget unavailable")
	ErrSinkDuplicate        = errors.New("provider sink duplicate has different content")
	ErrSinkGenerationUnsafe = errors.New("provider sink generation is not safely deduplicated")
	ErrSinkReplayConflict   = errors.New("provider sink generation replay has different content")
	ErrNormalizationInvalid = errors.New("provider normalized record is invalid")
	// ErrInvalidConfiguration and ErrRecoveryUnsafe are the canonical identity
	// for providersync's ErrInvalidConfiguration/ErrEffectRecoveryUnsafe
	// (internal/providersync/lease.go), aliased there rather than duplicated so
	// errors.Is comparisons keep working across the package boundary.
	// internal/teamattribution (extracted from providersync, CHAOS-3092 PR-A)
	// references these directly, since it cannot import providersync back.
	ErrInvalidConfiguration = errors.New("provider sync configuration is invalid")
	ErrRecoveryUnsafe       = errors.New("provider sync effect recovery is outside the bounded contract")
)

// ObjectTooLargeError reports that fetchObject's shared per-object response
// cap (nativeMaxObjectBytes) truncated a response before it could be decoded.
// It wraps ErrNormalizationInvalid so every existing errors.Is(err,
// ErrNormalizationInvalid) classification keeps matching unchanged; a route
// with its own truncation-recovery path (github/files' recursive tree walk)
// uses errors.As to react to the cap specifically instead of failing closed.
type ObjectTooLargeError struct {
	// Path is the request path fetchObject was reading when the cap was hit.
	Path string
	// CapBytes is the shared cap (nativeMaxObjectBytes) that was exceeded.
	CapBytes int
	// ContentLength is the server-reported response size in bytes, or -1
	// when the server sent no Content-Length header.
	ContentLength int64
}

func (e *ObjectTooLargeError) Error() string {
	if e.ContentLength >= 0 {
		return fmt.Sprintf("provider object at %s exceeds %d byte cap: %d bytes", e.Path, e.CapBytes, e.ContentLength)
	}
	return fmt.Sprintf("provider object at %s exceeds %d byte cap (content-length unknown)", e.Path, e.CapBytes)
}

func (e *ObjectTooLargeError) Unwrap() error { return ErrNormalizationInvalid }

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
	// deferred holds the fields whose stored value was not a string, as
	// decoded; Secret reads one the way Python's resolver does (see
	// pythonSecretText). Nil for a credential built from strings.
	deferred map[string]pyjson.Value
}

// NewCredential builds a Credential from already-decrypted fields, for a
// caller that resolves the credential row itself (the in-process budget
// estimator's PagerDuty hydration, CHAOS-6243). The maps are copied.
func NewCredential(provider, id string, config map[string]string, fields map[string]secrets.Value) Credential {
	copiedConfig := make(map[string]string, len(config))
	for key, value := range config {
		copiedConfig[key] = value
	}
	copiedFields := make(map[string]secrets.Value, len(fields))
	for key, value := range fields {
		copiedFields[key] = value
	}
	return Credential{Provider: provider, ID: id, Config: copiedConfig, fields: copiedFields}
}

func (c Credential) Secret(name string) (secrets.Value, bool) {
	if v, ok := c.fields[name]; ok {
		return v, true
	}
	if raw, ok := c.deferred[name]; ok {
		return pythonSecretText(raw)
	}
	return secrets.Value{}, false
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

// String, GoString and LogValue keep secret material out of every printed form
// of a Credential: fmt (%v, %+v, %#v) and slog would otherwise reflect over the
// private field maps and print the values (a secrets.Value's raw text, a
// decoded non-string value). Only metadata is shown.
func (c Credential) String() string {
	return fmt.Sprintf("providerfoundation.Credential{provider=%s fields=%d}", c.Provider, len(c.fields)+len(c.deferred))
}

func (c Credential) GoString() string { return c.String() }

func (c Credential) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("provider", c.Provider),
		slog.Bool("credential_id_configured", c.ID != ""),
		slog.Int("credential_field_count", len(c.fields)+len(c.deferred)),
	)
}

func (c Credential) SafeAttributes() map[string]any {
	return map[string]any{
		"provider": c.Provider, "credential_id_configured": c.ID != "",
		"credential_name_configured": c.Name != "", "credential_field_count": len(c.fields) + len(c.deferred),
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
	// RawConfig is the config column's JSON text exactly as read with
	// Ciphertext, so a caller can hash the same row the credential was
	// built from (the run-auth fingerprint hashes {**config, **decrypted},
	// which the string-only Config map cannot reproduce).
	RawConfig []byte
}

type CredentialRepository interface {
	ResolveEncrypted(context.Context, TenantScope) (EncryptedCredential, error)
}

type CredentialDecryptor interface {
	Decrypt(secrets.Value) ([]byte, error)
}

// CredentialCipher is the authenticated encryption boundary required by
// renewable OAuth credentials. Plaintext remains a secrets.Value only at the
// concrete credential boundary and is never returned by repositories.
type CredentialCipher interface {
	CredentialDecryptor
	Encrypt([]byte) (secrets.Value, error)
}

// CredentialHydrator attaches short-lived provider secrets referenced by a
// tokenless persisted descriptor. It runs after descriptor decryption and
// must preserve the claim's tenant and live lease boundaries.
type CredentialHydrator interface {
	Hydrate(context.Context, LeaseGuard, TenantScope, Credential) (Credential, error)
}

// CredentialResolver has no environment dependency. The encryption key is
// supplied once by process construction using the existing secret loader.
type CredentialResolver struct {
	Repository CredentialRepository
	Decryptor  CredentialDecryptor
	Hydrator   CredentialHydrator
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
	if r.Hydrator != nil {
		return r.Hydrator.Hydrate(ctx, lease, scope, credential)
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
	// Path is the request's URL path (no query string -- a query can carry
	// caller-supplied filter values that do not belong in an error string)
	// that produced this classification. Empty when the error never reached
	// an HTTP response (e.g. a rate-limit gate denial before the request
	// went out).
	Path string
	// Body is the provider's response body as read (bounded by
	// maxProviderErrorBody), kept for classification parsers that need the
	// provider's own rejection detail. Error() never formats it: error text
	// flows into logs and durable results unfiltered by key. No log path
	// reads it, and it is never marshalled: a ProviderError inside a
	// structured log value (a map, a struct, a struct value) carries class,
	// status, path and retry delay only.
	Body string `json:"-"`
}

const maxProviderErrorBodyInMessage = 500

func (e *ProviderError) Error() string {
	message := fmt.Sprintf("provider request failed: %s", e.Class)
	if e.StatusCode != 0 {
		message += fmt.Sprintf(" status=%d", e.StatusCode)
	}
	if e.Path != "" {
		message += fmt.Sprintf(" path=%s", e.Path)
	}
	return message
}

// ResponseBodySnippet returns the response body redacted by
// logging.RedactText and cut to maxProviderErrorBodyInMessage bytes, for a
// caller that needs the provider's reason outside a log (a test, a
// classifier). No log path calls it.
func (e *ProviderError) ResponseBodySnippet() string {
	if e == nil || e.Body == "" {
		return ""
	}
	body := logging.RedactText(e.Body)
	if len(body) > maxProviderErrorBodyInMessage {
		cut := maxProviderErrorBodyInMessage
		for cut > 0 && !utf8.RuneStart(body[cut]) {
			cut--
		}
		body = body[:cut]
	}
	return body
}

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
