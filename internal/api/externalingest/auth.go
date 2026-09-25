package externalingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// tokenPrefix is models/ingest_auth.py's TOKEN_PREFIX.
const tokenPrefix = "fcpush_"

// IngestAuthContext mirrors auth.py's IngestAuthContext: the resolved
// identity for an authenticated /api/v1/external-ingest/* request.
type IngestAuthContext struct {
	OrgID   string
	Scopes  map[string]bool
	TokenID string
	Source  *IngestSource // nil for an org-wide (unbound) token
}

// IngestSource mirrors models/ingest_auth.py's IngestSource.
type IngestSource struct {
	ID           string
	System       string
	Instance     string
	EntityFamily string
	Mode         string
	Enabled      bool
}

func (s *IngestSource) isWriteEligible() bool {
	return s != nil && s.Enabled && s.Mode == "customer_push"
}

// authLimiters are the two per-IP ingest-auth limits (rate_limit.py's
// INGEST_AUTH_ATTEMPT_IP_LIMIT/INGEST_AUTH_FAILURE_IP_LIMIT). auth.py drives
// slowapi's underlying `limits` limiter directly (hit() and test()) with a key
// that is only the caller's address: a FIXED window per address in the storage
// every api replica shares. Each is an httpapi.KeyedLimiter over the shared
// counter store with an empty path (the key carries no request path).
type authLimiters struct {
	attempt *httpapi.KeyedLimiter // every attempt, hit() before anything else
	failure *httpapi.KeyedLimiter // failures only: test() first, hit() on each failure
}

const (
	ingestAuthAttemptLimitPerMinute = 100
	ingestAuthFailureLimitPerMinute = 30
)

// newAuthLimiters counts in store; nil means an in-process store on now
// (development and tests). Each limiter has its own limit ID and its
// store-error series is declared at zero.
func newAuthLimiters(store httpapi.CounterStore, now func() time.Time) *authLimiters {
	if store == nil {
		store = httpapi.NewMemoryCounters(now)
	}
	perMinute := func(id string, count int) *httpapi.KeyedLimiter {
		limiter := httpapi.NewKeyedLimiter(store, httpapi.Limit{ID: id, Count: count, Window: time.Minute})
		limiter.Declare()
		return limiter
	}
	return &authLimiters{
		attempt: perMinute("external_ingest_auth_attempt", ingestAuthAttemptLimitPerMinute),
		failure: perMinute("external_ingest_auth_failure", ingestAuthFailureLimitPerMinute),
	}
}

// requireIngestScope ports auth.py's require_ingest_scope dependency. On
// success it returns the resolved context; on failure it returns the exact
// ingestError the route must answer with (the caller writes it and stops).
func (d Deps) requireIngestScope(
	ctx context.Context, r *http.Request, scope string, requireCustomerPushFeature bool,
) (*IngestAuthContext, *ingestError) {
	ip := forwardedIP(r)
	// A store that cannot answer is the Python api's unhandled 500 (the
	// `limits` storage raises and auth.py does not catch it), never a request
	// let through: each limiter logs and counts it.
	allowed, err := d.limiters.attempt.AllowCounted(ctx, ip, "")
	if err != nil {
		return nil, unhandledError()
	}
	if !allowed {
		return nil, newIngestError(http.StatusTooManyRequests, "rate_limited", "Too many authentication attempts")
	}
	below, err := d.limiters.failure.TestCounted(ctx, ip, "")
	if err != nil {
		return nil, unhandledError()
	}
	if !below {
		return nil, newIngestError(http.StatusTooManyRequests, "rate_limited", "Too many failed authentication attempts")
	}

	// fail records one failure (auth.py's _record_auth_failure_hit) and
	// answers the 401; a store that cannot record it is the unhandled 500.
	fail := func() *ingestError {
		if _, err := d.limiters.failure.AllowCounted(ctx, ip, ""); err != nil {
			return unhandledError()
		}
		return newIngestError(http.StatusUnauthorized, "invalid_token", "Missing or invalid ingest token")
	}

	raw := extractBearer(r)
	if raw == "" || !strings.HasPrefix(raw, tokenPrefix) {
		return nil, fail()
	}
	if d.Pool == nil {
		return nil, newIngestError(http.StatusInternalServerError, "internal_error", "external-ingest database is not configured")
	}

	digest := sha256.Sum256([]byte(raw))
	tokenHash := hex.EncodeToString(digest[:])

	var (
		tokenID    string
		orgID      string
		sourceID   *string
		scopesJSON []byte
		expiresAt  *time.Time
		revokedAt  *time.Time
	)
	err = d.Pool.QueryRow(ctx, `
		SELECT id, org_id, source_id, scopes, expires_at, revoked_at
		FROM external_ingest_tokens
		WHERE token_hash = $1
	`, tokenHash).Scan(&tokenID, &orgID, &sourceID, &scopesJSON, &expiresAt, &revokedAt)
	if err == pgx.ErrNoRows {
		return nil, fail()
	}
	if err != nil {
		return nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve ingest token")
	}

	now := time.Now().UTC()
	if revokedAt != nil || (expiresAt != nil && now.After(*expiresAt)) {
		return nil, fail()
	}

	var source *IngestSource
	if sourceID != nil {
		source, err = loadIngestSource(ctx, d.Pool, *sourceID)
		if err != nil {
			return nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve ingest source")
		}
	}

	bumpLastUsed(d.Pool, tokenID, ip) // recorded regardless of the scope check below (auth.py's _bump_last_used)

	var scopeList []string
	if len(scopesJSON) > 0 {
		_ = json.Unmarshal(scopesJSON, &scopeList)
	}
	scopes := make(map[string]bool, len(scopeList))
	for _, s := range scopeList {
		scopes[s] = true
	}
	if !scopes[scope] {
		if _, err := d.limiters.failure.AllowCounted(ctx, ip, ""); err != nil {
			return nil, unhandledError()
		}
		return nil, newIngestError(http.StatusForbidden, "insufficient_scope", "Token is missing required scope: "+scope)
	}

	if requireCustomerPushFeature {
		decision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(ctx, orgID, "customer_push_ingest")
		if err != nil {
			return nil, newIngestError(http.StatusInternalServerError, "internal_error", "failed to resolve feature state")
		}
		if !decision.Allowed {
			if _, err := d.limiters.failure.AllowCounted(ctx, ip, ""); err != nil {
				return nil, unhandledError()
			}
			return nil, newIngestError(http.StatusForbidden, "feature_not_enabled", "Customer push ingest is not enabled for this organization")
		}
	}

	return &IngestAuthContext{OrgID: orgID, Scopes: scopes, TokenID: tokenID, Source: source}, nil
}

func loadIngestSource(ctx context.Context, pool *pgxpool.Pool, id string) (*IngestSource, error) {
	var source IngestSource
	source.ID = id
	err := pool.QueryRow(ctx, `
		SELECT system, instance, entity_family, mode, enabled
		FROM external_ingest_sources
		WHERE id = $1
	`, id).Scan(&source.System, &source.Instance, &source.EntityFamily, &source.Mode, &source.Enabled)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &source, nil
}

func bumpLastUsed(pool *pgxpool.Pool, tokenID, ip string) {
	if pool == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = pool.Exec(ctx, `
		UPDATE external_ingest_tokens
		SET last_used_at = $2, last_used_ip = $3
		WHERE id = $1
	`, tokenID, time.Now().UTC(), ip)
}

func extractBearer(r *http.Request) string {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(strings.ToLower(header), "bearer ") {
		return ""
	}
	return strings.TrimSpace(header[len("Bearer "):])
}

// requireMatchingSource is auth.py's require_matching_source: a write
// request's declared source must match the token's bound source.
func requireMatchingSource(ctxAuth *IngestAuthContext, system, instance, entityFamily string) *ingestError {
	source := ctxAuth.Source
	if entityFamily == "" {
		entityFamily = legacyEntityFamily
	}
	sourceFamily := legacyEntityFamily
	if source != nil && source.EntityFamily != "" {
		sourceFamily = source.EntityFamily
	}
	if source == nil || source.System != system || source.Instance != instance || sourceFamily != entityFamily {
		return newIngestError(http.StatusForbidden, "source_mismatch", "Payload source does not match the token's bound source")
	}
	if !source.isWriteEligible() {
		return newIngestError(http.StatusForbidden, "source_disabled", "Source is disabled or not in customer_push mode")
	}
	return nil
}
