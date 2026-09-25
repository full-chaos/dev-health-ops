package externalingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/ratelimit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// forwardedIP is rate_limit.py's get_forwarded_ip, the api's one copy
// (ratelimit.ForwardedIP).
func forwardedIP(r *http.Request) string { return ratelimit.ForwardedIP(r) }

// routeLimiters is one httpapi.KeyedLimiter per rate_limit.py
// @limiter.limit decorator this package ports, at the exact per-minute
// ceiling and key shape router.py/status.py declare. slowapi's default
// strategy is a FIXED window per (key_func result, exact request path): two
// requests for different URLs (two batch ids, two schema versions) never
// share a budget, and a window starts at the first hit after the previous
// one expired. KeyedLimiter is that (measured against a live slowapi route
// in its own package), counted in the api's shared store. GET /availability
// has no decorator in Python and so has no limiter here.
type routeLimiters struct {
	schemasList *httpapi.KeyedLimiter // INGEST_READ_LIMIT, key=IP (unauthenticated route)
	schemasGet  *httpapi.KeyedLimiter // INGEST_READ_LIMIT, key=IP
	validate    *httpapi.KeyedLimiter // INGEST_VALIDATE_LIMIT, key=validated token
	batches     *httpapi.KeyedLimiter // INGEST_BATCH_LIMIT, key=validated token
	listBatches *httpapi.KeyedLimiter // INGEST_READ_LIMIT, key=validated token
	getBatch    *httpapi.KeyedLimiter // INGEST_READ_LIMIT, key=validated token
}

const (
	ingestReadLimitPerMinute     = 120
	ingestValidateLimitPerMinute = 60
	ingestBatchLimitPerMinute    = 60
)

// newRouteLimiters counts in store; nil means an in-process store on now
// (development and tests). Each limiter has its own limit ID, so the six
// routes never share a counter, and its store-error series is declared at
// zero.
func newRouteLimiters(store httpapi.CounterStore, now func() time.Time) *routeLimiters {
	if store == nil {
		store = httpapi.NewMemoryCounters(now)
	}
	perMinute := func(id string, count int) *httpapi.KeyedLimiter {
		limiter := httpapi.NewKeyedLimiter(store, httpapi.Limit{ID: id, Count: count, Window: time.Minute})
		limiter.Declare()
		return limiter
	}
	return &routeLimiters{
		schemasList: perMinute("external_ingest_schemas_list", ingestReadLimitPerMinute),
		schemasGet:  perMinute("external_ingest_schemas_get", ingestReadLimitPerMinute),
		validate:    perMinute("external_ingest_validate", ingestValidateLimitPerMinute),
		batches:     perMinute("external_ingest_batches", ingestBatchLimitPerMinute),
		listBatches: perMinute("external_ingest_batches_list", ingestReadLimitPerMinute),
		getBatch:    perMinute("external_ingest_batches_get", ingestReadLimitPerMinute),
	}
}

// ingestTokenRateLimitKey mirrors rate_limit.py's get_ingest_token_key: keys
// on the VALIDATED token id (set only after requireIngestScope succeeds),
// never the raw bearer text -- an unvalidated caller could otherwise mint a
// fresh bucket per request by rotating an arbitrary string.
func ingestTokenRateLimitKey(tokenID string) string {
	digest := sha256.Sum256([]byte(tokenID))
	return "ingest-token:" + hex.EncodeToString(digest[:])[:16]
}

// rateLimitedOrTooManyRequests is the shared check every rate-limited
// handler runs: nil means proceed, non-nil is the ingestError to write and
// return immediately (429 rate_limited, matching Python's slowapi response
// shape via this package's own error envelope). path is the request's
// decoded URL path: slowapi's bucket is per (key, exact path). A store that
// cannot answer is the Python api's unhandled 500 (slowapi has no
// swallow_errors), never a request let through: it is logged and counted by
// the limiter.
func rateLimitedOrTooManyRequests(ctx context.Context, limiter *httpapi.KeyedLimiter, key, path string) *ingestError {
	allowed, err := limiter.AllowCounted(ctx, key, path)
	if err != nil {
		return unhandledError()
	}
	if !allowed {
		return newIngestError(http.StatusTooManyRequests, "rate_limited", "Rate limit exceeded. Please try again later.")
	}
	return nil
}
