package externalingest

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// keyedBucket is a per-key token bucket, the same algorithm as
// httpapi.Bucket, but keyed (auth.py's limiter buckets by IP; httpapi's is
// one bucket per ROUTE for every caller, which is the wrong shape for
// per-caller ingest-auth throttling).
type keyedBucket struct {
	perSecond float64
	burst     float64
	now       func() time.Time

	mu      sync.Mutex
	buckets map[string]*bucketState
}

type bucketState struct {
	tokens float64
	last   time.Time
}

func newKeyedBucket(perMinute float64, burst int, now func() time.Time) *keyedBucket {
	if now == nil {
		now = time.Now
	}
	return &keyedBucket{
		perSecond: perMinute / 60,
		burst:     float64(burst),
		now:       now,
		buckets:   make(map[string]*bucketState),
	}
}

// allow consumes one token for key, refilling first. Matches
// auth.py's hit()/test() semantics with consume=true.
func (b *keyedBucket) allow(key string) bool {
	return b.reserve(key, true)
}

// test reports whether key currently has a token, WITHOUT consuming one
// (auth.py's _reject_if_already_ip_throttled: a request about to succeed
// never pays for this check).
func (b *keyedBucket) test(key string) bool {
	return b.reserve(key, false)
}

func (b *keyedBucket) reserve(key string, consume bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	state, ok := b.buckets[key]
	now := b.now()
	if !ok {
		state = &bucketState{tokens: b.burst, last: now}
		b.buckets[key] = state
	}
	if elapsed := now.Sub(state.last); elapsed > 0 {
		state.tokens += elapsed.Seconds() * b.perSecond
		if state.tokens > b.burst {
			state.tokens = b.burst
		}
		state.last = now
	}
	if state.tokens < 1 {
		return false
	}
	if consume {
		state.tokens--
	}
	return true
}

// forwardedIP mirrors rate_limit.py's get_forwarded_ip: the TCP peer,
// unless it is a configured trusted proxy AND the request carries
// X-Forwarded-For, in which case the first hop of that header wins.
// TRUSTED_PROXIES is read fresh (not cached) to match os.getenv's own
// per-call behavior in the Python original.
func forwardedIP(r *http.Request) string {
	peer := "unknown"
	if r.RemoteAddr != "" {
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			peer = host
		} else {
			peer = r.RemoteAddr
		}
	}
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded == "" {
		return peer
	}
	trusted := trustedProxies()
	if !trusted[peer] {
		return peer
	}
	first := strings.SplitN(forwarded, ",", 2)[0]
	return strings.TrimSpace(first)
}

func trustedProxies() map[string]bool {
	raw := os.Getenv("TRUSTED_PROXIES")
	set := make(map[string]bool)
	for _, part := range strings.Split(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			set[trimmed] = true
		}
	}
	return set
}

// routeLimiters is one keyedBucket per rate_limit.py @limiter.limit
// decorator this package ports: each route gets its OWN counter (slowapi
// keys per view function too, not just per key_func result), at the exact
// per-minute ceiling and key shape router.py/status.py declare. GET
// /availability has no decorator in Python and so has no bucket here.
type routeLimiters struct {
	schemasList *keyedBucket // INGEST_READ_LIMIT, key=IP (unauthenticated route)
	schemasGet  *keyedBucket // INGEST_READ_LIMIT, key=IP
	validate    *keyedBucket // INGEST_VALIDATE_LIMIT, key=validated token
	batches     *keyedBucket // INGEST_BATCH_LIMIT, key=validated token
	listBatches *keyedBucket // INGEST_READ_LIMIT, key=validated token
	getBatch    *keyedBucket // INGEST_READ_LIMIT, key=validated token
}

const (
	ingestReadLimitPerMinute     = 120
	ingestValidateLimitPerMinute = 60
	ingestBatchLimitPerMinute    = 60
)

func newRouteLimiters(now func() time.Time) *routeLimiters {
	return &routeLimiters{
		schemasList: newKeyedBucket(ingestReadLimitPerMinute, ingestReadLimitPerMinute, now),
		schemasGet:  newKeyedBucket(ingestReadLimitPerMinute, ingestReadLimitPerMinute, now),
		validate:    newKeyedBucket(ingestValidateLimitPerMinute, ingestValidateLimitPerMinute, now),
		batches:     newKeyedBucket(ingestBatchLimitPerMinute, ingestBatchLimitPerMinute, now),
		listBatches: newKeyedBucket(ingestReadLimitPerMinute, ingestReadLimitPerMinute, now),
		getBatch:    newKeyedBucket(ingestReadLimitPerMinute, ingestReadLimitPerMinute, now),
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
// shape via this package's own error envelope).
func rateLimitedOrTooManyRequests(bucket *keyedBucket, key string) *ingestError {
	if bucket != nil && !bucket.allow(key) {
		return newIngestError(http.StatusTooManyRequests, "rate_limited", "Rate limit exceeded")
	}
	return nil
}
