package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// impersonationCacheTTL is api/services/impersonation_cache.py's
// _TTL_SECONDS: entries self-expire as a drift bound; correctness comes
// from the explicit write-through in start/stop.
const impersonationCacheTTL = 30 * time.Second

// impersonationKeyPrefix is _KEY_PREFIX.
const impersonationKeyPrefix = "impersonation:active:"

// impersonationCircuitWindow is _CIRCUIT_SECONDS: after a Valkey error, skip
// the cache for this long instead of paying the connect timeout per request.
const impersonationCircuitWindow = 5 * time.Second

// impersonationNoneSentinel is _NONE_SENTINEL: caches "this admin has no
// active impersonation session" so the overwhelmingly common case (ordinary
// superuser traffic) skips a Postgres query.
const impersonationNoneSentinel = "none"

// cachedImpersonationSession is CachedImpersonationSession: a plain
// snapshot, serializable across the Valkey boundary the same way the
// dataclass is across the asyncio one.
type cachedImpersonationSession struct {
	ID           string    `json:"id"`
	AdminUserID  string    `json:"admin_user_id"`
	TargetUserID string    `json:"target_user_id"`
	TargetOrgID  string    `json:"target_org_id"`
	TargetRole   string    `json:"target_role"`
	TargetEmail  *string   `json:"target_email"`
	ExpiresAt    time.Time `json:"expires_at"`
}

// impersonationDBLoader is the Postgres fallback _load_from_db calls
// through: the admin's active, unexpired, unended session, joined to the
// target's email.
type impersonationDBLoader interface {
	activeImpersonationSession(ctx context.Context, adminUserID string) (*cachedImpersonationSession, error)
}

// impersonationCache is the Go side of impersonation_cache.py: Valkey
// cache-aside in front of Postgres, fail-correct (bypass to Postgres on
// Valkey trouble, never fail-stale), with a circuit breaker bounding the
// per-request connect cost during an outage and a negative-cache entry for
// "no active session" (the common case).
//
// A nil client (Valkey not configured) behaves exactly like a permanently
// open circuit: every call falls through to Postgres, matching
// _get_client() returning None.
type impersonationCache struct {
	client valkeygo.Client
	loader impersonationDBLoader
	logger *slog.Logger

	mu               sync.Mutex
	circuitOpenUntil time.Time
	// now is injectable for tests; nil means time.Now.
	now func() time.Time
}

func newImpersonationCache(client valkeygo.Client, loader impersonationDBLoader, logger *slog.Logger) *impersonationCache {
	if logger == nil {
		logger = slog.Default()
	}
	return &impersonationCache{client: client, loader: loader, logger: logger}
}

func (c *impersonationCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *impersonationCache) key(adminUserID string) string {
	return impersonationKeyPrefix + adminUserID
}

func (c *impersonationCache) circuitIsOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clock().Before(c.circuitOpenUntil)
}

func (c *impersonationCache) tripCircuit(err error) {
	c.mu.Lock()
	c.circuitOpenUntil = c.clock().Add(impersonationCircuitWindow)
	c.mu.Unlock()
	c.logger.Warn("valkey impersonation cache unavailable, bypassing",
		slog.Duration("window", impersonationCircuitWindow), slog.Any("error", err))
}

// cacheOutcome distinguishes a genuine cache hit (found, possibly nil for
// the negative sentinel) from a miss/bypass, the same three-way split
// _cache_get's _MISS sentinel gives the Python caller.
type cacheOutcome int

const (
	cacheMiss cacheOutcome = iota
	cacheHitSession
	cacheHitNone
)

func (c *impersonationCache) get(ctx context.Context, adminUserID string) (*cachedImpersonationSession, cacheOutcome) {
	if c.client == nil || c.circuitIsOpen() {
		return nil, cacheMiss
	}
	raw, err := c.client.Do(ctx, c.client.B().Get().Key(c.key(adminUserID)).Build()).ToString()
	if err != nil {
		if valkeygo.IsValkeyNil(err) {
			return nil, cacheMiss
		}
		c.tripCircuit(err)
		return nil, cacheMiss
	}
	if raw == impersonationNoneSentinel {
		return nil, cacheHitNone
	}
	var session cachedImpersonationSession
	if err := json.Unmarshal([]byte(raw), &session); err != nil {
		c.logger.Warn("corrupt impersonation cache entry", slog.String("admin_user_id", adminUserID))
		return nil, cacheMiss
	}
	return &session, cacheHitSession
}

func (c *impersonationCache) setBypassingCircuit(ctx context.Context, adminUserID string, session *cachedImpersonationSession) error {
	if c.client == nil {
		return nil
	}
	payload := impersonationNoneSentinel
	if session != nil {
		encoded, err := json.Marshal(session)
		if err != nil {
			return err
		}
		payload = string(encoded)
	}
	cmd := c.client.B().Set().Key(c.key(adminUserID)).Value(payload).
		Ex(impersonationCacheTTL).Build()
	if err := c.client.Do(ctx, cmd).Error(); err != nil {
		return err
	}
	return nil
}

// set is _cache_set: respects the circuit breaker (an ordinary cache fill
// is not worth paying the connect cost during an outage).
func (c *impersonationCache) set(ctx context.Context, adminUserID string, session *cachedImpersonationSession) {
	if c.client == nil || c.circuitIsOpen() {
		return
	}
	if err := c.setBypassingCircuit(ctx, adminUserID, session); err != nil {
		c.tripCircuit(err)
	}
}

// errDBLookupFailed marks _load_from_db's _DB_ERROR: the caller must not
// cache this outcome, and must fail open (no active session) rather than
// surface a 500 for a cache-layer read.
var errDBLookupFailed = errors.New("admin: impersonation session lookup failed")

// activeSession is get_active_session: cache first, Postgres on miss, write
// only a confirmed DB result back to the cache.
func (c *impersonationCache) activeSession(ctx context.Context, adminUserID string) (*cachedImpersonationSession, error) {
	if session, outcome := c.get(ctx, adminUserID); outcome != cacheMiss {
		if outcome == cacheHitNone {
			return nil, nil
		}
		return session, nil
	}
	session, err := c.loader.activeImpersonationSession(ctx, adminUserID)
	if err != nil {
		c.logger.Warn("postgres lookup for impersonation session failed", slog.Any("error", err))
		return nil, nil
	}
	c.set(ctx, adminUserID, session)
	return session, nil
}

// setActiveSession is set_active_session: the write-through after a
// committed start/stop. Deliberately bypasses the circuit breaker -- this
// is the one write that keeps replicas correct, so it always attempts
// Valkey (bounded by the client's own timeouts) even right after a read
// error. The caller must have committed first; write-through of
// pre-commit state would poison every replica for up to the TTL.
func (c *impersonationCache) setActiveSession(ctx context.Context, adminUserID string, session *cachedImpersonationSession) {
	if c.client == nil {
		return
	}
	if err := c.setBypassingCircuit(ctx, adminUserID, session); err != nil {
		c.tripCircuit(err)
		c.logger.Error("failed to write-through impersonation state; replicas may serve stale impersonation context",
			slog.String("admin_user_id", adminUserID), slog.Duration("ttl", impersonationCacheTTL), slog.Any("error", err))
	}
}
