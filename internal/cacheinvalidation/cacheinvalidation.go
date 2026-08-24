// Package cacheinvalidation is the Go side of the per-organization cache
// epoch the Python API folds into its filter-scoped cache keys
// (dev_health_ops.core.cache.org_cache_epoch_key, CHAOS-4226).
//
// The home/explain entries are keyed by the full serialized filter payload,
// so no writer can enumerate "every key for org X" to delete them. Instead
// every org has ONE epoch key in Valkey; readers fold its value into the
// cache key and writers that change the org's data INCR it. One INCR makes
// every older entry unreachable; the orphans age out by their own TTL.
//
// The key string is a cross-language contract. It is never hand-copied:
// tests/test_cache_epoch_contract.py regenerates
// contracts/cache-invalidation/v1/org_cache_epoch_key.json from the live
// Python producer, and contract_test.go asserts OrgCacheEpochKey against
// that file.
package cacheinvalidation

import (
	"context"
	"errors"
	"time"

	valkeygo "github.com/valkey-io/valkey-go"
)

// OrgCacheEpochKeyPrefix mirrors core.cache.ORG_CACHE_EPOCH_KEY_PREFIX.
const OrgCacheEpochKeyPrefix = "cache_epoch:org:"

// OrgCacheEpochTTL mirrors core.cache.ORG_CACHE_EPOCH_TTL_SECONDS: the
// expiry every bump (re)sets on the epoch key. It must dwarf the longest
// epoch-scoped entry TTL (Python refuses caches over
// EPOCH_SCOPED_CACHE_MAX_TTL_SECONDS = 3600s at the read site) -- an epoch
// key that expired while entries stamped N were alive would let those
// entries serve again after the next bump re-created the key at 1. 30 days
// against ≤ 1h entries is a margin of ~720x; contract_test.go pins both
// numbers against the fixture.
const OrgCacheEpochTTL = 30 * 24 * time.Hour

var (
	ErrNilClient = errors.New("cacheinvalidation: nil valkey client")
	ErrEmptyOrg  = errors.New("cacheinvalidation: empty org id")
)

// OrgCacheEpochKey is the Valkey key holding an organization's cache epoch.
func OrgCacheEpochKey(orgID string) string {
	return OrgCacheEpochKeyPrefix + orgID
}

// OrgCacheInvalidator is the narrow capability the native finalize depends
// on: make every cached filter-scoped view of an org unreachable.
type OrgCacheInvalidator interface {
	InvalidateOrg(ctx context.Context, orgID string) error
}

// ValkeyOrgCacheInvalidator bumps the epoch with INCR + EXPIRE -- the exact
// two commands Python's RedisBackend.incr issues, so a Go bump and a Python
// bump are indistinguishable to readers.
type ValkeyOrgCacheInvalidator struct {
	client valkeygo.Client
}

func NewValkeyOrgCacheInvalidator(client valkeygo.Client) (*ValkeyOrgCacheInvalidator, error) {
	if client == nil {
		return nil, ErrNilClient
	}
	return &ValkeyOrgCacheInvalidator{client: client}, nil
}

// InvalidateOrg increments the org's epoch and refreshes its expiry. Both
// commands are pipelined in one round trip; the INCR result is what
// matters, and a failed EXPIRE after a successful INCR is still reported
// as an error so the emitted/consumed telemetry pair shows it.
func (invalidator *ValkeyOrgCacheInvalidator) InvalidateOrg(ctx context.Context, orgID string) error {
	if invalidator == nil || invalidator.client == nil {
		return ErrNilClient
	}
	if orgID == "" {
		return ErrEmptyOrg
	}
	key := OrgCacheEpochKey(orgID)
	results := invalidator.client.DoMulti(ctx,
		invalidator.client.B().Incr().Key(key).Build(),
		invalidator.client.B().Expire().Key(key).Seconds(int64(OrgCacheEpochTTL.Seconds())).Build(),
	)
	for _, result := range results {
		if err := result.Error(); err != nil {
			return err
		}
	}
	return nil
}

var _ OrgCacheInvalidator = (*ValkeyOrgCacheInvalidator)(nil)
