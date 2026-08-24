package cacheinvalidation

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	valkeystore "github.com/full-chaos/dev-health-ops/internal/storage/valkey"
)

// orgCacheEpochKeyContractPath is the producer-derived fixture Python writes
// from dev_health_ops.core.cache.org_cache_epoch_key
// (tests/test_cache_epoch_contract.py regenerates it and fails on drift).
// This test asserts the Go side against the SAME file, so neither language
// hand-copies the other's key string (CHAOS-4226).
const orgCacheEpochKeyContractPath = "../../contracts/cache-invalidation/v1/org_cache_epoch_key.json"

type orgCacheEpochKeyContract struct {
	Version                  int `json:"version"`
	EpochTTLSeconds          int `json:"epoch_ttl_seconds"`
	ScopedCacheMaxTTLSeconds int `json:"scoped_cache_max_ttl_seconds"`
	ScopedCacheTTLMargin     int `json:"scoped_cache_ttl_margin"`
	Cases                    []struct {
		OrgID string `json:"org_id"`
		Key   string `json:"key"`
	} `json:"cases"`
}

func TestOrgCacheEpochKeyMatchesThePythonProducerFixture(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(filepath.Clean(orgCacheEpochKeyContractPath))
	if err != nil {
		t.Fatalf("read contract fixture: %v", err)
	}
	var contract orgCacheEpochKeyContract
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("decode contract fixture: %v", err)
	}
	if contract.Version != 1 {
		t.Fatalf("contract version=%d want=1", contract.Version)
	}
	if len(contract.Cases) < 3 {
		t.Fatalf("contract carries %d cases; want at least 3 (uuid, slug, punctuation)", len(contract.Cases))
	}
	if got := int(OrgCacheEpochTTL.Seconds()); got != contract.EpochTTLSeconds {
		t.Fatalf("OrgCacheEpochTTL=%ds, Python ORG_CACHE_EPOCH_TTL_SECONDS=%d", got, contract.EpochTTLSeconds)
	}
	// The expiry margin (team-lead constraint 3): the epoch key must outlive
	// the longest epoch-scoped entry TTL Python allows by the agreed factor.
	if contract.ScopedCacheMaxTTLSeconds <= 0 || contract.ScopedCacheTTLMargin <= 1 {
		t.Fatalf("fixture margin fields missing: max=%d margin=%d", contract.ScopedCacheMaxTTLSeconds, contract.ScopedCacheTTLMargin)
	}
	if floor := contract.ScopedCacheMaxTTLSeconds * contract.ScopedCacheTTLMargin; int(OrgCacheEpochTTL.Seconds()) < floor {
		t.Fatalf("OrgCacheEpochTTL=%ds below margin floor %ds (%d x %d)", int(OrgCacheEpochTTL.Seconds()), floor, contract.ScopedCacheMaxTTLSeconds, contract.ScopedCacheTTLMargin)
	}
	for _, testCase := range contract.Cases {
		if got := OrgCacheEpochKey(testCase.OrgID); got != testCase.Key {
			t.Fatalf("OrgCacheEpochKey(%q)=%q, Python producer wrote %q", testCase.OrgID, got, testCase.Key)
		}
	}
}

// TestValkeyFactoryPinsTheSharedKeyspace: the Python cache reads REDIS_URL
// and the Go worker reads VALKEY_URI; every checked-in deployment points
// both at redis://valkey:6379/1. The Go factory already refuses any DB but
// 1 (internal/storage/valkey/factory.go Validate) -- pinned here because the
// epoch bump is silently useless from any other keyspace.
func TestValkeyFactoryPinsTheSharedKeyspace(t *testing.T) {
	t.Parallel()
	if err := valkeystore.DefaultConfig("redis://valkey:6379/1").Validate(); err != nil {
		t.Fatalf("DB 1 rejected: %v", err)
	}
	for _, uri := range []string{"redis://valkey:6379/0", "redis://valkey:6379/2", "redis://valkey:6379"} {
		if err := valkeystore.DefaultConfig(uri).Validate(); err == nil {
			t.Fatalf("%s accepted: the Go epoch bump would land outside the Python cache keyspace", uri)
		}
	}
}

func TestOrgCacheEpochKeyDistinguishesOrgs(t *testing.T) {
	t.Parallel()
	if OrgCacheEpochKey("a") == OrgCacheEpochKey("b") {
		t.Fatal("two orgs collapsed onto one epoch key")
	}
}

func TestNewValkeyOrgCacheInvalidatorRejectsNilClient(t *testing.T) {
	t.Parallel()
	if _, err := NewValkeyOrgCacheInvalidator(nil); err == nil {
		t.Fatal("nil client accepted")
	}
}
