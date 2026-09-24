// Package ratelimitvalkey is the shared, Valkey-backed httpapi.CounterStore: the
// rate-limit counters live in the one Valkey every api replica already
// talks to (the backend the Python api reaches through REDIS_URL), so a
// limit holds across replicas instead of being counted per process.
package ratelimitvalkey

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	valkeygo "github.com/valkey-io/valkey-go"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// keyPrefix namespaces every key this store writes; the api's Valkey login
// needs EVAL, INCR, PEXPIRE, SADD, SISMEMBER, SCARD and PTTL on "dho:rl:*".
const keyPrefix = "dho:rl:"

// MaxPathsPerKey bounds how many distinct paths one caller key may hold live
// counters for within a window: a route whose path carries a caller-chosen
// id (a user id, an organization id) lets one caller mint unlimited paths,
// and every path is a Valkey key. The same bound the in-process limiter has
// (httpapi's per-key entry bound), and for the same reason: the minter is
// refused for NEW paths, no other caller is affected.
const MaxPathsPerKey = 1_000

// digest is the part of a key that identifies a caller or a request path,
// as it is stored: the first 128 bits of its SHA-256 in hex. The Valkey
// keyspace is readable by anything that can run KEYS or SCAN, and a caller
// key is an admin's user id while a path carries an organization or user id,
// so neither is stored verbatim (the Python api hashes its admin key for the
// same reason). The limit id stays readable: it names the limit, never a
// caller.
func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:16])
}

// CounterKey is the Valkey key that counts one (limit, caller key, path).
func CounterKey(limit httpapi.Limit, key, path string) string {
	return keyPrefix + limit.ID + ":" + digest(key) + ":" + digest(path)
}

// PathsKey is the Valkey key of a caller's set of live paths under a limit.
func PathsKey(limit httpapi.Limit, key string) string {
	return keyPrefix + "paths:" + limit.ID + ":" + digest(key)
}

// hitScript is one atomic step, one round trip.
//
//	KEYS[1] the (limit, key, path) counter, KEYS[2] the caller's path set
//	ARGV[1] window in ms, ARGV[2] the per-key path bound, ARGV[3] the path
//	(digest)
//
// It returns -1 when the path is new to the caller and the caller already
// holds the bound (a refusal that counts nothing), else the counter after
// the increment. The counter's TTL is set on its first hit, so the window
// starts at the caller's first hit and lasts exactly the window -- slowapi's
// fixed window -- and a refused hit still increments. INCR and PEXPIRE run
// in one script, so a crash between them cannot leave a counter that never
// expires. The path set carries the window's TTL from its first insertion:
// once it lapses, still-live counters of earlier paths are new to it again
// (at most twice the bound of live counters exist for one caller), which is
// the price of a single TTL per set.
const hitScript = `
if redis.call('SISMEMBER', KEYS[2], ARGV[3]) == 0 then
  if redis.call('SCARD', KEYS[2]) >= tonumber(ARGV[2]) then
    return -1
  end
  redis.call('SADD', KEYS[2], ARGV[3])
  if redis.call('PTTL', KEYS[2]) < 0 then
    redis.call('PEXPIRE', KEYS[2], ARGV[1])
  end
end
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n
`

// Store is the Valkey CounterStore.
type Store struct {
	client valkeygo.Client
	script *valkeygo.Lua
}

var _ httpapi.CounterStore = (*Store)(nil)

// New returns a store over client (which must be the api's Valkey client,
// database 1).
func New(client valkeygo.Client) (*Store, error) {
	if client == nil {
		return nil, errors.New("ratelimitvalkey: nil Valkey client")
	}
	return &Store{client: client, script: valkeygo.NewLuaScriptNoSha(hitScript)}, nil
}

// Backend implements httpapi.CounterStore: Python reports "redis" for the same
// role, so the /health bodies match.
func (s *Store) Backend() string { return "redis" }

// Increment implements httpapi.CounterStore.
func (s *Store) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	limit := hit.Limit
	result := s.script.Exec(ctx, s.client,
		[]string{CounterKey(limit, hit.Key, hit.Path), PathsKey(limit, hit.Key)},
		[]string{strconv.FormatInt(limit.Window.Milliseconds(), 10), strconv.Itoa(MaxPathsPerKey), digest(hit.Path)})
	count, err := result.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("rate limit script for %q: %w", limit.ID, err)
	}
	if count < 0 {
		return 0, httpapi.ErrPathBound
	}
	return count, nil
}
