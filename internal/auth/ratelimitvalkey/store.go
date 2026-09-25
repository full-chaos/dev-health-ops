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
// needs EVAL, INCR and PEXPIRE on "dho:rl:*".
const keyPrefix = "dho:rl:"

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

// hitScript is one atomic step, one round trip.
//
//	KEYS[1] the (limit, key, path) counter, ARGV[1] the window in ms
//
// It returns the counter after the increment. The counter's TTL is set on
// its first hit, so the window starts at the caller's first hit and lasts
// exactly the window -- slowapi's fixed window -- and a refused hit still
// increments. INCR and PEXPIRE run in one script, so a crash between them
// cannot leave a counter that never expires.
//
// Nothing bounds how many distinct paths one caller may count. slowapi
// keeps one counter per (caller key, exact path) and bounds nothing either:
// its storage limits itself by TTL, and so does this one -- every counter
// expires with its window, so the keyspace holds at most the hits of the
// last window. A bound on distinct paths (this store had one, 1,000 per
// caller) refuses a caller Python serves: a client reading its 1,001st
// distinct /batches/{id} in a window got 429 (CHAOS-6624).
const hitScript = `
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
		[]string{CounterKey(limit, hit.Key, hit.Path)},
		[]string{strconv.FormatInt(limit.Window.Milliseconds(), 10)})
	count, err := result.AsInt64()
	if err != nil {
		return 0, fmt.Errorf("rate limit script for %q: %w", limit.ID, err)
	}
	return count, nil
}

// Peek implements httpapi.CounterStore: the counter's value, 0 when it has no
// live counter (an expired or never-hit key does not exist).
func (s *Store) Peek(ctx context.Context, hit httpapi.Hit) (int64, error) {
	result := s.client.Do(ctx, s.client.B().Get().Key(CounterKey(hit.Limit, hit.Key, hit.Path)).Build())
	count, err := result.AsInt64()
	if valkeygo.IsValkeyNil(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("rate limit peek for %q: %w", hit.Limit.ID, err)
	}
	return count, nil
}
