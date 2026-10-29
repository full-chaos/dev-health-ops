package revisionfeed

import (
	"context"
	"errors"
	"strconv"
	"strings"
)

// PublishedFloors and ReadStatus are a STABLE interface (acp PLAN GO 10983, ruling 2). The CHAOS-11271 restore fence reads
// them as an ADVISORY second input, never a sole source: floor = max(WAL manifest, these floors), and only while
// ReadStatus says Usable. A change to a name, a return shape, the key layout or the usable rule is a contract change.

// ErrCorruptFloors: a value of the hash is not a positive decimal integer. The text never quotes the field (it holds an
// aggregate id) or the value.
var ErrCorruptFloors = errors.New("revisionfeed: the published floors are corrupt")

// PublishedFloors is the highest revision the sink has delivered per aggregate, keyed "<aggregate_type>:<aggregate_id>"
// (contracts.AggregateKey). An empty hash is an empty non-nil map, not an error: no event was delivered yet.
func PublishedFloors(ctx context.Context, client Client) (map[string]int64, error) {
	raw, err := client.HGetAll(ctx, HighWaterKey)
	if err != nil {
		return nil, ErrUnavailable
	}
	floors := make(map[string]int64, len(raw))
	for field, value := range raw {
		revision, err := strconv.ParseInt(value, 10, 64)
		if err != nil || revision < 1 {
			return nil, ErrCorruptFloors
		}
		floors[field] = revision
	}
	return floors, nil
}

// Status is whether the hash can be trusted as a floor source: the server must persist (appendonly yes) and must not
// evict the hash (maxmemory-policy noeviction, or volatile-* with no TTL on the hash). An evicted or lost hash gives a
// LOWER floor, the unsafe direction, so anything else is not usable.
type Status struct {
	AppendOnly      bool
	MaxMemoryPolicy string
	// HashHasTTL is read only under a volatile-* policy, the one case where a TTL decides eviction.
	HashHasTTL bool
	Usable     bool
	// Reason is one of StatusReasons.
	Reason string
}

// StatusReasons is the closed set of Status.Reason values.
func StatusReasons() []string {
	return []string{"ok", "appendonly_off", "policy_evicts", "hash_has_ttl"}
}

// ReadStatus reads CONFIG GET appendonly and maxmemory-policy (and the hash's TTL under a volatile-* policy). The login
// needs +config|get: the auth-service login does not have it, the fence's own login does.
func ReadStatus(ctx context.Context, client Client) (Status, error) {
	appendonly, err := client.ConfigGet(ctx, "appendonly")
	if err != nil {
		return Status{}, ErrUnavailable
	}
	policy, err := client.ConfigGet(ctx, "maxmemory-policy")
	if err != nil {
		return Status{}, ErrUnavailable
	}
	status := Status{AppendOnly: appendonly == "yes", MaxMemoryPolicy: policy}
	switch {
	case !status.AppendOnly:
		status.Reason = "appendonly_off"
	case policy == "noeviction":
		status.Usable, status.Reason = true, "ok"
	case strings.HasPrefix(policy, "volatile-"):
		ttl, err := client.TTL(ctx, HighWaterKey)
		if err != nil {
			return Status{}, ErrUnavailable
		}
		status.HashHasTTL = ttl >= 0
		if status.HashHasTTL {
			status.Reason = "hash_has_ttl"
		} else {
			status.Usable, status.Reason = true, "ok"
		}
	default:
		status.Reason = "policy_evicts"
	}
	return status, nil
}
