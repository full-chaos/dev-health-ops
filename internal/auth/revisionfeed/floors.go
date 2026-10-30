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

// MaxRevision is the largest valid revision, 2^53-1: the largest integer every consumer (a JSON number, a float64) holds
// exactly. A valid revision is 1..MaxRevision; one check serves every ingress point (the floors, the live messages, the
// catch-up).
const MaxRevision int64 = 1<<53 - 1

func validRevision(revision int64) bool { return revision >= 1 && revision <= MaxRevision }

// splitAggregateKey is the inverse of contracts.AggregateKey: the type ends at the first colon (aggregate types have none),
// the id is the rest (it may hold colons). Both must be non-empty.
func splitAggregateKey(field string) (typ, id string, ok bool) {
	typ, id, ok = strings.Cut(field, ":")
	if !ok || typ == "" || id == "" {
		return "", "", false
	}
	return typ, id, true
}

// ErrCorruptFloors: a field of the hash is not "<type>:<id>", or a value is not a decimal integer in 1..MaxRevision. The text never quotes the field (it holds an
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
		if _, _, ok := splitAggregateKey(field); !ok {
			return nil, ErrCorruptFloors
		}
		revision, err := strconv.ParseInt(value, 10, 64)
		if err != nil || !validRevision(revision) {
			return nil, ErrCorruptFloors
		}
		floors[field] = revision
	}
	return floors, nil
}

// Status is whether the hash can be trusted as a floor source. Usable needs ALL of: the AOF is on (aof_enabled 1), its last
// write and its last rewrite succeeded (aof_last_write_status ok, aof_last_bgrewrite_status ok), and the server does not
// evict the hash (maxmemory_policy noeviction or volatile-*) and the hash must have no TTL. An evicted, unflushed or lost hash
// gives a LOWER floor, the unsafe direction, so a missing, unknown or failed field is not usable.
//
// appendfsync is NOT in INFO persistence (INFO reports aof_delayed_fsync, a counter, not the policy), and CONFIG GET is
// not used (acp ruling on PR B r1), so the fsync policy is not read here: the chart sets no appendfsync (the server
// default, everysec) and a chart test pins that it never turns it off. Reading it needs a ruling.
type Status struct {
	AppendOnly      bool
	AOFWriteOK      bool
	AOFRewriteOK    bool
	MaxMemoryPolicy string
	// HashHasTTL is read under every policy that could otherwise be usable (noeviction and volatile-*): a hash with a TTL expires.
	HashHasTTL bool
	Usable     bool
	// Reason is one of StatusReasons.
	Reason string
}

// StatusReasons is the closed set of Status.Reason values.
func StatusReasons() []string {
	return []string{"ok", "appendonly_off", "aof_status_unknown", "aof_write_failed", "aof_rewrite_failed", "policy_evicts", "hash_has_ttl"}
}

// ReadStatus reads INFO persistence and INFO memory in one call (and the hash's TTL under a volatile-* policy). The login
// needs +info: the auth-service login does not have it, the fence's own login does.
func ReadStatus(ctx context.Context, client Client) (Status, error) {
	info, err := client.Info(ctx, "persistence", "memory")
	if err != nil {
		return Status{}, ErrUnavailable
	}
	enabled, write, rewrite, policy := info["aof_enabled"], info["aof_last_write_status"], info["aof_last_bgrewrite_status"], info["maxmemory_policy"]
	status := Status{AppendOnly: enabled == "1", AOFWriteOK: write == "ok", AOFRewriteOK: rewrite == "ok", MaxMemoryPolicy: policy}
	switch {
	case enabled != "0" && enabled != "1", write == "", rewrite == "":
		status.Reason = "aof_status_unknown"
	case !status.AppendOnly:
		status.Reason = "appendonly_off"
	case !status.AOFWriteOK:
		status.Reason = "aof_write_failed"
	case !status.AOFRewriteOK:
		status.Reason = "aof_rewrite_failed"
	case policy == "noeviction" || strings.HasPrefix(policy, "volatile-"):
		// A TTL on the hash is checked under every otherwise-usable policy: noeviction keeps an expiring key until it
		// expires, and an expired hash gives a lower floor, the unsafe direction.
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
