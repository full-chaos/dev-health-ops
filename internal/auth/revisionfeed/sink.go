package revisionfeed

import (
	"context"
	"errors"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// highWaterScript is the sink's one Lua call: it raises the aggregate's field in the high-water hash to the incoming
// revision only when that is higher (a delayed or redelivered lower revision never lowers it), and then publishes
// "<type>:<id>:<revision>" on the channel. One script means the hash and the message agree and no other client sees one
// without the other. EVAL only (the ACL user has +eval, not +evalsha), so every command inside it needs its own grant:
// +hget +hset +publish.
//
//	KEYS[1] the hash, ARGV[1] "<type>:<id>", ARGV[2] the revision, ARGV[3] the channel
const highWaterScript = `
local current = tonumber(redis.call('HGET', KEYS[1], ARGV[1]))
local incoming = tonumber(ARGV[2])
local raised = 0
if current == nil or incoming > current then
  redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
  raised = 1
end
redis.call('PUBLISH', ARGV[3], ARGV[1] .. ':' .. ARGV[2])
return raised
`

// deliveryError carries one of audit.AllFailureReasons and a fixed text: the publisher stores only the reason, and an
// error that quoted Valkey's text could quote the URI or the payload.
type deliveryError struct{ reason string }

func (e deliveryError) Error() string          { return "revisionfeed: delivery failed: " + e.reason }
func (e deliveryError) DeliveryReason() string { return e.reason }

// Sink is the audit.Sink that feeds the revision feed. Deliver returns nil only after the script succeeded.
type Sink struct{ client Client }

var _ audit.Sink = (*Sink)(nil)

// NewSink returns a sink over client.
func NewSink(client Client) (*Sink, error) {
	if client == nil {
		return nil, errors.New("revisionfeed: a client is required")
	}
	return &Sink{client: client}, nil
}

// Deliver maps the row to a provisioning event (a row whose key disagrees with its revision is consumer_rejected and
// never reaches Valkey), then runs the script. A Valkey that cannot be reached is consumer_unavailable; an error reply
// (an ACL refusal) is consumer_rejected.
func (s *Sink) Deliver(ctx context.Context, e audit.Event) error {
	mapped, err := FromOutbox(e)
	if err != nil {
		return deliveryError{reason: "consumer_rejected"}
	}
	field := contracts.AggregateKey(mapped.AggregateType, mapped.AggregateID)
	revision := strconv.FormatInt(int64(mapped.Payload.Revision), 10)
	if _, err := s.client.EvalInt(ctx, highWaterScript, []string{HighWaterKey}, []string{field, revision, Channel}); err != nil {
		if errors.Is(classify(err), ErrRejected) {
			return deliveryError{reason: "consumer_rejected"}
		}
		return deliveryError{reason: "consumer_unavailable"}
	}
	return nil
}
