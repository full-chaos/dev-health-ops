package restorefence

import (
	"context"
	"sort"
	"sync"
)

// FloorInput is one source of revision floors besides the manifest: a
// consumer's high-water marks per (aggregate_type, aggregate_id). The fence
// applies the MAXIMUM of every input and the restored value. A consumer of the
// auth outbox (an audit.Sink wired through audit.NewPublisher) must register
// one; TestEveryOutboxConsumerHasAFenceFloorInput fails when it does not.
type FloorInput interface {
	Name() string
	// Floors returns the highest revision the consumer holds, keyed
	// "aggregate_type:aggregate_id".
	Floors(ctx context.Context) (map[string]int64, error)
}

var (
	inputsMu sync.Mutex
	inputs   []FloorInput
)

// RegisterFloorInput adds a consumer's high-water input to the fence.
func RegisterFloorInput(in FloorInput) {
	inputsMu.Lock()
	defer inputsMu.Unlock()
	inputs = append(inputs, in)
}

// RegisteredFloorInputs returns the registered inputs in name order.
func RegisteredFloorInputs() []FloorInput {
	inputsMu.Lock()
	defer inputsMu.Unlock()
	out := append([]FloorInput(nil), inputs...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
	return out
}

// mergeFloors returns the per-aggregate maximum of the manifest floors and the
// inputs, in (type, id) order.
func mergeFloors(manifest []Floor, extra []map[string]int64) []Floor {
	best := map[string]Floor{}
	put := func(f Floor) {
		k := f.AggregateType + ":" + f.AggregateID
		if cur, ok := best[k]; !ok || f.Revision > cur.Revision {
			best[k] = f
		}
	}
	for _, f := range manifest {
		put(f)
	}
	for _, in := range extra {
		for k, rev := range in {
			typ, id, ok := splitFloorKey(k)
			if !ok || rev < 1 || rev > maxRevision {
				continue
			}
			put(Floor{AggregateType: typ, AggregateID: id, Revision: rev})
		}
	}
	out := make([]Floor, 0, len(best))
	for _, f := range best {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AggregateType != out[j].AggregateType {
			return out[i].AggregateType < out[j].AggregateType
		}
		return out[i].AggregateID < out[j].AggregateID
	})
	return out
}

func splitFloorKey(k string) (typ, id string, ok bool) {
	for i := 0; i < len(k); i++ {
		if k[i] == ':' {
			return k[:i], k[i+1:], i > 0 && i+1 < len(k)
		}
	}
	return "", "", false
}
