package restorefence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"sync"
)

// FloorInput is one source of revision floors besides the manifest: a
// consumer's high-water marks per (aggregate_type, aggregate_id). The fence
// applies the MAXIMUM of every input and the restored value. A consumer of the
// auth outbox (an audit.Sink wired through audit.NewPublisher) must register
// one; TestEveryOutboxConsumerHasAFenceFloorInput fails when it does not.
//
// A registered input is ADVISORY: it is used only while Status returns nil (the
// consumer's store is durable enough to trust as a lower bound); an input whose
// status fails is reported and skipped.
type FloorInput interface {
	Name() string
	// Status returns nil when the input may be used.
	Status(ctx context.Context) error
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

// InputReport is one input's line in the apply report and the status output.
type InputReport struct {
	Name string `json:"name"`
	// Kind is "wal_manifest", "blind_margin" or "consumer".
	Kind   string `json:"kind"`
	Used   bool   `json:"used"`
	Floors int    `json:"floors"`
	// Reason is a closed token for an input that was not used.
	Reason string `json:"reason,omitempty"`
}

type resolved struct {
	floors  []Floor
	reports []InputReport
	source  string // the state row's source: manifest, blind_margin or inputs
	digest  string // the state row's manifest_sha256
	margin  int64
}

// resolveInputs names every input of one apply and takes the floor as the
// maximum over them: the WAL manifest, then each registered consumer input
// whose status passes. The blind margin is the fallback of last resort: it is
// refused while a manifest or a usable consumer input exists.
func resolveInputs(ctx context.Context, m *Manifest, opts Options, registered []FloorInput) (resolved, error) {
	var out resolved
	var manifestFloors []Floor
	if m != nil {
		manifestFloors = m.Floors
		out.reports = append(out.reports, InputReport{Name: "wal_manifest", Kind: "wal_manifest", Used: true, Floors: len(m.Floors)})
		out.source, out.digest = "manifest", m.SHA256
	}
	var extra []map[string]int64
	usable := 0
	for _, in := range registered {
		rep := InputReport{Name: in.Name(), Kind: "consumer"}
		if err := in.Status(ctx); err != nil {
			rep.Reason = "status_failed"
			out.reports = append(out.reports, rep)
			continue
		}
		got, err := in.Floors(ctx)
		if err != nil {
			rep.Reason = "read_failed"
			out.reports = append(out.reports, rep)
			continue
		}
		rep.Used, rep.Floors = true, len(got)
		extra = append(extra, got)
		usable++
		out.reports = append(out.reports, rep)
	}
	switch {
	case opts.BlindMargin > 0 && (m != nil || usable > 0):
		return resolved{}, fmt.Errorf("%w: the blind margin applies only when no manifest and no usable consumer input exists", ErrApply)
	case opts.BlindMargin > 0:
		out.margin, out.source, out.digest = opts.BlindMargin, "blind_margin", stateDigest("blind_margin", []byte(strconv.FormatInt(opts.BlindMargin, 10)))
		out.reports = append(out.reports, InputReport{Name: "blind_margin", Kind: "blind_margin", Used: true})
	case m == nil && usable > 0:
		out.source = "inputs"
	case m == nil:
		return resolved{}, fmt.Errorf("%w: a manifest, a usable consumer input or a blind margin is required", ErrApply)
	}
	out.floors = mergeFloors(manifestFloors, extra)
	if out.source == "inputs" {
		raw, err := json.Marshal(out.floors)
		if err != nil {
			return resolved{}, fmt.Errorf("%w: encoding the input floors", ErrApply)
		}
		out.digest = stateDigest("inputs", raw)
	}
	return out, nil
}

// stateDigest is the state row's manifest_sha256 of an apply without a manifest: it names what was applied (the margin,
// or the merged input floors), so a retry with other values is new work and the same values are a no-op. The kind
// prefix keeps the two apart from each other and from a manifest digest.
func stateDigest(kind string, content []byte) string {
	sum := sha256.Sum256(append([]byte("restorefence/"+kind+"/v1\n"), content...))
	return hex.EncodeToString(sum[:])
}
