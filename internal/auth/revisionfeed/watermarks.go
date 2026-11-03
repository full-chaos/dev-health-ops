package revisionfeed

import (
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Watermarks is the latest revision seen per aggregate, read in process (the decision cache of 5.8 reads it). It only
// moves up: a stale or duplicate delivery never lowers it.
type Watermarks struct {
	mu     sync.RWMutex
	latest map[string]int64
}

// NewWatermarks returns an empty set.
func NewWatermarks() *Watermarks { return &Watermarks{latest: map[string]int64{}} }

// Latest is the latest revision seen for the aggregate, and false when none was seen.
func (w *Watermarks) Latest(aggregateType, aggregateID string) (int64, bool) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	revision, ok := w.latest[contracts.AggregateKey(aggregateType, aggregateID)]
	return revision, ok
}

func (w *Watermarks) record(aggregateType, aggregateID string, revision int64) {
	key := contracts.AggregateKey(aggregateType, aggregateID)
	w.mu.Lock()
	defer w.mu.Unlock()
	if revision > w.latest[key] {
		w.latest[key] = revision
	}
}
