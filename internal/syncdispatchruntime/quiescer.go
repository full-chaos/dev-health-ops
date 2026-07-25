package syncdispatchruntime

import (
	"context"
	"errors"
	"sync"
)

var ErrGenerationClosed = errors.New("sync dispatch generation is quiesced")

// GenerationTracker bounds post_sync pre-publish windows in one publisher
// process. It is not a distributed quiescence primitive and must not be used
// as a syncroute.Quiescer: a cross-process cutover needs a separately proven
// external barrier.
type GenerationTracker struct {
	mu            sync.Mutex
	active        map[int64]int
	closedThrough int64
	changed       chan struct{}
}

func NewGenerationTracker() *GenerationTracker {
	return &GenerationTracker{active: make(map[int64]int), changed: make(chan struct{})}
}

func (tracker *GenerationTracker) valid() bool {
	// active is initialized once and never replaced. changed is deliberately
	// excluded because signalLocked replaces it while holding mu.
	return tracker != nil && tracker.active != nil
}

// EnterLocalHandoff records a post_sync handoff in this process immediately
// before its external effect. The returned leave function is idempotent and
// must be deferred by the concrete handoff adapter.
func (tracker *GenerationTracker) EnterLocalHandoff(generation int64) (func(), error) {
	if !tracker.valid() || generation < 1 {
		return nil, ErrCapabilityUnavailable
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if generation <= tracker.closedThrough {
		return nil, ErrGenerationClosed
	}
	tracker.active[generation]++
	var once sync.Once
	return func() {
		once.Do(func() { tracker.leave(generation) })
	}, nil
}

// WaitForLocalHandoffs closes this process to the requested and older
// generations, then waits for its existing handoffs to leave. It deliberately
// makes no claim about handoffs in another process or a Celery worker.
func (tracker *GenerationTracker) WaitForLocalHandoffs(ctx context.Context, throughGeneration int64) error {
	if !tracker.valid() || ctx == nil || throughGeneration < 1 {
		return ErrCapabilityUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		tracker.mu.Lock()
		if throughGeneration > tracker.closedThrough {
			tracker.closedThrough = throughGeneration
			tracker.signalLocked()
		}
		if !tracker.activeThroughLocked(throughGeneration) {
			tracker.mu.Unlock()
			return nil
		}
		changed := tracker.changed
		tracker.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (tracker *GenerationTracker) leave(generation int64) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.active[generation] <= 1 {
		delete(tracker.active, generation)
	} else {
		tracker.active[generation]--
	}
	tracker.signalLocked()
}

func (tracker *GenerationTracker) activeThroughLocked(generation int64) bool {
	for activeGeneration, count := range tracker.active {
		if activeGeneration <= generation && count > 0 {
			return true
		}
	}
	return false
}

func (tracker *GenerationTracker) signalLocked() {
	close(tracker.changed)
	tracker.changed = make(chan struct{})
}
