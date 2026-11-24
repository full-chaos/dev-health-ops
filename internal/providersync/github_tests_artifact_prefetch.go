package providersync

import (
	"context"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsPrefetchReleaseTimeout bounds the release of a prefetch reservation on a context that is no longer
// cancelled with the walk, so a cancelled run never keeps the sub-budget slot until the key's TTL.
const githubTestsPrefetchReleaseTimeout = 5 * time.Second

// githubTestsPrefetchMinRemainingShare is the least remaining/limit share of the walk's latest rate reading that lets a
// prefetch start (CHAOS-14542): the same 0.45 as the rate gate the prefetch was measured against. Under it, or with no
// reading yet, the walk stays serial. It is a brake, not a guarantee: the reading is this walk's latest answer and can
// lag the installation's other users.
const githubTestsPrefetchMinRemainingShare = 0.45

// PrefetchBudgetCostClass names the own sub-budget class of a cost class's artifact prefetch, and whether the one
// budget table (providerfoundation.CostClassBudgetLimit) holds a row for it. A class without a row never prefetches.
func PrefetchBudgetCostClass(class CostClass) (CostClass, bool) {
	prefetch := class + "-prefetch"
	_, ok := providerfoundation.CostClassBudgetLimit(string(prefetch))
	return prefetch, ok
}

// githubTestsArtifactPrefetcher downloads the NEXT artifact of one run while the walk goroutine downloads and parses
// the current one (CHAOS-14542). It holds at most one download, taken from the same run's sorted artifacts, so it never
// crosses a run. The download runs on a copy of the walk's client with its own counting doer and rate observer (the
// walk's counter and observer are written by the walk goroutine only), under ONE slot of the class's own prefetch
// sub-budget held for the whole download: the walk keeps at most one slot of the unit's class key, as the serial walk
// did, so a sibling unit of the same bucket is never denied by the prefetch. A denied prefetch slot is not a unit
// budget denial: the walk downloads that artifact itself.
type githubTestsArtifactPrefetcher struct {
	ctx      context.Context
	client   *providerfoundation.HTTPClient
	base     *providerfoundation.HTTPClient
	root     string
	maxBytes int64
	items    []githubTestsArtifactPayload
	key      providerfoundation.BudgetKey
	pending  *githubTestsArtifactPrefetch
	// observed is the walk's own rate observer: written and read on the walk goroutine only.
	observed *githubTestsRateObserver
}

type githubTestsArtifactPrefetch struct {
	index    int
	cancel   context.CancelFunc
	done     chan struct{}
	archive  []byte
	notFound bool
	err      error
	requests int
	observed githubTestsRateObserver
}

// githubTestsPrefetchHeldBudget is the budget of the prefetch's client copy: the prefetch already holds its sub-budget
// slot for the whole download, so each call of that download is granted without a second acquire.
type githubTestsPrefetchHeldBudget struct{}

type githubTestsPrefetchHeldReservation struct{}

func (githubTestsPrefetchHeldBudget) Acquire(context.Context, providerfoundation.BudgetKey) (providerfoundation.Reservation, error) {
	return githubTestsPrefetchHeldReservation{}, nil
}

func (githubTestsPrefetchHeldReservation) Release(context.Context) error { return nil }

// newGitHubTestsArtifactPrefetcher returns nil (no prefetch: the walk stays serial) when the client has no budget,
// when its store blocks instead of denying (the in-process store of `dho sync`: a blocked prefetch could hold the walk's
// own call), or when the class has no prefetch row in the budget table. client is the walk's client; base is the
// client under the walk's counting doer.
func newGitHubTestsArtifactPrefetcher(
	ctx context.Context, client, base *providerfoundation.HTTPClient, observed *githubTestsRateObserver,
	root string, maxBytes int64, items []githubTestsArtifactPayload,
) *githubTestsArtifactPrefetcher {
	if client == nil || client.Budget == nil || base == nil || observed == nil || len(items) < 2 {
		return nil
	}
	if _, blocking := client.Budget.(*InProcessBudgetStore); blocking {
		return nil
	}
	class, ok := PrefetchBudgetCostClass(CostClass(client.BudgetKey.CostClass))
	if !ok {
		return nil
	}
	limit, _ := providerfoundation.CostClassBudgetLimit(string(class))
	key := client.BudgetKey
	key.CostClass, key.Limit = string(class), limit
	if key.Validate() != nil {
		return nil
	}
	return &githubTestsArtifactPrefetcher{
		ctx: ctx, client: client, base: base, observed: observed, root: root, maxBytes: maxBytes, items: items, key: key,
	}
}

// startNext starts the download of items[current+1] while the walk downloads items[current] itself, when no prefetch
// is pending and the listed sizes of both fit the per-artifact cap together. The walk calls it only for an artifact it
// downloads itself, so at most two bodies are held: the walk's and the prefetch's. A listed size that is unknown (-1) or above
// the cap is never prefetched; an unknown current size counts as the whole cap. A denied sub-budget slot starts
// nothing.
func (prefetcher *githubTestsArtifactPrefetcher) startNext(current int) {
	if prefetcher == nil || prefetcher.pending != nil {
		return
	}
	next := current + 1
	if current < 0 || next >= len(prefetcher.items) || prefetcher.items[next].Expired {
		return
	}
	nextSize, currentSize := prefetcher.items[next].listedSize(), prefetcher.items[current].listedSize()
	if currentSize < 0 {
		currentSize = prefetcher.maxBytes
	}
	if nextSize < 0 || nextSize > prefetcher.maxBytes || currentSize+nextSize > prefetcher.maxBytes {
		return
	}
	if prefetcher.ctx.Err() != nil || !prefetcher.headroom() {
		return
	}
	reservation, err := prefetcher.client.Budget.Acquire(prefetcher.ctx, prefetcher.key)
	if err != nil || reservation == nil {
		return
	}
	ctx, cancel := context.WithCancel(prefetcher.ctx)
	prefetch := &githubTestsArtifactPrefetch{index: next, cancel: cancel, done: make(chan struct{})}
	helper := *prefetcher.client
	helper.Doer = githubTestsChunkCountingDoer{
		delegate: prefetcher.base.Doer, attempts: &prefetch.requests, observed: &prefetch.observed, floor: &githubTestsRateFloorGate{},
	}
	helper.Budget = githubTestsPrefetchHeldBudget{}
	artifact := prefetcher.items[next]
	root, maxBytes := prefetcher.root, prefetcher.maxBytes
	go func() {
		defer close(prefetch.done)
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), githubTestsPrefetchReleaseTimeout)
			defer releaseCancel()
			_ = reservation.Release(releaseCtx)
		}()
		prefetch.archive, _, prefetch.notFound, prefetch.err = downloadGitHubTestsListedArtifact(ctx, &helper, root, string(artifact.ID), maxBytes, artifact.listedSize())
	}()
	prefetcher.pending = prefetch
}

// headroom reports whether the walk's latest rate reading leaves at least githubTestsPrefetchMinRemainingShare of the
// limit. No reading yet is no headroom: the walk stays serial until it has one.
func (prefetcher *githubTestsArtifactPrefetcher) headroom() bool {
	observed := prefetcher.observed
	if !observed.seen || observed.limit <= 0 {
		return false
	}
	return float64(observed.remaining) >= githubTestsPrefetchMinRemainingShare*float64(observed.limit)
}

// take hands the walk the prefetched download of items[index] when one is pending for it: it waits for the download,
// and returns its result with the wire requests it sent and its rate observer. ok is false when nothing was
// prefetched for index: the walk downloads it itself.
func (prefetcher *githubTestsArtifactPrefetcher) take(index int) (archive []byte, notFound bool, err error, requests int, observed githubTestsRateObserver, ok bool) {
	if prefetcher == nil || prefetcher.pending == nil || prefetcher.pending.index != index {
		return nil, false, nil, 0, githubTestsRateObserver{}, false
	}
	prefetch := prefetcher.pending
	prefetcher.pending = nil
	<-prefetch.done
	prefetch.cancel()
	return prefetch.archive, prefetch.notFound, prefetch.err, prefetch.requests, prefetch.observed, true
}

// close cancels a prefetch the walk will not take (the run ended early), waits for it, and returns the wire requests
// it sent, so every request of the attempt is counted once. It is safe on nil and after a take.
func (prefetcher *githubTestsArtifactPrefetcher) close() int {
	if prefetcher == nil || prefetcher.pending == nil {
		return 0
	}
	prefetch := prefetcher.pending
	prefetcher.pending = nil
	prefetch.cancel()
	<-prefetch.done
	return prefetch.requests
}
