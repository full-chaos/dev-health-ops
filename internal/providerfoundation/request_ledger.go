package providerfoundation

import (
	"context"
	"sort"
	"sync"
)

// LedgerUnclassified is the route family of traffic no route labelled, as in
// the recorder the Python sync kept: it is shown, not hidden, so calibration
// sees real provider calls that no estimate covered.
const LedgerUnclassified = "unclassified"

// ledgerMaxKeys bounds the distinct (route family, dimension) keys one unit
// keeps; the requests beyond it are counted in Dropped (Python's recorder
// capped at 50 keys the same way).
const ledgerMaxKeys = 50

// LedgerKey is one (route family, dimension) a request is accounted to.
type LedgerKey struct {
	RouteFamily string
	Dimension   string
}

// LedgerEntry is a key with the wire requests counted under it.
type LedgerEntry struct {
	LedgerKey
	Requests int
}

// RequestLedger counts the wire requests of one provider unit per (route
// family, dimension): the actuals the unit's budget estimates are compared with.
// Safe for concurrent use; a nil ledger counts nothing.
type RequestLedger struct {
	mu      sync.Mutex
	counts  map[LedgerKey]int
	dropped int
}

func NewRequestLedger() *RequestLedger { return &RequestLedger{counts: map[LedgerKey]int{}} }

// Record counts one wire request under key.
func (l *RequestLedger) Record(key LedgerKey) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, known := l.counts[key]; !known && len(l.counts) >= ledgerMaxKeys {
		l.dropped++
		return
	}
	l.counts[key]++
}

// Snapshot returns the entries sorted by (route family, dimension) and the
// number of requests dropped past the key cap (the comparison is incomplete
// when it is not zero).
func (l *RequestLedger) Snapshot() (entries []LedgerEntry, dropped int) {
	if l == nil {
		return nil, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, count := range l.counts {
		entries = append(entries, LedgerEntry{LedgerKey: key, Requests: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].RouteFamily != entries[j].RouteFamily {
			return entries[i].RouteFamily < entries[j].RouteFamily
		}
		return entries[i].Dimension < entries[j].Dimension
	})
	return entries, l.dropped
}

type requestAccountKey struct{}

type requestAccount struct{ routeFamily, dimension string }

// WithRequestAccount labels the requests made with ctx: the route family they
// belong to (the estimator's own name for it) and, when the wire cannot say, the
// budget dimension. An empty field leaves that part to the default.
func WithRequestAccount(ctx context.Context, routeFamily, dimension string) context.Context {
	return context.WithValue(ctx, requestAccountKey{}, requestAccount{routeFamily: routeFamily, dimension: dimension})
}

// recordWireRequest counts one wire request of this client under its account:
// the family and dimension the route labelled, else the family "unclassified"
// and the dimension the wire implies (WithBudgetDimension, then the request
// path). The first attempt and every retry each count: each draws on the
// provider's budget.
func (c *HTTPClient) recordWireRequest(ctx context.Context, path string) {
	if c.Ledger == nil {
		return
	}
	account, _ := ctx.Value(requestAccountKey{}).(requestAccount)
	key := LedgerKey{RouteFamily: account.routeFamily, Dimension: account.dimension}
	if key.RouteFamily == "" {
		key.RouteFamily = LedgerUnclassified
	}
	if key.Dimension == "" {
		key.Dimension = budgetDimension(ctx)
	}
	if key.Dimension == "" {
		key.Dimension = RateLimitDimension(c.Provider, path, "")
	}
	c.Ledger.Record(key)
}
