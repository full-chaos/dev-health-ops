package providerfoundation

import (
	"context"
	"net/http"
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
	if l.counts == nil {
		l.counts = map[LedgerKey]int{}
	}
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

// ledgerKey is the account one request of this client is counted under: the
// family and dimension the route labelled, else the family "unclassified" and
// the dimension the wire implies (WithBudgetDimension, then the request path).
func (c *HTTPClient) ledgerKey(ctx context.Context, path string) LedgerKey {
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
	return key
}

// recordWireRequest counts one wire request of this client under its account.
// The first attempt and every retry each count: each draws on the provider's
// budget. A redirect the client then follows is counted by recordRedirectHop.
func (c *HTTPClient) recordWireRequest(ctx context.Context, path string) {
	if ledger := c.ledgerFor(ctx); ledger != nil {
		ledger.Record(c.ledgerKey(ctx, path))
	}
}

type hopRecorderKey struct{}

type hopRecorder struct {
	ledger *RequestLedger
	key    LedgerKey
	mu     sync.Mutex
	seen   map[*http.Request]struct{}
}

// newHopRecorder is the recorder of one call: the ledger and the account its
// redirect hops are counted under (nil when the client has no ledger).
func (c *HTTPClient) newHopRecorder(ctx context.Context, path string) *hopRecorder {
	ledger := c.ledgerFor(ctx)
	if ledger == nil {
		return nil
	}
	return &hopRecorder{ledger: ledger, key: c.ledgerKey(ctx, path), seen: map[*http.Request]struct{}{}}
}

// record counts one followed redirect hop as a wire request, once: the request
// of a hop passes through every redirect guard of the chain, and each may ask.
func (r *hopRecorder) record(request *http.Request) {
	if r == nil {
		return
	}
	r.mu.Lock()
	_, counted := r.seen[request]
	r.seen[request] = struct{}{}
	r.mu.Unlock()
	if !counted {
		r.ledger.Record(r.key)
	}
}

// withHopRecorder carries the recorder in the request context. net/http gives
// a followed redirect the context of the request it follows, so a guard
// reached through a route's opaque decorator counts the hop under the same
// family and dimension. A guard the per-call chain installs asks the recorder
// directly, so a decorator that drops the context does not lose the hop.
func withHopRecorder(ctx context.Context, recorder *hopRecorder) context.Context {
	if recorder == nil {
		return ctx
	}
	return context.WithValue(ctx, hopRecorderKey{}, recorder)
}

// recordRedirectHop counts the hop of a redirect request through the recorder
// its context carries.
func recordRedirectHop(request *http.Request) {
	if recorder, ok := request.Context().Value(hopRecorderKey{}).(*hopRecorder); ok {
		recorder.record(request)
	}
}

// WithRequestFamily labels the requests made with ctx with a route family and
// leaves the dimension an outer account set.
func WithRequestFamily(ctx context.Context, routeFamily string) context.Context {
	account, _ := ctx.Value(requestAccountKey{}).(requestAccount)
	return WithRequestAccount(ctx, routeFamily, account.dimension)
}

type unitLedgerKey struct{}

// WithUnitLedger makes every HTTPClient that has no Ledger of its own count the
// requests it sends with ctx (or a context derived from it) into ledger: the
// execution of a unit counts all of its clients, whichever route built them. A
// client's own Ledger wins, so a request is never counted twice.
func WithUnitLedger(ctx context.Context, ledger *RequestLedger) context.Context {
	return context.WithValue(ctx, unitLedgerKey{}, ledger)
}

// ledgerFor is the ledger a request of this client with ctx is counted into.
func (c *HTTPClient) ledgerFor(ctx context.Context) *RequestLedger {
	if c.Ledger != nil {
		return c.Ledger
	}
	ledger, _ := ctx.Value(unitLedgerKey{}).(*RequestLedger)
	return ledger
}
