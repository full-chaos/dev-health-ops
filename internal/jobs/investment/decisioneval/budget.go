package decisioneval

import (
	"errors"
	"fmt"
	"sync"
)

// ErrBudgetRefused is returned when a request would pass the spend cap of its
// provider (or the cap is 0). The request is not sent.
var ErrBudgetRefused = errors.New("decisioneval: spend cap would be passed: request refused")

// Budget enforces a HARD spend cap for each provider. A reservation is taken
// before every send; a request whose estimate would take spent + reserved over
// the cap is refused. A cap of 0 (or no cap) refuses everything.
type Budget struct {
	mu       sync.Mutex
	caps     map[string]float64
	spent    map[string]float64
	inflight map[string]float64
	nextID   int
	open     map[int]reservation
}

type reservation struct {
	provider string
	usd      float64
}

// NewBudget builds a budget. spent is the ledger total (completed attempts plus
// orphan reservations) for each provider.
func NewBudget(caps, spent map[string]float64) *Budget {
	b := &Budget{caps: map[string]float64{}, spent: map[string]float64{}, inflight: map[string]float64{}, open: map[int]reservation{}}
	for k, v := range caps {
		b.caps[k] = v
	}
	for k, v := range spent {
		b.spent[k] = v
	}
	return b
}

// Reserve takes a reservation of est USD, or refuses.
func (b *Budget) Reserve(provider string, est float64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	cap := b.caps[provider]
	if cap <= 0 {
		return 0, fmt.Errorf("%w: cap for %s is %v", ErrBudgetRefused, provider, cap)
	}
	if b.spent[provider]+b.inflight[provider]+est > cap {
		return 0, fmt.Errorf("%w: %s spent %.6f + reserved %.6f + estimate %.6f > cap %.6f", ErrBudgetRefused, provider, b.spent[provider], b.inflight[provider], est, cap)
	}
	b.nextID++
	b.open[b.nextID] = reservation{provider, est}
	b.inflight[provider] += est
	return b.nextID, nil
}

// Settle replaces a reservation by the actual cost.
func (b *Budget) Settle(id int, actual float64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	r, ok := b.open[id]
	if !ok {
		return
	}
	delete(b.open, id)
	b.inflight[r.provider] -= r.usd
	b.spent[r.provider] += actual
}

// Spent returns the settled spend of a provider.
func (b *Budget) Spent(provider string) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.spent[provider]
}
