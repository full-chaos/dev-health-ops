package remaining

import (
	"context"
	"errors"
	"sync"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

type familyBudget struct {
	familyLimit int
	read        chan struct{}
	write       chan struct{}
}

// Budget enforces both the registry's per-organization family concurrency and
// the reviewed process-wide ClickHouse read/write ceilings.
type Budget struct {
	mu           sync.Mutex
	families     map[string]familyBudget
	organization map[string]chan struct{}
}

func NewBudget(inventory Inventory) (*Budget, error) {
	if err := inventory.Validate(); err != nil {
		return nil, ErrUnavailable
	}
	budget := &Budget{
		families:     make(map[string]familyBudget, len(inventory.Families)),
		organization: make(map[string]chan struct{}),
	}
	for _, family := range inventory.Families {
		budget.families[family.RouteKey] = familyBudget{
			familyLimit: family.MaxConcurrency,
			read:        make(chan struct{}, family.ClickHouseReadBudget),
			write:       make(chan struct{}, family.ClickHouseWriteBudget),
		}
	}
	return budget, nil
}

func (budget *Budget) Supports(scope string, limit int) bool {
	if budget == nil || scope != "organization" {
		return false
	}
	for _, family := range budget.families {
		if family.familyLimit == limit {
			return true
		}
	}
	return false
}

func (budget *Budget) Acquire(
	ctx context.Context,
	request jobruntime.BudgetRequest,
) (jobruntime.BudgetLease, error) {
	if budget == nil || ctx == nil || request.OrganizationID == nil ||
		request.ConcurrencyScope != "organization" {
		return nil, errors.New("remaining metrics budget is unavailable")
	}
	family, ok := budget.families[request.Kind]
	if !ok || request.ConcurrencyLimit != family.familyLimit {
		return nil, errors.New("remaining metrics budget is unavailable")
	}
	organizationKey := request.Kind + ":" + *request.OrganizationID
	budget.mu.Lock()
	organization, ok := budget.organization[organizationKey]
	if !ok {
		organization = make(chan struct{}, request.ConcurrencyLimit)
		budget.organization[organizationKey] = organization
	}
	budget.mu.Unlock()

	acquired := make([]chan struct{}, 0, 3)
	for _, semaphore := range []chan struct{}{organization, family.read, family.write} {
		select {
		case semaphore <- struct{}{}:
			acquired = append(acquired, semaphore)
		case <-ctx.Done():
			releaseSemaphores(acquired)
			return nil, ctx.Err()
		}
	}
	return &budgetLease{semaphores: acquired}, nil
}

type budgetLease struct {
	once       sync.Once
	semaphores []chan struct{}
}

func (lease *budgetLease) Release() {
	if lease == nil {
		return
	}
	lease.once.Do(func() { releaseSemaphores(lease.semaphores) })
}

func releaseSemaphores(semaphores []chan struct{}) {
	for index := len(semaphores) - 1; index >= 0; index-- {
		<-semaphores[index]
	}
}
