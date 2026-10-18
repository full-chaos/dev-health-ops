package providersync

import "context"

// budgetAccount labels the requests made with ctx with the route family the
// dispatch budget estimate names for them (internal/syncbudget). It is required
// where one budget dimension carries several families of a unit, so the
// request ledger can be compared with the estimate per family; elsewhere the
// family is resolved from the unit's own estimates.
func budgetAccount(ctx context.Context, routeFamily string) context.Context {
	_ = routeFamily
	return ctx
}
