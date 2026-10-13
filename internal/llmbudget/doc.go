// Package llmbudget is the Go side of llm/budget.py's organization BYO LLM
// monetary budget, in one place: the calendar-month UTC window, the operator
// and licensed ceilings, the price book's "is this provider/model/endpoint
// priced" decision, and the status a caller reads (used, limit, remaining and
// the reason enforcement is or is not available). Every Go reader of a
// budget goes through here (R299: one implementation), so the admin route and
// the runtime guard (guard.go: reserve, call, settle) can never price or window
// differently. A `reserved` row is never freed by age (operator.go): the
// provider may still bill, so only an operator verb voids one.
package llmbudget
