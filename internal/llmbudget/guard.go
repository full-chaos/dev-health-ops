package llmbudget

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// This file is llm/budget.py's guard_byo_call: reserve the call's maximum
// exposure in a committed row, run the provider with no transaction open, then
// settle the row from the usage the provider reports. A call the budget cannot
// admit is NOT made (the invoke function is never called).

// ErrBudgetExceeded is BYOBudgetExceeded: the organization's ceiling denied
// admission.
var ErrBudgetExceeded = errors.New("organization BYO LLM monetary budget is exhausted")

// AccountingError is BYOBudgetAccountingError: a reservation, a pricing
// decision or a reconciliation could not complete. Message is the fixed
// operator-facing text; Cause is never shown to a caller.
type AccountingError struct {
	Message string
	Cause   error
}

func (e *AccountingError) Error() string { return e.Message }
func (e *AccountingError) Unwrap() error { return e.Cause }

func accounting(message string, cause error) error {
	return &AccountingError{Message: message, Cause: cause}
}

// Outcome is the final status of a reservation.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"
	OutcomeFailed    Outcome = "failed"
	OutcomeCancelled Outcome = "cancelled"
	// OutcomeVoided means the provider provably never received the request.
	OutcomeVoided Outcome = "voided"
)

// Usage is what a provider reported. A nil field is unreported.
type Usage struct {
	InputTokens, OutputTokens, CachedInputTokens *int64
}

// UsageFrom builds a Usage from the int pointers the provider layer carries,
// with _agent_usage's rule: a completion that consumed no input and no output
// token did not report its usage, so it is unknown, never a $0 charge.
func UsageFrom(input, output, cached *int) Usage {
	conv := func(v *int) *int64 {
		if v == nil || *v < 0 {
			return nil
		}
		out := int64(*v)
		return &out
	}
	usage := Usage{InputTokens: conv(input), OutputTokens: conv(output), CachedInputTokens: conv(cached)}
	if usage.InputTokens != nil && usage.OutputTokens != nil && *usage.InputTokens == 0 && *usage.OutputTokens == 0 {
		return Usage{}
	}
	return usage
}

// NotDispatched is implemented by an error that proves the provider never
// received the request (provider_dispatched is False). Any other error is
// treated as possibly billed.
type NotDispatched interface{ ProviderNotDispatched() bool }

// UsageReporter is implemented by an error that carries the usage the provider
// reported before it failed.
type UsageReporter interface{ ReportedUsage() Usage }

// Admission is the result of admitting one call.
type Admission struct {
	// ID is the reservation row; empty when the call is unbudgeted.
	ID string
	// Unbudgeted: the call is unpriced and the organization has no budget, so
	// no reservation was made (Python preserves pre-budget behaviour).
	Unbudgeted       bool
	BudgetConfigured bool
}

// Store is the durable side of the guard.
type Store interface {
	// Admit decides under the organization's advisory lock and, when the call
	// is admitted, commits the reservation before it returns.
	Admit(ctx context.Context, request AdmitRequest) (Admission, error)
	// Settle moves a `reserved` row to its final state. A row that is no
	// longer `reserved` is an error: another owner settled it.
	Settle(ctx context.Context, settle SettleRequest) error
}

// AdmitRequest is what Admit needs.
type AdmitRequest struct {
	OrgID, Provider, Model, BaseURL, IdempotencyKey string
	// MaxCost is nil when the price book does not price the call.
	MaxCost *int64
	Now     time.Time
	Lookup  LookupEnv
}

// SettleRequest is what Settle needs. Cost is nil when usage is unavailable.
type SettleRequest struct {
	ID      string
	Outcome Outcome
	Usage   Usage
	Cost    *int64
	Now     time.Time
}

// Guard is one organization's budget guard for one provider/model/endpoint.
type Guard struct {
	Store    Store
	OrgID    string
	Provider string
	Model    string
	BaseURL  string
	Lookup   LookupEnv
	Now      func() time.Time
	// Settle uses a context that survives the caller's cancellation, bounded
	// by this timeout (default 15s).
	SettleTimeout time.Duration
}

func (g Guard) now() time.Time {
	if g.Now != nil {
		return g.Now()
	}
	return time.Now()
}

// Call describes one provider attempt.
type Call struct {
	// IdempotencyKey is stable for the attempt and unique per attempt.
	IdempotencyKey  string
	MaxInputTokens  int64
	MaxOutputTokens int64
}

// CostMicroUSD is cost_micro_usd: ceil of the priced token counts, nil when the
// call is unpriced or the counts are inconsistent.
func CostMicroUSD(provider, model, baseURL string, input, output, cached int64) (*int64, error) {
	rates, err := PriceRates(provider, model, baseURL)
	if err != nil || rates == nil {
		return nil, err
	}
	if input < 0 || output < 0 || cached < 0 || cached > input {
		return nil, nil
	}
	numerator := new(big.Int).Mul(big.NewInt(input-cached), big.NewInt(rates.Input))
	numerator.Add(numerator, new(big.Int).Mul(big.NewInt(cached), big.NewInt(rates.CachedInput)))
	numerator.Add(numerator, new(big.Int).Mul(big.NewInt(output), big.NewInt(rates.Output)))
	million := big.NewInt(1_000_000)
	quotient, remainder := new(big.Int).QuoRem(numerator, million, new(big.Int))
	if remainder.Sign() > 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if !quotient.IsInt64() {
		return nil, accounting("BYO LLM budget cost is out of range.", nil)
	}
	cost := quotient.Int64()
	return &cost, nil
}

// Run reserves, invokes and settles one call. invoke runs with no transaction
// open and returns the usage the provider reported.
func (g Guard) Run(ctx context.Context, call Call, invoke func(context.Context) (Usage, error)) error {
	maxCost, err := CostMicroUSD(g.Provider, g.Model, g.BaseURL, max(call.MaxInputTokens, 0), max(call.MaxOutputTokens, 0), 0)
	if err != nil {
		return accounting("BYO LLM budget reservation is temporarily unavailable.", err)
	}
	admission, err := g.Store.Admit(ctx, AdmitRequest{
		OrgID: g.OrgID, Provider: g.Provider, Model: g.Model, BaseURL: g.BaseURL,
		IdempotencyKey: call.IdempotencyKey, MaxCost: maxCost, Now: g.now(), Lookup: g.Lookup,
	})
	if err != nil {
		return err
	}
	if admission.Unbudgeted {
		_, err := invoke(ctx)
		return err
	}

	usage, invokeErr := invoke(ctx)
	if invokeErr != nil {
		var reported Usage
		outcome := OutcomeFailed
		var notDispatched NotDispatched
		switch {
		case errors.As(invokeErr, &notDispatched) && notDispatched.ProviderNotDispatched():
			outcome = OutcomeVoided
		case errors.Is(invokeErr, context.Canceled):
			outcome = OutcomeCancelled
			fallthrough
		default:
			var reporter UsageReporter
			if errors.As(invokeErr, &reporter) {
				reported = reporter.ReportedUsage()
			}
		}
		if settleErr := g.settle(ctx, admission.ID, outcome, reported); settleErr != nil {
			return errors.Join(accounting("BYO LLM budget accounting is temporarily unavailable.", settleErr), invokeErr)
		}
		return invokeErr
	}
	billed, settleErr := g.settleWithBilled(ctx, admission.ID, OutcomeSucceeded, usage)
	if settleErr != nil {
		return accounting("BYO LLM budget accounting is temporarily unavailable.", settleErr)
	}
	if billed == nil && admission.BudgetConfigured {
		return accounting("BYO LLM usage is unavailable for the configured budget.", nil)
	}
	return nil
}

func (g Guard) settle(ctx context.Context, id string, outcome Outcome, usage Usage) error {
	_, err := g.settleWithBilled(ctx, id, outcome, usage)
	return err
}

func (g Guard) settleWithBilled(ctx context.Context, id string, outcome Outcome, usage Usage) (*int64, error) {
	timeout := g.SettleTimeout
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	// The provider may have been cancelled by ctx, but the reservation must
	// still be settled: a stuck `reserved` row counts at its maximum forever.
	settleCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()
	request := SettleRequest{ID: id, Outcome: outcome, Usage: usage, Now: g.now()}
	if outcome != OutcomeVoided && usage.InputTokens != nil && usage.OutputTokens != nil && usage.CachedInputTokens != nil {
		cost, err := CostMicroUSD(g.Provider, g.Model, g.BaseURL, *usage.InputTokens, *usage.OutputTokens, *usage.CachedInputTokens)
		if err != nil {
			return nil, err
		}
		request.Cost = cost
	}
	if outcome == OutcomeVoided {
		zero := int64(0)
		request.Cost = &zero
	}
	if err := g.Store.Settle(settleCtx, request); err != nil {
		return nil, err
	}
	return request.Cost, nil
}

// ---- Postgres store -------------------------------------------------------

// Pool is the part of a pgx pool the Postgres store needs.
type Pool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// PgStore is the Postgres Store.
type PgStore struct{ Pool Pool }

var _ Store = PgStore{}

// IdempotencyDigest is the stored key: sha256 hex of the caller's key.
func IdempotencyDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

type txDB struct{ pgx.Tx }

// Admit is the admission half of guard_byo_call.
func (s PgStore) Admit(ctx context.Context, request AdmitRequest) (Admission, error) {
	unavailable := func(cause error) error {
		return accounting("BYO LLM budget reservation is temporarily unavailable.", cause)
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Admission{}, unavailable(err)
	}
	// Every path below ends the transaction; a rollback after a commit is a
	// no-op.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, LockKey(request.OrgID)); err != nil {
		return Admission{}, unavailable(err)
	}
	rawLimit, err := readSetting(ctx, tx, request.OrgID, Category, LimitKey)
	if err != nil {
		return Admission{}, unavailable(err)
	}
	status, err := Compute(ctx, txDB{tx}, request.Lookup, request.OrgID, Input{
		Provider: request.Provider, Model: request.Model, BaseURL: &request.BaseURL, RawLimit: rawLimit,
	}, request.Now)
	if err != nil {
		return Admission{}, unavailable(err)
	}
	configured := status.Limit != nil
	switch {
	case request.MaxCost == nil:
		if configured {
			return Admission{}, accounting("BYO LLM pricing is unavailable for the configured budget.", nil)
		}
		return Admission{Unbudgeted: true}, nil
	case configured && status.Reason == ReasonUsageUnavailable:
		return Admission{}, accounting("BYO LLM usage is unavailable for the configured budget.", nil)
	case configured && (status.Remaining == nil || big.NewInt(*request.MaxCost).Cmp(status.Remaining) > 0):
		return Admission{}, ErrBudgetExceeded
	}

	windowStart, _ := MonthWindow(request.Now)
	org, err := pythonparity.ParseUUID(request.OrgID)
	if err != nil {
		return Admission{}, unavailable(err)
	}
	digest := IdempotencyDigest(request.IdempotencyKey)
	var existing string
	err = tx.QueryRow(ctx, `SELECT id::text FROM byo_llm_budget_reservations WHERE org_id = $1 AND window_start = $2 AND idempotency_key = $3`,
		org, windowStart, digest).Scan(&existing)
	if err == nil {
		return Admission{}, accounting("BYO LLM request has already been reserved.", nil)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Admission{}, unavailable(err)
	}
	var id string
	if err := tx.QueryRow(ctx, `INSERT INTO byo_llm_budget_reservations
(id, org_id, window_start, idempotency_key, provider, model, reserved_micro_usd, status, pricing_version, created_at)
VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, $6, 'reserved', $7, $8) RETURNING id::text`,
		org, windowStart, digest, request.Provider, request.Model, *request.MaxCost, PricingVersion, request.Now.UTC()).Scan(&id); err != nil {
		return Admission{}, unavailable(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Admission{}, unavailable(err)
	}
	return Admission{ID: id, BudgetConfigured: configured}, nil
}

// readSetting reads one plain setting. An encrypted value is an error here:
// the budget limit is stored unencrypted, and a value this package cannot
// decrypt must stop admission, never read as "no budget".
func readSetting(ctx context.Context, tx pgx.Tx, orgID, category, key string) (*string, error) {
	var value *string
	var encrypted bool
	err := tx.QueryRow(ctx, `SELECT value, is_encrypted FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, category, key).Scan(&value, &encrypted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if encrypted {
		return nil, fmt.Errorf("setting %s/%s is encrypted", category, key)
	}
	return value, nil
}

// Settle is _reconcile_reservation. The update is guarded by status =
// 'reserved' so a row another owner settled is never overwritten.
func (s PgStore) Settle(ctx context.Context, request SettleRequest) error {
	var tag pgconn.CommandTag
	var err error
	now := request.Now.UTC()
	switch {
	case request.Outcome == OutcomeVoided:
		tag, err = s.Pool.Exec(ctx, `UPDATE byo_llm_budget_reservations
SET status = 'voided', input_tokens = 0, output_tokens = 0, cached_input_tokens = 0, actual_micro_usd = 0, reconciled_at = $2
WHERE id = $1 AND status = 'reserved'`, request.ID, now)
	case request.Cost == nil:
		tag, err = s.Pool.Exec(ctx, `UPDATE byo_llm_budget_reservations
SET status = 'usage_unavailable', actual_micro_usd = NULL, input_tokens = $3, output_tokens = $4, cached_input_tokens = $5, reconciled_at = $2
WHERE id = $1 AND status = 'reserved'`, request.ID, now, request.Usage.InputTokens, request.Usage.OutputTokens, request.Usage.CachedInputTokens)
	default:
		tag, err = s.Pool.Exec(ctx, `UPDATE byo_llm_budget_reservations
SET status = $6, actual_micro_usd = $3, input_tokens = $4, output_tokens = $5, cached_input_tokens = $7, reconciled_at = $2
WHERE id = $1 AND status = 'reserved'`, request.ID, now, *request.Cost, *request.Usage.InputTokens, *request.Usage.OutputTokens, string(request.Outcome), *request.Usage.CachedInputTokens)
	}
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("BYO LLM budget reservation is no longer owned by this attempt")
	}
	return nil
}
