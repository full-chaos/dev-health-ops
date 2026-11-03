package session

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// Queryer is the one read a Guard makes: a plain SELECT on the exchange's own
// transaction connection.
type Queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Guard decides, under the session lock and before a live credential is
// rotated, whether the exchange may go on. It returns nil to proceed and
// ErrGuardRefused to refuse; any other error fails the exchange. It reads
// through q only and writes nothing.
type Guard func(ctx context.Context, q Queryer, record Record) error

// ErrGuardRefused is a Guard's refusal: nothing was written.
var ErrGuardRefused = errors.New("session: exchange refused by its guard")

type guardRefusal struct{ record Record }

func (guardRefusal) Error() string { return ErrGuardRefused.Error() }

func (guardRefusal) Is(target error) bool { return target == ErrGuardRefused }

// runGuard is called by rotate under the session lock, before the successor
// is inserted.
func runGuard(ctx context.Context, tx Queryer, record Record, guard Guard) error {
	if guard == nil {
		return nil
	}
	if err := guard(ctx, tx, record); errors.Is(err, ErrGuardRefused) {
		return guardRefusal{record: record}
	} else if err != nil {
		return err
	}
	return nil
}

// ExchangeGuarded is Exchange with a Guard run before a live credential is
// rotated, so a refusal mints no successor. It then returns ErrGuardRefused
// with the locked session in Result.Session and an empty Reason; no exchange
// is counted for it. A grace replay and every other refusal are unguarded.
func (s *Store) ExchangeGuarded(ctx context.Context, credential contracts.SecretString, guard Guard) (Result, error) {
	return s.ExchangeGuardedWith(ctx, credential, guard, publicRowOf(ctx))
}

// ExchangeGuardedWith is ExchangeGuarded with public, when not nil, run in the
// exchange's transaction after an accepted rotation, with the session's
// principal and organization: go-api's public.audit_logs row of a compatibility
// refresh. An error from public rolls the rotation back with everything else.
func (s *Store) ExchangeGuardedWith(ctx context.Context, credential contracts.SecretString, guard Guard, public PublicRow) (Result, error) {
	result, err := s.exchange(ctx, credential, guard, public)
	var refusal guardRefusal
	if errors.As(err, &refusal) {
		return Result{Issued: Issued{Session: refusal.record}}, ErrGuardRefused
	}
	return result, err
}
