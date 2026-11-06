package identity

import (
	"context"
	"errors"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// ErrDecoyOutsideRehearsal reports a decoy provision whose context is not a rehearsal.
var ErrDecoyOutsideRehearsal = errors.New("identity: a decoy provision runs only in a rehearsal")

type decoyKey struct{}

// WithDecoyAddress marks ctx for the provision of a rehearsed sign-up's internal throwaway address: ProvisionLocal
// stores that address as given, without the register address validator. Only inside audit.WithRehearsal, so a
// decoy row never commits.
func WithDecoyAddress(ctx context.Context) context.Context {
	return context.WithValue(ctx, decoyKey{}, true)
}

// DecoyAddress reports whether ctx came from WithDecoyAddress.
func DecoyAddress(ctx context.Context) bool {
	decoy, _ := ctx.Value(decoyKey{}).(bool)
	return decoy
}

// provisionEmail is the stored address of a provision: a decoy's internal address as given, in a rehearsal only;
// any other address through the register validator.
func provisionEmail(ctx context.Context, raw string) (string, error) {
	if !DecoyAddress(ctx) {
		return NormalizeLocalRegisterEmail(raw)
	}
	if !audit.Rehearsing(ctx) {
		return "", ErrDecoyOutsideRehearsal
	}
	return raw, nil
}
