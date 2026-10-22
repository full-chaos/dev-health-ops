package social

import (
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// Classify names the status WriteError answers for every error class, so a
// caller can decide by status before it writes.
func TestClassifyIsTheStatusWriteErrorAnswers(t *testing.T) {
	for _, err := range []error{
		&ConflictError{ExistingMethod: "github"}, ErrProviderUnavailable, ErrProviderNotConfigured,
		fmt.Errorf("fetch: %w", ErrProviderRefused), ErrEmailUnverified, ErrPrincipalDisabled,
		identity.ErrSignupNotAvailable, ErrReauthenticationRequired, identity.ErrIdentityConflict, ErrProviderLinked,
		identity.ErrNotFound, identity.ErrLastSignInMethod, errors.New("anything else"),
	} {
		response := httptest.NewRecorder()
		WriteError(response, GitHub, err)
		status, reason := Classify(err)
		if status != response.Code {
			t.Errorf("%v: Classify says %d, WriteError answers %d", err, status, response.Code)
		}
		if reason == "" {
			t.Errorf("%v: Classify names no reason", err)
		}
	}
}

func TestClassifyNamesTheExchangeReasons(t *testing.T) {
	for err, want := range map[error]Reason{
		ErrProviderRefused:             ReasonInvalidCredentials,
		&ConflictError{}:               ReasonEmailCollision,
		ErrEmailUnverified:             ReasonEmailUnverified,
		ErrPrincipalDisabled:           ReasonPrincipalDeactivated,
		identity.ErrSignupNotAvailable: ReasonSignupUnavailable,
		identity.ErrIdentityConflict:   ReasonIdentityConflict,
		ErrProviderNotConfigured:       ReasonDependency,
		ErrProviderUnavailable:         ReasonDependency,
	} {
		if _, got := Classify(err); got != want {
			t.Errorf("%v: reason %q, want %q", err, got, want)
		}
	}
}
