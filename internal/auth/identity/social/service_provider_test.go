package social

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func TestSignInToRefusesAServiceProviderPrincipalAsDisabled(t *testing.T) {
	reason, err := signInTo(identity.Principal{ID: uuid.New(), IsActive: true})
	if err != nil || reason != ReasonAccepted {
		t.Fatalf("normal principal: %v %v, want accepted", reason, err)
	}
	disabledReason, disabledErr := signInTo(identity.Principal{ID: uuid.New(), IsActive: true, DisabledAt: ptrTime()})
	reason, err = signInTo(identity.Principal{ID: uuid.New(), IsActive: true, ServiceProvider: true})
	if !errors.Is(err, ErrPrincipalDisabled) || reason != ReasonPrincipalDeactivated || reason != disabledReason || !errors.Is(disabledErr, err) {
		t.Fatalf("service-provider principal: %v %v, want the disabled answer %v %v", reason, err, disabledReason, disabledErr)
	}
}

func ptrTime() *time.Time { t := time.Now(); return &t }
