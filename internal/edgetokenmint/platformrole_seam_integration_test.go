//go:build integration

package edgetokenmint_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// AMENDMENT 1: public is_superuser is always checked; the auth platform role
// only while owned. A principal that is a superuser in either is refused.
func TestEdgeMintChecksBothStoresOnlyWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	for _, c := range []struct {
		name                      string
		publicSuperuser, assigned bool
		on, off                   error
	}{
		{"neither", false, false, nil, nil},
		{"auth assignment only", false, true, edgetokenmint.ErrPrincipalSuperuser, nil},
		{"public superuser only", true, false, edgetokenmint.ErrPrincipalSuperuser, edgetokenmint.ErrPrincipalSuperuser},
	} {
		env := platformroleseam.Start(ctx, t, true)
		env.Project(ctx, t, uuid.MustParse(edgetokenmint.ProvePrincipalID), c.publicSuperuser, c.assigned)
		org := env.Member(ctx, t, uuid.MustParse(edgetokenmint.ProvePrincipalID), "viewer").String()
		for owned, want := range map[bool]error{true: c.on, false: c.off} {
			_, err := edgetokenmint.LookupPrincipal(ctx, env.Reader, org, edgetokenmint.Options{PlatformRolesOwned: owned, AuthSchema: "auth"})
			if (want == nil) != (err == nil) || (want != nil && !errors.Is(err, want)) {
				t.Errorf("%s, owned %v: %v, want %v", c.name, owned, err, want)
			}
		}
	}
}

// AMENDMENT 1: while owned, an auth read that fails refuses the mint.
func TestEdgeMintRefusesOnAuthReadErrorWhileOwned(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	env := platformroleseam.Start(ctx, t, false)
	env.Project(ctx, t, uuid.MustParse(edgetokenmint.ProvePrincipalID), false, false)
	org := env.Member(ctx, t, uuid.MustParse(edgetokenmint.ProvePrincipalID), "viewer").String()
	if _, err := edgetokenmint.LookupPrincipal(ctx, env.Reader, org, edgetokenmint.Options{}); err != nil {
		t.Fatalf("off: %v", err)
	}
	if principal, err := edgetokenmint.LookupPrincipal(ctx, env.Reader, org, edgetokenmint.Options{PlatformRolesOwned: true, AuthSchema: "auth"}); err == nil {
		t.Fatalf("on, no reader grant: %+v, want the read error", principal)
	}
}
