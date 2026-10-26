//go:build integration

package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/edgetokenmint"
)

func TestIdentityBySubjectFlagsAServiceProviderPrincipalAndTheStoreStillLinksIt(t *testing.T) {
	for name, store := range map[string]func(*env) *Store{
		"before the users cutover": func(e *env) *Store { return e.store },
		"users owned":              func(e *env) *Store { return e.owned },
	} {
		t.Run(name, func(t *testing.T) {
			e := getEnv(t)
			ctx := context.Background()
			s := store(e)
			normal := e.seedUser(t, ctx, uniqueEmail("normal"), true, true)
			service := e.seedUser(t, ctx, uniqueEmail("service"), true, true)
			e.project(t, ctx, normal)
			e.project(t, ctx, service)
			normalKey := googleKey("normal-" + uuid.NewString())
			serviceKey := googleKey("service-" + uuid.NewString())
			if _, err := e.store.LinkIdentity(ctx, normalKey, normal, Actor{}); err != nil {
				t.Fatal(err)
			}
			if _, err := e.store.LinkIdentity(ctx, serviceKey, service, Actor{}); err != nil {
				t.Fatal(err)
			}
			if _, err := e.admin.Exec(ctx, `UPDATE public.users SET auth_provider = $2 WHERE id = $1`, service, edgetokenmint.ServiceAuthProvider); err != nil {
				t.Fatal(err)
			}

			p, found, err := s.IdentityBySubject(ctx, normalKey.Issuer, normalKey.Provider, normalKey.Subject, nil)
			if err != nil || !found || p.ID != normal || p.ServiceProvider || !p.IsActive || p.DisabledAt != nil {
				t.Fatalf("normal principal: %+v found=%v err=%v", p, found, err)
			}
			p, found, err = s.IdentityBySubject(ctx, serviceKey.Issuer, serviceKey.Provider, serviceKey.Subject, nil)
			if err != nil || !found || p.ID != service || !p.ServiceProvider || !p.IsActive || p.DisabledAt != nil {
				t.Fatalf("service principal: %+v found=%v err=%v, want found with ServiceProvider set and no disabled mark", p, found, err)
			}

			if _, err := s.LinkIdentity(ctx, ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "more-" + uuid.NewString()}, service, Actor{}); err != nil {
				t.Fatalf("link to a service principal: %v, want it stored (the sign-in paths are the gate)", err)
			}
			if _, err := s.LinkIdentity(ctx, ExternalKey{Issuer: "https://github.com", Provider: ProviderGitHub, Subject: "more-" + uuid.NewString()}, normal, Actor{}); err != nil {
				t.Fatalf("link to a normal principal: %v", err)
			}
		})
	}
}
