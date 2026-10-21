package social

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// refusingStore finds no identity (or answers the lookup with lookupErr), lists
// none, and answers a link with linkErr.
type refusingStore struct {
	stubStore
	lookupErr error
	linkErr   error
}

func (s refusingStore) IdentityBySubject(context.Context, string, identity.Provider, string, *identity.LegacyMatch) (identity.Principal, bool, error) {
	return identity.Principal{}, false, s.lookupErr
}

func (refusingStore) ListIdentities(context.Context, uuid.UUID) ([]identity.Identity, error) {
	return nil, nil
}

func (s refusingStore) LinkIdentity(context.Context, identity.ExternalKey, uuid.UUID, identity.Actor) (uuid.UUID, error) {
	return uuid.Nil, s.linkErr
}

func TestLinkRefusalsCarryTheirReasonCodes(t *testing.T) {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		store  refusingStore
		link   bool
		reason string
	}{
		{"second identity at the provider on link", refusingStore{linkErr: identity.ErrProviderAlreadyLinked}, true, "provider_already_linked"},
		{"identity held by another principal on link", refusingStore{linkErr: identity.ErrIdentityConflict}, true, "identity_conflict"},
		{"second identity at the provider on a go-api sign-in", refusingStore{lookupErr: identity.ErrProviderAlreadyLinked}, false, "provider_already_linked"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFakeProviders(t)
			var logs bytes.Buffer
			svc, err := New(Config{Store: c.store, Providers: f.client(), Now: func() time.Time { return now },
				Logger: slog.New(slog.NewTextHandler(&logs, nil))})
			if err != nil {
				t.Fatal(err)
			}
			token := f.token(t.Name(), account{provider: GitHub, id: 9300, email: "codes@example.com", verified: true, public: true})
			if c.link {
				_, err = svc.Link(context.Background(), SignedIn{PrincipalID: uuid.New(), AuthTime: now}, GitHub, token)
			} else {
				_, err = svc.Exchange(context.Background(), GitHub, token, nil)
			}
			if err == nil {
				t.Fatal("the refusal was not returned")
			}
			if !strings.Contains(logs.String(), "reason_code="+c.reason) {
				t.Fatalf("log has no reason_code=%s: %s", c.reason, logs.String())
			}
		})
	}
}
