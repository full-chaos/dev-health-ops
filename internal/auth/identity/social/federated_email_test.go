package social

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// writeCountingStore finds no identity and no account, and counts the writes.
type writeCountingStore struct {
	stubStore
	provisioned atomic.Int32
}

func (s *writeCountingStore) ByEmail(context.Context, string) (*identity.Principal, error) {
	return nil, nil
}

func (s *writeCountingStore) ProvisionFederated(context.Context, identity.ProvisionRequest) (uuid.UUID, error) {
	s.provisioned.Add(1)
	return uuid.New(), nil
}

// A provider email that holds U+FFFD, written as a lone surrogate escape or as
// the character itself, is not the provider's address: it is refused as an
// unverified email before any write.
func TestAReplacementCharacterInTheProviderEmailWritesNothing(t *testing.T) {
	for _, raw := range []string{`e\ud800@example.com`, `\ud800@example.com`, "e\xef\xbf\xbd@example.com", `e@exa\udfffmple.com`} {
		f := newFakeProviders(t)
		store := &writeCountingStore{}
		svc, err := New(Config{Store: store, Providers: f.client()})
		if err != nil {
			t.Fatal(err)
		}
		tok := f.token("surrogate", account{provider: Google, googleID: "g-1", verified: true, rawEmail: raw})
		_, err = svc.Exchange(context.Background(), Google, tok, nil)
		if !errors.Is(err, ErrEmailUnverified) {
			t.Errorf("%q: err = %v, want ErrEmailUnverified", raw, err)
		}
		if n := store.provisioned.Load(); n != 0 {
			t.Errorf("%q: ProvisionFederated called %d times", raw, n)
		}
	}
}

type sessionRecorder struct{ written atomic.Int32 }

func (s *sessionRecorder) WriteSession(http.ResponseWriter, *http.Request, uuid.UUID, bool) {
	s.written.Add(1)
}

func TestAReplacementCharacterInTheProviderEmailIsAnswered403(t *testing.T) {
	f := newFakeProviders(t)
	store := &writeCountingStore{}
	svc, err := New(Config{Store: store, Providers: f.client()})
	if err != nil {
		t.Fatal(err)
	}
	tok := f.token("surrogate-http", account{provider: Google, googleID: "g-2", verified: true, rawEmail: `e\ud800@example.com`})
	rec := httptest.NewRecorder()
	sessions := &sessionRecorder{}
	Handlers{Service: svc, Sessions: sessions, Auth: noAuth{}}.Exchange(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"provider":"google","provider_access_token":"`+tok.Reveal()+`"}`)))
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "The provider has not verified this email address") {
		t.Errorf("answer = %d %s", rec.Code, rec.Body)
	}
	if store.provisioned.Load() != 0 || sessions.written.Load() != 0 {
		t.Error("a write ran")
	}
}
