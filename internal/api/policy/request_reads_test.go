package policy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

// withReads runs fn inside a request auth is bound to (ReadOnce).
func withReads(auth *Authenticator, fn func(ctx context.Context)) {
	handler := auth.ReadOnce(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { fn(r.Context()) }))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

// TestReadOnceReadsEachFactOncePerRequest: inside one bound request every
// fact is read from the store once and every later ask gets that answer, a
// failure included; a request that is not bound, and another Authenticator
// in a bound request, read for themselves.
func TestReadOnceReadsEachFactOncePerRequest(t *testing.T) {
	org := uuid.MustParse(memberOrg)
	token := sign(t, claims(nil))
	ask := func(ctx context.Context, auth *Authenticator) {
		for i := 0; i < 2; i++ {
			_, _ = auth.Authenticate(ctx, token)
			_, _ = auth.IsMember(ctx, userID.String(), org.String())
			_, _ = auth.activeImpersonation(ctx, userID)
		}
	}
	reads := func(store *fakeStore) [3]int {
		return [3]int{store.userCalls, len(store.membershipSeen), store.sessionCalls}
	}

	store := newStore()
	auth := authenticator(t, store)
	withReads(auth, func(ctx context.Context) { ask(ctx, auth) })
	if got := reads(store); got != [3]int{1, 1, 1} {
		t.Errorf("bound request: reads (user, membership, session) %v, want one each", got)
	}

	failing := newStore()
	failing.errUser, failing.errMember, failing.errSession = ErrUnavailable, errors.New("down"), errors.New("down")
	failingAuth := authenticator(t, failing)
	withReads(failingAuth, func(ctx context.Context) { ask(ctx, failingAuth) })
	if got := reads(failing); got != [3]int{1, 1, 1} {
		t.Errorf("bound request, every read failing: reads %v, want one each (the failure is the request's answer)", got)
	}

	unbound := newStore()
	unboundAuth := authenticator(t, unbound)
	ask(context.Background(), unboundAuth)
	if got := reads(unbound); got != [3]int{2, 2, 2} {
		t.Errorf("unbound request: reads %v, want every ask read", got)
	}

	other := newStore()
	otherAuth := authenticator(t, other)
	withReads(auth, func(ctx context.Context) { ask(ctx, otherAuth) })
	if got := reads(other); got != [3]int{2, 2, 2} {
		t.Errorf("another authenticator in a bound request: reads %v, want its own reads", got)
	}
}

// TestReadOnceAnswersAreNotShared: a consumer that changes the User it got
// does not change the answer the next consumer gets.
func TestReadOnceAnswersAreNotShared(t *testing.T) {
	store := newStore()
	auth := authenticator(t, store)
	token := sign(t, claims(nil))
	withReads(auth, func(ctx context.Context) {
		first, err := auth.Authenticate(ctx, token)
		if err != nil {
			t.Fatal(err)
		}
		first.IsSuperuser, first.OrgID = true, "changed"
		second, err := auth.Authenticate(ctx, token)
		if err != nil || second.IsSuperuser || second.OrgID != ownOrg {
			t.Fatalf("second answer %+v (err %v), want the store's answer, unchanged", second, err)
		}
	})
	if BoundAuthenticator(context.Background()) != nil {
		t.Fatal("an unbound request has a bound authenticator")
	}
	withReads(auth, func(ctx context.Context) {
		if BoundAuthenticator(ctx) != auth {
			t.Fatal("the bound authenticator is not the one ReadOnce bound")
		}
	})
}
