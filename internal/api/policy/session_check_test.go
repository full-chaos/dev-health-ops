package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// sessionStore is fakeStore plus the auth-service sessions.
type sessionStore struct {
	*fakeStore
	rows   map[uuid.UUID]SessionRow
	err    error
	calls  int
	lastID uuid.UUID
}

func (s *sessionStore) SessionState(_ context.Context, id uuid.UUID) (SessionRow, bool, error) {
	s.calls++
	s.lastID = id
	if s.err != nil {
		return SessionRow{}, false, s.err
	}
	row, found := s.rows[id]
	return row, found, nil
}

var liveSessionID = uuid.MustParse("77777777-7777-4777-8777-777777777777")

func withSID(id uuid.UUID) func(jwt.MapClaims) {
	return func(c jwt.MapClaims) { c["sid"] = session.WireSessionID(id) }
}

func newSessionStore() *sessionStore {
	return &sessionStore{fakeStore: newStore(), rows: map[uuid.UUID]SessionRow{liveSessionID: {PrincipalID: userID}}}
}

func outcomeOf(err error) string {
	switch {
	case err == nil:
		return "ok"
	case isRefusal(err):
		return "refused"
	case isUnavailable(err):
		return "unavailable"
	default:
		return "internal"
	}
}

// A token auth-service issued carries sid: it is good only while that
// session is live and belongs to the token's subject. A token without sid
// (go-api's own) is decided as before and reads no session.
func TestCheckClaimsEndsATokenWhoseSessionIsRevoked(t *testing.T) {
	other := uuid.MustParse("88888888-8888-4888-8888-888888888888")
	for _, test := range []struct {
		name      string
		claims    func(jwt.MapClaims)
		store     func(*sessionStore)
		want      string
		wantReads int
	}{
		{name: "no sid", want: "ok", wantReads: 0},
		{name: "live session", claims: withSID(liveSessionID), want: "ok", wantReads: 1},
		{name: "revoked session", claims: withSID(liveSessionID), store: func(s *sessionStore) {
			s.rows[liveSessionID] = SessionRow{PrincipalID: userID, Revoked: true}
		}, want: "refused", wantReads: 1},
		{name: "no such session", claims: withSID(other), want: "refused", wantReads: 1},
		{name: "another principal's session", claims: withSID(liveSessionID), store: func(s *sessionStore) {
			s.rows[liveSessionID] = SessionRow{PrincipalID: targetUserID}
		}, want: "refused", wantReads: 1},
		{name: "sid not in the wire form", claims: func(c jwt.MapClaims) { c["sid"] = liveSessionID.String() }, want: "refused"},
		{name: "sid empty", claims: func(c jwt.MapClaims) { c["sid"] = "" }, want: "refused"},
		{name: "sid not a string", claims: func(c jwt.MapClaims) { c["sid"] = 7 }, want: "refused"},
		{name: "sid null", claims: func(c jwt.MapClaims) { c["sid"] = nil }, want: "refused"},
		{name: "session lookup unavailable", claims: withSID(liveSessionID), store: func(s *sessionStore) { s.err = errors.Join(ErrUnavailable, errors.New("dial")) }, want: "unavailable", wantReads: 1},
		{name: "session lookup other failure", claims: withSID(liveSessionID), store: func(s *sessionStore) { s.err = errors.New("syntax") }, want: "internal", wantReads: 1},
		{name: "inactive user, sid not read", claims: withSID(liveSessionID), store: func(s *sessionStore) { s.users[userID] = UserState{} }, want: "refused"},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := newSessionStore()
			if test.store != nil {
				test.store(store)
			}
			_, err := authenticator(t, store).Authenticate(context.Background(), sign(t, claims(test.claims)))
			if got := outcomeOf(err); got != test.want {
				t.Fatalf("outcome %s (%v), want %s", got, err, test.want)
			}
			if store.calls != test.wantReads {
				t.Fatalf("session reads = %d, want %d", store.calls, test.wantReads)
			}
			if test.wantReads == 1 && test.claims != nil && store.lastID != liveSessionID && test.name != "no such session" {
				t.Fatalf("read session %s, want %s", store.lastID, liveSessionID)
			}
		})
	}
}

// A store that cannot read sessions refuses every token that carries sid:
// never a pass for want of the check.
func TestCheckClaimsRefusesASidTokenItCannotCheck(t *testing.T) {
	_, err := authenticator(t, newStore()).Authenticate(context.Background(), sign(t, claims(withSID(liveSessionID))))
	if got := outcomeOf(err); got != "refused" {
		t.Fatalf("outcome %s (%v), want refused", got, err)
	}
	if _, err := authenticator(t, newStore()).Authenticate(context.Background(), sign(t, claims(nil))); err != nil {
		t.Fatalf("a token without sid: %v", err)
	}
}
