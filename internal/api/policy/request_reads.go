package policy

import (
	"context"
	"net/http"
	"sync"

	"github.com/google/uuid"
)

// requestReads is one request's record of the identity facts its
// Authenticator read: the verified token with its users row, each
// membership, the impersonation session. Every consumer in the request (the
// org scope, the impersonation middleware, the handler's own carrier check)
// asks the same questions; without this record each asked the store again,
// so a row that changed between two asks gave two consumers of one request
// two different answers. The first answer, a failure included, is the
// request's answer.
type requestReads struct {
	auth     *Authenticator
	mu       sync.Mutex
	users    map[string]userRead
	members  map[[2]uuid.UUID]memberRead
	sessions map[uuid.UUID]sessionRead
}

type userRead struct {
	user *User
	err  error
}

type memberRead struct {
	member bool
	err    error
}

type sessionRead struct {
	session *Impersonation
	err     error
}

// ReadOnce binds a to every request through it, so each identity fact a
// reads for the request is read from the store once. Wrap it outside every
// middleware and handler that must see the same answers.
func (a *Authenticator) ReadOnce(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads := &requestReads{
			auth:     a,
			users:    map[string]userRead{},
			members:  map[[2]uuid.UUID]memberRead{},
			sessions: map[uuid.UUID]sessionRead{},
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), readsKey, reads)))
	})
}

// BoundAuthenticator returns the Authenticator ReadOnce bound to the request,
// or nil. A handler that must decide with the answers its middleware saw
// authenticates with this one, never with an instance of its own.
func BoundAuthenticator(ctx context.Context) *Authenticator {
	reads, _ := ctx.Value(readsKey).(*requestReads)
	if reads == nil {
		return nil
	}
	return reads.auth
}

// readsFor is the request's record when a is the Authenticator bound to it.
// Another instance reads for itself: its store and verifier need not be a's.
func (a *Authenticator) readsFor(ctx context.Context) *requestReads {
	reads, _ := ctx.Value(readsKey).(*requestReads)
	if reads == nil || reads.auth != a {
		return nil
	}
	return reads
}

// copyUser keeps one consumer's change to the returned User from reaching
// another consumer's answer.
func copyUser(user *User) *User {
	if user == nil {
		return nil
	}
	copied := *user
	return &copied
}

// activeImpersonation is the admin's active session, read once per request
// when the request is bound to a.
func (a *Authenticator) activeImpersonation(ctx context.Context, adminID uuid.UUID) (*Impersonation, error) {
	reads := a.readsFor(ctx)
	if reads == nil {
		return a.store.ActiveImpersonation(ctx, adminID)
	}
	reads.mu.Lock()
	defer reads.mu.Unlock()
	if got, ok := reads.sessions[adminID]; ok {
		return got.session, got.err
	}
	session, err := a.store.ActiveImpersonation(ctx, adminID)
	reads.sessions[adminID] = sessionRead{session: session, err: err}
	return session, err
}
