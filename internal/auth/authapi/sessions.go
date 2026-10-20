package authapi

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// current is GET /v1/sessions/current: the bearer's own session as session.v1.
func (h handlers) current(w http.ResponseWriter, r *http.Request) {
	_, record, ok := h.authenticate(w, r, opSessionCurrent)
	if !ok {
		return
	}
	h.rec.record(r.Context(), opSessionCurrent, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, record.Wire())
}

// revoke is POST /v1/sessions/revoke: end the bearer's own session and every
// refresh credential of it. Access tokens already minted stay valid until
// they expire (go-api's readers check users.token_version, which go-api owns
// until the users cutover).
func (h handlers) revoke(w http.ResponseWriter, r *http.Request) {
	bearer, _, ok := h.authenticate(w, r, opSessionRevoke)
	if !ok {
		return
	}
	principal := bearer.PrincipalID
	_, err := h.Sessions.Revoke(r.Context(), bearer.SessionID, session.Actor{PrincipalID: &principal, RequestID: requestid.Audit(r.Context())})
	switch {
	case errors.Is(err, session.ErrSessionNotFound):
		h.rec.refuse(w, r, opSessionRevoke, unauthorized(reasonSessionRevoked), nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opSessionRevoke, unavailable(reasonStoreUnavailable), err)
		return
	}
	h.rec.record(r.Context(), opSessionRevoke, "success", reasonAccepted, nil)
	writeNoContent(w)
}

// revokeAll is POST /v1/sessions/revoke-all: end every live session of the
// bearer's principal, the bearer's own included.
func (h handlers) revokeAll(w http.ResponseWriter, r *http.Request) {
	bearer, _, ok := h.authenticate(w, r, opSessionRevokeAll)
	if !ok {
		return
	}
	principal := bearer.PrincipalID
	if _, err := h.Sessions.RevokeAll(r.Context(), principal, session.Actor{PrincipalID: &principal, RequestID: requestid.Audit(r.Context())}); err != nil {
		h.rec.refuse(w, r, opSessionRevokeAll, unavailable(reasonStoreUnavailable), err)
		return
	}
	h.rec.record(r.Context(), opSessionRevokeAll, "success", reasonAccepted, nil)
	writeNoContent(w)
}

func writeNoContent(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
