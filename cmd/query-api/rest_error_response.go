// This file is the one shared REST error-response path for every
// /api/v1/* route this binary mounts (quadrant, filters/options,
// drilldown/prs, drilldown/issues, meta, investment/explain, explain,
// people): the exact status, Content-Type and JSON body FastAPI's default
// exception handling produces for an HTTPException, confirmed live
// against the real Python app (see this file's own functions' doc
// comments and this route set's TEST-EVIDENCE for the capture commands
// and raw output). No route file keeps its own local copy of this
// envelope -- TestNoAPIV1RoutePlainTextErrors (rest_error_response_test.go)
// guards both a plain-text fallback AND a route-local re-implementation.
//
// FastAPI's own default HTTPException handler renders
// {"detail": exc.detail} -- exc.detail is either a plain string (every
// literal `HTTPException(status_code=N, detail="...")` call in main.py)
// or a nested object (get_current_user's error_detail() helper,
// {"message": "..."}, api/utils/errors.py); this file's restErrorBody
// carries either shape as `any` rather than declaring two response
// types, matching the ONE response shape the wire actually needs
// per call site. The 422 path (RequestValidationError's own
// {"detail": [...]} shape) is a SEPARATE, already-correct contract --
// see pydantic_validation_error.go's own package doc comment for why it
// is not folded in here.
package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/authctx"
	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
)

// restErrorBody is FastAPI's default HTTPException response envelope.
type restErrorBody struct {
	Detail any `json:"detail"`
}

// restAuthErrorDetail is error_detail(message)'s wire shape
// (api/utils/errors.py) -- get_current_user's three 401 branches and its
// database-unavailable 503 all carry exactly this, never a bare string.
type restAuthErrorDetail struct {
	Message string `json:"message"`
}

// writeRESTError answers a REST /api/v1/* request with FastAPI's own
// {"detail": detail} envelope at the given status -- the one place any
// such route sets this Content-Type/body shape, replacing a raw
// w.Write/http.Error call. An encode failure is logged with the
// caller-supplied X-Request-Id and orgID (empty for a request that never
// authenticated) rather than silently dropped, matching every other
// JSON-response path in this package (writePydanticValidationError,
// writeDrilldownPRsResponse).
//
// extraHeaders is an optional list of (name, value) pairs set on the
// response BEFORE Content-Type/status -- e.g. a route whose own Python
// counterpart's default 405 carries a captured "Allow" value (see
// writeRESTMethodNotAllowed's own doc comment). Every existing call site
// omits it; that is what makes this a genuinely optional extension, not
// a second response path.
func writeRESTError(w http.ResponseWriter, r *http.Request, component, orgID string, status int, detail any, extraHeaders ...[2]string) {
	for _, h := range extraHeaders {
		w.Header().Set(h[0], h[1])
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if encodeErr := json.NewEncoder(w).Encode(restErrorBody{Detail: detail}); encodeErr != nil {
		log.Printf("query-api: %s: encode error response failed: org_id=%s request_id=%s err=%v",
			component, orgID, envelopeRequestID(r), encodeErr)
	}
}

// writeRESTUnauthorized answers 401 with error_detail(message)'s exact
// shape and a "WWW-Authenticate: Bearer" header -- get_current_user
// (auth/routers/dependencies.py) sets this header on all three of its
// 401 raises, confirmed live for the missing-header and
// malformed-header branches and true by inspection (identical
// HTTPException(..., headers={"WWW-Authenticate": "Bearer"}) call shape)
// for the third (invalid/expired token, only reachable with a real
// Postgres-backed auth service -- see this route set's TEST-EVIDENCE for
// why that branch's capture stubs the auth service rather than using a
// live database).
func writeRESTUnauthorized(w http.ResponseWriter, r *http.Request, component, message string) {
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeRESTError(w, r, component, "", http.StatusUnauthorized, restAuthErrorDetail{Message: message})
}

// writeRESTMethodNotAllowed answers 405 with Starlette's own default
// body for a path that matches but whose method does not -- confirmed
// live for every route this binary mounts. Starlette also sets an
// "Allow" header, but its value is the FIRST partially-matching route's
// own method set in source-declaration order (each `@app.get`/`@app.post`
// on the same path is a SEPARATE Route object, not one route with two
// methods) -- an artifact of Starlette's route-matching internals, not a
// documented general contract. Most call sites omit allow and get no
// Allow header at all (not attempted); a route that HAS captured its own
// exact live value (explain_route.go's own package doc comment: POST
// registered before GET, so a third method answers "Allow: POST" alone)
// passes it through.
func writeRESTMethodNotAllowed(w http.ResponseWriter, r *http.Request, component string, allow ...string) {
	if len(allow) > 0 && allow[0] != "" {
		writeRESTError(w, r, component, "", http.StatusMethodNotAllowed, "Method Not Allowed", [2]string{"Allow", allow[0]})
		return
	}
	writeRESTError(w, r, component, "", http.StatusMethodNotAllowed, "Method Not Allowed")
}

// writeRESTDataUnavailable answers 503 with the literal string every
// ClickHouse-backed REST route in this binary's Python counterpart falls
// back to on any unexpected downstream failure (`except Exception:
// raise HTTPException(status_code=503, detail="Data unavailable")`).
//
// Unlike Python, this degradation site does not swallow the cause: err
// (the wrapped error every caller already holds, from the failed
// BuildResponse/BuildIssuesResponse/etc. call or, where no such error
// exists, a locally constructed one describing the rejected input -- see
// e.g. sankey_route.go's invalid-scope-level call site) is logged at
// error level with the route, the org id and the caller-supplied
// X-Request-Id BEFORE the response is written, so an operator can
// correlate a live 503 back to the failure that produced it. err must
// never be nil: TestDataUnavailableCallSitesLogTheCause guards every
// call site in this package for that.
func writeRESTDataUnavailable(w http.ResponseWriter, r *http.Request, component, orgID string, err error) {
	log.Printf("query-api: %s: degraded to 503 Data unavailable: org_id=%s request_id=%s err=%v",
		component, orgID, envelopeRequestID(r), err)
	writeRESTError(w, r, component, orgID, http.StatusServiceUnavailable, "Data unavailable")
}

// authenticateRESTRequest reproduces get_current_user's own three-way
// auth-failure classification (auth/routers/dependencies.py) ahead of a
// REST route's business logic: an absent Authorization header, a header
// present but not a well-formed "Bearer <token>" value, and a
// present-and-well-formed token that fails verification, each answer
// their own distinct body (see writeRESTUnauthorized's doc comment) --
// never one generic "unauthorized" message for all three. ok is false
// whenever this function has already written the response and the
// caller must return immediately; on success it returns the claims to
// attach to the request context, matching every REST route's existing
// entryHandler shape.
func authenticateRESTRequest(w http.ResponseWriter, r *http.Request, verifier *principal.Verifier, component string) (authctx.Claims, bool) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		writeRESTUnauthorized(w, r, component, "Not authenticated")
		return authctx.Claims{}, false
	}
	token, parsed := bearerToken(authHeader)
	if !parsed {
		writeRESTUnauthorized(w, r, component, "Invalid authorization header")
		return authctx.Claims{}, false
	}
	verifyCtx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
	claims, err := verifier.Verify(verifyCtx, token)
	if err != nil {
		writeRESTUnauthorized(w, r, component, "Invalid or expired token")
		return authctx.Claims{}, false
	}
	return authctx.Claims{OrgID: claims.OrgID}, true
}
