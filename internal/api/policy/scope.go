package policy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

type contextKey int

const (
	orgIDKey contextKey = iota
	impersonationKey
)

// OrgIDFrom returns the request's resolved org (the contextvar
// _current_org_id): the verified X-Org-Id, else the caller's own org, and
// the impersonated org while an impersonation session is active. "" means
// no org context.
func OrgIDFrom(ctx context.Context) string {
	value, _ := ctx.Value(orgIDKey).(string)
	return value
}

// ImpersonationFrom returns the active impersonation session of the request
// (get_impersonation_context()), or nil.
func ImpersonationFrom(ctx context.Context) *Impersonation {
	value, _ := ctx.Value(impersonationKey).(*Impersonation)
	return value
}

// Scope holds the two request-wide middlewares. They run for every request
// on the api listener, public routes and unknown paths included, exactly as
// the Python middlewares do.
type Scope struct {
	auth   *Authenticator
	logger *slog.Logger
}

// NewScope builds the middlewares over auth.
func NewScope(auth *Authenticator, logger *slog.Logger) *Scope {
	if logger == nil {
		logger = slog.Default()
	}
	return &Scope{auth: auth, logger: logger}
}

// acrInternalPrefix is the path prefix OrgIdMiddleware skips.
const acrInternalPrefix = "/api/v1/internal/acr/"

// headerAuthenticate is get_authenticated_user_from_headers: the first
// Authorization header, a refused credential as nil, a store failure as an
// error.
func (s *Scope) headerAuthenticate(r *http.Request) (*User, error) {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return nil, nil
	}
	token, ok := BearerToken(values[0])
	if !ok {
		return nil, nil
	}
	user, err := s.auth.Authenticate(r.Context(), token)
	if err == nil {
		return user, nil
	}
	if isRefusal(err) {
		return nil, nil
	}
	return nil, err
}

// OrgScope is OrgIdMiddleware. With an X-Org-Id header and an authenticated
// caller, the header is accepted only for a superuser, the caller's own
// org, or an org the caller has a membership in; anything else is a 403
// before the route runs. With no header, the caller's own org is the
// scope. An anonymous or refused caller passes through with no org: the
// route's own guard answers 401.
func (s *Scope) OrgScope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, acrInternalPrefix) {
			next.ServeHTTP(w, r)
			return
		}
		headerOrgID := ""
		if values := r.Header.Values("X-Org-Id"); len(values) > 0 {
			headerOrgID = strings.TrimFunc(Latin1(values[0]), isPySpace)
		}
		user, err := s.headerAuthenticate(r)
		if err != nil {
			s.logger.ErrorContext(r.Context(), "api org scope: caller lookup failed",
				slog.String("path", r.URL.Path), slog.Bool("unavailable", isUnavailable(err)))
			WriteInternal(w)
			return
		}
		resolved := ""
		switch {
		case headerOrgID != "" && user != nil:
			allowed, err := s.mayUseOrg(r.Context(), user, headerOrgID)
			if err != nil {
				s.logger.ErrorContext(r.Context(), "api org scope: membership lookup failed",
					slog.String("user_id", user.UserID), slog.Bool("unavailable", isUnavailable(err)))
				WriteInternal(w)
				return
			}
			if !allowed {
				s.logger.WarnContext(r.Context(), "X-Org-Id rejected",
					slog.String("user_id", user.UserID), slog.String("org_id", headerOrgID))
				writeOrgDenied(w)
				return
			}
			resolved = headerOrgID
		case user != nil && user.OrgID != "":
			resolved = user.OrgID
		}
		if resolved != "" {
			r = r.WithContext(context.WithValue(r.Context(), orgIDKey, resolved))
		}
		next.ServeHTTP(w, r)
	})
}

// mayUseOrg is OrgIdMiddleware's acceptance rule for a requested org.
func (s *Scope) mayUseOrg(ctx context.Context, user *User, orgID string) (bool, error) {
	if user.IsSuperuser || orgID == user.OrgID {
		return true, nil
	}
	return s.auth.IsMember(ctx, user.UserID, orgID)
}

// IsMember is user_is_member_of_org: an id that does not parse is not a
// member.
func (a *Authenticator) IsMember(ctx context.Context, userID, orgID string) (bool, error) {
	user, okUser := ParsePyUUID(userID)
	org, okOrg := ParsePyUUID(orgID)
	if !okUser || !okOrg {
		return false, nil
	}
	return a.store.IsMember(ctx, user, org)
}

// Impersonation is ImpersonationMiddleware. For a caller whose token claims
// superuser and whose users row confirms it, an active session makes the
// target's org the request's org, sets the impersonation context, and
// stamps X-Impersonating / X-Impersonated-User-Id on the response. A
// session lookup failure means no impersonation (fail open, logged).
func (s *Scope) Impersonation(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !maybeSuperuser(r) {
			next.ServeHTTP(w, r)
			return
		}
		user, err := s.headerAuthenticate(r)
		if err != nil {
			s.logger.ErrorContext(r.Context(), "api impersonation: caller lookup failed",
				slog.Bool("unavailable", isUnavailable(err)))
			WriteInternal(w)
			return
		}
		if user == nil || !user.IsSuperuser {
			next.ServeHTTP(w, r)
			return
		}
		session, err := s.auth.store.ActiveImpersonation(r.Context(), user.ID)
		if err != nil {
			s.logger.WarnContext(r.Context(), "api impersonation: session lookup failed; not impersonating",
				slog.String("admin_user_id", user.UserID), slog.Bool("unavailable", isUnavailable(err)))
			session = nil
		}
		if session == nil {
			next.ServeHTTP(w, r)
			return
		}
		ctx := context.WithValue(r.Context(), orgIDKey, session.TargetOrgID.String())
		ctx = context.WithValue(ctx, impersonationKey, session)
		header := w.Header()
		header.Add("X-Impersonating", "true")
		header.Add("X-Impersonated-User-Id", session.TargetUserID.String())
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// maybeSuperuser is _may_be_superuser's header peek: the first
// Authorization header's bearer token, its payload segment decoded without
// verification, and Python truthiness of "is_superuser". It only decides
// whether the full check runs.
func maybeSuperuser(r *http.Request) bool {
	values := r.Header.Values("Authorization")
	if len(values) == 0 {
		return false
	}
	token, ok := BearerToken(values[0])
	if !ok {
		return false
	}
	segments := strings.Split(token, ".")
	if len(segments) < 2 {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segments[1], "="))
	if err != nil {
		return false
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return false
	}
	return pyTruthy(claims["is_superuser"])
}

// pyTruthy is bool() of a decoded JSON value.
func pyTruthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case float64:
		return typed != 0
	case string:
		return typed != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

// orgDeniedBody is OrgIdMiddleware._deny's body: json.dumps with its default
// separators, so ": " with a space, unlike every JSONResponse body.
var orgDeniedBody = []byte(`{"detail": "X-Org-Id not permitted for this user"}`)

// writeOrgDenied is OrgIdMiddleware._deny: 403 with exactly content-type and
// content-length.
func writeOrgDenied(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(orgDeniedBody)))
	w.WriteHeader(http.StatusForbidden)
	_, _ = w.Write(orgDeniedBody)
}
