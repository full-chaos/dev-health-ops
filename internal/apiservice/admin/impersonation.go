package admin

import (
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

const impersonationPrefix = "/api/v1/admin"

func (h *handlers) impersonationRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: impersonationPrefix + "/impersonate",
			Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.startImpersonation))},
		{Method: http.MethodPost, Pattern: impersonationPrefix + "/impersonate/stop",
			Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.stopImpersonation))},
		{Method: http.MethodGet, Pattern: impersonationPrefix + "/impersonate/status",
			Handler: h.guard.Wrap(policy.Authenticated, http.HandlerFunc(h.impersonationStatus))},
	}
}

func (h *handlers) now() time.Time {
	if h.store.Now != nil {
		return h.store.Now()
	}
	return time.Now()
}

// impersonationTTL is _impersonation_ttl_minutes(): IMPERSONATION_TTL_MINUTES
// (default 60), read fresh per call as the Python function does (an
// operator can change the environment between requests in a long-running
// process only via a restart either way, but the Python code re-reads it
// every call, so a test that stubs os.Setenv mid-process still matches).
func impersonationTTL() (time.Duration, *pybody.Error) {
	raw := strings.TrimSpace(os.Getenv("IMPERSONATION_TTL_MINUTES"))
	if raw == "" {
		raw = "60"
	}
	minutes, err := strconv.Atoi(raw)
	if err != nil {
		return 0, &pybody.Error{Type: "value_error", Msg: "Invalid IMPERSONATION_TTL_MINUTES configuration"}
	}
	if minutes <= 0 {
		return 0, &pybody.Error{Type: "value_error", Msg: "IMPERSONATION_TTL_MINUTES must be > 0"}
	}
	return time.Duration(minutes) * time.Minute, nil
}

// startImpersonation is impersonation.py's start_impersonation.
func (h *handlers) startImpersonation(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())

	body, outcome, failure, err := pybody.Read(r)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	if outcome == pybody.ParseFailed {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid request body", nil)
		return
	}
	if outcome == pybody.DecodeFailed {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
		return
	}
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var targetUserIDRaw string
	if ok {
		targetUserIDRaw, _ = errs.RequiredString(object, "target_user_id", 0, 0)
	}
	if len(errs) > 0 {
		policy.WritePyJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return
	}

	targetUserID, parseErr := uuid.Parse(targetUserIDRaw)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid target_user_id", nil)
		return
	}
	adminUserID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}

	// Prevent self-impersonation.
	if targetUserID == adminUserID {
		policy.WriteDetail(w, http.StatusBadRequest, "Cannot impersonate yourself", nil)
		return
	}

	ctx := r.Context()
	target, storeErr := h.store.userByID(ctx, targetUserID)
	if storeErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: target lookup failed", slog.Any("error", storeErr))
		policy.WriteInternal(w)
		return
	}
	if target == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Target user not found", nil)
		return
	}
	if !target.IsActive {
		policy.WriteDetail(w, http.StatusBadRequest, "Target user is not active", nil)
		return
	}
	if target.IsSuperuser {
		policy.WriteDetail(w, http.StatusForbidden, "Cannot impersonate a superuser", nil)
		return
	}

	membership, storeErr := h.store.firstMembership(ctx, targetUserID)
	if storeErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: membership lookup failed", slog.Any("error", storeErr))
		policy.WriteInternal(w)
		return
	}
	if membership == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Target user has no organization membership", nil)
		return
	}

	ttl, ttlErr := impersonationTTL()
	if ttlErr != nil {
		policy.WriteDetail(w, http.StatusInternalServerError, ttlErr.Msg, nil)
		return
	}
	expiresAt := h.now().UTC().Add(ttl)

	tx, txErr := h.store.Pool.Begin(ctx)
	if txErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: begin tx failed", slog.Any("error", txErr))
		policy.WriteInternal(w)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := h.store.endActiveImpersonationSessions(ctx, tx, adminUserID); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: end prior sessions failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	sessionID, err := h.store.insertImpersonationSession(ctx, tx, adminUserID, targetUserID, membership.OrgID, membership.Role, expiresAt)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: insert session failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if _, err := h.audit.Write(ctx, requestAuditEntry(r, audit.Entry{
		OrgID:        membership.OrgID,
		UserID:       &adminUserID,
		Action:       audit.ActionImpersonationStart,
		ResourceType: audit.ResourceSession,
		ResourceID:   target.ID.String(),
	})); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: audit write failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate: commit failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}

	targetEmail := target.Email
	h.cache.setActiveSession(ctx, user.UserID, &cachedImpersonationSession{
		ID:           sessionID.String(),
		AdminUserID:  adminUserID.String(),
		TargetUserID: targetUserID.String(),
		TargetOrgID:  membership.OrgID.String(),
		TargetRole:   membership.Role,
		TargetEmail:  &targetEmail,
		ExpiresAt:    expiresAt,
	})

	response := pyjson.NewObject()
	response.Set("status", "active")
	targetObject := pyjson.NewObject()
	targetObject.Set("id", target.ID.String())
	targetObject.Set("email", target.Email)
	targetObject.Set("org_id", membership.OrgID.String())
	targetObject.Set("role", membership.Role)
	response.Set("target_user", targetObject)
	response.Set("expires_at", pyjson.TimeString(expiresAt))
	policy.WritePyJSON(w, http.StatusOK, response, nil)
}

// stopImpersonation is impersonation.py's stop_impersonation.
func (h *handlers) stopImpersonation(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	adminUserID, parseErr := uuid.Parse(user.UserID)
	if parseErr != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}
	ctx := r.Context()

	tx, txErr := h.store.Pool.Begin(ctx)
	if txErr != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: begin tx failed", slog.Any("error", txErr))
		policy.WriteInternal(w)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()

	active, err := h.store.activeImpersonationSessionTx(ctx, tx, adminUserID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: lookup failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if active == nil {
		policy.WriteDetail(w, http.StatusBadRequest, "No active impersonation session", nil)
		return
	}
	if err := h.store.endImpersonationSession(ctx, tx, active.ID); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: end session failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if _, err := h.audit.Write(ctx, requestAuditEntry(r, audit.Entry{
		OrgID:        active.TargetOrgID,
		UserID:       &adminUserID,
		Action:       audit.ActionImpersonationStop,
		ResourceType: audit.ResourceSession,
		ResourceID:   active.TargetUserID.String(),
	})); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: audit write failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate stop: commit failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}

	h.cache.setActiveSession(ctx, user.UserID, nil)

	response := pyjson.NewObject()
	response.Set("status", "stopped")
	policy.WritePyJSON(w, http.StatusOK, response, nil)
}

// impersonationStatus is impersonation.py's impersonation_status: reads
// through the shared cache (this admin's active session or the negative
// sentinel), never a Postgres query on the hot path for the overwhelmingly
// common "not impersonating" case.
func (h *handlers) impersonationStatus(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	// ImpersonationStatusResponse is one pydantic model regardless of
	// branch: is_impersonating plus four `| None = None` fields FastAPI
	// always serializes (present, null, when the branch never sets them) --
	// never fewer than 5 keys.
	notImpersonating := func() *pyjson.Object {
		response := pyjson.NewObject()
		response.Set("is_impersonating", false)
		response.Set("target_user_id", nil)
		response.Set("target_email", nil)
		response.Set("target_org_id", nil)
		response.Set("expires_at", nil)
		return response
	}
	if !user.IsSuperuser {
		policy.WritePyJSON(w, http.StatusOK, notImpersonating(), nil)
		return
	}
	// _parse_uuid raises 400 on a garbage user_id claim -- cache keys are
	// keyed by the canonical string form, so this validates the id shape
	// before it becomes part of a Valkey key, matching Python's own
	// pre-cache-read validation.
	if _, err := uuid.Parse(user.UserID); err != nil {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid user_id", nil)
		return
	}

	ctx := r.Context()
	active, err := h.cache.activeSession(ctx, user.UserID)
	if err != nil {
		h.logger.ErrorContext(ctx, "admin impersonate status: lookup failed", slog.Any("error", err))
		policy.WriteInternal(w)
		return
	}
	if active == nil || !active.ExpiresAt.After(h.now().UTC()) {
		policy.WritePyJSON(w, http.StatusOK, notImpersonating(), nil)
		return
	}

	response := pyjson.NewObject()
	response.Set("is_impersonating", true)
	response.Set("target_user_id", active.TargetUserID)
	if active.TargetEmail != nil {
		response.Set("target_email", *active.TargetEmail)
	} else {
		response.Set("target_email", nil)
	}
	response.Set("target_org_id", active.TargetOrgID)
	response.Set("expires_at", pyjson.TimeString(active.ExpiresAt))
	policy.WritePyJSON(w, http.StatusOK, response, nil)
}

// requestAuditEntry fills entry's RequestMetadata the way
// api/utils/audit.py's extract_request_metadata does: X-Forwarded-For
// (first hop) or else the direct peer, User-Agent, and X-Request-ID, each
// present only when the header/value is non-empty.
func requestAuditEntry(r *http.Request, entry audit.Entry) audit.Entry {
	metadata := pyjson.NewObject()
	hasAny := false
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		ip, _, _ := strings.Cut(forwarded, ",")
		metadata.Set("ip_address", strings.TrimSpace(ip))
		hasAny = true
	} else if host, _, splitErr := net.SplitHostPort(r.RemoteAddr); splitErr == nil && host != "" {
		metadata.Set("ip_address", host)
		hasAny = true
	}
	if ua := r.Header.Get("User-Agent"); ua != "" {
		metadata.Set("user_agent", ua)
		hasAny = true
	}
	if requestID := r.Header.Get("X-Request-ID"); requestID != "" {
		metadata.Set("request_id", requestID)
		hasAny = true
	}
	if hasAny {
		encoded, err := pyjson.Marshal(metadata)
		if err == nil {
			entry.RequestMetadata = encoded
		}
	}
	return entry
}
