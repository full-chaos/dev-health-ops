package admin

import (
	"context"
	"net/http"
	"net/url"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// governancePrefix is the mount prefix of the governance routes: audit logs,
// feature flags and overrides, the IP allowlist, and platform stats
// (api/admin/routers/{audit_logs,features,ip_allowlist,platform}.py).
const governancePrefix = "/api/v1/admin"

func (h *handlers) governanceRoutes() []httpapi.Route {
	var out []httpapi.Route
	out = append(out, h.platformRoutes()...)
	out = append(out, h.featureRoutes()...)
	out = append(out, h.auditLogRoutes()...)
	out = append(out, h.ipAllowlistRoutes()...)
	out = append(out, h.retentionRoutes()...)
	out = append(out, h.settingsRoutes()...)
	out = append(out, h.llmSettingsRoutes()...)
	return out
}

// adminOrgID is middleware.py's get_admin_org_id after require_admin: the
// caller's org_id claim, 403 when it has none -- a superuser included.
func adminOrgID(w http.ResponseWriter, user *policy.User) (string, bool) {
	if user.OrgID == "" {
		policy.WriteDetail(w, http.StatusForbidden, "Organization context required", nil)
		return "", false
	}
	return user.OrgID, true
}

// internalError logs a failed step with the request context and answers the
// generic 500 an unhandled Python exception produces.
func (h *handlers) internalError(ctx context.Context, w http.ResponseWriter, what string, err error) {
	h.logger.ErrorContext(ctx, "admin: "+what+" failed", "error", err)
	policy.WriteInternal(w)
}

// queryPtr is Starlette's QueryParams.get: the LAST value of a repeated
// parameter, nil when absent.
func queryPtr(values url.Values, name string) *string {
	value, present := queryLastValue(values, name)
	if !present {
		return nil
	}
	return &value
}

// truthyQuery is a filter parameter gated by `if value:` -- absent and empty
// are both "no filter".
func truthyQuery(values url.Values, name string) string {
	value, _ := queryLastValue(values, name)
	return value
}

// pageParams validates the limit (ge=1, le=maxLimit) and offset (ge=0) query
// parameters as FastAPI's Query() does, appending any error to errs.
func pageParams(errs *pybody.Errors, values url.Values, defaultLimit, maxLimit int64) (limit, offset int64) {
	one, zero := int64(1), int64(0)
	if value, ok := errs.QueryInt("limit", queryPtr(values, "limit"), defaultLimit, &one, &maxLimit); ok {
		limit = value.Int64()
	}
	if value, ok := errs.QueryInt("offset", queryPtr(values, "offset"), 0, &zero, nil); ok {
		if value.IsInt64() {
			offset = value.Int64()
		} else {
			offset = -1
		}
	}
	return limit, offset
}

// limitOnly validates a route's lone limit (ge=1, le=maxLimit) parameter.
func limitOnly(errs *pybody.Errors, values url.Values, defaultLimit, maxLimit int64) int64 {
	one := int64(1)
	if value, ok := errs.QueryInt("limit", queryPtr(values, "limit"), defaultLimit, &one, &maxLimit); ok {
		return value.Int64()
	}
	return 0
}

// writeValidation answers FastAPI's 422 for the collected errors.
func writeValidation(w http.ResponseWriter, errs pybody.Errors) {
	policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
}

// pathUUID is `uuid.UUID(path_param)`: a ValueError there is unhandled, so
// the caller answers the generic 500.
func pathUUID(r *http.Request, name string) (uuid.UUID, bool) {
	id, err := pythonparity.ParseUUID(r.PathValue(name))
	return id, err == nil
}

// jsonColumnValue decodes a stored `json` column as SQLAlchemy's JSON type
// does; SQL NULL and a JSON null are both nil.
func jsonColumnValue(raw []byte) (pyjson.Value, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	return pyjson.DecodeString(string(raw))
}

// jsonColumnObject is jsonColumnValue for a `dict[str, Any] | None` response
// field: anything but an object or null fails response validation (a 500).
func jsonColumnObject(raw []byte) (pyjson.Value, bool, error) {
	value, err := jsonColumnValue(raw)
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, true, nil
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return nil, false, nil
	}
	return object, true, nil
}
