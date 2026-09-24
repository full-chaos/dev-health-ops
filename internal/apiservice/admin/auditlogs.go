package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const auditLogFeature = "audit_log"

func (h *handlers) auditLogRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/audit-logs", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listAuditLogs))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/platform/audit-logs", Handler: h.guard.Wrap(policy.Superuser, http.HandlerFunc(h.listPlatformAuditLogs))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/audit-logs/{log_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getAuditLog))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/audit-logs/resource/{resource_type}/{resource_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getResourceAuditHistory))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/audit-logs/user/{user_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getUserAuditActivity))},
	}
}

type auditLog struct {
	ID, OrgID                        uuid.UUID
	UserID                           *uuid.UUID
	Action, ResourceType, ResourceID string
	Description                      *string
	Changes, RequestMetadata         []byte
	Status                           string
	ErrorMessage                     *string
	CreatedAt                        time.Time
}

const auditLogColumns = `id, org_id, user_id, action, resource_type, resource_id, description, changes, request_metadata, status, error_message, created_at`

func scanAuditLog(row pgx.Row) (*auditLog, error) {
	var log auditLog
	err := row.Scan(&log.ID, &log.OrgID, &log.UserID, &log.Action, &log.ResourceType, &log.ResourceID,
		&log.Description, &log.Changes, &log.RequestMetadata, &log.Status, &log.ErrorMessage, &log.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &log, nil
}

// auditLogObject is audit_logs.py's _audit_log_response: changes and
// request_metadata must be an object or null (dict[str, Any] | None), or
// response validation fails (a 500).
func auditLogObject(log *auditLog) (*pyjson.Object, error) {
	changes, ok, err := jsonColumnObject(log.Changes)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("audit log changes is not an object")
	}
	metadata, ok, err := jsonColumnObject(log.RequestMetadata)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("audit log request_metadata is not an object")
	}
	out := pyjson.NewObject()
	out.Set("id", log.ID.String())
	out.Set("org_id", log.OrgID.String())
	out.Set("user_id", uuidOrNil(log.UserID))
	out.Set("action", log.Action)
	out.Set("resource_type", log.ResourceType)
	out.Set("resource_id", log.ResourceID)
	out.Set("description", optionalString(log.Description))
	out.Set("changes", changes)
	out.Set("request_metadata", metadata)
	out.Set("status", log.Status)
	out.Set("error_message", optionalString(log.ErrorMessage))
	out.Set("created_at", pyTimeString(log.CreatedAt))
	return out, nil
}

func auditLogList(logs []*auditLog) ([]pyjson.Value, error) {
	list := make([]pyjson.Value, 0, len(logs))
	for _, log := range logs {
		object, err := auditLogObject(log)
		if err != nil {
			return nil, err
		}
		list = append(list, object)
	}
	return list, nil
}

// auditFilter is AuditLogFilter: every field is applied only when truthy.
type auditFilter struct {
	userID, action, resourceType, resourceID, status string
	start, end                                       *time.Time
}

// auditListQuery validates the list routes' shared query parameters in
// declaration order and returns the filter and page.
func auditListQuery(r *http.Request) (auditFilter, int64, int64, pybody.Errors) {
	values := r.URL.Query()
	var errs pybody.Errors
	filter := auditFilter{
		userID: truthyQuery(values, "user_id"), action: truthyQuery(values, "action"),
		resourceType: truthyQuery(values, "resource_type"), resourceID: truthyQuery(values, "resource_id"),
		status: truthyQuery(values, "status"),
	}
	if parsed, ok := errs.QueryDatetime("start_date", queryPtr(values, "start_date")); ok && parsed != nil {
		instant := parsed.Time.UTC()
		filter.start = &instant
	}
	if parsed, ok := errs.QueryDatetime("end_date", queryPtr(values, "end_date")); ok && parsed != nil {
		instant := parsed.Time.UTC()
		filter.end = &instant
	}
	limit, offset := pageParams(&errs, values, 50, 500)
	return filter, limit, offset, errs
}

// where renders the filter's conditions after any leading ones. A malformed
// user_id is `uuid.UUID(...)` raising, an unhandled 500.
func (f auditFilter) where(conditions []string, args []any) ([]string, []any, bool) {
	add := func(column string, value any) {
		args = append(args, value)
		conditions = append(conditions, fmt.Sprintf("%s $%d", column, len(args)))
	}
	if f.userID != "" {
		id, err := pythonparity.ParseUUID(f.userID)
		if err != nil {
			return nil, nil, false
		}
		add("user_id =", id)
	}
	if f.action != "" {
		add("action =", f.action)
	}
	if f.resourceType != "" {
		add("resource_type =", f.resourceType)
	}
	if f.resourceID != "" {
		add("resource_id =", f.resourceID)
	}
	if f.status != "" {
		add("status =", f.status)
	}
	if f.start != nil {
		add("created_at >=", *f.start)
	}
	if f.end != nil {
		add("created_at <=", *f.end)
	}
	return conditions, args, true
}

func (s pgStore) auditPage(ctx context.Context, conditions []string, args []any, limit, offset int64) ([]*auditLog, int64, error) {
	where := ""
	if len(conditions) > 0 {
		where = " WHERE " + strings.Join(conditions, " AND ")
	}
	var total int64
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_logs`+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	pageArgs := append(append([]any{}, args...), limit, offset)
	logs, err := s.queryAuditLogs(ctx, `SELECT `+auditLogColumns+` FROM audit_logs`+where+
		fmt.Sprintf(` ORDER BY created_at DESC LIMIT $%d OFFSET $%d`, len(args)+1, len(args)+2), pageArgs...)
	return logs, total, err
}

func (s pgStore) queryAuditLogs(ctx context.Context, sql string, args ...any) ([]*auditLog, error) {
	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var logs []*auditLog
	for rows.Next() {
		log, err := scanAuditLog(rows)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}
	return logs, rows.Err()
}

func (h *handlers) writeAuditPage(ctx context.Context, w http.ResponseWriter, logs []*auditLog, total, limit, offset int64) {
	items, err := auditLogList(logs)
	if err != nil {
		h.internalError(ctx, w, "encode audit logs", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", limit)
	out.Set("offset", offset)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// listAuditLogs is audit_logs.py's list_audit_logs.
func (h *handlers) listAuditLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	filter, limit, offset, errs := auditListQuery(r)
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireFeature(ctx, w, auditLogFeature, orgID) {
		return
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	conditions, args, valid := filter.where([]string{"org_id = $1"}, []any{org})
	if !valid || offset < 0 {
		policy.WriteInternal(w)
		return
	}
	logs, total, err := h.store.auditPage(ctx, conditions, args, limit, offset)
	if err != nil {
		h.internalError(ctx, w, "list audit logs", err)
		return
	}
	h.writeAuditPage(ctx, w, logs, total, limit, offset)
}

// listPlatformAuditLogs is audit_logs.py's list_platform_audit_logs.
func (h *handlers) listPlatformAuditLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	filter, limit, offset, errs := auditListQuery(r)
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	conditions, args, valid := filter.where(nil, nil)
	if !valid || offset < 0 {
		policy.WriteInternal(w)
		return
	}
	logs, total, err := h.store.auditPage(ctx, conditions, args, limit, offset)
	if err != nil {
		h.internalError(ctx, w, "list platform audit logs", err)
		return
	}
	h.writeAuditPage(ctx, w, logs, total, limit, offset)
}

// getAuditLog is audit_logs.py's get_audit_log.
func (h *handlers) getAuditLog(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireFeature(ctx, w, auditLogFeature, orgID) {
		return
	}
	org, orgErr := pythonparity.ParseUUID(orgID)
	logID, logOK := pathUUID(r, "log_id")
	if orgErr != nil || !logOK {
		policy.WriteInternal(w)
		return
	}
	log, err := scanAuditLog(h.store.Pool.QueryRow(ctx,
		`SELECT `+auditLogColumns+` FROM audit_logs WHERE id = $1 AND org_id = $2`, logID, org))
	if err != nil {
		h.internalError(ctx, w, "get audit log", err)
		return
	}
	if log == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Audit log not found", nil)
		return
	}
	object, err := auditLogObject(log)
	if err != nil {
		h.internalError(ctx, w, "encode audit log", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, object, nil)
}

// getResourceAuditHistory is audit_logs.py's get_resource_audit_history.
func (h *handlers) getResourceAuditHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	var errs pybody.Errors
	limit := limitOnly(&errs, r.URL.Query(), 50, 500)
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireFeature(ctx, w, auditLogFeature, orgID) {
		return
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	logs, err := h.store.queryAuditLogs(ctx, `SELECT `+auditLogColumns+`
FROM audit_logs WHERE org_id = $1 AND resource_type = $2 AND resource_id = $3
ORDER BY created_at DESC LIMIT $4`, org, r.PathValue("resource_type"), r.PathValue("resource_id"), limit)
	if err != nil {
		h.internalError(ctx, w, "get resource audit history", err)
		return
	}
	h.writeAuditList(ctx, w, logs)
}

// getUserAuditActivity is audit_logs.py's get_user_audit_activity.
func (h *handlers) getUserAuditActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	var errs pybody.Errors
	limit := limitOnly(&errs, r.URL.Query(), 50, 500)
	if len(errs) > 0 {
		writeValidation(w, errs)
		return
	}
	if !h.requireFeature(ctx, w, auditLogFeature, orgID) {
		return
	}
	org, orgErr := pythonparity.ParseUUID(orgID)
	userID, userOK := pathUUID(r, "user_id")
	if orgErr != nil || !userOK {
		policy.WriteInternal(w)
		return
	}
	logs, err := h.store.queryAuditLogs(ctx, `SELECT `+auditLogColumns+`
FROM audit_logs WHERE org_id = $1 AND user_id = $2 ORDER BY created_at DESC LIMIT $3`, org, userID, limit)
	if err != nil {
		h.internalError(ctx, w, "get user audit activity", err)
		return
	}
	h.writeAuditList(ctx, w, logs)
}

func (h *handlers) writeAuditList(ctx context.Context, w http.ResponseWriter, logs []*auditLog) {
	items, err := auditLogList(logs)
	if err != nil {
		h.internalError(ctx, w, "encode audit logs", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, items, nil)
}
