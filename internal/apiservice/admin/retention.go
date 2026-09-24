package admin

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const retentionFeature = "custom_retention"

// retentionResourceTypes is RetentionResourceType's values, in enum order.
var retentionResourceTypes = []string{"audit_logs", "metrics_daily", "work_items", "git_commits", "sync_logs"}

func (h *handlers) retentionRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/retention-policies", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listRetentionPolicies))},
		{Method: http.MethodPost, Pattern: governancePrefix + "/retention-policies", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.createRetentionPolicy))},
		// GET /retention-policies/resource-types is a literal beside the
		// /{policy_id} routes, which the route table cannot register side by
		// side (see checkOrMethodNotAllowed). It is mounted on the wildcard's
		// GET and told apart by the segment; FastAPI's 405 for any other
		// method names GET, the first route that matched the path.
		{Method: http.MethodGet, Pattern: governancePrefix + "/retention-policies/{policy_id}", Allow: "GET", Handler: h.resourceTypesOrGetPolicy()},
		{Method: http.MethodPatch, Pattern: governancePrefix + "/retention-policies/{policy_id}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.updateRetentionPolicy))},
		{Method: http.MethodDelete, Pattern: governancePrefix + "/retention-policies/{policy_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteRetentionPolicy))},
		{Method: http.MethodPost, Pattern: governancePrefix + "/retention-policies/{policy_id}/execute", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.executeRetentionPolicy))},
	}
}

// resourceTypesOrGetPolicy answers GET /retention-policies/resource-types and
// GET /retention-policies/{policy_id}, both guarded at Admin.
func (h *handlers) resourceTypesOrGetPolicy() http.Handler {
	resourceTypes := h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listRetentionResourceTypes))
	getPolicy := h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getRetentionPolicy))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("policy_id") == "resource-types" {
			resourceTypes.ServeHTTP(w, r)
			return
		}
		getPolicy.ServeHTTP(w, r)
	})
}

type retentionPolicy struct {
	ID, OrgID           uuid.UUID
	ResourceType        string
	RetentionDays       int32
	Description         *string
	IsActive            bool
	LastRunAt           *time.Time
	LastRunDeletedCount *int32
	NextRunAt           *time.Time
	CreatedByID         *uuid.UUID
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

const retentionColumns = `id, org_id, resource_type, retention_days, description, is_active, last_run_at, last_run_deleted_count, next_run_at, created_by_id, created_at, updated_at`

func scanRetentionPolicy(row pgx.Row) (*retentionPolicy, error) {
	var p retentionPolicy
	err := row.Scan(&p.ID, &p.OrgID, &p.ResourceType, &p.RetentionDays, &p.Description, &p.IsActive,
		&p.LastRunAt, &p.LastRunDeletedCount, &p.NextRunAt, &p.CreatedByID, &p.CreatedAt, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func retentionPolicyObject(p *retentionPolicy) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", p.ID.String())
	out.Set("org_id", p.OrgID.String())
	out.Set("resource_type", p.ResourceType)
	out.Set("retention_days", int64(p.RetentionDays))
	out.Set("description", optionalString(p.Description))
	out.Set("is_active", p.IsActive)
	out.Set("last_run_at", optionalTime(p.LastRunAt))
	if p.LastRunDeletedCount == nil {
		out.Set("last_run_deleted_count", nil)
	} else {
		out.Set("last_run_deleted_count", int64(*p.LastRunDeletedCount))
	}
	out.Set("next_run_at", optionalTime(p.NextRunAt))
	out.Set("created_by_id", uuidOrNil(p.CreatedByID))
	out.Set("created_at", pyTimeString(p.CreatedAt))
	out.Set("updated_at", pyTimeString(p.UpdatedAt))
	return out
}

// listRetentionResourceTypes is retention.py's list_retention_resource_types.
// The route is decorated with @require_feature but takes neither `session` nor
// `org_id`, so the decorator's per-org check has nothing to read and always
// denies: with no process licence the route answers 402 to every admin,
// whatever the org's licence. Go answers the same, whatever the org.
func (h *handlers) listRetentionResourceTypes(w http.ResponseWriter, r *http.Request) {
	writeFeatureNotLicensed(w, retentionFeature)
}

// listRetentionPolicies is retention.py's list_retention_policies.
func (h *handlers) listRetentionPolicies(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	values := r.URL.Query()
	var errs pybody.Errors
	activeOnly := false
	if value, failure := queryBool(values, "active_only", false); failure != nil {
		errs = append(errs, *failure)
	} else {
		activeOnly = value
	}
	limit, offset := pageParams(&errs, values, 100, 500)
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, errs)
	if !ok {
		return
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil || offset < 0 {
		policy.WriteInternal(w)
		return
	}
	where := ` WHERE org_id = $1`
	if activeOnly {
		where += ` AND is_active = true`
	}
	var total int64
	if err := h.store.Pool.QueryRow(ctx, `SELECT count(*) FROM org_retention_policies`+where, org).Scan(&total); err != nil {
		h.internalError(ctx, w, "count retention policies", err)
		return
	}
	rows, err := h.store.Pool.Query(ctx, `SELECT `+retentionColumns+` FROM org_retention_policies`+where+
		` ORDER BY created_at LIMIT $2 OFFSET $3`, org, limit, offset)
	if err != nil {
		h.internalError(ctx, w, "list retention policies", err)
		return
	}
	defer rows.Close()
	items := []pyjson.Value{}
	for rows.Next() {
		policyRow, err := scanRetentionPolicy(rows)
		if err != nil {
			h.internalError(ctx, w, "scan retention policy", err)
			return
		}
		items = append(items, retentionPolicyObject(policyRow))
	}
	if err := rows.Err(); err != nil {
		h.internalError(ctx, w, "list retention policies", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", limit)
	out.Set("offset", offset)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// createRetentionPolicy is retention.py's create_retention_policy.
func (h *handlers) createRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		resourceType string
		days         = big.NewInt(90)
		description  *string
	)
	if ok {
		resourceType, _ = errs.RequiredString(object, "resource_type", 0, 0)
		if value, present := errs.DefaultedMinInt(object, "retention_days", 1); present {
			days = value
		}
		if value, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &value
		}
	}
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, errs)
	if !ok {
		return
	}
	// The uuid.UUID(...) arguments sit inside the route's try/except
	// ValueError, so a malformed org claim or X-User-Id is a 400 carrying
	// the exception text; the service's own checks follow.
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		detail, _ := pythonparity.UUIDValidationDetail(orgID)
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return
	}
	var createdBy *uuid.UUID
	if header := firstHeader(r, "X-User-Id"); header != "" {
		id, err := pythonparity.ParseUUID(header)
		if err != nil {
			detail, _ := pythonparity.UUIDValidationDetail(header)
			policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
			return
		}
		createdBy = &id
	}
	if !containsString(retentionResourceTypes, resourceType) {
		policy.WriteDetail(w, http.StatusBadRequest, "Invalid resource type: "+resourceType, nil)
		return
	}
	var exists bool
	if err := h.store.Pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM org_retention_policies WHERE org_id = $1 AND resource_type = $2)`, org, resourceType).Scan(&exists); err != nil {
		h.internalError(ctx, w, "look up retention policy", err)
		return
	}
	if exists {
		policy.WriteDetail(w, http.StatusBadRequest, fmt.Sprintf("Policy for resource type '%s' already exists", resourceType), nil)
		return
	}
	// retention_days is an Integer column; a value past int32 fails the
	// insert (a 500) exactly as it does in Python.
	if days.Cmp(big.NewInt(math.MaxInt32)) > 0 {
		h.internalError(ctx, w, "insert retention policy", errors.New("retention_days out of range for integer"))
		return
	}
	now := h.store.now().UTC()
	created := &retentionPolicy{
		ID: uuid.New(), OrgID: org, ResourceType: resourceType, RetentionDays: int32(days.Int64()), Description: description,
		IsActive: true, CreatedByID: createdBy, CreatedAt: now, UpdatedAt: now,
	}
	if _, err := h.store.Pool.Exec(ctx, `
INSERT INTO org_retention_policies
	(id, org_id, resource_type, retention_days, description, is_active, created_by_id, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, true, $6, $7, $7)`,
		created.ID, org, resourceType, created.RetentionDays, description, createdBy, now); err != nil {
		h.internalError(ctx, w, "insert retention policy", err)
		return
	}
	policy.WriteJSON(w, http.StatusCreated, retentionPolicyObject(created), nil)
}

// loadRetentionPolicy is RetentionService.get_policy for the route's
// policy_id; it answers the 500 or 404 itself and returns false.
func (h *handlers) loadRetentionPolicy(ctx context.Context, w http.ResponseWriter, r *http.Request, orgID string) (*retentionPolicy, bool) {
	org, orgErr := pythonparity.ParseUUID(orgID)
	policyID, policyOK := pathUUID(r, "policy_id")
	if orgErr != nil || !policyOK {
		policy.WriteInternal(w)
		return nil, false
	}
	found, err := h.store.retentionPolicyByID(ctx, org, policyID)
	if err != nil {
		h.internalError(ctx, w, "get retention policy", err)
		return nil, false
	}
	if found == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Retention policy not found", nil)
		return nil, false
	}
	return found, true
}

func (s pgStore) retentionPolicyByID(ctx context.Context, org, id uuid.UUID) (*retentionPolicy, error) {
	return scanRetentionPolicy(s.Pool.QueryRow(ctx,
		`SELECT `+retentionColumns+` FROM org_retention_policies WHERE id = $1 AND org_id = $2`, id, org))
}

// getRetentionPolicy is retention.py's get_retention_policy.
func (h *handlers) getRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, nil)
	if !ok {
		return
	}
	found, ok := h.loadRetentionPolicy(ctx, w, r, orgID)
	if !ok {
		return
	}
	policy.WriteJSON(w, http.StatusOK, retentionPolicyObject(found), nil)
}

// updateRetentionPolicy is retention.py's update_retention_policy.
func (h *handlers) updateRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		days        *big.Int
		description *string
		isActive    *bool
	)
	if ok {
		if value, present := errs.OptionalMinInt(object, "retention_days", 1); present {
			days = value
		}
		if value, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &value
		}
		if value, present := errs.OptionalBool(object, "is_active"); present {
			isActive = &value
		}
	}
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, errs)
	if !ok {
		return
	}
	// update_policy's uuid.UUID(...) arguments sit inside the route's own
	// try/except ValueError: a malformed id is a 400 with the exception text.
	if detail, bad := malformedID(orgID, r.PathValue("policy_id")); bad {
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return
	}
	found, ok := h.loadRetentionPolicy(ctx, w, r, orgID)
	if !ok {
		return
	}
	if days != nil {
		if days.Cmp(big.NewInt(math.MaxInt32)) > 0 {
			h.internalError(ctx, w, "update retention policy", errors.New("retention_days out of range for integer"))
			return
		}
		found.RetentionDays = int32(days.Int64())
	}
	if description != nil {
		found.Description = description
	}
	if isActive != nil {
		found.IsActive = *isActive
	}
	// updated_at is always assigned a fresh now(), so the row is always
	// written.
	found.UpdatedAt = h.store.now().UTC()
	if _, err := h.store.Pool.Exec(ctx, `
UPDATE org_retention_policies
SET retention_days = $2, description = $3, is_active = $4, updated_at = $5
WHERE id = $1`, found.ID, found.RetentionDays, found.Description, found.IsActive, found.UpdatedAt); err != nil {
		h.internalError(ctx, w, "update retention policy", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, retentionPolicyObject(found), nil)
}

// deleteRetentionPolicy is retention.py's delete_retention_policy.
func (h *handlers) deleteRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, nil)
	if !ok {
		return
	}
	found, ok := h.loadRetentionPolicy(ctx, w, r, orgID)
	if !ok {
		return
	}
	if _, err := h.store.Pool.Exec(ctx, `DELETE FROM org_retention_policies WHERE id = $1`, found.ID); err != nil {
		h.internalError(ctx, w, "delete retention policy", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// executeRetentionPolicy is retention.py's execute_retention_policy and
// RetentionService.execute_policy: a dry run counts the org's audit_logs older
// than the policy's cutoff; a real run deletes them and stamps the policy.
func (h *handlers) executeRetentionPolicy(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	dryRun := true
	// The body is `RetentionExecuteRequest | None = None`: an empty body or a
	// JSON null is the default (a dry run).
	if !body.Missing {
		if object, ok := errs.Object(body); ok {
			if value, present := errs.DefaultedBool(object, "dry_run"); present {
				dryRun = value
			}
		}
	}
	orgID, ok := h.gatedOrg(ctx, w, retentionFeature, errs)
	if !ok {
		return
	}
	org, orgErr := pythonparity.ParseUUID(orgID)
	policyID, policyOK := pathUUID(r, "policy_id")
	if orgErr != nil || !policyOK {
		policy.WriteInternal(w)
		return
	}
	found, err := h.store.retentionPolicyByID(ctx, org, policyID)
	if err != nil {
		h.internalError(ctx, w, "get retention policy", err)
		return
	}
	count, message := int64(0), (*string)(nil)
	switch {
	case found == nil:
		text := "Policy not found"
		message = &text
	case !found.IsActive:
		text := "Policy is not active"
		message = &text
	case found.ResourceType != "audit_logs":
		text := "Cleanup not implemented for resource type: " + found.ResourceType
		message = &text
	default:
		now := h.store.now().UTC()
		// datetime.now() - timedelta(days=...) raises OverflowError before
		// the service's try block (an unhandled 500) when the days exceed
		// timedelta's range or take the date before year 1.
		cutoff := now.AddDate(0, 0, -int(found.RetentionDays))
		if found.RetentionDays > 999999999 || cutoff.Year() < 1 {
			policy.WriteInternal(w)
			return
		}
		count, err = h.store.applyRetention(ctx, found, cutoff, dryRun, now)
		if err != nil {
			h.internalError(ctx, w, "execute retention policy", err)
			return
		}
	}
	out := pyjson.NewObject()
	out.Set("deleted_count", count)
	out.Set("error", optionalString(message))
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// applyRetention counts (dry run) or deletes the org's audit_logs older than
// cutoff and, for a real run, stamps the policy in the same transaction.
func (s pgStore) applyRetention(ctx context.Context, p *retentionPolicy, cutoff time.Time, dryRun bool, now time.Time) (int64, error) {
	if dryRun {
		var count int64
		err := s.Pool.QueryRow(ctx,
			`SELECT count(*) FROM audit_logs WHERE org_id = $1 AND created_at < $2`, p.OrgID, cutoff).Scan(&count)
		return count, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `DELETE FROM audit_logs WHERE org_id = $1 AND created_at < $2`, p.OrgID, cutoff)
	if err != nil {
		return 0, err
	}
	count := tag.RowsAffected()
	if _, err := tx.Exec(ctx, `
UPDATE org_retention_policies
SET last_run_at = $2, last_run_deleted_count = $3, next_run_at = $4, updated_at = $2
WHERE id = $1`, p.ID, now, count, now.Add(24*time.Hour)); err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}
