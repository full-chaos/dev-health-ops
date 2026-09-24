package admin

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const ipAllowlistFeature = "ip_allowlist"

func (h *handlers) ipAllowlistRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: governancePrefix + "/ip-allowlist", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listIPAllowlist))},
		{Method: http.MethodPost, Pattern: governancePrefix + "/ip-allowlist", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.createIPAllowlistEntry))},
		{Method: http.MethodGet, Pattern: governancePrefix + "/ip-allowlist/{entry_id}", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.getIPAllowlistEntry))},
		{Method: http.MethodPatch, Pattern: governancePrefix + "/ip-allowlist/{entry_id}", Handler: h.bodyFirst(policy.Admin, http.HandlerFunc(h.updateIPAllowlistEntry))},
		{Method: http.MethodDelete, Pattern: governancePrefix + "/ip-allowlist/{entry_id}", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.deleteIPAllowlistEntry))},
		// POST /ip-allowlist/check shares its path shape with the
		// /{entry_id} routes, and the route table registers a method-less
		// fallback per path that net/http's mux refuses beside a
		// method-specific wildcard. So the check route is mounted on the
		// wildcard itself and told apart by the segment; any other POST on
		// it is FastAPI's 405 (whose Allow names the first route that matched the
		// path, the GET one).
		{Method: http.MethodPost, Pattern: governancePrefix + "/ip-allowlist/{entry_id}", Handler: h.checkOrMethodNotAllowed()},
	}
}

type ipEntry struct {
	ID, OrgID   uuid.UUID
	IPRange     string
	Description *string
	IsActive    bool
	CreatedByID *uuid.UUID
	CreatedAt   time.Time
	UpdatedAt   time.Time
	// Expires is the response's value: the request's own datetime after a
	// write (naive or offset), the stored instant after a read.
	Expires *pytime.DateTime
}

const ipEntryColumns = `id, org_id, ip_range, description, is_active, created_by_id, created_at, updated_at, expires_at`

func scanIPEntry(row pgx.Row) (*ipEntry, error) {
	var (
		entry     ipEntry
		expiresAt *time.Time
	)
	err := row.Scan(&entry.ID, &entry.OrgID, &entry.IPRange, &entry.Description, &entry.IsActive,
		&entry.CreatedByID, &entry.CreatedAt, &entry.UpdatedAt, &expiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if expiresAt != nil {
		value := pytime.UTC(*expiresAt)
		entry.Expires = &value
	}
	return &entry, nil
}

func ipEntryObject(entry *ipEntry) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", entry.ID.String())
	out.Set("org_id", entry.OrgID.String())
	out.Set("ip_range", entry.IPRange)
	out.Set("description", optionalString(entry.Description))
	out.Set("is_active", entry.IsActive)
	out.Set("created_by_id", uuidOrNil(entry.CreatedByID))
	out.Set("created_at", pyTimeString(entry.CreatedAt))
	out.Set("updated_at", pyTimeString(entry.UpdatedAt))
	if entry.Expires == nil {
		out.Set("expires_at", nil)
	} else {
		out.Set("expires_at", pytime.Pydantic(*entry.Expires))
	}
	return out
}

func instantOf(value *pytime.DateTime) *time.Time {
	if value == nil {
		return nil
	}
	at := value.Time.UTC()
	return &at
}

// gatedOrg is the preamble every ip-allowlist route shares: the caller's org
// claim (403 without one), then the query/body errors already collected,
// then the ip_allowlist license gate.
func (h *handlers) gatedOrg(ctx context.Context, w http.ResponseWriter, feature string, errs pybody.Errors) (string, bool) {
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return "", false
	}
	if len(errs) > 0 {
		writeValidation(w, errs)
		return "", false
	}
	if !h.requireFeature(ctx, w, feature, orgID) {
		return "", false
	}
	return orgID, true
}

// listIPAllowlist is ip_allowlist.py's list_ip_allowlist_entries.
func (h *handlers) listIPAllowlist(w http.ResponseWriter, r *http.Request) {
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
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, errs)
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
	if err := h.store.Pool.QueryRow(ctx, `SELECT count(*) FROM org_ip_allowlist`+where, org).Scan(&total); err != nil {
		h.internalError(ctx, w, "count ip allowlist", err)
		return
	}
	rows, err := h.store.Pool.Query(ctx, `SELECT `+ipEntryColumns+` FROM org_ip_allowlist`+where+
		` ORDER BY created_at LIMIT $2 OFFSET $3`, org, limit, offset)
	if err != nil {
		h.internalError(ctx, w, "list ip allowlist", err)
		return
	}
	defer rows.Close()
	items := []pyjson.Value{}
	for rows.Next() {
		entry, err := scanIPEntry(rows)
		if err != nil {
			h.internalError(ctx, w, "scan ip allowlist entry", err)
			return
		}
		items = append(items, ipEntryObject(entry))
	}
	if err := rows.Err(); err != nil {
		h.internalError(ctx, w, "list ip allowlist", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", total)
	out.Set("limit", limit)
	out.Set("offset", offset)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// createIPAllowlistEntry is ip_allowlist.py's create_ip_allowlist_entry.
func (h *handlers) createIPAllowlistEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		ipRange     string
		description *string
		expires     *pytime.DateTime
	)
	if ok {
		ipRange, _ = errs.RequiredString(object, "ip_range", 0, 0)
		if value, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &value
		}
		expires = optionalDatetimeField(&errs, object, "expires_at")
	}
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, errs)
	if !ok {
		return
	}
	// The uuid.UUID(...) calls sit inside the route's own try/except
	// ValueError, so a malformed org claim or X-User-Id is a 422 carrying
	// the exception text; a well-formed X-User-Id naming no user then fails
	// the foreign key on flush (a 500).
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		detail, _ := pythonparity.UUIDValidationDetail(orgID)
		policy.WriteDetail(w, http.StatusUnprocessableEntity, detail, nil)
		return
	}
	var createdBy *uuid.UUID
	if header := firstHeader(r, "X-User-Id"); header != "" {
		id, err := pythonparity.ParseUUID(header)
		if err != nil {
			detail, _ := pythonparity.UUIDValidationDetail(header)
			policy.WriteDetail(w, http.StatusUnprocessableEntity, detail, nil)
			return
		}
		createdBy = &id
	}
	if !isValidIPOrCIDR(ipRange) {
		policy.WriteDetail(w, http.StatusUnprocessableEntity, "Invalid IP address or CIDR range: "+ipRange, nil)
		return
	}
	now := h.store.now().UTC()
	entry := &ipEntry{
		ID: uuid.New(), OrgID: org, IPRange: ipRange, Description: description, IsActive: true,
		CreatedByID: createdBy, CreatedAt: now, UpdatedAt: now, Expires: expires,
	}
	if _, err := h.store.Pool.Exec(ctx, `
INSERT INTO org_ip_allowlist
	(id, org_id, ip_range, description, is_active, created_by_id, created_at, updated_at, expires_at)
VALUES ($1, $2, $3, $4, true, $5, $6, $6, $7)`,
		entry.ID, org, ipRange, description, createdBy, now, instantOf(expires)); err != nil {
		h.internalError(ctx, w, "insert ip allowlist entry", err)
		return
	}
	policy.WriteJSON(w, http.StatusCreated, ipEntryObject(entry), nil)
}

// firstHeader is Starlette's Headers.get: the first value, decoded latin-1.
func firstHeader(r *http.Request, name string) string {
	values := r.Header.Values(name)
	if len(values) == 0 {
		return ""
	}
	return policy.Latin1(values[0])
}

// getIPAllowlistEntry is ip_allowlist.py's get_ip_allowlist_entry.
func (h *handlers) getIPAllowlistEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, nil)
	if !ok {
		return
	}
	entry, ok := h.loadIPEntry(ctx, w, r, orgID)
	if !ok {
		return
	}
	policy.WriteJSON(w, http.StatusOK, ipEntryObject(entry), nil)
}

// loadIPEntry is IPAllowlistService.get_entry for the route's entry_id; it
// answers the 500 or 404 itself and returns false.
func (h *handlers) loadIPEntry(ctx context.Context, w http.ResponseWriter, r *http.Request, orgID string) (*ipEntry, bool) {
	org, orgErr := pythonparity.ParseUUID(orgID)
	entryID, entryOK := pathUUID(r, "entry_id")
	if orgErr != nil || !entryOK {
		policy.WriteInternal(w)
		return nil, false
	}
	entry, err := scanIPEntry(h.store.Pool.QueryRow(ctx,
		`SELECT `+ipEntryColumns+` FROM org_ip_allowlist WHERE id = $1 AND org_id = $2`, entryID, org))
	if err != nil {
		h.internalError(ctx, w, "get ip allowlist entry", err)
		return nil, false
	}
	if entry == nil {
		policy.WriteDetail(w, http.StatusNotFound, "IP allowlist entry not found", nil)
		return nil, false
	}
	return entry, true
}

// updateIPAllowlistEntry is ip_allowlist.py's update_ip_allowlist_entry.
func (h *handlers) updateIPAllowlistEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var (
		ipRange, description *string
		isActive             *bool
		expires              *pytime.DateTime
	)
	if ok {
		if value, present := errs.OptionalString(object, "ip_range", 0, 0); present {
			ipRange = &value
		}
		if value, present := errs.OptionalString(object, "description", 0, 0); present {
			description = &value
		}
		if value, present := errs.OptionalBool(object, "is_active"); present {
			isActive = &value
		}
		expires = optionalDatetimeField(&errs, object, "expires_at")
	}
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, errs)
	if !ok {
		return
	}
	// update_entry's uuid.UUID(...) arguments sit inside the route's own
	// try/except ValueError: a malformed id is a 400 carrying the exception
	// text, not the 500 the read routes give.
	if detail, bad := malformedID(orgID, r.PathValue("entry_id")); bad {
		policy.WriteDetail(w, http.StatusBadRequest, detail, nil)
		return
	}
	entry, ok := h.loadIPEntry(ctx, w, r, orgID)
	if !ok {
		return
	}
	if ipRange != nil {
		if !isValidIPOrCIDR(*ipRange) {
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid IP address or CIDR range: "+*ipRange, nil)
			return
		}
		entry.IPRange = *ipRange
	}
	if description != nil {
		entry.Description = description
	}
	if isActive != nil {
		entry.IsActive = *isActive
	}
	if expires != nil {
		entry.Expires = expires
	}
	// updated_at is always assigned a fresh now(), so the row is always
	// written.
	entry.UpdatedAt = h.store.now().UTC()
	if _, err := h.store.Pool.Exec(ctx, `
UPDATE org_ip_allowlist
SET ip_range = $2, description = $3, is_active = $4, expires_at = $5, updated_at = $6
WHERE id = $1`, entry.ID, entry.IPRange, entry.Description, entry.IsActive, instantOf(entry.Expires), entry.UpdatedAt); err != nil {
		h.internalError(ctx, w, "update ip allowlist entry", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, ipEntryObject(entry), nil)
}

// deleteIPAllowlistEntry is ip_allowlist.py's delete_ip_allowlist_entry.
func (h *handlers) deleteIPAllowlistEntry(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, nil)
	if !ok {
		return
	}
	entry, ok := h.loadIPEntry(ctx, w, r, orgID)
	if !ok {
		return
	}
	if _, err := h.store.Pool.Exec(ctx, `DELETE FROM org_ip_allowlist WHERE id = $1`, entry.ID); err != nil {
		h.internalError(ctx, w, "delete ip allowlist entry", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("deleted", true)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// checkIPAllowed is ip_allowlist.py's check_ip_allowed.
func (h *handlers) checkIPAllowed(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	body := bodyFromContext(ctx)
	var errs pybody.Errors
	object, ok := errs.Object(body)
	var ipAddress string
	if ok {
		ipAddress, _ = errs.RequiredString(object, "ip_address", 0, 0)
	}
	orgID, ok := h.gatedOrg(ctx, w, ipAllowlistFeature, errs)
	if !ok {
		return
	}
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		policy.WriteInternal(w)
		return
	}
	rows, err := h.store.Pool.Query(ctx, `SELECT ip_range, expires_at FROM org_ip_allowlist
WHERE org_id = $1 AND is_active = true ORDER BY created_at LIMIT 1000`, org)
	if err != nil {
		h.internalError(ctx, w, "list active ip allowlist", err)
		return
	}
	defer rows.Close()
	now := h.store.now().UTC()
	entries, allowed := 0, false
	for rows.Next() {
		var (
			ipRange   string
			expiresAt *time.Time
		)
		if err := rows.Scan(&ipRange, &expiresAt); err != nil {
			h.internalError(ctx, w, "scan ip allowlist entry", err)
			return
		}
		entries++
		if allowed || (expiresAt != nil && expiresAt.Before(now)) {
			continue
		}
		allowed = ipRangeMatches(ipRange, ipAddress)
	}
	if err := rows.Err(); err != nil {
		h.internalError(ctx, w, "list active ip allowlist", err)
		return
	}
	if entries == 0 {
		allowed = true
	}
	out := pyjson.NewObject()
	out.Set("allowed", allowed)
	out.Set("ip_address", ipAddress)
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// checkOrMethodNotAllowed answers POST /ip-allowlist/check with the guarded
// check handler and every other POST on /ip-allowlist/{entry_id} with the 405
// FastAPI gives a path that has no POST route.
func (h *handlers) checkOrMethodNotAllowed() http.Handler {
	check := h.bodyFirst(policy.Admin, http.HandlerFunc(h.checkIPAllowed))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("entry_id") == "check" {
			check.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Allow", "GET")
		policy.WriteDetail(w, http.StatusMethodNotAllowed, "Method Not Allowed", nil)
	})
}

// malformedID is the exception text of the first of ids that
// uuid.UUID(...) refuses, in argument order.
func malformedID(ids ...string) (string, bool) {
	for _, id := range ids {
		if detail, bad := pythonparity.UUIDValidationDetail(id); bad {
			return detail, true
		}
	}
	return "", false
}
