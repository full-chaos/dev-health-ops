package billing

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const auditColumns = `id, org_id, actor_id, action, resource_type, resource_id, description, stripe_event_id,
	local_state::text, stripe_state::text, reconciliation_status, created_at`

// auditEntry is one billing_audit_log row.
type auditEntry struct {
	ID, OrgID, ResourceID              uuid.UUID
	ActorID                            *uuid.UUID
	Action, ResourceType, Description  string
	StripeEventID, ReconciliationState *string
	LocalState, StripeState            pyjson.Value
	CreatedAt                          *time.Time
}

func scanAudit(row pgx.Row) (auditEntry, error) {
	var entry auditEntry
	var local, stripe *string
	err := row.Scan(&entry.ID, &entry.OrgID, &entry.ActorID, &entry.Action, &entry.ResourceType, &entry.ResourceID,
		&entry.Description, &entry.StripeEventID, &local, &stripe, &entry.ReconciliationState, &entry.CreatedAt)
	if err != nil {
		return entry, err
	}
	if entry.LocalState, err = storedJSON(local); err != nil {
		return entry, err
	}
	entry.StripeState, err = storedJSON(stripe)
	return entry, err
}

// storedJSON decodes a JSON column: SQL NULL and a JSON null are both None.
func storedJSON(stored *string) (pyjson.Value, error) {
	if stored == nil {
		return nil, nil
	}
	return pyjson.DecodeString(*stored)
}

// optionalDict is a `dict[str, Any] | None` response field: None or an
// object; any other stored value fails the response model (the 500).
func optionalDict(value pyjson.Value) (pyjson.Value, error) {
	if value == nil {
		return nil, nil
	}
	if object, isObject := value.(*pyjson.Object); isObject {
		return object, nil
	}
	return nil, errUnexpected(value)
}

// auditJSON is BillingAuditLogResponse.model_validate(entry).
func auditJSON(entry auditEntry) (*pyjson.Object, error) {
	local, err := optionalDict(entry.LocalState)
	if err != nil {
		return nil, err
	}
	stripe, err := optionalDict(entry.StripeState)
	if err != nil {
		return nil, err
	}
	out := pyjson.NewObject()
	out.Set("id", entry.ID.String())
	out.Set("org_id", entry.OrgID.String())
	out.Set("actor_id", uuidOrNull(entry.ActorID))
	out.Set("action", entry.Action)
	out.Set("resource_type", entry.ResourceType)
	out.Set("resource_id", entry.ResourceID.String())
	out.Set("description", entry.Description)
	out.Set("stripe_event_id", nullableString(entry.StripeEventID))
	out.Set("local_state", local)
	out.Set("stripe_state", stripe)
	out.Set("reconciliation_status", nullableString(entry.ReconciliationState))
	out.Set("created_at", pydanticTime(entry.CreatedAt))
	return out, nil
}

// queryPlainInt is a bare `name: int = fallback` query parameter.
func queryPlainInt(errs *pybody.Errors, r *http.Request, name string, fallback int64) *big.Int {
	value, _ := errs.QueryInt(name, pybody.LastQueryValue(r.URL.Query(), name), fallback, nil, nil)
	return value
}

// listAudit is list_billing_audit. limit and offset are echoed as given;
// the query uses max(1, limit) and max(0, offset), with no upper bound.
func (h handlers) listAudit(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	query := r.URL.Query()
	var orgID uuid.UUID
	if raw := pybody.LastQueryValue(query, "org_id"); raw == nil {
		errs = append(errs, pybody.Error{Type: "missing", Loc: []pyjson.Value{"query", "org_id"}, Msg: "Field required"})
	} else if parsed, _ := errs.QueryUUID("org_id", raw); parsed != nil {
		orgID = *parsed
	}
	resourceType := pybody.LastQueryValue(query, "resource_type")
	resourceID, _ := errs.QueryUUID("resource_id", pybody.LastQueryValue(query, "resource_id"))
	action := pybody.LastQueryValue(query, "action")
	status := pybody.LastQueryValue(query, "reconciliation_status")
	from, _ := errs.QueryDatetime("from_date", pybody.LastQueryValue(query, "from_date"))
	to, _ := errs.QueryDatetime("to_date", pybody.LastQueryValue(query, "to_date"))
	limit := queryPlainInt(&errs, r, "limit", 50)
	offset := queryPlainInt(&errs, r, "offset", 0)
	if len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "list billing audit", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		queryLimit, queryOffset := new(big.Int).Set(limit), new(big.Int).Set(offset)
		if queryLimit.Cmp(one) < 0 {
			queryLimit.Set(one)
		}
		if queryOffset.Sign() < 0 {
			queryOffset.SetInt64(0)
		}
		if !queryLimit.IsInt64() || !queryOffset.IsInt64() {
			return reply{}, errOverflow
		}
		where, args := `WHERE org_id = $1`, []any{orgID}
		add := func(clause string, value any) {
			args = append(args, value)
			where += ` AND ` + clause + ` $` + itoa(len(args))
		}
		if resourceType != nil && *resourceType != "" {
			add("resource_type =", *resourceType)
		}
		if resourceID != nil {
			add("resource_id =", *resourceID)
		}
		if action != nil && *action != "" {
			add("action =", *action)
		}
		if status != nil && *status != "" {
			add("reconciliation_status =", *status)
		}
		if from != nil {
			add("created_at >=", *pybody.Instant(from))
		}
		if to != nil {
			add("created_at <=", *pybody.Instant(to))
		}
		rows, err := tx.Query(ctx, `SELECT `+auditColumns+` FROM billing_audit_log `+where+
			` ORDER BY created_at DESC LIMIT `+queryLimit.String()+` OFFSET `+queryOffset.String(), args...)
		if err != nil {
			return reply{}, err
		}
		var entries []auditEntry
		for rows.Next() {
			entry, err := scanAudit(rows)
			if err != nil {
				rows.Close()
				return reply{}, err
			}
			entries = append(entries, entry)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return reply{}, err
		}
		var total int64
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM billing_audit_log `+where, args...).Scan(&total); err != nil {
			return reply{}, err
		}
		items := make([]pyjson.Value, 0, len(entries))
		for _, entry := range entries {
			item, err := auditJSON(entry)
			if err != nil {
				return reply{}, err
			}
			items = append(items, item)
		}
		out := pyjson.NewObject()
		out.Set("items", items)
		out.Set("total", total)
		out.Set("limit", pyjson.Int{Int: limit})
		out.Set("offset", pyjson.Int{Int: offset})
		return ok(out), nil
	})
}

func loadAudit(ctx context.Context, q querier, id uuid.UUID) (*auditEntry, error) {
	entry, err := scanAudit(q.QueryRow(ctx, `SELECT `+auditColumns+` FROM billing_audit_log WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &entry, nil
}

var auditNotFound = detail(http.StatusNotFound, "Audit entry not found")

// getAudit is get_billing_audit.
func (h handlers) getAudit(w http.ResponseWriter, r *http.Request) {
	var errs pybody.Errors
	id, valid := pathUUID(&errs, "audit_id", r.PathValue("audit_id"))
	if !valid {
		h.write(w, validation(errs))
		return
	}
	if !policy.UserFrom(r.Context()).IsSuperuser {
		h.write(w, superadminRequired)
		return
	}
	h.serve(w, r, "get billing audit", func(tx pgx.Tx) (reply, error) {
		entry, err := loadAudit(r.Context(), tx, id)
		if err != nil {
			return reply{}, err
		}
		if entry == nil {
			return auditNotFound, nil
		}
		body, err := auditJSON(*entry)
		if err != nil {
			return reply{}, err
		}
		return ok(body), nil
	})
}

func parseResolve(m pybody.Model) (string, bool) {
	resolution := pybody.Get(m, "resolution", pybody.Required, pybody.Str)
	return resolution.Value, resolution.OK
}

// auditRecord is one BillingAuditService.log call. A nil state is written
// as a JSON null, as SQLAlchemy writes an explicit None to a JSON column.
type auditRecord struct {
	OrgID, ResourceID                 uuid.UUID
	ActorID                           *uuid.UUID
	Action, ResourceType, Description string
	Status                            string
	LocalState, StripeState           *pyjson.Object
}

// writeAudit is BillingAuditService.log: one savepoint, a failure logged
// and swallowed (nil entry).
func (h handlers) writeAudit(ctx context.Context, tx pgx.Tx, record auditRecord) *auditEntry {
	entry := auditEntry{
		ID: uuid.New(), OrgID: record.OrgID, ActorID: record.ActorID, Action: record.Action,
		ResourceType: record.ResourceType, ResourceID: record.ResourceID, Description: record.Description,
		ReconciliationState: &record.Status,
	}
	now := h.nowUTC()
	entry.CreatedAt = &now
	states := [2]string{"null", "null"}
	for index, state := range []*pyjson.Object{record.LocalState, record.StripeState} {
		if state == nil {
			continue
		}
		text, err := pyjson.Dumps(state)
		if err != nil {
			h.logger.ErrorContext(ctx, "billing: failed to write audit log", "error", err.Error())
			return nil
		}
		states[index] = text
	}
	if record.LocalState != nil {
		entry.LocalState = record.LocalState
	}
	if record.StripeState != nil {
		entry.StripeState = record.StripeState
	}
	err := savepoint(ctx, tx, func(sp pgx.Tx) error {
		_, err := sp.Exec(ctx, `INSERT INTO billing_audit_log
			(id, org_id, actor_id, action, resource_type, resource_id, description, local_state, stripe_state,
			reconciliation_status, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10, $11)`,
			entry.ID, entry.OrgID, entry.ActorID, entry.Action, entry.ResourceType, entry.ResourceID,
			entry.Description, states[0], states[1], record.Status, now)
		return err
	})
	if err != nil {
		h.logger.ErrorContext(ctx, "billing: failed to write audit log", "error", err.Error())
		return nil
	}
	return &entry
}

// resolveAudit is resolve_billing_mismatch: it writes a NEW
// "reconciliation.completed" entry for the audited resource and answers
// with it; the audited entry is left as it was.
func (h handlers) resolveAudit(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var errs pybody.Errors
	id, _ := pathUUID(&errs, "audit_id", r.PathValue("audit_id"))
	resolution, valid := parseBody(&errs, body, parseResolve)
	if !valid || len(errs) > 0 {
		h.write(w, validation(errs))
		return
	}
	user := policy.UserFrom(r.Context())
	if !user.IsSuperuser {
		h.write(w, superadminRequired)
		return
	}
	if _, err := h.stripe.Client(); err != nil {
		h.internal(w, r, "resolve billing mismatch", err)
		return
	}
	actor, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		h.internal(w, r, "resolve billing mismatch", err)
		return
	}
	h.serve(w, r, "resolve billing mismatch", func(tx pgx.Tx) (reply, error) {
		ctx := r.Context()
		row, err := loadAudit(ctx, tx, id)
		if err != nil {
			return reply{}, err
		}
		if row == nil {
			return auditNotFound, nil
		}
		entry := h.writeAudit(ctx, tx, auditRecord{
			OrgID: row.OrgID, ResourceID: row.ResourceID, ActorID: &actor, Action: "reconciliation.completed",
			ResourceType: row.ResourceType, Description: "Mismatch resolved: " + resolution, Status: "matched",
			LocalState: ensureDict(row.LocalState), StripeState: ensureDict(row.StripeState),
		})
		if entry == nil {
			return auditNotFound, nil
		}
		out, err := auditJSON(*entry)
		if err != nil {
			return reply{}, err
		}
		return ok(out), nil
	})
}
