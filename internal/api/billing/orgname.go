package billing

import (
	"context"
	"errors"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// savepointer is a transaction that can open a nested one (a savepoint).
type savepointer interface {
	Begin(context.Context) (pgx.Tx, error)
}

// withOrgName sets org_name on a billing payload that carries org_id: the
// name of that same organisation, read by its id, so another organisation's
// name is never served. org_name is null when the organisation has no
// readable name; the id is never a fallback. The read runs under a
// savepoint: a failed read is logged and leaves org_name null, and the
// payload (and the surrounding transaction) are kept. Go-only additive key.
func (h handlers) withOrgName(ctx context.Context, q querier, payload *pyjson.Object) {
	payload.Set("org_name", h.orgName(ctx, q, payload))
}

func (h handlers) orgName(ctx context.Context, q querier, payload *pyjson.Object) pyjson.Value {
	raw, _ := payload.Get("org_id")
	text, isText := raw.(string)
	if !isText {
		return nil
	}
	org, err := uuid.Parse(text)
	if err != nil {
		return nil
	}
	var name *string
	read := func(rq querier) error {
		err := rq.QueryRow(ctx, `SELECT name FROM organizations WHERE id = $1`, org).Scan(&name)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	}
	if sp, canNest := q.(savepointer); canNest {
		nested, err := sp.Begin(ctx)
		if err != nil {
			h.logger.WarnContext(ctx, "billing: org name lookup skipped", "org_id", text, "error", err.Error())
			return nil
		}
		if err := read(nested); err != nil {
			_ = nested.Rollback(ctx)
			h.logger.WarnContext(ctx, "billing: org name lookup failed; org_name is null", "org_id", text, "error", err.Error())
			return nil
		}
		if err := nested.Commit(ctx); err != nil {
			h.logger.WarnContext(ctx, "billing: org name lookup release failed; org_name is null", "org_id", text, "error", err.Error())
			return nil
		}
	} else if err := read(q); err != nil {
		h.logger.WarnContext(ctx, "billing: org name lookup failed; org_name is null", "org_id", text, "error", err.Error())
		return nil
	}
	if name == nil || strings.TrimSpace(*name) == "" {
		return nil
	}
	return *name
}
