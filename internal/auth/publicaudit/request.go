package publicaudit

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/clientaddr"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// Querier is the one read the organization and time helpers make.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RequestMetadata is go-api's request_metadata of the request bound to ctx (internal/api/audit.RequestMetadata):
// ip_address (the client address auth-service resolved behind its trusted proxies), user_agent, then request_id,
// each only when non-empty, in that order. request_id is the correlation id the go-api proxy forwards in
// X-Request-Id (Middleware), else auth-service's own request id (requestid.Audit). nil when none is bound.
func RequestMetadata(ctx context.Context) ([]byte, error) {
	metadata := pyjson.NewObject()
	if ip := clientaddr.Text(ctx); ip != nil && *ip != "" {
		metadata.Set("ip_address", *ip)
	}
	if agent, ok := clientaddr.UserAgent(ctx); ok && agent != "" {
		metadata.Set("user_agent", agent)
	}
	if id := requestID(ctx); id != "" {
		metadata.Set("request_id", id)
	}
	if metadata.Len() == 0 {
		return nil, nil
	}
	text, err := pyjson.Dumps(metadata)
	if err != nil {
		return nil, err
	}
	return []byte(text), nil
}

// ForwardedRequestIDHeader is the correlation id the go-api proxy forwards (go-api's own X-Request-ID: the
// client's when it sent one, else go-api's).
const ForwardedRequestIDHeader = "X-Request-Id"

type forwardedKey struct{}

// WithForwardedRequestID binds the forwarded correlation id to ctx; "" binds nothing.
func WithForwardedRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, forwardedKey{}, id)
}

// ForwardedRequestID is the forwarded correlation id bound to ctx, or "".
func ForwardedRequestID(ctx context.Context) string {
	id, _ := ctx.Value(forwardedKey{}).(string)
	return id
}

// Middleware binds the request's X-Request-Id for the request_metadata of go-api's row, after go-api's own rule
// (internal/apiservice: any non-empty value). It is used for that row only: auth-service's own request id, the one
// its responses and auth.* rows carry, is still minted by requestid.Middleware.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if id := r.Header.Get(ForwardedRequestIDHeader); id != "" {
			r = r.WithContext(WithForwardedRequestID(r.Context(), id))
		}
		next.ServeHTTP(w, r)
	})
}

func requestID(ctx context.Context) string {
	if forwarded := ForwardedRequestID(ctx); forwarded != "" {
		return forwarded
	}
	if own := requestid.Audit(ctx); own != nil {
		return *own
	}
	return ""
}

// Changes encodes a changes object (Python spacing and key order).
func Changes(object *pyjson.Object) ([]byte, error) {
	text, err := pyjson.Dumps(object)
	if err != nil {
		return nil, err
	}
	return []byte(text), nil
}

// TxTime is the transaction's time (now()): the occurred_at the auth event of the same transaction gets by default
// (auth migration 0005), so the public row and the auth row carry one instant.
func TxTime(ctx context.Context, q Querier) (time.Time, error) {
	var at time.Time
	err := q.QueryRow(ctx, `SELECT now()`).Scan(&at)
	return at, err
}

// FirstOrganization is the organization of a user's first public membership in the order the login ranks
// memberships without organization activity (authapi chooseMembership over PGUsers.Memberships): the earliest join,
// joined_at falling back to created_at, ties in go-api's ORDER BY joined_at, created_at, then the organization id
// (the runtime role reads no membership id: authschema.PublicReadColumns). nil when the user
// has no membership: go-api writes no row then (the row's org_id is NOT NULL).
func FirstOrganization(ctx context.Context, q Querier, userID uuid.UUID) (*uuid.UUID, error) {
	var org uuid.UUID
	err := q.QueryRow(ctx, `SELECT org_id FROM public.memberships WHERE user_id = $1
ORDER BY COALESCE(joined_at, created_at), joined_at NULLS LAST, created_at NULLS LAST, org_id LIMIT 1`, userID).Scan(&org)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &org, nil
}

// OrganizationOr is preferred when it is set, else FirstOrganization.
func OrganizationOr(ctx context.Context, q Querier, preferred *uuid.UUID, userID uuid.UUID) (*uuid.UUID, error) {
	if preferred != nil {
		return preferred, nil
	}
	return FirstOrganization(ctx, q, userID)
}

// UserRow writes the row of a user event in org at the transaction's time with the request's metadata: resource
// user/<user>, changes as given, status success. It is the row of go-api's auditUserEvent and register.
func UserRow(ctx context.Context, tx interface {
	Execer
	Querier
}, org, user uuid.UUID, action, description string, changes []byte) error {
	return Write(ctx, tx, Row{OrgID: org, UserID: &user, Action: action, ResourceType: "user", ResourceID: user.String(),
		Description: &description, Changes: changes})
}

// Write fills the request metadata and the transaction time of row and inserts it.
func Write(ctx context.Context, tx interface {
	Execer
	Querier
}, row Row) error {
	metadata, err := RequestMetadata(ctx)
	if err != nil {
		return err
	}
	at, err := TxTime(ctx, tx)
	if err != nil {
		return err
	}
	row.RequestMetadata, row.CreatedAt = metadata, at
	return InsertInAuditTx(ctx, tx, row)
}
