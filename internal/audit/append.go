package audit

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var appendOnlyEventTypes = map[string]bool{}

// EventLoginFailed is a refused sign-in.
const EventLoginFailed = "auth.login.failed"

// ErrNotAppendOnly reports an Append of an event type that is not append-only.
var ErrNotAppendOnly = fmt.Errorf("%w: the audit event type is not append-only", ErrMutationFailed)

// Append is not built yet.
func Append(context.Context, *pgxpool.Pool, authschema.ValidatedIdentifier, AuditEvent) error {
	return nil
}
