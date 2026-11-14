package identity

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// A5 (acp): a real is_active change is exactly one auth audit row and its named outbox events, no more: a reactivate
// is one auth.principal.enabled row and one principal.enabled event; a deactivate one auth.principal.disabled row and
// the principal.disabled + user.tokens_invalidated pair. A4: an equal is_active writes no principal audit row and no
// outbox event (the PATCH is the public no-change touch).
func TestAdminUpdateActiveWritesExactlyOneAuditRowAndItsEvents(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	yes, no := true, false
	for _, c := range []struct {
		name       string
		cur        AdminUpdateCurrent
		active     *bool
		wantAudit  []string
		wantEvents []string
	}{
		{"reactivate", AdminUpdateCurrent{Email: "a@example.test", Active: false}, &yes,
			[]string{"auth.principal.enabled"}, []string{"principal.enabled"}},
		{"deactivate", AdminUpdateCurrent{Email: "a@example.test", Active: true}, &no,
			[]string{"auth.principal.disabled"}, []string{"principal.disabled", "user.tokens_invalidated"}},
		{"no-op true", AdminUpdateCurrent{Email: "a@example.test", Active: true}, &yes,
			[]string{audit.EventUserTouched}, nil},
		{"no-op false", AdminUpdateCurrent{Email: "a@example.test", Active: false}, &no,
			[]string{audit.EventUserTouched}, nil},
	} {
		var auditRows, events []string
		oldMutation, oldProjection := commitMutation, commitPublicProjection
		commitMutation = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, m audit.Mutation) error {
			auditRows = append(auditRows, m.Audit.EventType)
			for _, also := range m.Also {
				auditRows = append(auditRows, also.EventType)
			}
			for _, e := range m.Events {
				events = append(events, e.EventType)
			}
			return nil
		}
		commitPublicProjection = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, event audit.AuditEvent,
			_ func(context.Context, audit.TxOps) (int64, error)) error {
			auditRows = append(auditRows, event.EventType)
			return nil
		}
		req := AdminUpdateRequest{Principal: uuid.New(), Actor: uuid.New(), Authorize: none, Active: c.active}
		err := store.AdminUpdateUser(context.Background(), req, c.cur)
		commitMutation, commitPublicProjection = oldMutation, oldProjection
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !reflect.DeepEqual(auditRows, c.wantAudit) {
			t.Errorf("%s: audit rows %v, want exactly %v", c.name, auditRows, c.wantAudit)
		}
		if !reflect.DeepEqual(events, c.wantEvents) {
			t.Errorf("%s: outbox events %v, want exactly %v", c.name, events, c.wantEvents)
		}
	}
}
