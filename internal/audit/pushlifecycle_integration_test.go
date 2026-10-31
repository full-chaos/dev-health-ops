//go:build integration

package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
)

const (
	pushOrg    = "org_EXAMPLE0000000000000001"
	pushSource = "src_EXAMPLE0000000000000001"
)

func pushCreated(id string, scopes ...string) PushLifecycleEvent {
	return PushLifecycleEvent{EventType: externalpush.Created, AggregateID: id, OrganizationID: pushOrg, SourceID: pushSource,
		Change: externalpush.Change{Status: "active", Fingerprint: "0123456789abcdef", Scopes: scopes}}
}

func pushRevoked(id string) PushLifecycleEvent {
	snapshot := externalpush.Change{Status: "active", Fingerprint: "0123456789abcdef", Scopes: []string{"ingest:write"}}
	return PushLifecycleEvent{EventType: externalpush.Revoked, AggregateID: id, OrganizationID: pushOrg, SourceID: pushSource,
		Change: externalpush.Change{Status: "revoked"}, Snapshot: &snapshot}
}

type pushRow struct {
	eventType, key string
	revision       int64
	payload        []byte
	createdAt      time.Time
}

func pushRows(t *testing.T, ctx context.Context, env *auditFixture, aggregateID string) []pushRow {
	t.Helper()
	rows, err := env.pool.Query(ctx, `SELECT event_type, idempotency_key, (payload->>'revision')::bigint, payload, created_at
		FROM auth.auth_outbox_events WHERE aggregate_type = 'external_push_credential' AND aggregate_id = $1 ORDER BY id`, aggregateID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []pushRow
	for rows.Next() {
		var r pushRow
		if err := rows.Scan(&r.eventType, &r.key, &r.revision, &r.payload, &r.createdAt); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

// envelope is the wire event the outbox row stands for: the row is the payload
// and the aggregate, and its created_at is the database clock.
func (r pushRow) envelope(aggregateID string) []byte {
	raw, _ := json.Marshal(map[string]any{
		"schema_version": contracts.ExternalPushLifecycleEventSurface, "aggregate_type": contracts.ExternalPushCredentialAggregate,
		"aggregate_id": aggregateID, "event_type": r.eventType, "idempotency_key": r.key,
		"created_at": r.createdAt.UTC().Format(time.RFC3339Nano), "payload": json.RawMessage(r.payload),
	})
	return raw
}

func withPushTx(t *testing.T, ctx context.Context, env *auditFixture, commit bool, fn func(pgx.Tx) error) error {
	t.Helper()
	tx, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if !commit {
		return tx.Rollback(ctx)
	}
	return tx.Commit(ctx)
}

func TestAppendPushLifecycleWritesParseableEventsInTheCallersTransaction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	root := repoRoot(t)

	if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
		return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushCreated("00000000-0000-4000-8000-000000000001", "ingest:write")})
	}); err != nil {
		t.Fatal(err)
	}
	rows := pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000001")
	if len(rows) != 1 || rows[0].key != "external_push_credential:00000000-0000-4000-8000-000000000001:1" || rows[0].revision != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	event, err := contracts.ParseExternalPushLifecycleEvent(root, rows[0].envelope("00000000-0000-4000-8000-000000000001"))
	if err != nil {
		t.Fatalf("the outbox row is not a valid wire event: %v\n%s", err, rows[0].envelope("00000000-0000-4000-8000-000000000001"))
	}
	if event.Payload.Change.Fingerprint != "0123456789abcdef" {
		t.Fatalf("event = %+v", event)
	}
	if _, outcome := pushlifecycle.Apply(pushlifecycle.State{}, event, nil); outcome != pushlifecycle.Applied {
		t.Fatalf("the shared projection core refuses the first event: %s", outcome)
	}

	t.Run("a rollback drops the event and the revision", func(t *testing.T) {
		if err := withPushTx(t, ctx, env, false, func(tx pgx.Tx) error {
			return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushCreated("00000000-0000-4000-8000-000000000002", "ingest:write")})
		}); err != nil {
			t.Fatal(err)
		}
		var revisions int
		if err := env.pool.QueryRow(ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_id = '00000000-0000-4000-8000-000000000002'`).Scan(&revisions); err != nil {
			t.Fatal(err)
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000002")); got != 0 || revisions != 0 {
			t.Fatalf("after rollback: %d outbox rows, %d revision rows, want 0 and 0", got, revisions)
		}
	})

	t.Run("the guard is disarmed on return", func(t *testing.T) {
		err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			if err := AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushCreated("00000000-0000-4000-8000-000000000003", "ingest:write")}); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `INSERT INTO auth.auth_outbox_events (aggregate_type, aggregate_id, event_type, payload, idempotency_key)
				VALUES ('organization', 'org_x', 'organization.created', '{}', 'organization:org_x:1')`)
			return err
		})
		if err == nil {
			t.Fatal("a guarded write after AppendPushLifecycle was accepted")
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000003")); got != 0 {
			t.Fatalf("the refused transaction left %d rows", got)
		}
	})

	t.Run("a refused event writes nothing", func(t *testing.T) {
		bad := pushCreated("00000000-0000-4000-8000-000000000004", "ingest:write")
		bad.Change.Fingerprint = "NOT-A-FINGERPRINT"
		err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushCreated("00000000-0000-4000-8000-000000000005", "ingest:write"), bad})
		})
		if !errors.Is(err, ErrPushLifecycle) {
			t.Fatalf("err = %v, want ErrPushLifecycle", err)
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000005")); got != 0 {
			t.Fatalf("the good event of a refused call was written (%d rows)", got)
		}
	})

	t.Run("a created event needs a credential with no earlier event", func(t *testing.T) {
		err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushCreated("00000000-0000-4000-8000-000000000001", "ingest:write")})
		})
		if !errors.Is(err, ErrPushLifecycle) {
			t.Fatalf("a second created for cred-1: err = %v", err)
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000001")); got != 1 {
			t.Fatalf("cred-1 has %d rows, want 1", got)
		}
	})
}

func TestAppendPushLifecycleLegacyCredentialGetsCreatedThenTheEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)
	root := repoRoot(t)

	if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
		return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushRevoked("00000000-0000-4000-8000-000000000006")})
	}); err != nil {
		t.Fatal(err)
	}
	rows := pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000006")
	if len(rows) != 2 || rows[0].eventType != externalpush.Created || rows[1].eventType != externalpush.Revoked ||
		rows[0].revision != 1 || rows[1].revision != 2 {
		t.Fatalf("rows = %+v", rows)
	}
	state := pushlifecycle.State{}
	for _, r := range rows {
		event, err := contracts.ParseExternalPushLifecycleEvent(root, r.envelope("00000000-0000-4000-8000-000000000006"))
		if err != nil {
			t.Fatalf("%s: %v", r.eventType, err)
		}
		var outcome pushlifecycle.Outcome
		if state, outcome = pushlifecycle.Apply(state, event, nil); outcome != pushlifecycle.Applied {
			t.Fatalf("%s: %s", r.eventType, outcome)
		}
	}
	if state.Status != pushlifecycle.StatusRevoked || state.Revision != 2 {
		t.Fatalf("state = %+v", state)
	}
	if !rows[0].createdAt.Equal(rows[1].createdAt) {
		t.Fatalf("created_at differs inside one transaction: %v vs %v", rows[0].createdAt, rows[1].createdAt)
	}

	t.Run("two writers of one new credential emit created once", func(t *testing.T) {
		var wg sync.WaitGroup
		errs := make([]error, 2)
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
					return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{pushRevoked("00000000-0000-4000-8000-000000000007")})
				})
			}()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		got := pushRows(t, ctx, env, "00000000-0000-4000-8000-000000000007")
		if len(got) != 3 || got[0].eventType != externalpush.Created {
			t.Fatalf("rows = %+v, want created then two revoked", got)
		}
		for i, r := range got {
			if r.revision != int64(i+1) || r.key != fmt.Sprintf("external_push_credential:00000000-0000-4000-8000-000000000007:%d", i+1) {
				t.Fatalf("row %d = %+v, revisions must be contiguous", i, r)
			}
		}
	})
}

func TestAppendPushLifecycleRotationWritesBothAggregatesAtOnceOrNone(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newAuditFixture(t, ctx)

	rotated := PushLifecycleEvent{EventType: externalpush.Rotated, AggregateID: "00000000-0000-4000-8000-0000000000b2", OrganizationID: pushOrg, SourceID: pushSource,
		Change: externalpush.Change{Status: "active", Fingerprint: "fedcba9876543210", Scopes: []string{"ingest:write"}, RotationParentID: "00000000-0000-4000-8000-0000000000c3"}}
	if err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
		return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{rotated, pushRevoked("00000000-0000-4000-8000-0000000000c3")})
	}); err != nil {
		t.Fatal(err)
	}
	oldRows, newRows := pushRows(t, ctx, env, "00000000-0000-4000-8000-0000000000c3"), pushRows(t, ctx, env, "00000000-0000-4000-8000-0000000000b2")
	if len(oldRows) != 2 || len(newRows) != 1 || newRows[0].eventType != externalpush.Rotated {
		t.Fatalf("old = %+v new = %+v", oldRows, newRows)
	}
	if !oldRows[1].createdAt.Equal(newRows[0].createdAt) {
		t.Fatalf("the rotation and the parent's retirement have different created_at: %v vs %v", oldRows[1].createdAt, newRows[0].createdAt)
	}

	t.Run("a refused parent event rolls the rotation back", func(t *testing.T) {
		// cred-old is already revoked at revision 2; a rotated event for a credential
		// that already has events is refused after the parent's event is written.
		again := rotated
		again.AggregateID = "00000000-0000-4000-8000-0000000000b2"
		err := withPushTx(t, ctx, env, true, func(tx pgx.Tx) error {
			return AppendPushLifecycle(ctx, tx, env.schema, []PushLifecycleEvent{again, pushRevoked("00000000-0000-4000-8000-0000000000a0")})
		})
		if !errors.Is(err, ErrPushLifecycle) {
			t.Fatalf("err = %v", err)
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-0000000000a0")); got != 0 {
			t.Fatalf("the parent's revoked event survived the refusal (%d rows)", got)
		}
		if got := len(pushRows(t, ctx, env, "00000000-0000-4000-8000-0000000000b2")); got != 1 {
			t.Fatalf("cred-new has %d rows, want the original 1", got)
		}
	})
}
