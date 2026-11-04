package authruntime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type sinkTx struct {
	pgx.Tx
	commits *atomic.Int64
}

func (sinkTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (t sinkTx) Commit(context.Context) error {
	time.Sleep(20 * time.Millisecond)
	t.commits.Add(1)
	return nil
}

func (sinkTx) Rollback(context.Context) error { return nil }

// TestHighRiskCallsShareCommits: the authorize routes' audit sink answers 64
// concurrent high-risk calls with shared commits, not one commit per call.
func TestHighRiskCallsShareCommits(t *testing.T) {
	const calls = 64
	var commits atomic.Int64
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	sink, err := authorizeAuditSink(func(context.Context) (pgx.Tx, error) { return sinkTx{commits: &commits}, nil }, schema)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]any{
		"decision_id": "dec_AAAAAAAAAAAAAAAAAAAAAAAAAA", "decision_reason_code": "allowed_by_role",
		"action_key": "settings.write", "resource_type": "organization", "policy_revision": int64(1),
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, calls)
	for i := range calls {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
			<-start
			errs <- sink(context.Background(), []audit.AuditEvent{{EventType: audit.EventAuthorizeHighRiskDecided,
				Outcome: audit.OutcomeAllowed, Attributes: attrs, RequestID: &id}})
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := commits.Load(); got > calls/4 {
		t.Fatalf("commits = %d for %d concurrent calls, want <= %d", got, calls, calls/4)
	}
}
