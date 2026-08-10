package providersync

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitLabBlameEffectsFenceScopeAndLeaseBeforeClickHouse(t *testing.T) {
	ctx := context.Background()
	claim := nativeTestClaim("gitlab", "blame")
	valid := gitLabBlameEffectRow(t, claim, "org-acme")
	effect := gitLabBlameEffect(t, valid)
	validLease := providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })

	for _, test := range []struct {
		name   string
		claim  Claim
		effect EffectBatch
	}{
		{name: "provider", claim: nativeTestClaim("github", "blame"), effect: effect},
		{name: "dataset", claim: nativeTestClaim("gitlab", "files"), effect: effect},
		{name: "destination", claim: claim, effect: func() EffectBatch {
			wrong := effect
			wrong.Destination = "git_files"
			return wrong
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &gitLabBlameRecordingConn{}
			sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: validLease}
			if err := sink.WriteEffect(ctx, test.claim, test.effect); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("write error=%v", err)
			}
			if inspection, err := sink.InspectEffect(ctx, test.claim, test.effect); !errors.Is(err, ErrInvalidConfiguration) || inspection != EffectConflict {
				t.Fatalf("inspection=%s error=%v", inspection, err)
			}
			if conn.prepares != 0 || conn.queries != 0 {
				t.Fatalf("invalid scope reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
			}
		})
	}

	t.Run("forged row tenant", func(t *testing.T) {
		forged := valid
		forged.OrgID = "org-other"
		conn := &gitLabBlameRecordingConn{}
		sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: validLease}
		forgedEffect := gitLabBlameEffect(t, forged)
		if err := sink.WriteEffect(ctx, claim, forgedEffect); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("forged write error=%v", err)
		}
		if conn.prepares != 0 {
			t.Fatalf("forged row reached ClickHouse: prepares=%d", conn.prepares)
		}
	})

	t.Run("lost lease", func(t *testing.T) {
		conn := &gitLabBlameRecordingConn{}
		sink := GitLabBlameClickHouseEffects{
			Conn: conn,
			Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error {
				return providerfoundation.ErrLeaseLost
			}),
		}
		if err := sink.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("write lease error=%v", err)
		}
		if inspection, err := sink.InspectEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) || inspection != EffectConflict {
			t.Fatalf("inspection=%s error=%v", inspection, err)
		}
		if conn.prepares != 0 || conn.queries != 0 {
			t.Fatalf("lost lease reached ClickHouse: prepares=%d queries=%d", conn.prepares, conn.queries)
		}
	})

	t.Run("second lease assertion fences send", func(t *testing.T) {
		conn := &gitLabBlameRecordingConn{}
		guard := &gitLabBlameLeaseLostOnSecondAssert{}
		sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: guard}
		if err := sink.WriteEffect(ctx, claim, effect); !errors.Is(err, providerfoundation.ErrLeaseLost) {
			t.Fatalf("write lease error=%v", err)
		}
		if guard.calls != 2 || conn.prepares != 1 || conn.batch == nil ||
			conn.batch.appends != 1 || conn.batch.sends != 0 || conn.batch.aborts != 1 {
			t.Fatalf("fence calls=%d prepares=%d batch=%+v", guard.calls, conn.prepares, conn.batch)
		}
	})

	t.Run("valid row", func(t *testing.T) {
		conn := &gitLabBlameRecordingConn{}
		sink := GitLabBlameClickHouseEffects{Conn: conn, Lease: validLease}
		if err := sink.WriteEffect(ctx, claim, effect); err != nil {
			t.Fatal(err)
		}
		if conn.prepares != 1 || conn.batch == nil || conn.batch.appends != 1 || conn.batch.sends != 1 {
			t.Fatalf("write prepares=%d batch=%+v", conn.prepares, conn.batch)
		}
	})
}

func gitLabBlameEffectRow(t *testing.T, claim Claim, orgID string) gitBlameRow {
	t.Helper()
	email, name, hash, line := "ada@example.com", "Ada", "abc123", "line"
	return gitBlameRow{
		RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", Path: "src/main.go", LineNo: 1,
		AuthorEmail: &email, AuthorName: &name, CommitHash: &hash, Line: &line,
		LastSynced: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC), OrgID: orgID,
	}
}

func gitLabBlameEffect(t *testing.T, row gitBlameRow) EffectBatch {
	t.Helper()
	effect, err := effectBatchFromValues("git_blame", EffectReadbackRequired, []gitBlameRow{row})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

type gitLabBlameRecordingConn struct {
	driver.Conn
	prepares int
	queries  int
	batch    *gitLabBlameRecordingBatch
}

func (conn *gitLabBlameRecordingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	conn.prepares++
	if conn.batch == nil {
		conn.batch = &gitLabBlameRecordingBatch{}
	}
	return conn.batch, nil
}

func (conn *gitLabBlameRecordingConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	return gitLabBlameEmptyRows{}, nil
}

type gitLabBlameRecordingBatch struct {
	driver.Batch
	appends int
	sends   int
	aborts  int
}

func (batch *gitLabBlameRecordingBatch) Append(...any) error {
	batch.appends++
	return nil
}

func (batch *gitLabBlameRecordingBatch) Send() error {
	batch.sends++
	return nil
}

func (batch *gitLabBlameRecordingBatch) Abort() error {
	batch.aborts++
	return nil
}

type gitLabBlameEmptyRows struct{}

func (gitLabBlameEmptyRows) Next() bool                       { return false }
func (gitLabBlameEmptyRows) Scan(...any) error                { return nil }
func (gitLabBlameEmptyRows) ScanStruct(any) error             { return nil }
func (gitLabBlameEmptyRows) ColumnTypes() []driver.ColumnType { return nil }
func (gitLabBlameEmptyRows) Totals(...any) error              { return nil }
func (gitLabBlameEmptyRows) Columns() []string                { return nil }
func (gitLabBlameEmptyRows) Close() error                     { return nil }
func (gitLabBlameEmptyRows) Err() error                       { return nil }
func (gitLabBlameEmptyRows) HasData() bool                    { return false }

type gitLabBlameLeaseLostOnSecondAssert struct{ calls int }

func (guard *gitLabBlameLeaseLostOnSecondAssert) Assert(context.Context) error {
	guard.calls++
	if guard.calls == 2 {
		return providerfoundation.ErrLeaseLost
	}
	return nil
}
