package usersbackfill

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// keepSink is memSink that honours identity.BackfillTarget.InsertOnly as the store does: an existing user is Kept and
// nothing is written. It records whether every target asked for insert-only.
type keepSink struct {
	memSink
	insertOnly []bool
}

func (k *keepSink) BackfillUser(ctx context.Context, t identity.BackfillTarget, dryRun bool) (identity.BackfillResult, error) {
	k.insertOnly = append(k.insertOnly, t.InsertOnly)
	if _, ok := k.seen[t.ID]; ok && t.InsertOnly {
		return identity.BackfillResult{Kept: true}, nil
	}
	return k.memSink.BackfillUser(ctx, t, dryRun)
}

// CHAOS-13107: compose's auth-users-backfill runs where auth-service owns users, so it is insert-only: every target asks
// the store to keep an existing user, a changed public row is not written over it, and the report says insert-only
// with the kept count. The default run is unchanged: no target asks for it, and its report lines are as before.
func TestRunInsertOnlyKeepsEveryExistingUser(t *testing.T) {
	rows := rowsFor(4)
	sink := &keepSink{memSink: memSink{seen: map[uuid.UUID]identity.BackfillTarget{}}}
	if rep, err := Run(context.Background(), pagedSource{rows: rows[:2]}, sink, Options{}); err != nil || rep.Inserted != 2 {
		t.Fatalf("default run: %+v %v", rep, err)
	}
	for i, insertOnly := range sink.insertOnly {
		if insertOnly {
			t.Fatalf("default run: target %d asked for insert-only", i)
		}
	}
	sink.insertOnly = nil
	rows[0].Active = false // public.users differs from the auth user: insert-only must not write it
	rep, err := Run(context.Background(), pagedSource{rows: rows}, sink, Options{InsertOnly: true})
	if err != nil || rep.Inserted != 2 || rep.Kept != 2 || rep.Updated != 0 || !rep.InsertOnly {
		t.Fatalf("insert-only run: %+v %v", rep, err)
	}
	if len(sink.insertOnly) != 4 {
		t.Fatalf("targets %d, want 4", len(sink.insertOnly))
	}
	for i, insertOnly := range sink.insertOnly {
		if !insertOnly {
			t.Errorf("insert-only run: target %d did not ask for insert-only", i)
		}
	}
	if !sink.seen[rows[0].ID].Active {
		t.Error("insert-only run wrote over an existing user")
	}
	lines := strings.Join(rep.Lines(), "\n")
	if !strings.Contains(lines, "mode=insert-only") || !strings.Contains(lines, "kept=2") {
		t.Errorf("insert-only report:\n%s", lines)
	}
	def := strings.Join(Report{Seen: 1, Inserted: 1, Skipped: map[string]int{}}.Lines(), "\n")
	if !strings.HasPrefix(def, "mode=write\n") || strings.Contains(def, "kept=") {
		t.Errorf("the default report changed:\n%s", def)
	}
}

// The verb takes -insert-only (CHAOS-13372: owned runs are covered below).
func TestVerbAcceptsInsertOnly(t *testing.T) {
	if code, stderr := runVerb("-insert-only", "-dry-run"); code != cli.ExitFailure || stderr == "" {
		t.Fatalf("-insert-only without a database: exit %d (want %d, a missing database, not a usage error), stderr %q", code, cli.ExitFailure, stderr)
	}
}

// CHAOS-13372: while users are owned the verb runs with -insert-only (it only creates missing users) and still refuses
// without it. The connect seam proves the run got past the ownership gate.
func ownedInsertOnlyRun(t *testing.T, args ...string) (code int, reached bool, stderr string) {
	t.Helper()
	orig := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		reached = true
		return nil, authschema.ValidatedIdentifier{}, cli.ExitFailure
	}
	t.Cleanup(func() { connectFn = orig })
	var errBuf bytes.Buffer
	lookup := func(k string) (string, bool) { return "true", k == "AUTH_USERS_OWNED" }
	code = Command().Children[0].Run(context.Background(), cli.Env{Args: args, Lookup: lookup, Stdout: &bytes.Buffer{}, Stderr: &errBuf})
	return code, reached, errBuf.String()
}

func TestOwnedInsertOnlyRunsPastTheOwnershipGate(t *testing.T) {
	for _, args := range [][]string{{"-insert-only"}, {"-insert-only", "-dry-run"}} {
		code, reached, stderr := ownedInsertOnlyRun(t, args...)
		if code == cli.ExitRefused || !reached {
			t.Fatalf("args %v: exit %d, connect reached %v, stderr %q", args, code, reached, stderr)
		}
	}
}

func TestOwnedWithoutInsertOnlyStillRefuses(t *testing.T) {
	for _, args := range [][]string{nil, {"-dry-run"}} {
		code, reached, stderr := ownedInsertOnlyRun(t, args...)
		want := "users-backfill: refused: AUTH_USERS_OWNED is true, auth.users is the authority and this command copies from public.users\n"
		if code != cli.ExitRefused || reached || stderr != want {
			t.Fatalf("args %v: exit %d, connect reached %v, stderr %q", args, code, reached, stderr)
		}
	}
}
