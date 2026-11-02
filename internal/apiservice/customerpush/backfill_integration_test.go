//go:build integration

package customerpush

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
)

func (e *lifecycleEnv) legacyOrgToken(t *testing.T, ctx context.Context) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := e.pool.Exec(ctx, `INSERT INTO external_ingest_tokens (id, org_id, source_id, name, token_hash, token_prefix, scopes, created_at)
		VALUES ($1, $2, NULL, 'legacy-org', md5(gen_random_uuid()::text), 'fcpush_yyyyy', '["ingest:status"]', now())`, id, e.org.String()); err != nil {
		t.Fatal(err)
	}
	return id
}

func (e *lifecycleEnv) backfill(t *testing.T, ctx context.Context, apply bool) BackfillReport {
	t.Helper()
	rep, err := BackfillLifecycle(ctx, e.pool, "auth", apply, 2)
	if err != nil {
		t.Fatalf("BackfillLifecycle: %v", err)
	}
	return rep
}

func (e *lifecycleEnv) outboxAndRevisions(t *testing.T, ctx context.Context) (int, int) {
	return e.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_type = 'external_push_credential'`),
		e.count(t, ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'external_push_credential'`)
}

func TestBackfillDryRunCountsTokensWithNoAggregateAndWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	env.legacyToken(t, ctx)
	env.legacyToken(t, ctx)
	env.legacyOrgToken(t, ctx)
	env.createToken(t)
	outboxBefore, revBefore := env.outboxAndRevisions(t, ctx)

	rep := env.backfill(t, ctx, false)
	if rep.Candidates != 3 || rep.SourceBound != 2 || rep.OrganizationWide != 1 || rep.Written != 0 {
		t.Fatalf("report = %+v", rep)
	}
	if o, r := env.outboxAndRevisions(t, ctx); o != outboxBefore || r != revBefore {
		t.Fatalf("a dry run wrote: outbox %d->%d revisions %d->%d", outboxBefore, o, revBefore, r)
	}
}

func TestBackfillApplyWritesCreatedRevisionOneThenARerunWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	src1, src2, orgWide := env.legacyToken(t, ctx), env.legacyToken(t, ctx), env.legacyOrgToken(t, ctx)
	already := env.createToken(t)
	alreadyBefore := env.outbox(t, ctx, already)

	rep := env.backfill(t, ctx, true)
	if rep.Candidates != 3 || rep.Written != 3 || rep.Failed != 0 {
		t.Fatalf("report = %+v", rep)
	}
	for _, id := range []uuid.UUID{src1, src2, orgWide} {
		if got := env.outbox(t, ctx, id.String()); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
			t.Fatalf("%s events: %+v", id, got)
		}
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND payload->>'binding' = 'organization' AND NOT (payload ? 'source_id')`, orgWide.String()); n != 1 {
		t.Fatalf("organization-wide token payload has the wrong binding or a source_id (%d)", n)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.auth_outbox_events WHERE aggregate_id = $1 AND payload->>'binding' = 'source' AND payload->>'source_id' = $2`, src1.String(), env.source.String()); n != 1 {
		t.Fatalf("source token payload has the wrong binding or source_id (%d)", n)
	}
	if got := env.outbox(t, ctx, already); !slices.Equal(got, alreadyBefore) {
		t.Fatalf("a token that had an aggregate changed: %+v -> %+v", alreadyBefore, got)
	}

	outboxBefore, revBefore := env.outboxAndRevisions(t, ctx)
	again := env.backfill(t, ctx, true)
	if again.Candidates != 0 || again.Written != 0 {
		t.Fatalf("rerun report = %+v", again)
	}
	if o, r := env.outboxAndRevisions(t, ctx); o != outboxBefore || r != revBefore {
		t.Fatalf("rerun wrote: outbox %d->%d revisions %d->%d", outboxBefore, o, revBefore, r)
	}
}

func TestBackfillRevokedTokenGetsCreatedThenRevokedAndExpiredGetsCreatedOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	revoked, expired := env.legacyToken(t, ctx), env.legacyToken(t, ctx)
	if _, err := env.pool.Exec(ctx, `UPDATE external_ingest_tokens SET revoked_at = now() WHERE id = $1`, revoked); err != nil {
		t.Fatal(err)
	}
	if _, err := env.pool.Exec(ctx, `UPDATE external_ingest_tokens SET expires_at = now() - interval '1 day' WHERE id = $1`, expired); err != nil {
		t.Fatal(err)
	}
	rep := env.backfill(t, ctx, true)
	if rep.Written != 2 || rep.Revoked != 1 {
		t.Fatalf("report = %+v", rep)
	}
	if got := env.outbox(t, ctx, revoked.String()); len(got) != 2 || got[0] != (outboxRow{externalpush.Created, 1}) || got[1] != (outboxRow{externalpush.Revoked, 2}) {
		t.Fatalf("revoked token events: %+v", got)
	}
	if got := env.outbox(t, ctx, expired.String()); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
		t.Fatalf("expired token events: %+v", got)
	}
}

func TestBackfillSkipsATokenThatGainedAnAggregateAfterTheCandidateRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	tokens := make([]uuid.UUID, 6)
	for i := range tokens {
		tokens[i] = env.legacyToken(t, ctx)
	}
	var wg sync.WaitGroup
	var rep BackfillReport
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		rep, runErr = BackfillLifecycle(ctx, env.pool, "auth", true, 2)
	}()
	for i := 0; i < len(tokens); i += 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w := httptest.NewRecorder()
			env.h.revokeToken(w, env.request(nil, "token_id", tokens[i].String()))
			if w.Code != http.StatusOK {
				t.Errorf("revoke %d: %d %s", i, w.Code, w.Body.String())
			}
		}()
	}
	wg.Wait()
	if runErr != nil || rep.Failed != 0 {
		t.Fatalf("backfill: %v %+v", runErr, rep)
	}
	env.backfill(t, ctx, true)
	for i, id := range tokens {
		got := env.outbox(t, ctx, id.String())
		want := []outboxRow{{externalpush.Created, 1}}
		if i%2 == 0 {
			want = append(want, outboxRow{externalpush.Revoked, 2})
		}
		if !slices.Equal(got, want) {
			t.Errorf("token %d events %+v, want %+v", i, got, want)
		}
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_type = 'external_push_credential'`); n != len(tokens) {
		t.Fatalf("%d revision rows, want %d", n, len(tokens))
	}
}

func TestBackfillTwoConcurrentRunsEmitEachCreatedOnce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	ids := make([]uuid.UUID, 8)
	for i := range ids {
		ids[i] = env.legacyToken(t, ctx)
	}
	var wg sync.WaitGroup
	reps := make([]BackfillReport, 2)
	errs := make([]error, 2)
	for i := range reps {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reps[i], errs[i] = BackfillLifecycle(ctx, env.pool, "auth", true, 3)
		}()
	}
	wg.Wait()
	for i := range reps {
		if errs[i] != nil || reps[i].Failed != 0 {
			t.Fatalf("run %d: %v %+v", i, errs[i], reps[i])
		}
	}
	if reps[0].Written+reps[1].Written != len(ids) {
		t.Fatalf("written %d + %d, want %d in all", reps[0].Written, reps[1].Written, len(ids))
	}
	for _, id := range ids {
		if got := env.outbox(t, ctx, id.String()); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
			t.Fatalf("%s events: %+v", id, got)
		}
	}
}

func TestBackfillEventsAndLogsCarryNoSecret(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	id := env.legacyToken(t, ctx)
	var hash, prefix, name string
	if err := env.pool.QueryRow(ctx, `SELECT token_hash, token_prefix, name FROM external_ingest_tokens WHERE id = $1`, id).Scan(&hash, &prefix, &name); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	rep := env.backfill(t, ctx, true)
	var payload string
	if err := env.pool.QueryRow(ctx, `SELECT payload::text FROM auth.auth_outbox_events WHERE aggregate_id = $1`, id.String()).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	for label, text := range map[string]string{"payload": payload, "logs": logs.String(), "report": fmt.Sprintf("%+v", rep)} {
		for secret, value := range map[string]string{"hash": hash, "prefix": prefix, "name": name} {
			if strings.Contains(text, value) {
				t.Errorf("the %s holds the token %s", label, secret)
			}
		}
	}
	var members []string
	rows, err := env.pool.Query(ctx, `SELECT jsonb_object_keys(payload->'change') FROM auth.auth_outbox_events WHERE aggregate_id = $1`, id.String())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		members = append(members, k)
	}
	for _, k := range members {
		if !slices.Contains([]string{"status", "fingerprint", "scopes", "expires_at"}, k) {
			t.Errorf("change member %q is outside the created form", k)
		}
	}
}

func TestBackfillCommitsEachTokenAloneSoAFailureLeavesTheRestWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	ids := []uuid.UUID{env.legacyToken(t, ctx), env.legacyToken(t, ctx), env.legacyToken(t, ctx)}
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return strings.Compare(a.String(), b.String()) })
	victim := ids[1]
	if _, err := env.pool.Exec(ctx, `CREATE FUNCTION auth.refuse_victim() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.aggregate_id = '`+victim.String()+`' THEN RAISE EXCEPTION 'test refusal'; END IF; RETURN NEW; END $$`); err != nil {
		t.Fatal(err)
	}
	if _, err := env.pool.Exec(ctx, `CREATE TRIGGER refuse_victim BEFORE INSERT ON auth.auth_outbox_events FOR EACH ROW EXECUTE FUNCTION auth.refuse_victim()`); err != nil {
		t.Fatal(err)
	}
	rep := env.backfill(t, ctx, true)
	if rep.Written != 2 || rep.Failed != 1 {
		t.Fatalf("report = %+v", rep)
	}
	if got := env.outbox(t, ctx, victim.String()); len(got) != 0 {
		t.Fatalf("the victim has events: %+v", got)
	}
	if n := env.count(t, ctx, `SELECT count(*) FROM auth.aggregate_revisions WHERE aggregate_id = $1`, victim.String()); n != 0 {
		t.Fatalf("the victim kept a revision row")
	}
	for _, id := range []uuid.UUID{ids[0], ids[2]} {
		if got := env.outbox(t, ctx, id.String()); len(got) != 1 {
			t.Fatalf("%s events: %+v", id, got)
		}
	}
	if _, err := env.pool.Exec(ctx, `DROP TRIGGER refuse_victim ON auth.auth_outbox_events`); err != nil {
		t.Fatal(err)
	}
	if rep := env.backfill(t, ctx, true); rep.Written != 1 || rep.Failed != 0 {
		t.Fatalf("rerun report = %+v", rep)
	}
	if got := env.outbox(t, ctx, victim.String()); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
		t.Fatalf("victim events after rerun: %+v", got)
	}
}

// TestBackfillWaitsForTheRowLockThenSkipsAToken: a route-like writer holds the
// token row, writes its events and commits while the backfill waits; the backfill
// then finds the aggregate under the lock and skips instead of failing.
func TestBackfillWaitsForTheRowLockThenSkipsAToken(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	env := newLifecycleEnv(t, ctx, true)
	id := env.legacyToken(t, ctx)
	holder, err := env.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	row, err := scanToken(holder.QueryRow(ctx, `SELECT `+tokenColumns+` FROM external_ingest_tokens WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		t.Fatal(err)
	}
	events, err := storedLifecycleEvents(row)
	if err != nil {
		t.Fatal(err)
	}
	if err := audit.AppendPushLifecycle(ctx, holder, env.h.lifecycle.schema, events); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var rep BackfillReport
	var runErr error
	go func() {
		defer close(done)
		rep, runErr = BackfillLifecycle(ctx, env.pool, "auth", true, 5)
	}()
	select {
	case <-done:
		t.Fatalf("the backfill finished while another transaction held the token row: %+v %v", rep, runErr)
	case <-time.After(1500 * time.Millisecond):
	}
	if err := holder.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
	if runErr != nil || rep.Candidates != 1 || rep.Skipped != 1 || rep.Written != 0 || rep.Failed != 0 {
		t.Fatalf("report = %+v, %v", rep, runErr)
	}
	if got := env.outbox(t, ctx, id.String()); len(got) != 1 || got[0] != (outboxRow{externalpush.Created, 1}) {
		t.Fatalf("events: %+v", got)
	}
}
