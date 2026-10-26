//go:build integration

package usersbackfill

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// purgeRuntime is a second runtime login on the shared database, granted with
// AUTH_USERS_OWNED on (the purge grant is switch-gated); e.runtime keeps the
// switch-off grants.
var (
	purgeRuntimeOnce sync.Once
	purgeRuntimePool *pgxpool.Pool
	purgeRuntimeErr  error
)

func (e *reconEnv) ownedRuntime(t *testing.T) *pgxpool.Pool {
	t.Helper()
	purgeRuntimeOnce.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		role := "purge_owned_runtime"
		const password = "purge-test-only-not-a-credential"
		if _, err := e.admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, password)); err != nil {
			purgeRuntimeErr = err
			return
		}
		reconClose = append(reconClose, func() { containers.DropRole(e.admin, role, func(string, ...any) {}) })
		if _, err := authschema.Apply(ctx, e.admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
			purgeRuntimeErr = fmt.Errorf("Apply with users owned: %w", err)
			return
		}
		cfg := e.admin.Config().Copy()
		cfg.ConnConfig.User, cfg.ConnConfig.Password = role, password
		purgeRuntimePool, purgeRuntimeErr = pgxpool.NewWithConfig(ctx, cfg)
		if purgeRuntimeErr == nil {
			reconClose = append(reconClose, purgeRuntimePool.Close)
		}
	})
	if purgeRuntimeErr != nil {
		t.Fatal(purgeRuntimeErr)
	}
	return purgeRuntimePool
}

type legacyRows struct{ verification, reset int }

// seedLegacy resets both legacy tables, then seeds verification and reset rows
// (valid and expired) for two users, and one auth.link_credentials row.
func (e *reconEnv) seedLegacy(t *testing.T, ctx context.Context) legacyRows {
	t.Helper()
	if _, err := e.admin.Exec(ctx, `DELETE FROM public.email_verification_tokens; DELETE FROM public.password_reset_tokens`); err != nil {
		t.Fatal(err)
	}
	u1 := e.seed(t, ctx, uniq("legacy"), true, false)
	u2 := e.seed(t, ctx, uniq("legacy"), true, true)
	rows := []struct {
		table string
		user  uuid.UUID
		ttl   string
	}{
		{"email_verification_tokens", u1, "1 hour"}, {"email_verification_tokens", u2, "-1 hour"},
		{"password_reset_tokens", u1, "1 hour"}, {"password_reset_tokens", u2, "1 hour"}, {"password_reset_tokens", u2, "-2 hours"},
	}
	for _, r := range rows {
		if _, err := e.admin.Exec(ctx, fmt.Sprintf(`INSERT INTO public.%s (id, user_id, token_hash, expires_at, created_at)
VALUES ($1, $2, $3, now() + interval '%s', now())`, r.table, r.ttl), uuid.New(), r.user, "h-"+uuid.NewString()); err != nil {
			t.Fatalf("seed %s: %v", r.table, err)
		}
	}
	var principal uuid.UUID
	// The shared database also serves the reconcile tests: leave no user or principal behind.
	t.Cleanup(func() {
		c := context.Background()
		_, _ = e.admin.Exec(c, `DELETE FROM public.email_verification_tokens; DELETE FROM public.password_reset_tokens`)
		_, _ = e.admin.Exec(c, `DELETE FROM public.users WHERE id = ANY($1)`, []uuid.UUID{u1, u2})
		_, _ = e.admin.Exec(c, `DELETE FROM auth.principals WHERE id = $1`, principal)
	})
	if err := e.admin.QueryRow(ctx, `INSERT INTO auth.principals (kind) VALUES ('user') RETURNING id`).Scan(&principal); err != nil {
		t.Fatal(err)
	}
	for _, purpose := range []string{"email_verification", "password_reset"} {
		if _, err := e.admin.Exec(ctx, `INSERT INTO auth.link_credentials (principal_id, purpose, token_hash, expires_at)
VALUES ($1, $2, sha256($3::bytea), now() + interval '1 hour')`, principal, purpose, []byte(uuid.NewString())); err != nil {
			t.Fatal(err)
		}
	}
	return legacyRows{verification: 2, reset: 3}
}

func (e *reconEnv) legacyCount(t *testing.T, ctx context.Context) legacyRows {
	t.Helper()
	return legacyRows{
		verification: e.count(t, ctx, `SELECT count(*) FROM public.email_verification_tokens`),
		reset:        e.count(t, ctx, `SELECT count(*) FROM public.password_reset_tokens`),
	}
}

func (e *reconEnv) linkCredentialsDigest(t *testing.T, ctx context.Context) string {
	t.Helper()
	var digest string
	if err := e.admin.QueryRow(ctx, `SELECT coalesce(md5(string_agg(c::text, '|' ORDER BY id)), '') || ':' || count(*) FROM auth.link_credentials c`).Scan(&digest); err != nil {
		t.Fatal(err)
	}
	return digest
}

func (e *reconEnv) purgeAudit(t *testing.T, ctx context.Context) []map[string]any {
	t.Helper()
	rows, err := e.admin.Query(ctx, `SELECT outcome, attributes FROM auth.security_audit_events WHERE event_type = 'auth.legacy_link_tokens.purged' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var outcome string
		var raw []byte
		if err := rows.Scan(&outcome, &raw); err != nil {
			t.Fatal(err)
		}
		attrs := map[string]any{}
		if err := json.Unmarshal(raw, &attrs); err != nil {
			t.Fatal(err)
		}
		attrs["_outcome"] = outcome
		out = append(out, attrs)
	}
	return out
}

func TestLegacyLinkPurgeLeavesZeroPublicRowsAndTouchesNoAuthLinkCredential(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	seeded := e.seedLegacy(t, ctx)
	before := e.linkCredentialsDigest(t, ctx)
	rep, err := PurgeLegacyLinks(ctx, e.ownedRuntime(t), e.schema, PurgeOptions{RunID: "roll-1"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.EmailVerificationTokens != int64(seeded.verification) || rep.PasswordResetTokens != int64(seeded.reset) {
		t.Fatalf("report = %+v, want %+v", rep, seeded)
	}
	if got := e.legacyCount(t, ctx); got != (legacyRows{}) {
		t.Fatalf("public legacy rows left: %+v", got)
	}
	if after := e.linkCredentialsDigest(t, ctx); after != before {
		t.Fatalf("auth.link_credentials changed: %s -> %s", before, after)
	}
}

func TestLegacyLinkPurgeIsIdempotentAndCounted(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e.seedLegacy(t, ctx)
	pool := e.ownedRuntime(t)
	first, err := PurgeLegacyLinks(ctx, pool, e.schema, PurgeOptions{RunID: "roll-2a"})
	if err != nil || first.EmailVerificationTokens != 2 || first.PasswordResetTokens != 3 {
		t.Fatalf("first run: %+v, %v", first, err)
	}
	second, err := PurgeLegacyLinks(ctx, pool, e.schema, PurgeOptions{RunID: "roll-2b"})
	if err != nil || second.EmailVerificationTokens != 0 || second.PasswordResetTokens != 0 {
		t.Fatalf("second run: %+v, %v", second, err)
	}
	if got := e.legacyCount(t, ctx); got != (legacyRows{}) {
		t.Fatalf("rows after the second run: %+v", got)
	}
}

func TestLegacyLinkPurgeWritesOneAuditRowPerRunWithCountsAndRunIDOnly(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	e.seedLegacy(t, ctx)
	before := len(e.purgeAudit(t, ctx))
	if _, err := PurgeLegacyLinks(ctx, e.ownedRuntime(t), e.schema, PurgeOptions{RunID: "roll-3"}); err != nil {
		t.Fatal(err)
	}
	rows := e.purgeAudit(t, ctx)
	if len(rows) != before+1 {
		t.Fatalf("audit rows = %d, want %d", len(rows), before+1)
	}
	got := rows[len(rows)-1]
	want := map[string]any{
		"purged_email_verification_tokens": float64(2), "purged_password_reset_tokens": float64(3),
		"run_id": "roll-3", "_outcome": "allowed",
	}
	if len(got) != len(want) {
		t.Fatalf("audit attributes = %v, want exactly %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attribute %s = %v, want %v", k, got[k], v)
		}
	}
	if _, err := PurgeLegacyLinks(ctx, e.ownedRuntime(t), e.schema, PurgeOptions{}); err != nil {
		t.Fatal(err)
	}
	last := e.purgeAudit(t, ctx)
	if len(last) != before+2 {
		t.Fatalf("a run of zero rows wrote %d audit rows, want one", len(last)-(before+1))
	}
	if _, ok := last[len(last)-1]["run_id"]; ok {
		t.Fatalf("a run without a run id recorded one: %v", last[len(last)-1])
	}
}

func TestLegacyLinkPurgeDryRunCountsAndWritesNothing(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	seeded := e.seedLegacy(t, ctx)
	audits := len(e.purgeAudit(t, ctx))
	rep, err := PurgeLegacyLinks(ctx, e.ownedRuntime(t), e.schema, PurgeOptions{DryRun: true, RunID: "roll-4"})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.DryRun || rep.EmailVerificationTokens != int64(seeded.verification) || rep.PasswordResetTokens != int64(seeded.reset) {
		t.Fatalf("dry report = %+v, want counts %+v", rep, seeded)
	}
	if got := e.legacyCount(t, ctx); got != seeded {
		t.Fatalf("a dry run changed the rows: %+v, want %+v", got, seeded)
	}
	if n := len(e.purgeAudit(t, ctx)); n != audits {
		t.Fatalf("a dry run wrote %d audit rows", n-audits)
	}
}

// A NotOwned deployment's runtime role holds no DELETE on the legacy tables:
// a purge there is refused by the database, rolls back, and keeps every valid
// token and writes no audit row.
func TestLegacyLinkPurgeKeepsEveryTokenWhereTheSwitchIsOff(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	seeded := e.seedLegacy(t, ctx)
	audits := len(e.purgeAudit(t, ctx))
	if _, err := PurgeLegacyLinks(ctx, e.runtime, e.schema, PurgeOptions{RunID: "roll-5"}); err == nil {
		t.Fatal("the purge ran on a runtime role without the switch-gated grant")
	}
	if got := e.legacyCount(t, ctx); got != seeded {
		t.Fatalf("rows after the refused purge: %+v, want %+v", got, seeded)
	}
	if n := len(e.purgeAudit(t, ctx)); n != audits {
		t.Fatalf("a refused purge wrote %d audit rows", n-audits)
	}
}

// The verb: users owned off keeps every row (refused before it connects); on,
// it purges and prints counts only.
func TestLegacyLinkPurgeVerbEndToEnd(t *testing.T) {
	e := getReconEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	seeded := e.seedLegacy(t, ctx)
	old := connectFn
	connectFn = func(context.Context, cli.Env, string) (*pgxpool.Pool, authschema.ValidatedIdentifier, int) {
		return e.ownedRuntime(t), e.schema, cli.ExitOK
	}
	t.Cleanup(func() { connectFn = old })

	if code, _, _ := runPurgeVerb(t, map[string]string{}, "--batch=500", "--run-id=roll-6"); code != cli.ExitRefused {
		t.Fatalf("switch off: exit %d, want %d", code, cli.ExitRefused)
	}
	if got := e.legacyCount(t, ctx); got != seeded {
		t.Fatalf("switch off changed the rows: %+v", got)
	}
	var stdout bytes.Buffer
	code := purgeVerb(t).Run(ctx, cli.Env{
		Args:   []string{"--batch=500", "--run-id=roll-6"},
		Lookup: func(k string) (string, bool) { return "true", k == "AUTH_USERS_OWNED" },
		Stdout: &stdout, Stderr: &bytes.Buffer{},
	})
	if code != cli.ExitOK {
		t.Fatalf("switch on: exit %d", code)
	}
	want := "mode=purge\nemail_verification_tokens_purged=2\npassword_reset_tokens_purged=3\n"
	if stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if strings.Contains(stdout.String(), "h-") {
		t.Fatal("the output carries a token hash")
	}
	if got := e.legacyCount(t, ctx); got != (legacyRows{}) {
		t.Fatalf("rows left: %+v", got)
	}
}
