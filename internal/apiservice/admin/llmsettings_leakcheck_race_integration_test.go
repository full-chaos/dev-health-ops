//go:build integration

package admin_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/apiservice"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestLLMSettingsConcurrentSavesNeverStoreAnotherScopesKey is CHAOS-14247 over
// the real route and rows: two saves in two scopes of one org, at the same
// time, must not together store a base_url that carries the other scope's key.
// Save A (its base_url carries the key save B writes) is held after its checks
// and before its commit by a row lock a test transaction holds on A's scope;
// save B runs; then A is let go. The order is the assertion: B answers or
// waits on A before A is released. A save that waits on the org's save lock
// past its lock_timeout answers 409 setting_changed, writes nothing and logs
// one WARN line.
func TestLLMSettingsConcurrentSavesNeverStoreAnotherScopesKey(t *testing.T) {
	ctx := context.Background()
	orgDefault, orgPurpose := uuid.New(), uuid.New()
	userDefault, userPurpose := uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		GoOnly: true,
		Root:   repoRoot(t),
		Seed: withTokenMemberships(func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			for i, org := range []uuid.UUID{orgDefault, orgPurpose} {
				user := []uuid.UUID{userDefault, userPurpose}[i]
				slug := fmt.Sprintf("llmrace%d", i)
				if _, err := admin.Exec(ctx, `INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'team', 'stripe', true, now(), now())`, org, slug); err != nil {
					t.Fatalf("seed org: %v", err)
				}
				if _, err := admin.Exec(ctx, `INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, user, slug+"-admin@example.com"); err != nil {
					t.Fatalf("seed user: %v", err)
				}
			}
			return map[string]map[string]any{
				"default": {"user_id": userDefault.String(), "email": "llmrace0-admin@example.com", "org_id": orgDefault.String(), "role": "admin"},
				"purpose": {"user_id": userPurpose.String(), "email": "llmrace1-admin@example.com", "org_id": orgPurpose.String(), "role": "admin"},
			}
		}),
	})
	logs := &lockedBuffer{}
	base, _ := startGoServerLogged(t, ctx, venue, slog.New(slog.NewTextHandler(logs, nil)), func(deps *apiservice.Deps) {
		decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(llmVenueEncryptionKey), "")
		if err != nil {
			t.Fatalf("build decryptor: %v", err)
		}
		deps.Decryptor = decryptor
	})
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	t.Cleanup(admin.Close)

	type answer struct {
		status int
		errTok any
		err    error
	}
	put := func(token, body string) answer {
		req, err := http.NewRequest(http.MethodPut, base+"/api/v1/admin/llm-settings", strings.NewReader(body))
		if err != nil {
			return answer{err: err}
		}
		req.Header.Set("Authorization", "Bearer "+venue.Tokens[token])
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return answer{err: err}
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		detail, _ := out["detail"].(map[string]any)
		return answer{status: resp.StatusCode, errTok: detail["error"]}
	}
	async := func(token, body string) <-chan answer {
		done := make(chan answer, 1)
		go func() { done <- put(token, body) }()
		return done
	}
	mustOK := func(name, token, body string) {
		t.Helper()
		if got := put(token, body); got.err != nil || got.status != http.StatusOK {
			t.Fatalf("%s: %+v, want 200", name, got)
		}
	}
	// blocked counts the org's backends that wait on any lock.
	blocked := func() int {
		t.Helper()
		var n int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0`).Scan(&n); err != nil {
			t.Fatalf("read blocked backends: %v", err)
		}
		return n
	}
	waitBlocked := func(name string, want int, early <-chan answer) (answer, bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			select {
			case got := <-early:
				return got, true
			default:
			}
			if blocked() >= want {
				return answer{}, false
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: neither answered nor waited within 30s", name)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	value := func(org uuid.UUID, key string) string {
		t.Helper()
		var v string
		err := admin.QueryRow(ctx, `SELECT coalesce(value, '') FROM settings WHERE org_id = $1 AND category = 'llm' AND key = $2`, org.String(), key).Scan(&v)
		if err != nil && !strings.Contains(err.Error(), "no rows") {
			t.Fatalf("read %s: %v", key, err)
		}
		return v
	}

	// race holds save A on a row lock of its own scope, runs save B, then lets A go.
	race := func(name string, org uuid.UUID, lockKey, tokenA, bodyA, tokenB, bodyB string) (a, b answer) {
		t.Helper()
		holder, err := admin.Begin(ctx)
		if err != nil {
			t.Fatalf("%s: begin holder: %v", name, err)
		}
		released := false
		defer func() {
			if !released {
				_ = holder.Rollback(ctx)
			}
		}()
		var held int
		if err := holder.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM settings WHERE org_id = $1 AND category = 'llm' AND key = $2 FOR UPDATE) AS l`, org.String(), lockKey).Scan(&held); err != nil || held != 1 {
			t.Fatalf("%s: hold %s row: %d, %v", name, lockKey, held, err)
		}
		doneA := async(tokenA, bodyA)
		if got, early := waitBlocked(name+": save A", 1, doneA); early {
			t.Fatalf("%s: save A answered %+v before it waited on the held %s row: the cell did not hold it after its checks", name, got, lockKey)
		}
		doneB := async(tokenB, bodyB)
		b, bAnswered := waitBlocked(name+": save B", 2, doneB)
		t.Logf("%s: save B answered while A was held: %v", name, bAnswered)
		if err := holder.Rollback(ctx); err != nil {
			t.Fatalf("%s: release holder: %v", name, err)
		}
		released = true
		select {
		case a = <-doneA:
		case <-time.After(30 * time.Second):
			t.Fatalf("%s: save A did not finish after release", name)
		}
		if !bAnswered {
			select {
			case b = <-doneB:
			case <-time.After(30 * time.Second):
				t.Fatalf("%s: save B did not finish after A", name)
			}
		}
		t.Logf("%s: save A %+v, save B %+v", name, a, b)
		return a, b
	}
	// check fails when both saves committed and the stored base_url carries the
	// other scope's stored key; else it wants A committed and B refused.
	check := func(name string, a, b answer, leaked bool) {
		t.Helper()
		if a.err != nil || b.err != nil {
			t.Fatalf("%s: transport error: A %v, B %v", name, a.err, b.err)
		}
		if a.status == http.StatusOK && b.status == http.StatusOK && leaked {
			t.Errorf("%s: LEAK: both saves committed (A 200, B 200) and the stored base_url carries the other scope's key", name)
			return
		}
		if a.status != http.StatusOK || b.status != http.StatusBadRequest || b.errTok != "invalid_base_url" || leaked {
			t.Errorf("%s: A %+v, B %+v, leaked %v; want A 200, B 400 invalid_base_url, no leak", name, a, b, leaked)
		}
	}

	const keyDefault, keyNew, keyExplain = "sk-race-default-" + "0123456789", "sk-race-new-" + "0123456789", "sk-race-explain-" + "0123456789"

	// 1: save A = default row, base_url carries the explain key save B writes.
	mustOK("default seed", "default", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1"}`, keyDefault))
	a, b := race("default carries explain key", orgDefault, "base_url",
		"default", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1/%s"}`, keyDefault, keyExplain),
		"default", fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q}`, keyExplain))
	check("default carries explain key", a, b,
		llmorgsettings.BaseURLCarriesKey(value(orgDefault, "base_url"), keyExplain) && value(orgDefault, llmorgsettings.PurposeKey(llmorgsettings.PurposeExplain, "api_key")) != "")

	// 2: save A = explain override, base_url carries the default key save B writes.
	mustOK("purpose seed default", "purpose", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q}`, keyDefault))
	mustOK("purpose seed explain", "purpose", fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1"}`, keyExplain))
	explainBaseURL := llmorgsettings.PurposeKey(llmorgsettings.PurposeExplain, "base_url")
	a, b = race("explain carries default key", orgPurpose, explainBaseURL,
		"purpose", fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1/%s"}`, keyExplain, keyNew),
		"purpose", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q}`, keyNew))
	defaultUpdated := a.status == http.StatusOK && b.status == http.StatusOK
	check("explain carries default key", a, b,
		llmorgsettings.BaseURLCarriesKey(value(orgPurpose, explainBaseURL), keyNew) && defaultUpdated)

	// 3: save timing, the PUT round trip (an upper bound of the save
	// transaction) of 50 saves, default and explain, for the lock_timeout.
	var took []time.Duration
	for i := range 50 {
		body := fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1"}`, keyDefault)
		if i%2 == 1 {
			body = fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q}`, keyExplain)
		}
		start := time.Now()
		mustOK("timing save", "default", body)
		took = append(took, time.Since(start))
	}
	slices.Sort(took)
	t.Logf("save timing: n=%d p50=%s p99=%s max=%s", len(took), took[len(took)/2], took[len(took)*99/100], took[len(took)-1])

	// 4: another save holds the org's save lock past the lock_timeout.
	rows := func(org uuid.UUID) string {
		t.Helper()
		var out string
		if err := admin.QueryRow(ctx, `SELECT count(*)::text || ' rows, md5 ' || md5(coalesce(string_agg(key || '=' || coalesce(value, '') || '@' || updated_at::text, ',' ORDER BY key), '')) FROM settings WHERE org_id = $1`, org.String()).Scan(&out); err != nil {
			t.Fatalf("read rows: %v", err)
		}
		return out
	}
	const keyTimeout = "sk-race-timeout-" + "0123456789"
	for _, tc := range []struct{ scope, body string }{
		{"default", fmt.Sprintf(`{"provider":"openai","model":"gpt-5","api_key":%q,"base_url":"https://8.8.8.8/v1"}`, keyTimeout)},
		{"explain", fmt.Sprintf(`{"purpose":"explain","provider":"openai","model":"gpt-5","api_key":%q}`, keyTimeout)},
	} {
		name := "lock timeout, " + tc.scope
		before := rows(orgPurpose)
		holder, err := admin.Begin(ctx)
		if err != nil {
			t.Fatalf("%s: begin holder: %v", name, err)
		}
		if _, err := holder.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "llm-settings-save:"+orgPurpose.String()); err != nil {
			t.Fatalf("%s: hold save lock: %v", name, err)
		}
		done := async("purpose", tc.body)
		var got answer
		select {
		case got = <-done:
		case <-time.After(15 * time.Second):
			_ = holder.Rollback(ctx)
			t.Fatalf("%s: the save still waited on the held save lock after 15s: no lock_timeout", name)
		}
		if err := holder.Rollback(ctx); err != nil {
			t.Fatalf("%s: release holder: %v", name, err)
		}
		if got.err != nil || got.status != http.StatusConflict || got.errTok != "setting_changed" {
			t.Errorf("%s: %+v, want 409 setting_changed", name, got)
		}
		if after := rows(orgPurpose); after != before {
			t.Errorf("%s: rows changed: before %s, after %s", name, before, after)
		}
		want := fmt.Sprintf(`level=WARN msg="admin: llm settings save lock timed out; another save of the org holds it" org_id=%s scope=%s`, orgPurpose, tc.scope)
		var seen []string
		for _, line := range strings.Split(logs.String(), "\n") {
			if strings.Contains(line, "llm settings save lock timed out") && strings.Contains(line, "scope="+tc.scope) {
				seen = append(seen, line)
			}
		}
		if len(seen) != 1 || !strings.Contains(seen[0], want) || strings.Contains(seen[0], keyTimeout) || strings.Contains(seen[0], "8.8.8.8") {
			t.Errorf("%s: WARN lines %q, want one %q with no key or url", name, seen, want)
		} else {
			t.Logf("%s: %+v; rows unchanged; %s", name, got, seen[0])
		}
	}
	venueoracle.WriteGoOnlyProof(t, "CHAOS-14247: two concurrent LLM settings saves in two scopes never both store a base_url carrying the other's key; a save past the save lock timeout is 409 setting_changed")
}
