//go:build integration

package admin_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// Admin user create and update refuse a bad text form with 422 and write nothing: the row count and the stored values do not change.
func TestAdminUserWritesRefuseBadTextFormsAndWriteNothing(t *testing.T) {
	ctx := context.Background()
	pool, _ := migratedPostgres(t, ctx)
	root, target := uuid.New(), uuid.New()
	for id, email := range map[uuid.UUID]string{root: "ir-root@example.com", target: "ir-target@example.com"} {
		if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, full_name, avatar_url, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, 'x', 'Before', 'https://example.test/before.png', true, true, $3, 0, now(), now())`, id, email, id == root); err != nil {
			t.Fatal(err)
		}
	}
	const jwtKey = "admin-input-rule-test-signing-key-32-bytes!!"
	verifier, err := edgetoken.New(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := edgetoken.NewSigner(jwtKey, "dev-health-ops", "dev-health-api")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	auth, err := policy.NewAuthenticator(verifier, policy.PGStore{Pool: pool}, logger)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range admin.Routes(admin.Deps{Pool: pool, Guard: policy.NewGuard(auth, logger), Logger: logger}) {
		if strings.HasPrefix(route.Pattern, "/api/v1/admin/users") {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	bearer, err := signer.Access(edgetoken.AccessClaims{UserID: root.String(), Email: "ir-root@example.com", IsSuperuser: true}, time.Now(), uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	send := func(method, path, body string) int {
		t.Helper()
		request, err := http.NewRequestWithContext(ctx, method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer "+bearer)
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		return response.StatusCode
	}
	count := func() (n int) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	stored := func() (name, avatar, email string) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT full_name, avatar_url, email FROM users WHERE id = $1`, target).Scan(&name, &avatar, &email); err != nil {
			t.Fatal(err)
		}
		return name, avatar, email
	}
	forms := map[string]string{"replacement_char": `�`, "lone_high": `\ud800`, "lone_low": `\udc00`, "nul": `\u0000`,
		"control_c0": `\u0001`, "tab": `\t`, "lf": `\n`, "cr": `\r`, "del": `\u007f`, "c1": `\u0085`}
	before := count()
	for name, escape := range forms {
		for field, value := range map[string]string{
			"email": `"ir-` + name + `-` + `a` + escape + `b@example.com"`, "full_name": `"A` + escape + `B"`, "avatar_url": `"https://example.test/` + escape + `x"`,
		} {
			wantRefused := !(field == "full_name" && name == "replacement_char")
			if field != "avatar_url" {
				create := `{"email":"ir-create-` + name + `@example.com","` + field + `":` + value + `}`
				if field == "email" {
					create = `{"email":` + value + `}`
				}
				got := send(http.MethodPost, "/api/v1/admin/users", create)
				if wantRefused && got != http.StatusUnprocessableEntity {
					t.Errorf("create %s %s: status %d, want 422", field, name, got)
				}
			}
			if wantRefused {
				if got := send(http.MethodPatch, "/api/v1/admin/users/"+target.String(), `{"`+field+`":`+value+`}`); got != http.StatusUnprocessableEntity {
					t.Errorf("update %s %s: status %d, want 422", field, name, got)
				}
			}
		}
	}
	if after := count(); after != before+countAcceptedCreates(forms) {
		t.Errorf("users rows %d -> %d: a refused create wrote a row", before, after)
	}
	if name, avatar, email := stored(); name != "Before" || avatar != "https://example.test/before.png" || email != "ir-target@example.com" {
		t.Errorf("a refused update changed the row: %q %q %q", name, avatar, email)
	}
}

// Only the U+FFFD full_name create is accepted, and it writes one row.
func countAcceptedCreates(forms map[string]string) int {
	if _, ok := forms["replacement_char"]; ok {
		return 1
	}
	return 0
}
