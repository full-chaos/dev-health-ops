//go:build integration

package session

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// pinnedNonTextRow is the one pinned cell for an email member that is not text (PLAN-12026 Q4): an unknown identity is
// go-api's 500 with no row written, a linked one signs in.
const pinnedNonTextRow = "gitlab_number_email_verified"

// subjectRewrite serves inner with every account id 7 (and Google's "g7") rewritten to n, in UTF-8 and UTF-16LE
// bodies, so each row and scenario has its own provider identity.
func subjectRewrite(inner http.Handler, n int) http.Handler {
	digits := strconv.Itoa(n)
	pairs := [][2]string{{`"id": 7`, `"id": ` + digits}, {`"resource_owner_id": 7`, `"resource_owner_id": ` + digits}, {`"g7"`, `"g` + digits + `"`}}
	utf16le := func(s string) []byte {
		var out []byte
		for _, u := range utf16.Encode([]rune(s)) {
			out = append(out, byte(u), byte(u>>8))
		}
		return out
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, r)
		body := rec.Body.Bytes()
		for _, p := range pairs {
			body = bytes.ReplaceAll(body, []byte(p[0]), []byte(p[1]))
			body = bytes.ReplaceAll(body, utf16le(p[0]), utf16le(p[1]))
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(body)
	})
}

// go-api's social login on a real database for every row of ci/social_provider_email_parity.tsv (the reference plane
// of PLAN-12026 Q6), for an unknown identity and for one linked to a users row: a refused row answers the table's
// answer and writes nothing, linked or not; a passing row signs the linked user in (a refresh token row). The pinned
// non-text row answers an unknown identity with the 500 and writes nothing. ("id=" rows stay unit cells.)
func TestSocialLoginParityTableOnARealDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pg, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, pg.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	count := func(table string) int {
		t.Helper()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for i, row := range readParityRows(t, filepath.Join("..", "..", "..", "ci", "social_provider_email_parity.tsv")) {
		if strings.HasPrefix(row.shape, "id=") {
			continue
		}
		t.Run(row.id, func(t *testing.T) {
			name := strings.SplitN(row.provider, "_", 2)[0]
			for _, linked := range []bool{false, true} {
				n := 10000 + 2*i
				if linked {
					n++
				}
				subject := strconv.Itoa(n)
				if name == "google" {
					subject = "g" + subject
				}
				if linked {
					if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, auth_provider_id, is_active,
is_verified, is_superuser, token_version, created_at, updated_at) VALUES ($1, $2, NULL, $3, $4, true, true, false, 0, now(), now())`,
						uuid.New(), fmt.Sprintf("linked-%d@example.com", n), name, subject); err != nil {
						t.Fatal(err)
					}
				}
				server := httptest.NewServer(subjectRewrite(goAPIProviderProfile(row), n))
				deps := completeDeps(t)
				deps.Pool = pool
				deps.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
				deps.Getenv = func(string) string { return "set" }
				deps.OAuth = &oauthprovider.Client{HTTP: server.Client(), Endpoints: oauthprovider.Endpoints{
					GitHubUser: server.URL + "/github/user", GitHubEmails: server.URL + "/github/emails",
					GitLabBase: server.URL + "/gitlab", GoogleUser: server.URL + "/google/userinfo"}}
				var handler http.Handler
				for _, route := range Routes(deps) {
					if route.Pattern == "/api/v1/auth/social-login" {
						handler = route.Handler
					}
				}
				users, tokens := count("users"), count("refresh_tokens")
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
					strings.NewReader(`{"provider": "`+name+`", "provider_access_token": "x"}`)).WithContext(ctx)
				r.Header.Set("Content-Type", "application/json")
				handler.ServeHTTP(w, r)
				server.Close()
				got := strconv.Itoa(w.Code) + " " + detailMessage(w.Body.String())
				switch {
				case row.want != "pass" || (row.id == pinnedNonTextRow && !linked):
					want := row.want
					if row.want == "pass" {
						want = "500 Internal Server Error"
					}
					if got != want || count("users") != users || count("refresh_tokens") != tokens {
						t.Fatalf("linked %v: %s, users %d -> %d, refresh tokens %d -> %d; want %s and no row", linked, got,
							users, count("users"), tokens, count("refresh_tokens"), want)
					}
				case linked:
					if w.Code != http.StatusOK || count("refresh_tokens") != tokens+1 || count("users") != users {
						t.Fatalf("linked: %s (%s), refresh tokens %d -> %d; want the sign-in", got, w.Body.String(), tokens, count("refresh_tokens"))
					}
				}
			}
		})
	}
}
