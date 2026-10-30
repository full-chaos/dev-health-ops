//go:build integration

package session

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
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
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// pinnedNonTextRow is the one pinned cell for an email member that is not text (PLAN-12026 Q4): an unknown identity is
// go-api's 500 with no row written, a linked one signs in.
const pinnedNonTextRow = "gitlab_number_email_verified"

// subjectRewrite serves inner with every account id 7 (and Google's "g7") rewritten to n, in UTF-8 and UTF-16LE
// bodies, so each row and scenario has its own provider identity; the usable address and the login / username are made
// per row too, so new accounts never collide (users.username is unique), and the address is at example.com: the
// account email check refuses the special-use .test domain.
func subjectRewrite(inner http.Handler, n int) http.Handler {
	digits := strconv.Itoa(n)
	pairs := [][2]string{{`"id": 7`, `"id": ` + digits}, {`"resource_owner_id": 7`, `"resource_owner_id": ` + digits}, {`"g7"`, `"g` + digits + `"`},
		{`user@example.test`, `user-` + digits + `@example.com`}, {`"login": "u"`, `"login": "u-` + digits + `"`},
		{`"username": "u"`, `"username": "u-` + digits + `"`}}
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

// bodyKeys is the top-level member names of a JSON answer: a failure names them and never prints a token.
func bodyKeys(body []byte) []string {
	var object map[string]json.RawMessage
	if json.Unmarshal(body, &object) != nil {
		return []string{"<not a JSON object>"}
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// unknownPassAnswer is what an unknown identity gets on a "pass" row, from the row's email form (go-api's rule, both
// planes): a JSON string the provider verified makes a new account, an unverified one is refused 403, and a verified
// value that is not a string is the 500.
func unknownPassAnswer(row parityRow) string {
	switch {
	case row.email[0] != '"':
		return "500 Internal Server Error"
	case row.verified == "false":
		return "403 The provider has not verified this email address"
	}
	return "new account"
}

// go-api's social login on a real database for every row of ci/social_provider_email_parity.tsv (the reference plane
// of PLAN-12026 Q6), for an unknown identity and for one linked to a users row: a refused row answers the table's
// answer and writes nothing, linked or not; a passing row signs the linked user in (go-api records the refresh token
// only for a user with a membership, so the linked user is a member of one organization: one refresh_tokens row). An
// unknown identity on a passing row gets unknownPassAnswer: a new account (one users row, no refresh row), or the 403 or
// 500 with nothing written (the pinned non-text row among them). Each row serves its own address. ("id=" rows stay unit
// cells.)
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
	org := uuid.New()
	pgseed.Org(ctx, t, pool, org.String(), "community")
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
					user := uuid.New()
					if _, err := pool.Exec(ctx, `INSERT INTO users (id, email, password_hash, auth_provider, auth_provider_id, is_active,
is_verified, is_superuser, token_version, created_at, updated_at) VALUES ($1, $2, NULL, $3, $4, true, true, false, 0, now(), now())`,
						user, fmt.Sprintf("linked-%d@example.com", n), name, subject); err != nil {
						t.Fatal(err)
					}
					pgseed.Membership(ctx, t, pool, uuid.NewString(), user.String(), org.String(), "member")
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
				want := row.want
				if want == "pass" && !linked {
					want = unknownPassAnswer(row)
				}
				switch {
				case want == "new account":
					if w.Code != http.StatusOK || count("users") != users+1 || count("refresh_tokens") != tokens {
						t.Fatalf("unknown: %d with body keys %v, users %d -> %d, refresh tokens %d -> %d; want a new account (no membership, no refresh row)",
							w.Code, bodyKeys(w.Body.Bytes()), users, count("users"), tokens, count("refresh_tokens"))
					}
				case want != "pass":
					if got != want || count("users") != users || count("refresh_tokens") != tokens {
						t.Fatalf("linked %v: %s, users %d -> %d, refresh tokens %d -> %d; want %s and no row", linked, got,
							users, count("users"), tokens, count("refresh_tokens"), want)
					}
				case linked:
					if w.Code != http.StatusOK || count("refresh_tokens") != tokens+1 || count("users") != users {
						t.Fatalf("linked: %d with body keys %v, refresh tokens %d -> %d; want the sign-in", w.Code, bodyKeys(w.Body.Bytes()),
							tokens, count("refresh_tokens"))
					}
				}
			}
		})
	}
}
