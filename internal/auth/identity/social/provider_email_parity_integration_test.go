//go:build integration

package social

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf16"
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

// unknownPassAnswer is what an unknown identity gets on a "pass" row, from the row's email form (go-api's rule, both
// planes): an unverified address is refused 403 (go-api checks verified before the text), a verified value that is not
// a string is the 500, and a verified JSON string makes a new account.
func unknownPassAnswer(row parityRow) string {
	switch {
	case row.verified == "false":
		return "403 The provider has not verified this email address"
	case row.email[0] != '"':
		return "500 Internal Server Error"
	}
	return "new account"
}

// The full sign-in on a real identity store for every row of ci/social_provider_email_parity.tsv (PLAN-12026 Q6), for
// an unknown identity and for one linked to a go-api account: a refused row answers the table's answer and writes
// nothing, linked or not; a passing row signs the linked account in, and gives an unknown identity unknownPassAnswer:
// a new account (users owned by auth-service: 200 and a session; not owned: the sign-up refusal, nothing written), or
// the 403 or 500 with nothing written (the pinned non-text row among them). Each row serves its own address. ("id=" rows
// serve one fixed id and stay unit cells.)
func TestExchangeParityTableOnARealIdentityStore(t *testing.T) {
	ctx := context.Background()
	e := newEnv(t, ctx)
	answered := 0
	for i, row := range readParityRows(t) {
		if strings.HasPrefix(row.shape, "id=") {
			continue
		}
		t.Run(row.id, func(t *testing.T) {
			provider, _ := ParseProvider(strings.SplitN(row.provider, "_", 2)[0])
			for _, scenario := range []struct {
				name   string
				linked bool
				owns   bool
			}{{"unknown", false, false}, {"unknown, users owned", false, true}, {"linked", true, false}} {
				n := 10000 + 3*i
				switch {
				case scenario.linked:
					n += 2
				case scenario.owns:
					n++
				}
				subject := strconv.Itoa(n)
				if provider == Google {
					subject = "g" + subject
				}
				if scenario.linked {
					e.goAPIUser(t, ctx, fmt.Sprintf("linked-%d@example.com", n), provider, subject, true)
				}
				server := httptest.NewServer(subjectRewrite(rawProviders(row), n))
				svc, err := New(Config{Store: e.store(scenario.owns), Providers: rawClient(server), Now: func() time.Time { return e.now }})
				if err != nil {
					t.Fatal(err)
				}
				before := e.state(t, ctx)
				users := e.count(t, ctx, `SELECT count(*) FROM public.users`)
				sessions := &sessionRecorder{}
				rec := httptest.NewRecorder()
				Handlers{Service: svc, Sessions: sessions, Auth: noAuth{}}.Exchange(rec, httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(`{"provider":"`+string(provider)+`","provider_access_token":"tok"}`)))
				server.Close()
				got := strconv.Itoa(rec.Code) + " " + parityDetail(rec.Body.String())
				want := row.want
				if want == "pass" && !scenario.linked {
					want = unknownPassAnswer(row)
					if want == "new account" && !scenario.owns {
						want = "403 Sign-up with " + providerLabel(provider) + " is not available"
					}
				}
				switch {
				case want == "new account":
					if rec.Code != http.StatusOK || sessions.written.Load() != 1 {
						t.Fatalf("%s: %s with %d sessions, want a new account and its session", scenario.name, got, sessions.written.Load())
					}
				case want != "pass":
					if got != want || sessions.written.Load() != 0 {
						t.Fatalf("%s: %s with %d sessions, want %s and none", scenario.name, got, sessions.written.Load(), want)
					}
					e.unchanged(t, ctx, before)
					if after := e.count(t, ctx, `SELECT count(*) FROM public.users`); after != users {
						t.Fatalf("%s: public.users %d -> %d", scenario.name, users, after)
					}
				case scenario.linked:
					if rec.Code != http.StatusOK || sessions.written.Load() != 1 {
						t.Fatalf("%s: %s with %d sessions, want the sign-in", scenario.name, got, sessions.written.Load())
					}
				}
			}
			answered++
		})
	}
	if rows, idRows := parityTableCount(t); answered != rows-idRows {
		t.Fatalf("auth-service answered %d rows, the table holds %d apart from its %d \"id=\" rows", answered, rows-idRows, idRows)
	}
}
