package session

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// parityRow is one row of ci/social_provider_email_parity.tsv: a provider email form and the answer both planes give.
type parityRow struct {
	id, provider, verified, want, shape string
	email, name                         []byte
}

func readParityRows(t *testing.T, path string) []parityRow {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []parityRow
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 7 {
			t.Fatalf("row %q: %d fields, want 7", line, len(f))
		}
		email, err := hex.DecodeString(f[2])
		if err != nil {
			t.Fatalf("row %s: email_hex: %v", f[0], err)
		}
		name := []byte(`"N"`)
		if f[4] != "-" {
			if name, err = hex.DecodeString(f[4]); err != nil {
				t.Fatalf("row %s: name_hex: %v", f[0], err)
			}
		}
		rows = append(rows, parityRow{id: f[0], provider: f[1], email: email, verified: f[3], name: name, want: f[5], shape: f[6]})
	}
	if len(rows) == 0 {
		t.Fatal("the parity table holds no rows")
	}
	return rows
}

// goAPIProviderProfile serves the row's profile as GitHub, GitLab and Google answer go-api, written raw so the email
// bytes reach the decoder unchanged.
func goAPIProviderProfile(row parityRow) http.Handler {
	confirmed := "null"
	if row.verified == "true" {
		confirmed = `"2026-01-01T00:00:00Z"`
	}
	write := func(w http.ResponseWriter, parts ...[]byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(shapeID(row.shape, bytes.Join(parts, nil)))
	}
	writeProfile := func(w http.ResponseWriter, parts ...[]byte) {
		write(w, shapeBody(row.shape, bytes.Join(parts, nil)))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/github/user" && row.provider == "github_user":
			writeProfile(w, []byte(`{"id": 7, "login": "u", "name": `), row.name, []byte(`, "email": `), row.email, []byte(`}`))
		case r.URL.Path == "/github/user" && row.provider == "github_emails":
			writeProfile(w, []byte(`{"id": 7, "login": "u", "name": `), row.name, []byte(`, "email": null}`))
		case r.URL.Path == "/github/emails" && row.provider == "github_user":
			write(w, []byte(`[]`))
		case r.URL.Path == "/github/emails" && row.provider == "github_emails":
			writeProfile(w, []byte(`[{"email": `), row.email, []byte(`, "primary": true, "verified": `+row.verified+`}]`))
		case r.URL.Path == "/gitlab/api/v4/user" && row.provider == "gitlab":
			writeProfile(w, []byte(`{"id": 7, "username": "u", "name": `), row.name, []byte(`, "email": `), row.email,
				[]byte(`, "confirmed_at": `+confirmed+`}`))
		case r.URL.Path == "/google/userinfo" && row.provider == "google":
			writeProfile(w, []byte(`{"id": "g7", "name": `), row.name, []byte(`, "email": `), row.email,
				[]byte(`, "verified_email": `+row.verified+`}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

// detailMessage is the refusal text of a {"detail": ...} body: the detail string, or its message member.
func detailMessage(body string) string {
	var parsed struct {
		Detail json.RawMessage `json:"detail"`
	}
	if json.Unmarshal([]byte(body), &parsed) != nil {
		return "<unparsed " + body + ">"
	}
	var text string
	if json.Unmarshal(parsed.Detail, &text) == nil {
		return text
	}
	var object struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(parsed.Detail, &object)
	return object.Message
}

// go-api's social login answers every row of the shared parity table as the table says; "pass" means the email guard
// let the profile through (the handler went on to the database, which the zero pool turns into a panic).
func TestSocialLoginAnswersTheProviderEmailParityTable(t *testing.T) {
	for _, row := range readParityRows(t, filepath.Join("..", "..", "..", "ci", "social_provider_email_parity.tsv")) {
		t.Run(row.id, func(t *testing.T) {
			provider := httptest.NewServer(goAPIProviderProfile(row))
			t.Cleanup(provider.Close)
			deps := completeDeps(t)
			var logs bytes.Buffer
			deps.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			deps.Getenv = func(string) string { return "set" }
			deps.OAuth = &oauthprovider.Client{HTTP: provider.Client(), Endpoints: oauthprovider.Endpoints{
				GitHubUser: provider.URL + "/github/user", GitHubEmails: provider.URL + "/github/emails",
				GitLabBase: provider.URL + "/gitlab", GoogleUser: provider.URL + "/google/userinfo"}}
			var handler http.Handler
			for _, route := range Routes(deps) {
				if route.Pattern == "/api/v1/auth/social-login" {
					handler = route.Handler
				}
			}
			name := strings.SplitN(row.provider, "_", 2)[0]
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
				strings.NewReader(`{"provider": "`+name+`", "provider_access_token": "x"}`))
			r.Header.Set("Content-Type", "application/json")
			pastGuard := false
			func() {
				defer func() {
					if recover() != nil {
						pastGuard = true
					}
				}()
				handler.ServeHTTP(w, r)
			}()
			got := "pass"
			if !pastGuard {
				got = strconv.Itoa(w.Code) + " " + detailMessage(w.Body.String())
			}
			if got != row.want {
				t.Fatalf("go-api answered %q, the table wants %q (body %s)", got, row.want, w.Body.String())
			}
			if strings.Contains(logs.String(), "MARK") {
				t.Fatal("the email value is in a log line (any level, Debug included)")
			}
		})
	}
}

// shapeBody applies a row's shape to a served profile body: "key=<K>" renames the email member to K, "utf16le" sends the
// body as UTF-16LE with a byte order mark (a body Python's response.json() reads), "-" leaves it as it is.
func shapeBody(shape string, body []byte) []byte {
	if key, ok := strings.CutPrefix(shape, "key="); ok {
		body = bytes.ReplaceAll(body, []byte(`"email": `), []byte(`"`+key+`": `))
	}
	if first, ok := strings.CutPrefix(shape, "dupfirst="); ok {
		value, err := hex.DecodeString(first)
		if err != nil {
			panic(err)
		}
		body = bytes.Replace(body, []byte(`"email": `), append(append([]byte(`"email": `), value...), []byte(`, "email": `)...), 1)
	}
	if shape == "utf16le" {
		units := utf16.Encode([]rune("\ufeff" + string(body)))
		out := make([]byte, 0, 2*len(units))
		for _, u := range units {
			out = append(out, byte(u), byte(u>>8))
		}
		return out
	}
	return body
}

// shapeID applies an "id=<digits>" shape: every numeric account id the provider serves (the profile id, GitHub's
// token-check user id, GitLab's token owner) becomes that integer, written as JSON digits.
func shapeID(shape string, body []byte) []byte {
	digits, ok := strings.CutPrefix(shape, "id=")
	if !ok {
		return body
	}
	for _, field := range []string{`"id": `, `"resource_owner_id": `} {
		body = bytes.ReplaceAll(body, []byte(field+"7"), []byte(field+digits))
	}
	return body
}
