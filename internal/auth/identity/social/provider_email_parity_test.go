package social

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"unicode/utf16"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// parityRow is one row of ci/social_provider_email_parity.tsv: a provider email form and the answer both planes give.
type parityRow struct {
	id, provider, verified, want, shape string
	email, name                         []byte
}

func readParityRows(t *testing.T) []parityRow {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "ci", "social_provider_email_parity.tsv"))
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

// rawProviders serves the row's profile through the token checks and profile endpoints auth-service's provider client
// calls, written raw so the email bytes reach the decoder unchanged.
func rawProviders(row parityRow) http.Handler {
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
		case r.Method == http.MethodPost && r.URL.Path == "/github/applications/"+testClientID+"/token":
			write(w, []byte(`{"app": {"client_id": "`+testClientID+`"}, "user": {"id": 7}}`))
		case r.URL.Path == "/github/user" && row.provider == "github_user":
			writeProfile(w, []byte(`{"id": 7, "login": "u", "name": `), row.name, []byte(`, "email": `), row.email, []byte(`}`))
		case r.URL.Path == "/github/user" && row.provider == "github_emails":
			writeProfile(w, []byte(`{"id": 7, "login": "u", "name": `), row.name, []byte(`, "email": null}`))
		case r.URL.Path == "/github/user/emails" && row.provider == "github_user":
			write(w, []byte(`[]`))
		case r.URL.Path == "/github/user/emails" && row.provider == "github_emails":
			writeProfile(w, []byte(`[{"email": `), row.email, []byte(`, "primary": true, "verified": `+row.verified+`}]`))
		case r.URL.Path == "/gitlab/oauth/token/info":
			write(w, []byte(`{"resource_owner_id": 7, "application": {"uid": "`+testClientID+`"}}`))
		case r.URL.Path == "/gitlab/api/v4/user" && row.provider == "gitlab":
			writeProfile(w, []byte(`{"id": 7, "username": "u", "name": `), row.name, []byte(`, "email": `), row.email,
				[]byte(`, "confirmed_at": `+confirmed+`}`))
		case r.URL.Path == "/google/tokeninfo":
			write(w, []byte(`{"issued_to": "`+testClientID+`", "audience": "`+testClientID+`", "user_id": "g7"}`))
		case r.URL.Path == "/google/userinfo" && row.provider == "google":
			writeProfile(w, []byte(`{"id": "g7", "name": `), row.name, []byte(`, "email": `), row.email,
				[]byte(`, "verified_email": `+row.verified+`}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
}

// parityDetail is the refusal text of a {"detail": ...} body: the detail string, or its message member.
func parityDetail(body string) string {
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

// auth-service's social exchange, through its real provider client, answers every row of the shared parity table as
// go-api does; "pass" means the email guard let the profile through to the identity lookup.
func TestExchangeAnswersTheProviderEmailParityTable(t *testing.T) {
	for _, row := range readParityRows(t) {
		t.Run(row.id, func(t *testing.T) {
			server := httptest.NewServer(rawProviders(row))
			t.Cleanup(server.Close)
			creds := NewClientCredentials(testClientID, testClientSecret)
			client := &ProviderClient{HTTP: server.Client(), Endpoints: Endpoints{
				GitHubAPI: server.URL + "/github", GitLabBase: server.URL + "/gitlab",
				GoogleUserInfo: server.URL + "/google/userinfo", GoogleTokenInfo: server.URL + "/google/tokeninfo"},
				Clients: map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds}}
			store := &orderStore{linked: &identity.Principal{IsActive: true}}
			var logs strings.Builder
			svc, err := New(Config{Store: store, Providers: client,
				Logger: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))})
			if err != nil {
				t.Fatal(err)
			}
			name := strings.SplitN(row.provider, "_", 2)[0]
			rec := httptest.NewRecorder()
			Handlers{Service: svc, Sessions: &sessionRecorder{}, Auth: noAuth{}}.Exchange(rec,
				httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"provider":"`+name+`","provider_access_token":"tok"}`)))
			got := strconv.Itoa(rec.Code) + " " + parityDetail(rec.Body.String())
			if store.lookups > 0 {
				got = "pass"
			}
			if got != row.want {
				t.Fatalf("auth-service answered %q, the table wants %q (body %s)", got, row.want, rec.Body.String())
			}
			if digits, ok := strings.CutPrefix(row.shape, "id="); ok && store.subject != digits {
				t.Fatalf("looked up subject %q, want the provider's integer id %s exactly", store.subject, digits)
			}
			if strings.Contains(logs.String(), "MARK") {
				t.Fatal("the email value is in a log line (any level, Debug included)")
			}
		})
	}
}

// The provider client makes a profile unverified only for a lone surrogate in the email member: a lone surrogate in
// the name leaves a verified profile verified (go-api replaces it), and a literal U+FFFD leaves it verified too.
func TestFetchUnverifiesOnlyALoneSurrogateEmail(t *testing.T) {
	for _, row := range readParityRows(t) {
		if row.verified != "true" || row.shape != "-" || strings.HasSuffix(row.id, "not_utf8_verified") || row.email[0] != '"' {
			continue
		}
		t.Run(row.id, func(t *testing.T) {
			server := httptest.NewServer(rawProviders(row))
			t.Cleanup(server.Close)
			creds := NewClientCredentials(testClientID, testClientSecret)
			client := &ProviderClient{HTTP: server.Client(), Endpoints: Endpoints{
				GitHubAPI: server.URL + "/github", GitLabBase: server.URL + "/gitlab",
				GoogleUserInfo: server.URL + "/google/userinfo", GoogleTokenInfo: server.URL + "/google/tokeninfo"},
				Clients: map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds}}
			provider, _ := ParseProvider(strings.SplitN(row.provider, "_", 2)[0])
			profile, err := client.Fetch(context.Background(), provider, secretOf("tok"))
			if err != nil {
				t.Fatal(err)
			}
			wantVerified := !strings.Contains(row.id, "lone_surrogate_verified")
			if profile.EmailVerified != wantVerified {
				t.Fatalf("EmailVerified = %v, want %v", profile.EmailVerified, wantVerified)
			}
		})
	}
}

// noEmailMember is an "email" member with its value and the separator after it.
var noEmailMember = regexp.MustCompile(`"email": (?:"(?:[^"\\]|\\.)*"|[^,}\]]*)(?:, )?`)

// shapeBody applies a row's shape to a served profile body: "a+b" applies a then b, "noid" drops the profile's "id"
// member, "noemail" drops every "email" member, "key=<K>" renames the email member to K, "utf16le" sends the
// body as UTF-16LE with a byte order mark (a body Python's response.json() reads), "-" leaves it as it is.
func shapeBody(shape string, body []byte) []byte {
	if strings.Contains(shape, "+") {
		for _, one := range strings.Split(shape, "+") {
			body = shapeBody(one, body)
		}
		return body
	}
	if shape == "noid" {
		body = bytes.Replace(body, []byte(`{"id": 7, `), []byte(`{`), 1)
		body = bytes.Replace(body, []byte(`{"id": "g7", `), []byte(`{`), 1)
	}
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
	if shape == "noemail" {
		body = bytes.ReplaceAll(noEmailMember.ReplaceAll(body, nil), []byte(", }"), []byte("}"))
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

// The email member is the exact key "email", as go-api reads it: a member under "Email" is no email, so with both keys
// the email and the lone-surrogate check read "email" only.
func TestFetchReadsTheExactEmailMember(t *testing.T) {
	for name, tc := range map[string]struct {
		body         string
		wantPrefix   string
		wantVerified bool
	}{
		"surrogate under email, usable under Email": {`{"id": 7, "login": "u", "email": "MARK\ud800@example.test", "Email": "user@example.test"}`, "MARK", false},
		"usable under email, surrogate under Email": {`{"id": 7, "login": "u", "email": "user@example.test", "Email": "MARK\ud800@example.test"}`, "user@", true},
		"usable under Email, surrogate under email": {`{"id": 7, "login": "u", "Email": "user@example.test", "email": "MARK\ud800@example.test"}`, "MARK", false},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/github/applications/" + testClientID + "/token":
					_, _ = w.Write([]byte(`{"app": {"client_id": "` + testClientID + `"}, "user": {"id": 7}}`))
				case "/github/user":
					_, _ = w.Write([]byte(tc.body))
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			t.Cleanup(server.Close)
			client := &ProviderClient{HTTP: server.Client(), Endpoints: Endpoints{GitHubAPI: server.URL + "/github"},
				Clients: map[Provider]ClientCredentials{GitHub: NewClientCredentials(testClientID, testClientSecret)}}
			profile, err := client.Fetch(context.Background(), GitHub, secretOf("tok"))
			if err != nil {
				t.Fatal(err)
			}
			if profile.EmailVerified != tc.wantVerified || !strings.HasPrefix(profile.Email, tc.wantPrefix) {
				t.Fatalf("verified %v (email prefix %v), want %v from the member starting %q", profile.EmailVerified,
					strings.HasPrefix(profile.Email, tc.wantPrefix), tc.wantVerified, tc.wantPrefix)
			}
		})
	}
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
