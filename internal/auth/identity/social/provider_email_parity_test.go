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
		for _, p := range parts {
			_, _ = w.Write(p)
		}
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
		if row.verified != "true" || row.shape != "-" || strings.HasSuffix(row.id, "not_utf8_verified") {
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

// shapeBody applies a row's shape to a served profile body: "key=<K>" renames the email member to K, "utf16le" sends the
// body as UTF-16LE with a byte order mark (a body Python's response.json() reads), "-" leaves it as it is.
func shapeBody(shape string, body []byte) []byte {
	if key, ok := strings.CutPrefix(shape, "key="); ok {
		body = bytes.ReplaceAll(body, []byte(`"email": `), []byte(`"`+key+`": `))
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
