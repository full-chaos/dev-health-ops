package social

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

func rawClient(server *httptest.Server) *ProviderClient {
	creds := NewClientCredentials(testClientID, testClientSecret)
	return &ProviderClient{HTTP: server.Client(), Endpoints: Endpoints{
		GitHubAPI: server.URL + "/github", GitLabBase: server.URL + "/gitlab",
		GoogleUserInfo: server.URL + "/google/userinfo", GoogleTokenInfo: server.URL + "/google/tokeninfo"},
		Clients: map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds}}
}

// Link refuses every row of the parity table that Exchange refuses, before the identity lookup: a linked identity
// cannot be bound by a profile go-api would refuse.
func TestLinkRefusesTheRefusedParityRowsBeforeTheLookup(t *testing.T) {
	for _, row := range readParityRows(t) {
		if row.want == "pass" {
			continue
		}
		t.Run(row.id, func(t *testing.T) {
			server := httptest.NewServer(rawProviders(row))
			t.Cleanup(server.Close)
			store := &orderStore{linked: &identity.Principal{IsActive: true}}
			now := time.Unix(1_800_000_000, 0)
			svc, err := New(Config{Store: store, Providers: rawClient(server), Now: func() time.Time { return now }})
			if err != nil {
				t.Fatal(err)
			}
			provider, _ := ParseProvider(strings.SplitN(row.provider, "_", 2)[0])
			_, err = svc.Link(context.Background(), SignedIn{PrincipalID: uuid.New(), AuthTime: now}, provider, secretOf("tok"))
			if err == nil || store.calls() != 0 {
				t.Fatalf("link: %v after %d store calls; want a refusal before any", err, store.calls())
			}
			rec := httptest.NewRecorder()
			writeError(rec, provider, err)
			if got := rec.Code; strings.Fields(row.want)[0] != strconv.Itoa(got) {
				t.Fatalf("link answered %d %s, the table wants %s", got, rec.Body.String(), row.want)
			}
		})
	}
}

// A present email member that is not a JSON string (null included) is no address, as on go-api: its guard is skipped,
// a linked identity signs in, and an unknown one is refused 403 when unverified and 500 when verified (go-api's
// str check on the email fails after its lookup). Verified follows go-api: GitHub's /user email is verified when it
// is truthy (no /user/emails read), an emails entry when it is picked, GitLab by confirmed_at, Google by
// verified_email.
func TestANonTextEmailMemberIsAnsweredAsGoAPI(t *testing.T) {
	const internal = `{"detail":"Internal Server Error"}`
	for name, tc := range map[string]struct {
		row          parityRow
		unknown      string
		emailsRead   int32
		fetchRefused bool
	}{
		"gitlab number, confirmed":   {parityRow{provider: "gitlab", email: []byte(`7`), verified: "true"}, "500 " + internal, 0, false},
		"gitlab number, unconfirmed": {parityRow{provider: "gitlab", email: []byte(`7`), verified: "false"}, "403 " + unverifiedEmailBody, 0, false},
		"gitlab null, confirmed":     {parityRow{provider: "gitlab", email: []byte(`null`), verified: "true"}, "500 " + internal, 0, false},
		"gitlab object, confirmed":   {parityRow{provider: "gitlab", email: []byte(`{"a": 1}`), verified: "true"}, "500 " + internal, 0, false},
		"google number, verified":    {parityRow{provider: "google", email: []byte(`7`), verified: "true"}, "500 " + internal, 0, false},
		"google list, unverified":    {parityRow{provider: "google", email: []byte(`[1]`), verified: "false"}, "403 " + unverifiedEmailBody, 0, false},
		"github user number":         {parityRow{provider: "github_user", email: []byte(`7`), verified: "true"}, "500 " + internal, 0, false},
		"github user true":           {parityRow{provider: "github_user", email: []byte(`true`), verified: "true"}, "500 " + internal, 0, false},
		"github user zero":           {parityRow{provider: "github_user", email: []byte(`0`), verified: "true"}, "403 " + unverifiedEmailBody, 1, true},
		"github emails entry number": {parityRow{provider: "github_emails", email: []byte(`7`), verified: "true"}, "500 " + internal, 1, false},
	} {
		t.Run(name, func(t *testing.T) {
			tc.row.name, tc.row.shape = []byte(`"N"`), "-"
			for _, linked := range []bool{true, false} {
				var emailsRead atomic.Int32
				inner := rawProviders(tc.row)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/github/user/emails" {
						emailsRead.Add(1)
					}
					inner.ServeHTTP(w, r)
				}))
				store := &orderStore{}
				if linked {
					store.linked = &identity.Principal{IsActive: true}
				}
				svc, err := New(Config{Store: store, Providers: rawClient(server)})
				if err != nil {
					t.Fatal(err)
				}
				sessions := &sessionRecorder{}
				rec := httptest.NewRecorder()
				name := strings.SplitN(tc.row.provider, "_", 2)[0]
				Handlers{Service: svc, Sessions: sessions, Auth: noAuth{}}.Exchange(rec,
					httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"provider":"`+name+`","provider_access_token":"tok"}`)))
				server.Close()
				got := strconv.Itoa(rec.Code) + " " + strings.TrimSpace(rec.Body.String())
				if linked && tc.fetchRefused && (got != tc.unknown || sessions.written.Load() != 0 || store.calls() != 0) {
					t.Fatalf("linked: %s with %d sessions and %d store calls, want %s before any lookup", got, sessions.written.Load(), store.calls(), tc.unknown)
				}
				if linked && !tc.fetchRefused && (sessions.written.Load() != 1 || rec.Code != http.StatusOK) {
					t.Fatalf("linked: %s with %d sessions, want the sign-in", got, sessions.written.Load())
				}
				if !linked {
					if got != tc.unknown || sessions.written.Load() != 0 || store.writes != 0 || store.byEmail != 0 {
						t.Fatalf("unknown: %s (sessions %d, writes %d, email reads %d), want %s", got, sessions.written.Load(), store.writes, store.byEmail, tc.unknown)
					}
				}
				if emailsRead.Load() != tc.emailsRead {
					t.Fatalf("/user/emails read %d times, want %d", emailsRead.Load(), tc.emailsRead)
				}
			}
		})
	}
}

// GitHub with no verified address is go-api's no-verified-email refusal, from the provider client, so Exchange and
// Link answer it before any lookup.
func TestGitHubWithNoVerifiedAddressIsAFetchRefusal(t *testing.T) {
	row := parityRow{provider: "github_emails", email: []byte(`"user@example.test"`), verified: "false", name: []byte(`"N"`), shape: "-"}
	server := httptest.NewServer(rawProviders(row))
	t.Cleanup(server.Close)
	_, err := rawClient(server).Fetch(context.Background(), GitHub, secretOf("tok"))
	var perr *ProviderError
	if !errors.As(err, &perr) || perr.Reason != ReasonEmailUnverified {
		t.Fatalf("fetch: %v, want a ProviderError with reason %s", err, ReasonEmailUnverified)
	}
}

// go-api checks the email member before the id (oauthprovider requiredFields): a GitLab or Google profile with neither
// is the refused token, not the invalid user id.
func TestTheEmailMemberIsCheckedBeforeTheID(t *testing.T) {
	for _, provider := range []string{"gitlab", "google"} {
		t.Run(provider, func(t *testing.T) {
			row := parityRow{provider: provider, email: []byte(`"user@example.test"`), verified: "true", name: []byte(`"N"`), shape: "noemail"}
			inner := rawProviders(row)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				rec := httptest.NewRecorder()
				inner.ServeHTTP(rec, r)
				body := strings.Replace(rec.Body.String(), `{"id": 7, `, `{`, 1)
				body = strings.Replace(body, `{"id": "g7", `, `{`, 1)
				w.WriteHeader(rec.Code)
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)
			p, _ := ParseProvider(provider)
			_, err := rawClient(server).Fetch(context.Background(), p, secretOf("tok"))
			var perr *ProviderError
			if !errors.As(err, &perr) || perr.Reason != ReasonInvalidCredentials || perr.InvalidID {
				t.Fatalf("fetch: %+v (%v), want the refused token, not the invalid id", perr, err)
			}
		})
	}
}
