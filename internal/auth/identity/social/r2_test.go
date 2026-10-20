package social

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// A provider that is down answers like go-api's unhandled failure (500,
// policy.WriteInternal); only the provider refusing the token is a 401.
func TestProviderFailuresAnswerLikeGoAPI(t *testing.T) {
	const internal = `{"detail":"Internal Server Error"}`
	const refused = `{"detail":{"message":"Invalid or expired provider token"}}`
	cases := []struct {
		name   string
		status int // 0: the provider is not reachable
		want   int
		body   string
	}{
		{"provider 500", http.StatusInternalServerError, http.StatusInternalServerError, internal},
		{"provider 503", http.StatusServiceUnavailable, http.StatusInternalServerError, internal},
		{"provider unreachable", 0, http.StatusInternalServerError, internal},
		{"provider refuses the token", http.StatusUnauthorized, http.StatusUnauthorized, refused},
		{"provider forbids the token", http.StatusForbidden, http.StatusUnauthorized, refused},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "{}", c.status)
			}))
			url := server.URL
			if c.status == 0 {
				server.Close()
			} else {
				defer server.Close()
			}
			creds := NewClientCredentials(testClientID, testClientSecret)
			client := &ProviderClient{
				HTTP:      &http.Client{},
				Endpoints: Endpoints{GitHubAPI: url, GitLabBase: url, GoogleUserInfo: url, GoogleTokenInfo: url},
				Clients:   map[Provider]ClientCredentials{GitHub: creds, GitLab: creds, Google: creds},
			}
			svc, err := New(Config{Store: stubStore{}, Providers: client})
			if err != nil {
				t.Fatal(err)
			}
			for _, p := range Providers() {
				rec := httptest.NewRecorder()
				Handlers{Service: svc}.Exchange(rec, httptest.NewRequest(http.MethodPost, "/",
					strings.NewReader(`{"provider":"`+string(p)+`","provider_access_token":"tok"}`)))
				if rec.Code != c.want || strings.TrimSpace(rec.Body.String()) != c.body {
					t.Errorf("%s: %d %q, want %d %q", p, rec.Code, rec.Body.String(), c.want, c.body)
				}
			}
		})
	}
}
