package providerstub

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

func corpusFile(t *testing.T, parts ...string) string {
	t.Helper()
	_, file, _, _ := moduleroot.Caller(0)
	return filepath.Join(append([]string{filepath.Dir(file), "..", "..", ".."}, parts...)...)
}

func TestSocialStubFixturesAnswerTheSocialRow(t *testing.T) {
	stub, err := LoadDir(corpusFile(t, "ci", "auth_corpus_stub"))
	if err != nil {
		t.Fatal(err)
	}
	ask := func(token string) int {
		request, _ := http.NewRequest(http.MethodGet, "https://api.github.com/user", nil)
		request.Host = "api.github.com"
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := &responseRecorder{header: http.Header{}}
		stub.ServeHTTP(recorder, request)
		return recorder.code
	}
	if code := ask("gh-corpus"); code != 200 {
		t.Fatalf("the corpus token answered %d, want 200", code)
	}
	if code := ask("another-token"); code != 599 {
		t.Fatalf("a token no fixture names answered %d, want 599 (unstubbed)", code)
	}
	if err := CheckRecorded(stub.Requests(), "github", 1); err == nil {
		t.Fatal("the unstubbed request was not reported")
	}
}

func TestSocialRowFailsWhenTheStubSawNoRequest(t *testing.T) {
	if err := CheckRecorded(nil, "github", 1); err == nil {
		t.Fatal("no recorded request passed")
	}
	if err := CheckRecorded([]Recorded{{Provider: "pagerduty", Matched: true}}, "github", 1); err == nil {
		t.Fatal("a request for another provider passed")
	}
	if err := CheckRecorded([]Recorded{{Provider: "github", Matched: true}}, "github", 1); err != nil {
		t.Fatalf("one matched github request failed: %v", err)
	}
}

var aliasHost = regexp.MustCompile(`^\s*-\s*"([^":]+):[0-9.]+"\s*$`)

func TestStubHostsResolveOnlyProviderNames(t *testing.T) {
	for _, name := range []string{"auth_corpus_external.override.yml", "auth_corpus_external.acp.override.yml"} {
		raw, err := os.ReadFile(corpusFile(t, "ci", name))
		if err != nil {
			t.Fatal(err)
		}
		inAliases, found := false, 0
		for _, line := range strings.Split(string(raw), "\n") {
			switch {
			case strings.TrimSpace(line) == "extra_hosts:":
				inAliases = true
			case inAliases && aliasHost.MatchString(line):
				host := aliasHost.FindStringSubmatch(line)[1]
				found++
				if ProviderFor(host) == "" {
					t.Errorf("%s aliases %q, which is not a provider host", name, host)
				}
			default:
				inAliases = false
			}
		}
		if found == 0 {
			t.Errorf("%s has no extra_hosts alias for the stub", name)
		}
	}
}

type responseRecorder struct {
	header http.Header
	code   int
	body   strings.Builder
}

func (r *responseRecorder) Header() http.Header  { return r.header }
func (r *responseRecorder) WriteHeader(code int) { r.code = code }
func (r *responseRecorder) Write(b []byte) (int, error) {
	if r.code == 0 {
		r.code = 200
	}
	return r.body.Write(b)
}

// auth-service checks a GitHub token with the client's own credentials before it reads the user: the stub answers
// that check for the corpus client only, and a wrong client secret, client id or token stays unstubbed.
func TestSocialStubAnswersTheTokenCheckOnlyForTheCorpusClient(t *testing.T) {
	stub, err := LoadDir(corpusFile(t, "ci", "auth_corpus_stub"))
	if err != nil {
		t.Fatal(err)
	}
	ask := func(client, secret, token string) (int, string) {
		request, _ := http.NewRequest(http.MethodPost, "https://api.github.com/applications/"+client+"/token",
			strings.NewReader(`{"access_token":"`+token+`"}`))
		request.Host = "api.github.com"
		request.SetBasicAuth(client, secret)
		recorder := &responseRecorder{header: http.Header{}}
		stub.ServeHTTP(recorder, request)
		return recorder.code, recorder.body.String()
	}
	code, body := ask("gh-client", "gh-secret", "gh-corpus")
	var answer struct {
		App struct {
			ClientID string `json:"client_id"`
		} `json:"app"`
		User struct{ ID int } `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &answer); err != nil || code != 200 || answer.App.ClientID != "gh-client" || answer.User.ID != 910001 {
		t.Fatalf("the corpus client's token check answered %d %q, want 200 with its client id and the stubbed user (%v)", code, body, err)
	}
	for name, got := range map[string]int{
		"wrong secret":    func() int { c, _ := ask("gh-client", "other-secret", "gh-corpus"); return c }(),
		"wrong client id": func() int { c, _ := ask("other-client", "gh-secret", "gh-corpus"); return c }(),
		"wrong token":     func() int { c, _ := ask("gh-client", "gh-secret", "another-token"); return c }(),
	} {
		if got != 599 {
			t.Errorf("%s answered %d, want 599 (unstubbed)", name, got)
		}
	}
}
