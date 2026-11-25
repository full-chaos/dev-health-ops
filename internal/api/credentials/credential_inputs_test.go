package credentials

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

const wantKeyPathRefusal = `{"detail":"private_key_path is not supported: send the key as private_key"}`

const inputsStubBase = "https://stub.example.test"

// inputsHarness serves the credential handlers with an in-memory provider that counts every request it receives.
type inputsHarness struct {
	h    handlers
	hits atomic.Int32
}

func newInputsHarness(t *testing.T) *inputsHarness {
	t.Helper()
	harness := &inputsHarness{}
	transport := roundTripperFunc(func(request *http.Request) (*http.Response, error) {
		harness.hits.Add(1)
		return &http.Response{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"message":"Bad credentials"}`)),
			Request:    request,
		}, nil
	})
	harness.h = newHandlers(Deps{
		HTTPClient: &http.Client{Transport: transport},
		HostLookup: func(context.Context, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("93.184.216.34")}, nil
		},
		Logger: slog.New(slog.DiscardHandler),
	})
	return harness
}

// serve runs handler on a JSON body for an org admin of "org-1". The unit harness has no store, so a handler that goes
// on to a store read stops there; that is reported as reachedStore, never as an answer.
func (harness *inputsHarness) serve(t *testing.T, handler func(handlers, http.ResponseWriter, *http.Request), method, payload string, pathValues map[string]string) (recorder *httptest.ResponseRecorder, reachedStore any) {
	t.Helper()
	request := httptest.NewRequest(method, "/", strings.NewReader(payload))
	request.Header.Set("Content-Type", "application/json")
	for name, value := range pathValues {
		request.SetPathValue(name, value)
	}
	body, outcome, _, err := pybody.Read(request)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("read the body: %v %v", outcome, err)
	}
	ctx := policy.WithUser(policy.WithBody(context.Background(), body), &policy.User{OrgID: "org-1", Role: "admin"})
	recorder = httptest.NewRecorder()
	func() {
		defer func() { reachedStore = recover() }()
		handler(harness.h, recorder, request.WithContext(ctx))
	}()
	return recorder, reachedStore
}

// keyPathStates are a readable key file, a path that cannot be read as a file, and a path that does not exist.
func keyPathStates(t *testing.T) map[string]string {
	t.Helper()
	dir := t.TempDir()
	readable := filepath.Join(dir, "app.pem")
	if err := os.WriteFile(readable, []byte(testPrivateKey(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(dir, "a-directory")
	if err := os.Mkdir(unreadable, 0o700); err != nil {
		t.Fatal(err)
	}
	return map[string]string{"readable": readable, "unreadable": unreadable, "absent": filepath.Join(dir, "absent.pem")}
}

func testConnectionPayload(spelling, path string) string {
	return `{"provider":"github","credentials":{"app_id":"1","installation_id":"2",` + strconv.Quote(spelling) + `:` + strconv.Quote(path) +
		`,"base_url":"` + inputsStubBase + `"}}`
}

func assertKeyPathRefused(t *testing.T, recorder *httptest.ResponseRecorder, reachedStore any, path string) {
	t.Helper()
	if reachedStore != nil {
		t.Errorf("the handler went on to the store (%v); want the refusal before any store read", reachedStore)
		return
	}
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("status %d, want 400", recorder.Code)
	}
	if got := strings.TrimSpace(recorder.Body.String()); got != wantKeyPathRefusal {
		t.Errorf("body %s, want %s", got, wantKeyPathRefusal)
	}
	if path != "" && strings.Contains(recorder.Body.String(), filepath.Base(path)) {
		t.Errorf("the answer echoes the path it was sent: %s", recorder.Body.String())
	}
}

func TestConnectionTestRefusesPrivateKeyPath(t *testing.T) {
	harness := newInputsHarness(t)
	path := keyPathStates(t)["readable"]
	for _, spelling := range []string{"private_key_path", "privateKeyPath"} {
		t.Run(spelling, func(t *testing.T) {
			before := harness.hits.Load()
			recorder, reachedStore := harness.serve(t, handlers.testConnection, http.MethodPost, testConnectionPayload(spelling, path), nil)
			if sent := harness.hits.Load() - before; sent != 0 {
				t.Errorf("the test sent %d request(s) to the provider; want 0", sent)
			}
			assertKeyPathRefused(t, recorder, reachedStore, path)
		})
	}
	// Control: the same provider is reached by an inline key, so the zero above is not a dead stub.
	before := harness.hits.Load()
	_, _, _ = harness.h.probe(context.Background(), "github", decodeObject(t,
		`{"app_id":"1","installation_id":"2","private_key":`+strconv.Quote(testPrivateKey(t))+`,"base_url":"`+inputsStubBase+`"}`))
	if sent := harness.hits.Load() - before; sent != 1 {
		t.Errorf("control: an inline key sent %d request(s) to the provider; want 1", sent)
	}
}

func TestConnectionTestAnswersEveryPathStateAlike(t *testing.T) {
	harness := newInputsHarness(t)
	states := keyPathStates(t)
	routeBodies := map[string]string{}
	probeAnswers := map[string]string{}
	for _, state := range []string{"readable", "unreadable", "absent"} {
		path := states[state]
		recorder, reachedStore := harness.serve(t, handlers.testConnection, http.MethodPost, testConnectionPayload("private_key_path", path), nil)
		t.Run(state, func(t *testing.T) { assertKeyPathRefused(t, recorder, reachedStore, path) })
		routeBodies[state] = strconv.Itoa(recorder.Code) + " " + recorder.Body.String()

		success, details, err := harness.h.probe(context.Background(), "github", decodeObject(t,
			`{"app_id":"1","installation_id":"2","private_key_path":`+strconv.Quote(path)+`,"base_url":"`+inputsStubBase+`"}`))
		answer := strconv.FormatBool(success) + " " + strconv.FormatBool(err != nil)
		if details != nil {
			answer += " " + dump(t, details)
		}
		probeAnswers[state] = answer
	}
	for _, state := range []string{"unreadable", "absent"} {
		if routeBodies[state] != routeBodies["readable"] {
			t.Errorf("the route answers a %s path %q and a readable one %q; want one answer", state, routeBodies[state], routeBodies["readable"])
		}
		if probeAnswers[state] != probeAnswers["readable"] {
			t.Errorf("the probe answers a %s path %q and a readable one %q; want one answer", state, probeAnswers[state], probeAnswers["readable"])
		}
	}
	if sent := harness.hits.Load(); sent != 0 {
		t.Errorf("%d request(s) reached the provider; want 0", sent)
	}
}

func TestCredentialCreateRefusesPrivateKeyPath(t *testing.T) {
	harness := newInputsHarness(t)
	path := keyPathStates(t)["readable"]
	for _, spelling := range []string{"private_key_path", "privateKeyPath"} {
		t.Run(spelling, func(t *testing.T) {
			payload := `{"provider":"github","name":"app","credentials":{"app_id":"1","installation_id":"2",` + strconv.Quote(spelling) + `:` + strconv.Quote(path) + `}}`
			recorder, reachedStore := harness.serve(t, handlers.create, http.MethodPost, payload, nil)
			assertKeyPathRefused(t, recorder, reachedStore, path)
		})
	}
}

func TestCredentialPatchRefusesPrivateKeyPath(t *testing.T) {
	harness := newInputsHarness(t)
	path := keyPathStates(t)["readable"]
	for _, spelling := range []string{"private_key_path", "privateKeyPath"} {
		t.Run(spelling, func(t *testing.T) {
			payload := `{"credentials":{"app_id":"1","installation_id":"2",` + strconv.Quote(spelling) + `:` + strconv.Quote(path) + `}}`
			recorder, reachedStore := harness.serve(t, handlers.update, http.MethodPatch, payload, map[string]string{"provider": "github", "name": "app"})
			assertKeyPathRefused(t, recorder, reachedStore, path)
		})
	}
}
