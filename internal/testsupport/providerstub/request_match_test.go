package providerstub

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func send(t *testing.T, handler http.Handler, host, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Host = host
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestBodyContainsSelectsTheFixtureByTheRequestBody(t *testing.T) {
	stub, err := New(
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "viewer", Status: 201, Body: json.RawMessage(`{"which":"viewer"}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "teams", Status: 202, Body: json.RawMessage(`{"which":"teams"}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	for body, want := range map[string]int{`{"query":"{ viewer { id } }"}`: 201, `{"query":"{ teams { nodes { id } } }"}`: 202, `{"query":"{ issues { nodes { id } } }"}`: 599,
		`{"query":"{ viewer { id } teams { nodes { id } } }"}`: 201 /* first declared wins */, ``: 599} {
		if got := send(t, stub, "api.linear.app", "POST", "/graphql", body, nil); got.Code != want {
			t.Errorf("body %q answered %d, want %d", body, got.Code, want)
		}
	}
}

func TestRequestHeadersMatchByExactValueAndCanonicalName(t *testing.T) {
	stub, err := New(
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", RequestHeaders: map[string]string{"authorization": "stub-linear-key-401"}, Status: 401, Body: json.RawMessage(`{"errors":[]}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", Status: 200, Body: json.RawMessage(`{"data":{}}`)},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		headers map[string]string
		want    int
	}{
		{map[string]string{"Authorization": "stub-linear-key-401"}, 401}, {map[string]string{"Authorization": "stub-linear-key-ok"}, 200},
		{map[string]string{"Authorization": "STUB-LINEAR-KEY-401"}, 200}, {nil, 200}, {map[string]string{"Authorization": "stub-linear-key-401 "}, 200},
	} {
		if got := send(t, stub, "api.linear.app", "POST", "/graphql", `{}`, tc.headers); got.Code != tc.want {
			t.Errorf("headers %v answered %d, want %d", tc.headers, got.Code, tc.want)
		}
	}
}

func TestBodyAndHeaderValuesAreNeverRecordedLoggedOrEchoed(t *testing.T) {
	stub, _ := New(Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "viewer", RequestHeaders: map[string]string{"X-Probe": "x"}, Status: 200, Body: json.RawMessage(`{}`)})
	var lines []string
	stub.Log = func(line string) { lines = append(lines, line) }
	miss := send(t, stub, "api.linear.app", "POST", "/graphql", `{"query":"body-canary-value"}`, map[string]string{"X-Probe": "header-canary-value", "Authorization": "auth-canary-value"})
	if miss.Code != 599 {
		t.Fatalf("status %d", miss.Code)
	}
	raw, _ := json.Marshal(stub.Requests())
	all := string(raw) + strings.Join(lines, "\n") + miss.Body.String()
	for _, canary := range []string{"body-canary-value", "header-canary-value", "auth-canary-value"} {
		if strings.Contains(all, canary) {
			t.Fatalf("%s leaked: %s", canary, all)
		}
	}
}

func TestAnOversizedBodyNeverMatchesABodyFixtureAndNeverCrashes(t *testing.T) {
	stub, _ := New(Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "needle-after-the-limit", Status: 200, Body: json.RawMessage(`{}`)})
	huge := strings.Repeat("a", maxMatchBody+10) + "needle-after-the-limit"
	if got := send(t, stub, "api.linear.app", "POST", "/graphql", huge, nil); got.Code != 599 {
		t.Fatalf("an oversized body answered %d, want 599", got.Code)
	}
	// a body longer than the read limit never matches, even when the needle sits inside the part that was read
	early := "needle-after-the-limit" + strings.Repeat("a", maxMatchBody+10)
	if got := send(t, stub, "api.linear.app", "POST", "/graphql", early, nil); got.Code != 599 {
		t.Fatalf("an oversized body with an early needle answered %d, want 599", got.Code)
	}
	if got := send(t, stub, "api.linear.app", "POST", "/graphql", "x needle-after-the-limit", nil); got.Code != 200 {
		t.Fatalf("a small matching body answered %d", got.Code)
	}
}

func TestRequestMatchFieldValidation(t *testing.T) {
	base := Fixture{Provider: "linear", Method: "POST", Path: "/graphql", Status: 200}
	for name, mutate := range map[string]func(*Fixture){
		"body_contains on GET":  func(f *Fixture) { f.Method = "GET"; f.BodyContains = "x" },
		"empty header name":     func(f *Fixture) { f.RequestHeaders = map[string]string{"": "x"} },
		"empty header value":    func(f *Fixture) { f.RequestHeaders = map[string]string{"Authorization": ""} },
		"oversized body needle": func(f *Fixture) { f.BodyContains = strings.Repeat("n", maxMatchBody+1) },
		"Host header selector":  func(f *Fixture) { f.RequestHeaders = map[string]string{"Host": "api.linear.app"} },
		"host (lowercase)":      func(f *Fixture) { f.RequestHeaders = map[string]string{"host": "api.linear.app"} },
		"Transfer-Encoding":     func(f *Fixture) { f.RequestHeaders = map[string]string{"Transfer-Encoding": "chunked"} },
	} {
		fixture := base
		mutate(&fixture)
		if _, err := New(fixture); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}

// failingBody yields the first chunk, then the given error: a client that stalls or breaks mid-body.
type failingBody struct {
	first string
	err   error
	sent  bool
	reads int
}

func (b *failingBody) Read(p []byte) (int, error) {
	b.reads++
	if !b.sent {
		b.sent = true
		return copy(p, b.first), nil
	}
	return 0, b.err
}

func sendReader(handler http.Handler, method, target string, body *failingBody) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, body)
	request.Host = "api.linear.app"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func TestABodyReadErrorNeverMatchesABodyFixture(t *testing.T) {
	stub, _ := New(Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "viewer", Status: 200, Body: json.RawMessage(`{}`)})
	// the needle arrived in full before the error: a body that did not finish is still never a match
	if got := sendReader(stub, "POST", "/graphql", &failingBody{first: `{"query":"viewer"}`, err: io.ErrUnexpectedEOF}); got.Code != 599 {
		t.Fatalf("a body that ended in a read error answered %d, want 599", got.Code)
	}
	// exactly at the limit with an error: the r1 probe (1 MiB, then an error, must not read as complete)
	if got := sendReader(stub, "POST", "/graphql", &failingBody{first: "viewer" + strings.Repeat("a", maxMatchBody-6), err: io.ErrUnexpectedEOF}); got.Code != 599 {
		t.Fatalf("a body cut at the limit by a read error answered %d, want 599", got.Code)
	}
}

func TestAFixtureWithoutABodySelectorNeverReadsTheRequestBody(t *testing.T) {
	stub, _ := New(
		Fixture{Provider: "github", Method: "GET", Path: "/user", Status: 200, Body: json.RawMessage(`{}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "viewer", Status: 201, Body: json.RawMessage(`{}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/other", Status: 202, Body: json.RawMessage(`{}`)},
	)
	for _, tc := range []struct{ method, host, path string }{{"GET", "api.github.com", "/user"}, {"HEAD", "api.github.com", "/user"}, {"OPTIONS", "api.github.com", "/user"}, {"POST", "api.linear.app", "/other"}} {
		body := &failingBody{err: errors.New("stalled")}
		request := httptest.NewRequest(tc.method, tc.path, body)
		request.Host = tc.host
		recorder := httptest.NewRecorder()
		stub.ServeHTTP(recorder, request)
		if body.reads != 0 {
			t.Errorf("%s %s read the request body %d times: a fixture with no body selector must not wait on it", tc.method, tc.path, body.reads)
		}
	}
	// a request that reaches a body fixture reads the body once, however many body fixtures follow
	stub, _ = New(
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "viewer", Status: 201, Body: json.RawMessage(`{}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", BodyContains: "teams", Status: 202, Body: json.RawMessage(`{}`)},
	)
	body := &failingBody{first: "teams", err: io.EOF}
	if got := sendReader(stub, "POST", "/graphql", body); got.Code != 202 {
		t.Fatalf("answered %d, want 202", got.Code)
	}
	if body.reads != 2 { // first chunk + EOF: one ReadAll, not one per fixture
		t.Errorf("body read calls = %d, want 2 (a single ReadAll)", body.reads)
	}
}

func TestADuplicatedRequestHeaderNeverMatches(t *testing.T) {
	stub, _ := New(
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", RequestHeaders: map[string]string{"Authorization": "stub-key"}, Status: 401, Body: json.RawMessage(`{}`)},
		Fixture{Provider: "linear", Method: "POST", Path: "/graphql", Status: 200, Body: json.RawMessage(`{}`)},
	)
	for _, values := range [][]string{{"stub-key", "different-key"}, {"different-key", "stub-key"}, {"stub-key", "stub-key"}} {
		request := httptest.NewRequest("POST", "/graphql", strings.NewReader("{}"))
		request.Host = "api.linear.app"
		request.Header["Authorization"] = values
		recorder := httptest.NewRecorder()
		stub.ServeHTTP(recorder, request)
		if recorder.Code != 200 {
			t.Errorf("Authorization %v answered %d, want the general 200 (a repeated header never selects a header fixture)", values, recorder.Code)
		}
	}
}
