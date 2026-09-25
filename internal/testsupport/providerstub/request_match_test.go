package providerstub

import (
	"encoding/json"
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
	} {
		fixture := base
		mutate(&fixture)
		if _, err := New(fixture); err == nil {
			t.Errorf("%s must be refused", name)
		}
	}
}
