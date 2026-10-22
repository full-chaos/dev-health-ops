package venueoracle

import (
	"strings"
	"testing"
)

func response(status int, body string, headers map[string]string) Response {
	return Response{Status: status, Body: body, Headers: headers}
}

// Compare is the oracle's verdict; each case plants one difference it must
// see, or one ruled difference it must not.
func TestCompareDecisionTable(t *testing.T) {
	base := map[string]string{"content-type": "application/json", "content-length": "2", "date": "d1", "server": "s1", "x-request-id": "r1"}
	with := func(extra map[string]string, drop ...string) map[string]string {
		out := map[string]string{}
		for key, value := range base {
			out[key] = value
		}
		for key, value := range extra {
			out[key] = value
		}
		for _, key := range drop {
			delete(out, key)
		}
		return out
	}
	blankID := func(_ Request, body string) string {
		if strings.HasPrefix(body, `{"id":`) {
			return `{"id":"<id>"}`
		}
		return body
	}
	for _, tc := range []struct {
		name     string
		py, gr   Response
		options  DiffOptions
		request  Request
		wantSame bool
	}{
		{"identical", response(200, "{}", base), response(200, "{}", base), DiffOptions{}, Request{}, true},
		{"volatile headers differ", response(200, "{}", base), response(200, "{}", with(map[string]string{"date": "d2", "server": "s2", "x-request-id": "r2"})), DiffOptions{}, Request{}, true},
		{"status differs", response(200, "{}", base), response(201, "{}", base), DiffOptions{}, Request{}, false},
		{"body differs", response(200, "{}", base), response(200, "[]", base), DiffOptions{}, Request{}, false},
		{"header value differs", response(200, "{}", base), response(200, "{}", with(map[string]string{"content-type": "text/plain"})), DiffOptions{}, Request{}, false},
		{"header only on go", response(200, "{}", base), response(200, "{}", with(map[string]string{"vary": "Origin"})), DiffOptions{}, Request{}, false},
		{"header only on python", response(200, "{}", with(map[string]string{"vary": "Origin"})), response(200, "{}", base), DiffOptions{}, Request{}, false},
		{"empty header only on go", response(200, "{}", base), response(200, "{}", with(map[string]string{"vary": ""})), DiffOptions{}, Request{}, false},
		{"content-length differs", response(200, "{}", base), response(200, "{}", with(map[string]string{"content-length": "3"})), DiffOptions{}, Request{}, false},
		{"allow order differs", response(405, "{}", with(map[string]string{"allow": "POST, GET"})), response(405, "{}", with(map[string]string{"allow": "GET, POST"})), DiffOptions{}, Request{}, true},
		{"allow set differs", response(405, "{}", with(map[string]string{"allow": "POST, GET"})), response(405, "{}", with(map[string]string{"allow": "GET"})), DiffOptions{}, Request{}, false},
		{"admin 405 Allow widened to every served method", response(405, "{}", with(map[string]string{"allow": "GET"})), response(405, "{}", with(map[string]string{"allow": "DELETE, GET, PATCH"})), DiffOptions{}, Request{Method: "PUT", Path: "/api/v1/admin/teams/eng"}, true},
		{"widened Allow off the admin prefix", response(405, "{}", with(map[string]string{"allow": "GET"})), response(405, "{}", with(map[string]string{"allow": "DELETE, GET, PATCH"})), DiffOptions{}, Request{Method: "PUT", Path: "/api/v1/teams/eng"}, false},
		{"admin Allow that drops a frozen method", response(405, "{}", with(map[string]string{"allow": "GET, POST"})), response(405, "{}", with(map[string]string{"allow": "DELETE, GET, PATCH"})), DiffOptions{}, Request{Method: "PUT", Path: "/api/v1/admin/teams/eng"}, false},
		{"admin Allow of the same size and another method", response(405, "{}", with(map[string]string{"allow": "GET"})), response(405, "{}", with(map[string]string{"allow": "POST"})), DiffOptions{}, Request{Method: "PUT", Path: "/api/v1/admin/teams/eng"}, false},
		{"widened Allow with another body", response(405, "{}", with(map[string]string{"allow": "GET"})), response(405, `{"x":1}`, with(map[string]string{"allow": "DELETE, GET"})), DiffOptions{}, Request{Method: "PUT", Path: "/api/v1/admin/teams/eng"}, false},
		{"normalized body skips length", response(200, `{"id":"a"}`, with(map[string]string{"content-length": "10"})), response(200, `{"id":"bb"}`, with(map[string]string{"content-length": "11"})), DiffOptions{Normalize: blankID}, Request{}, true},
		{"normalized body still differs", response(200, `{"id":"a"}`, base), response(200, `{"x":1}`, base), DiffOptions{Normalize: blankID}, Request{}, false},
		{"unnormalized body keeps length", response(200, "{}", base), response(200, "{}", with(map[string]string{"content-length": "3"})), DiffOptions{Normalize: blankID}, Request{}, false},
		{"ruled length skip", response(200, "", base), response(200, "", with(map[string]string{"content-length": "9"})), DiffOptions{SkipContentLength: func(r Request) bool { return r.Method == "HEAD" }}, Request{Method: "HEAD"}, true},
		{"ruled length skip other request", response(200, "", base), response(200, "", with(map[string]string{"content-length": "9"})), DiffOptions{SkipContentLength: func(r Request) bool { return r.Method == "HEAD" }}, Request{Method: "GET"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pyHeaders := clone(tc.py).Headers
			same, _, _, _ := Compare(tc.request, tc.py, tc.gr, tc.options)
			if same != tc.wantSame {
				t.Fatalf("same = %v, want %v", same, tc.wantSame)
			}
			for key, value := range pyHeaders {
				if tc.py.Headers[key] != value {
					t.Fatalf("Compare changed the caller's headers: %s", key)
				}
			}
		})
	}
}

func TestAdminNoStoreIsAnIntendedDivergenceOnlyWhenTheGoldenLacksIt(t *testing.T) {
	request := Request{Method: "GET", Path: "/api/v1/admin/llm-settings/models"}
	python := func(headers map[string]string) Response { return Response{Status: 200, Headers: headers, Body: "{}"} }
	cases := []struct {
		name    string
		request Request
		py      map[string]string
		gr      map[string]string
		same    bool
	}{
		{"golden lacks it, go sends no-store", request, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"go sends another value", request, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-cache"}, false},
		{"go sends no-store, with a suffix", request, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store, private"}, false},
		{"golden has it, values match", request, map[string]string{"a": "1", "cache-control": "no-store"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"golden has it, values differ", request, map[string]string{"a": "1", "cache-control": "public"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"credentials route", Request{Method: "GET", Path: "/api/v1/admin/credentials"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"customer-push route", Request{Method: "POST", Path: "/api/v1/admin/customer-push/tokens"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"sync-configs route", Request{Method: "GET", Path: "/api/v1/admin/sync-configs/abc"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"integrations route", Request{Method: "POST", Path: "/api/v1/admin/integrations/abc/sync"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"teams route", Request{Method: "GET", Path: "/api/v1/admin/teams/eng"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"credentials route, golden has another value", Request{Method: "GET", Path: "/api/v1/admin/credentials"}, map[string]string{"a": "1", "cache-control": "private"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"trailing-slash redirect of an admin route", Request{Method: "GET", Path: "/api/v1/admin/credentials/"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"admin-prefixed path no package mounts", Request{Method: "GET", Path: "/api/v1/admin/unmounted"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"billing route is not admin-prefixed", Request{Method: "GET", Path: "/api/v1/billing/plans"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"admin route with a path parameter", Request{Method: "GET", Path: "/api/v1/admin/integrations/pagerduty/webhook-bindings/abc"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"admin route, wrong method", Request{Method: "PATCH", Path: "/api/v1/admin/llm-settings/models"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"encoded slash that decodes to an admin route", Request{Method: "GET", Path: "/api%2Fv1/admin/llm-settings/models"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"encoded slash inside a parameter is a 404, not exempt", Request{Method: "GET", Path: "/api/v1/admin/users/a%2Fb"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
		{"dot segment fills a parameter", Request{Method: "GET", Path: "/api/v1/admin/users/.."}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, true},
		{"outside admin scope", Request{Method: "GET", Path: "/api/v1/other"}, map[string]string{"a": "1"}, map[string]string{"a": "1", "cache-control": "no-store"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			same, _, _, _ := Compare(tc.request, python(tc.py), Response{Status: 200, Headers: tc.gr, Body: "{}"}, DiffOptions{})
			if same != tc.same {
				t.Fatalf("same = %v, want %v", same, tc.same)
			}
		})
	}
}
