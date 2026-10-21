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
