package pyheaders

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeadersAddOnlyWhatIsMissing(t *testing.T) {
	cases := map[string]struct {
		handler http.HandlerFunc
		check   func(t *testing.T, header http.Header)
	}{
		"handler writes nothing": {
			handler: func(http.ResponseWriter, *http.Request) {},
			check: func(t *testing.T, header http.Header) {
				for _, pair := range securityHeaders {
					if header.Get(pair[0]) != pair[1] {
						t.Errorf("%s = %q", pair[0], header.Get(pair[0]))
					}
				}
			},
		},
		"handler writes a body without a status": {
			handler: func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("x")) },
			check: func(t *testing.T, header http.Header) {
				if header.Get("X-Frame-Options") != "DENY" {
					t.Error("header missing after an implicit 200")
				}
			},
		},
		"handler sets its own value": {
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("X-Frame-Options", "SAMEORIGIN")
				w.WriteHeader(http.StatusTeapot)
			},
			check: func(t *testing.T, header http.Header) {
				if got := header.Values("X-Frame-Options"); len(got) != 1 || got[0] != "SAMEORIGIN" {
					t.Errorf("handler value overridden: %q", got)
				}
			},
		},
		"handler flushes first": {
			handler: func(w http.ResponseWriter, _ *http.Request) { w.(http.Flusher).Flush() },
			check: func(t *testing.T, header http.Header) {
				if header.Get("Content-Security-Policy") == "" {
					t.Error("header missing after a flush")
				}
			},
		},
	}
	for name, test := range cases {
		recorder := httptest.NewRecorder()
		SecurityHeaders(test.handler).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
		// Result().Header is the header set as it was when the status line was
		// written, so a header added after that point does not count.
		t.Run(name, func(t *testing.T) { test.check(t, recorder.Result().Header) })
	}
}

func TestHeaderWriterUnwrapsAndRefusesHijackWhenUnsupported(t *testing.T) {
	recorder := httptest.NewRecorder()
	writer := &headerWriter{ResponseWriter: recorder, commit: func(http.Header) {}}
	if writer.Unwrap() != recorder {
		t.Fatal("Unwrap must return the wrapped writer")
	}
	if _, _, err := writer.Hijack(); err == nil {
		t.Fatal("Hijack on a non-hijacker must fail")
	}
}

// TestCORSJoinsAnExistingVary pins Starlette's MutableHeaders.add_vary_header:
// a Vary the handler already set is kept and "Origin" is appended to it.
func TestCORSJoinsAnExistingVary(t *testing.T) {
	for _, origins := range [][]string{{"https://a.example"}, {"*"}} {
		handler := NewCORS(origins).Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Vary", "Accept-Encoding")
			w.WriteHeader(http.StatusOK)
		}))
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.Header.Set("Origin", "https://a.example")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if got := recorder.Result().Header.Values("Vary"); len(got) != 1 || got[0] != "Accept-Encoding, Origin" {
			t.Fatalf("origins %v: Vary %q, want [Accept-Encoding, Origin]", origins, got)
		}
	}
}

// TestCORSAppendsOriginToEveryHandlerVary pins starlette 1.7.0's simple
// response: every Vary value the handler set, in order, then "Origin", as one
// Vary header -- with no Origin, an empty one, a foreign one, or an allowed
// one (the allowed one also echoed). A preflight carries its own four-part
// Vary instead.
func TestCORSAppendsOriginToEveryHandlerVary(t *testing.T) {
	cors := NewCORS([]string{"https://a.example"})
	for _, tc := range []struct {
		origin *string
		vary   []string
		want   string
		echo   string
	}{
		{nil, nil, "Origin", ""},
		{nil, []string{"Accept-Encoding"}, "Accept-Encoding, Origin", ""},
		{nil, []string{"Accept-Encoding", "X-Two"}, "Accept-Encoding, X-Two, Origin", ""},
		{ptr(""), []string{"X-Two"}, "X-Two, Origin", ""},
		{ptr("https://evil.example"), nil, "Origin", ""},
		{ptr("https://a.example"), []string{"Accept-Encoding", "X-Two"}, "Accept-Encoding, X-Two, Origin", "https://a.example"},
	} {
		vary := tc.vary
		handler := cors.Wrap(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			for _, value := range vary {
				w.Header().Add("Vary", value)
			}
			w.WriteHeader(http.StatusNotFound)
		}))
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		if tc.origin != nil {
			request.Header.Set("Origin", *tc.origin)
		}
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		got := recorder.Result().Header
		if values := got.Values("Vary"); len(values) != 1 || values[0] != tc.want || got.Get("Access-Control-Allow-Origin") != tc.echo {
			t.Errorf("origin=%v vary=%v: Vary %q ACAO %q, want %q %q", tc.origin, tc.vary, values, got.Get("Access-Control-Allow-Origin"), tc.want, tc.echo)
		}
	}
	// An allowed list holding "" (NewCORS takes any list) never echoes a
	// request that sent no Origin: Starlette's origin is None, not "".
	emptyAllowed := NewCORS([]string{"", "https://a.example"}).Wrap(http.NotFoundHandler())
	absent := httptest.NewRecorder()
	emptyAllowed.ServeHTTP(absent, httptest.NewRequest(http.MethodGet, "/", nil))
	if got := absent.Result().Header; len(got.Values("Access-Control-Allow-Origin")) != 0 || got.Get("Vary") != "Origin" {
		t.Errorf("no Origin with \"\" allowed: ACAO %q Vary %q", got.Values("Access-Control-Allow-Origin"), got.Get("Vary"))
	}
	request := httptest.NewRequest(http.MethodOptions, "/", nil)
	request.Header.Set("Origin", "https://a.example")
	request.Header.Set("Access-Control-Request-Method", "GET")
	recorder := httptest.NewRecorder()
	cors.Wrap(http.NotFoundHandler()).ServeHTTP(recorder, request)
	if got := recorder.Result().Header.Values("Vary"); len(got) != 1 ||
		got[0] != "Origin, Access-Control-Request-Method, Access-Control-Request-Headers, Access-Control-Request-Private-Network" {
		t.Errorf("preflight Vary %q", got)
	}
}

func ptr(value string) *string { return &value }
