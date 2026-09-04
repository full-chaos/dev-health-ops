package workgraph

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// r3 P1.  Go formats a malformed response into the transport
// error with %q, which ESCAPES a quote -- so a token containing one reaches
// err.Error() in a form the raw-substring redaction cannot match.
func TestMalformedResponseNeverLeaksWireBytesIntoTheDurableDetail(t *testing.T) {
	token := `ab"cd`
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		// Consume the request so the client gets to read a response.
		reader := bufio.NewReader(conn)
		for {
			line, readErr := reader.ReadString('\n')
			if readErr != nil || strings.TrimSpace(line) == "" {
				break
			}
		}
		// Valid status line, then a MALFORMED HEADER echoing the credential --
		// Go quotes the whole offending line into the error with %q.
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nX-Echo " + token + " nocolon\r\n\r\n"))
	}()
	endpoint := "http://" + listener.Addr().String() + "/internal/worker/workgraph/v1/execute"
	executor, err := NewHTTPCompatibilityExecutor(&http.Client{Timeout: 5 * time.Second},
		HTTPCompatibilityConfig{Endpoint: endpoint, BearerToken: token, AllowInsecureInternal: true})
	if err != nil {
		t.Fatal(err)
	}
	_, execErr := executor.Execute(t.Context(), *testClaim(time.Second))
	if execErr == nil {
		t.Fatal("Execute succeeded against a malformed response")
	}
	detail := compatibilityAmbiguousDetail(execErr)
	if !strings.Contains(detail, "transport failure after the request was sent") {
		t.Fatalf("ledger detail %q is not the classified description", detail)
	}
	// Control: the token really WAS on the wire, so the absence below is the
	// fix working, not the construction failing to echo it.
	if !strings.Contains(token, `"`) {
		t.Fatal("the token under test must contain a quote or this proves nothing")
	}
	for _, form := range []string{token, `ab\"cd`, `ab\\"cd`} {
		if strings.Contains(detail, form) {
			t.Fatalf("bearer token reaches the durable detail as %q", form)
		}
	}
}

// The class guard. Two separate findings put a bearer token into the durable
// ledger detail -- once through the response body, once through the transport
// error's text -- and both times the fix was "sanitise harder", which failed
// because sanitising is a guess about every encoding the source might use.
//
// This asserts the PROPERTY instead of the instances: nothing the bridge puts
// on the wire, in any encoding, reaches the detail. Each case embeds a unique
// marker; the marker must never appear. A future change that starts echoing
// bridge text again fails here regardless of which path it uses or how the
// bytes are escaped.
func TestNoBridgeSuppliedByteEverReachesTheDurableDetail(t *testing.T) {
	const marker = "Zq7KxMarkerVireo"
	token := `tok"` + marker

	for _, testCase := range []struct {
		name string
		body string
		code int
	}{
		{name: "4xx body", code: http.StatusBadRequest, body: `{"detail":"` + marker + `"}`},
		{name: "5xx body", code: http.StatusInternalServerError, body: `{"detail":"` + marker + `"}`},
		{name: "409 body", code: http.StatusConflict, body: `{"reason":"` + marker + `"}`},
		{name: "2xx unrecognised status", code: http.StatusOK, body: `{"status":"` + marker + `"}`},
		// output_evidence must be genuinely ABSENT: validEvidence accepts any
		// valid JSON over one byte, so a bare string would be accepted and
		// Execute would succeed, making this case vacuous.
		{name: "2xx unusable evidence", code: http.StatusOK, body: `{"status":"success","note":"` + marker + `"}`},
		{name: "2xx undecodable", code: http.StatusOK, body: `{"status":"` + marker},
		{name: "echoed authorization header", code: http.StatusForbidden, body: `{"detail":"AUTH"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writer.WriteHeader(testCase.code)
				body := testCase.body
				if body == `{"detail":"AUTH"}` {
					body = `{"detail":"` + request.Header.Get("Authorization") + `"}`
				}
				_, _ = writer.Write([]byte(body))
			}))
			defer server.Close()
			executor, err := NewHTTPCompatibilityExecutor(&http.Client{Timeout: 5 * time.Second},
				HTTPCompatibilityConfig{
					Endpoint:    server.URL + "/internal/worker/workgraph/v1/execute",
					BearerToken: token, AllowInsecureInternal: true,
				})
			if err != nil {
				t.Fatal(err)
			}
			_, execErr := executor.Execute(t.Context(), *testClaim(time.Second))
			if execErr == nil {
				t.Fatal("Execute succeeded, want a classified failure")
			}
			detail := compatibilityAmbiguousDetail(execErr)
			if strings.Contains(detail, marker) {
				t.Fatalf("bridge-supplied bytes reached the detail: %q", detail)
			}
			// Escaped forms too -- the exact shape that defeated both prior fixes.
			for _, escaped := range []string{`\"` + marker, `\\"` + marker, token} {
				if strings.Contains(detail, escaped) {
					t.Fatalf("bridge bytes reached the detail in escaped form %q: %q", escaped, detail)
				}
			}
			// Control: the detail is not empty, so the absence above is the
			// guard working rather than there being nothing to inspect.
			if len(detail) < 20 {
				t.Fatalf("detail %q is too short to be a real diagnostic", detail)
			}
		})
	}
}
