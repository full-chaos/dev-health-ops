package workgraph

import (
	"bufio"
	"net"
	"net/http"
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
