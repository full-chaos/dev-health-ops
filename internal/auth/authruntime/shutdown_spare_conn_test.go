package authruntime

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// A connection that never sent a request (a Go client's spare dial) must not hold the operator listener's stop:
// http.Server.Shutdown waits up to 5 s for such a connection, longer than the operator's share of the shutdown
// timeout, and Execute would exit 1.
func TestAConnectionThatSentNoRequestDoesNotHoldTheOperatorStop(t *testing.T) {
	ports := reservePorts(t, 2)
	logs, stop := runService(t, brokenEnvironment(t, ports[0], ports[1]))
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	operator := "http://" + ports[1] + "/healthz"
	awaitResponse(t, client, operator).Body.Close()

	held, err := net.Dial("tcp", ports[1])
	if err != nil {
		t.Fatalf("dial the operator listener: %v", err)
	}
	defer held.Close()
	// The listener accepts in order and tracks each connection before it accepts the next one, so once a request on
	// a later connection is answered, the held connection is tracked as one that sent no request.
	response := awaitResponse(t, client, operator)
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d with an idle client connection on the operator listener, want 0; logs:\n%s", code, logs.String())
	}
}
