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
	logs, stop, _, operatorAddress := startOnAnyPort(t, brokenEnvironment(t, anyAPIAddress, anyOperatorAddress))
	client := serviceClient(10 * time.Second)
	operator := "http://" + operatorAddress + "/healthz"
	awaitResponse(t, client, operator).Body.Close()

	held, err := net.Dial("tcp", operatorAddress)
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

// The helper's client must not keep connections: a kept one is what a later stop would wait for.
func TestServiceClientKeepsNoIdleConnection(t *testing.T) {
	transport, ok := serviceClient(time.Second).Transport.(*http.Transport)
	if !ok || !transport.DisableKeepAlives {
		t.Fatalf("serviceClient transport = %#v, want an *http.Transport with DisableKeepAlives", serviceClient(time.Second).Transport)
	}
}
