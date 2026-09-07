package operational

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// isAmbiguous reports whether err is (or wraps) an *AmbiguousSendError.
func isAmbiguous(err error) (*AmbiguousSendError, bool) {
	var ambiguous *AmbiguousSendError
	return ambiguous, errors.As(err, &ambiguous)
}

// ---------------------------------------------------------------------------
// CHAOS-5399: resend outcome classification.
// ---------------------------------------------------------------------------

func TestResendSenderClassifiesHTTPStatusOutcomes(t *testing.T) {
	for _, test := range []struct {
		name          string
		status        int
		body          string
		wantErr       bool
		wantAmbiguous bool
	}{
		{"2xx with no error object is a clean send", http.StatusOK, `{"id":"em_1"}`, false, false},
		{"2xx with an explicit error object is a clean rejection",
			http.StatusOK, `{"error":{"message":"invalid recipient"}}`, true, false},
		{"2xx with an unparseable body is ambiguous",
			http.StatusOK, `not json`, true, true},
		{"4xx is a clean rejection", http.StatusUnprocessableEntity, `{"message":"bad request"}`, true, false},
		{"429 is a clean rejection, not ambiguous", http.StatusTooManyRequests, `{}`, true, false},
		{"500 is ambiguous", http.StatusInternalServerError, `{}`, true, true},
		{"503 is ambiguous", http.StatusServiceUnavailable, `{}`, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(
				func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(test.status)
					_, _ = w.Write([]byte(test.body))
				}))
			defer server.Close()
			t.Setenv("RESEND_API_BASE_URL", server.URL)

			sender := &resendEmailSender{
				from: "billing@example.test", apiKey: "k",
				client: &http.Client{Timeout: 5 * time.Second},
			}
			err := sender.Send(context.Background(), EmailMessage{
				To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
			})
			if !test.wantErr {
				if err != nil {
					t.Fatalf("Send() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Send() = nil, want an error")
			}
			_, ambiguous := isAmbiguous(err)
			if ambiguous != test.wantAmbiguous {
				t.Fatalf("Send() ambiguous = %v, want %v (err: %v)", ambiguous, test.wantAmbiguous, err)
			}
		})
	}
}

// TestResendSenderTreatsAResponseTimeoutAsAmbiguous is the concrete CHAOS-5399
// repro: the connection is established and the request is fully written (the
// server reads it), but the response never comes back before the caller's
// context deadline. This must NOT be classified as "nothing was sent".
func TestResendSenderTreatsAResponseTimeoutAsAmbiguous(t *testing.T) {
	received := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			close(received)
			// Outlast the client's timeout below (so Send() genuinely times
			// out waiting for a response) but still return, so the test
			// server's Close() does not block forever on a hung handler.
			time.Sleep(300 * time.Millisecond)
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 50 * time.Millisecond},
	}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("the server never saw the request; this test proves nothing about a post-send timeout")
	}
	if err == nil {
		t.Fatal("Send() = nil, want a timeout error")
	}
	if _, ambiguous := isAmbiguous(err); !ambiguous {
		t.Fatalf("Send() = %v, want an *AmbiguousSendError -- the request reached the "+
			"server before the timeout, so this must not be treated as a clean non-send", err)
	}
}

// TestResendSenderTreatsAConnectionRefusalAsDefinitelyNotSent is the negative
// control: a dial failure means NO bytes reached the network, which is the
// one class of transport error that stays a clean non-send.
func TestResendSenderTreatsAConnectionRefusalAsDefinitelyNotSent(t *testing.T) {
	// Bind to get a genuinely free port, then close it immediately so the
	// connection is refused deterministically -- no reliance on a
	// well-known unused port.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RESEND_API_BASE_URL", "http://"+addr)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 2 * time.Second},
	}
	sendErr := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if sendErr == nil {
		t.Fatal("Send() = nil, want a connection-refused error")
	}
	if _, ambiguous := isAmbiguous(sendErr); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a refused connection never "+
			"reached the network and must be a clean non-send", sendErr)
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5399: SMTP outcome classification, via a minimal hand-rolled SMTP
// server. net/smtp has no test double in this codebase; the protocol is
// simple enough that driving it directly over a raw net.Listener is more
// reliable than reaching for a third-party fake.
// ---------------------------------------------------------------------------

// fakeSMTPServer is a bare-bones, single-connection SMTP server. behavior
// controls what happens once the client finishes writing the DATA content
// (the terminating "." line has been read): "ack" replies 250 as usual,
// "hangup" closes the connection with NO reply at all -- exactly "accepted
// DATA, then the connection was lost before the acknowledgement" -- and
// "reject" replies with an explicit SMTP error code, the server's definite,
// stated answer rather than a lost one.
type fakeSMTPServer struct {
	listener net.Listener
	behavior string // "ack", "hangup", or "reject"
}

func newFakeSMTPServer(t *testing.T, behavior string) *fakeSMTPServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeSMTPServer{listener: listener, behavior: behavior}
	go server.serveOne(t)
	return server
}

func (server *fakeSMTPServer) addr() string { return server.listener.Addr().String() }

func (server *fakeSMTPServer) serveOne(t *testing.T) {
	conn, err := server.listener.Accept()
	if err != nil {
		return // listener closed by test cleanup
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	reply := func(line string) {
		if _, err := conn.Write([]byte(line + "\r\n")); err != nil {
			t.Logf("fake smtp server write failed: %v", err)
		}
	}
	reply("220 fake.smtp.test ESMTP")
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		verb := strings.ToUpper(fields[0])
		switch verb {
		case "EHLO", "HELO":
			reply("250 fake.smtp.test")
		case "MAIL":
			reply("250 OK")
		case "RCPT":
			reply("250 OK")
		case "DATA":
			reply("354 Start mail input; end with <CRLF>.<CRLF>")
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
			}
			switch server.behavior {
			case "hangup":
				// The message content (including its terminator) was fully
				// received -- then the connection is dropped with no reply
				// at all, simulating a lost connection right after the
				// server committed the message.
				return
			case "reject":
				// The server DID reply -- its definite, stated answer is a
				// refusal, not a lost acknowledgement.
				reply("550 rejected after DATA")
			default:
				reply("250 OK: queued")
			}
		case "QUIT":
			reply("221 Bye")
			return
		default:
			reply("500 unrecognized command")
		}
	}
}

func TestSMTPSenderTreatsALostConnectionAfterDataAsAmbiguous(t *testing.T) {
	server := newFakeSMTPServer(t, "hangup")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want an error -- the final SMTP reply was never received")
	}
	if _, ambiguous := isAmbiguous(err); !ambiguous {
		t.Fatalf("Send() = %v, want an *AmbiguousSendError -- the server read the full "+
			"message before the connection was lost, so this is not a clean non-send", err)
	}
}

func TestSMTPSenderTreatsAnOrdinaryAcknowledgementAsSent(t *testing.T) {
	server := newFakeSMTPServer(t, "ack")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	if err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	}); err != nil {
		t.Fatalf("Send() = %v, want nil", err)
	}
}

func TestSMTPSenderTreatsAConnectionRefusalAsDefinitelyNotSent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	host, port := splitHostPort(t, listener.Addr().String())
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	sendErr := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if sendErr == nil {
		t.Fatal("Send() = nil, want a connection-refused error")
	}
	if _, ambiguous := isAmbiguous(sendErr); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a refused connection never "+
			"reached the server and must be a clean non-send", sendErr)
	}
}

func splitHostPort(t *testing.T, addr string) (string, int) {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatal(err)
	}
	return host, port
}
