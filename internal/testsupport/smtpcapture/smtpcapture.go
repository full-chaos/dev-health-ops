// Package smtpcapture is a minimal in-process SMTP server that records every
// message byte for byte. It is the mail sink of the live-Python oracles: both
// planes send the same message to it and the raw wire bytes are compared,
// which is strictly more precise than reading either back through a mail
// catcher's parsed view.
package smtpcapture

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Mail is one message as an SMTP server saw it on the wire: the
// envelope arguments exactly as sent (everything after "MAIL FROM:" /
// "RCPT TO:") and the DATA payload, dot-unstuffed, CRLF preserved.
type Mail struct {
	MailFrom string
	RcptTo   []string
	Data     string
}

// Server is a minimal in-process SMTP server that records every
// message byte-for-byte. It is the oracle's mail sink: both planes send the
// same message to it and the raw wire bytes are compared, which is strictly
// more precise than reading either back through a mail catcher's parsed view.
type Server struct{ *Sink }

// DefaultListen is the loopback address with a free port.
const DefaultListen = "127.0.0.1:0"

func Start(t *testing.T) *Server { return StartAt(t, DefaultListen) }

// ValidateListen is an error unless addr is host:port with a port number.
func ValidateListen(addr string) error {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return fmt.Errorf("listen address %q has no valid port", addr)
	}
	return nil
}

// StartAt starts the sink on addr.
func StartAt(t *testing.T, addr string) *Server {
	t.Helper()
	if err := ValidateListen(addr); err != nil {
		t.Fatal(err)
	}
	sink, err := NewSink(addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sink.Close)
	return &Server{sink}
}

func (server *Server) HostPort(t *testing.T) (string, int) {
	t.Helper()
	host, port, err := net.SplitHostPort(server.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}
	return host, number
}

func (server *Server) Take(t *testing.T) Mail {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		if len(server.mails) > 0 {
			mail := server.mails[0]
			server.mails = server.mails[1:]
			server.mu.Unlock()
			return mail
		}
		server.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the capture SMTP server never received a message")
	return Mail{}
}

// assertNone fails when the server received (or is about to receive) a
// message: it waits long enough for one already in flight to land.
func (server *Server) AssertNone(t *testing.T) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.mails) != 0 {
		t.Fatalf("the SMTP server received a message that must have been refused: %q", server.mails[0].Data)
	}
}

var boundaryPattern = regexp.MustCompile(`boundary="([^"]+)"`)

// Normalize replaces the random MIME boundary (Python: "===============
// 123456789==", Go: a hex string) with a fixed token, everywhere it appears.
// The boundary is the only wire byte that is random by design.
func Normalize(mail Mail) Mail {
	match := boundaryPattern.FindStringSubmatch(mail.Data)
	if match != nil {
		mail.Data = strings.ReplaceAll(mail.Data, match[1], "BOUNDARY")
	}
	return mail
}

// Drain returns every message received so far, waiting until the server has
// been quiet for a moment so a message still in flight lands first. It is for
// a caller that does not know in advance how many messages a plane sent.
func (server *Server) Drain(t *testing.T) []Mail {
	t.Helper()
	time.Sleep(500 * time.Millisecond)
	server.mu.Lock()
	defer server.mu.Unlock()
	out := server.mails
	server.mails = nil
	return out
}

// AssertNoConnections fails if any client ever connected. It proves a sender
// refused BEFORE dialing, which AssertNone (no message captured) cannot: a
// sender that connects and then refuses would pass that.
func (server *Server) AssertNoConnections(t *testing.T) {
	t.Helper()
	time.Sleep(300 * time.Millisecond)
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.accepts != 0 {
		t.Fatalf("the SMTP server accepted %d connection(s); the sender must refuse before dialing", server.accepts)
	}
}
