// Package smtpcapture is a minimal in-process SMTP server that records every
// message byte for byte. It is the mail sink of the live-Python oracles: both
// planes send the same message to it and the raw wire bytes are compared,
// which is strictly more precise than reading either back through a mail
// catcher's parsed view.
package smtpcapture

import (
	"bufio"
	"bytes"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
type Server struct {
	listener net.Listener
	mu       sync.Mutex
	mails    []Mail
	accepts  int
}

func Start(t *testing.T) *Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{listener: listener}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.mu.Lock()
			server.accepts++
			server.mu.Unlock()
			go server.serve(conn)
		}
	}()
	return server
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

func (server *Server) serve(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	reply := func(line string) { _, _ = conn.Write([]byte(line + "\r\n")) }
	reply("220 capture.smtp.test ESMTP")
	var current Mail
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		trimmed := strings.TrimRight(line, "\r\n")
		upper := strings.ToUpper(trimmed)
		switch {
		case strings.HasPrefix(upper, "EHLO"), strings.HasPrefix(upper, "HELO"):
			reply("250 capture.smtp.test")
		case strings.HasPrefix(upper, "MAIL FROM:"):
			current = Mail{MailFrom: trimmed[len("MAIL FROM:"):]}
			reply("250 OK")
		case strings.HasPrefix(upper, "RCPT TO:"):
			current.RcptTo = append(current.RcptTo, trimmed[len("RCPT TO:"):])
			reply("250 OK")
		case upper == "DATA":
			reply("354 go ahead")
			var data bytes.Buffer
			for {
				dataLine, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				if dataLine == ".\r\n" {
					break
				}
				data.WriteString(strings.TrimPrefix(dataLine, ".")) // dot-unstuff
			}
			current.Data = data.String()
			server.mu.Lock()
			server.mails = append(server.mails, current)
			server.mu.Unlock()
			current = Mail{}
			reply("250 OK queued")
		case upper == "QUIT":
			reply("221 bye")
			return
		default:
			reply("250 OK")
		}
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
