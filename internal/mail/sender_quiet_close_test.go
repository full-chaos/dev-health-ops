package mail

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A successful SMTP send must not log anything at warning level: QUIT closes
// the connection, and a second Close used to report "use of closed network
// connection" on every good send, burying real teardown failures.
func TestSuccessfulSMTPSendLogsNoWarning(t *testing.T) {
	server := startCaptureSMTPServer(t)
	host, port := server.hostPort(t)
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	sender := &smtpSender{from: "billing@example.test", host: host, port: port}
	if err := sender.Send(context.Background(), Message{To: "to@example.test", Subject: "s", HTML: "<p>x</p>"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	server.take(t)
	if strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("successful send logged a warning:\n%s", logs.String())
	}
}

// A relay that accepts the TCP connection and never greets must not hold Send
// past its context: the exchange after the dial is bounded by it too.
func TestSMTPSendHonoursContextAfterDial(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	release := make(chan struct{})
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		<-release
		_ = conn.Close()
	}()
	defer close(release)
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	sender := &smtpSender{from: "billing@example.test", host: host, port: port}
	done := make(chan error, 1)
	go func() {
		done <- sender.Send(ctx, Message{To: "to@example.test", Subject: "s", HTML: "<p>x</p>"})
	}()
	select {
	case sendErr := <-done:
		if sendErr == nil {
			t.Fatal("send to a silent relay succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("send outlived its 150ms context by more than 1.8s")
	}
}

// A non-ASCII envelope address is refused before any connection, matching
// smtplib (UnicodeEncodeError, nothing sent). Display names are unaffected.
func TestSMTPSenderRefusesNonASCIIEnvelopeAddresses(t *testing.T) {
	server := startCaptureSMTPServer(t)
	host, port := server.hostPort(t)
	for _, tc := range []struct {
		name, from, to string
		refused        bool
	}{
		{"recipient", "billing@example.test", "jörg@example.test", true},
		{"recipient with a display name", "billing@example.test", "Jorg <jörg@example.test>", true},
		{"sender", "dév@example.test", "to@example.test", true},
		{"non-ascii display name only", "Dév <billing@example.test>", "to@example.test", false},
	} {
		sender := &smtpSender{from: tc.from, host: host, port: port}
		err := sender.Send(context.Background(), Message{To: tc.to, Subject: "s", HTML: "<p>x</p>"})
		if tc.refused {
			if !errors.Is(err, errEnvelopeNotASCII) {
				t.Errorf("%s: err = %v, want errEnvelopeNotASCII", tc.name, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		server.take(t)
	}
	server.assertNone(t)
}
