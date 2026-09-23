package apiservice

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// echoingValkeyServer answers every RESP command with an error reply that
// repeats the planted credential, the way a driver or server error can echo
// what it was sent. It returns the listener address.
func echoingValkeyServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					// A command is an array header; answer each one once.
					if strings.HasPrefix(line, "*") {
						_, _ = conn.Write([]byte("-ERR AUTH rejected password " + testCredential + "\r\n"))
					}
				}
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// TestDriverErrorTextNeverCarriesTheCredentialToTheLog runs configure against
// a Valkey whose error reply echoes the DSN password: the dependency line keeps
// the driver's text (so the cause is visible) and the password never reaches
// any log line.
func TestDriverErrorTextNeverCarriesTheCredentialToTheLog(t *testing.T) {
	var logs bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg := config.Config{APIAddress: "127.0.0.1:0", ValkeyURI: secrets.NewValue("redis://:" + testCredential + "@" + echoingValkeyServer(t) + "/1")}
	components, err := configure(ctx, cfg, health.NewRegistry(time.Second), slog.New(slog.NewJSONHandler(&logs, nil)))
	closeComponents(components)
	if err == nil {
		t.Fatal("configure succeeded against a Valkey that rejects every command")
	}
	output := logs.String()
	if !strings.Contains(output, "AUTH rejected password "+secrets.RedactedMarker) {
		t.Fatalf("the log does not carry the driver's redacted text:\n%s", output)
	}
	if strings.Contains(output, testCredential) || strings.Contains(err.Error(), testCredential) {
		t.Fatalf("the credential echoed by the server reached the log or the error:\n%s\nerror: %v", output, err)
	}
}
