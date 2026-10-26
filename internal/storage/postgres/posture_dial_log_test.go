package postgres

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
)

func refusedDialToDocumentedAddress() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 5432},
		Err:  os.NewSyscallError("connect", syscall.ECONNREFUSED),
	}
}

func TestCachedPostureCheckLogsNoDialAddressForAnUnreachableEndpoint(t *testing.T) {
	var buffer syncBuffer
	check, _ := newTestCachedPostureCheck(func(context.Context) error { return refusedDialToDocumentedAddress() },
		PostureCheckOptions{Logger: slog.New(slog.NewJSONHandler(&buffer, nil))})
	_ = check.Check(context.Background())
	waitFor(t, "the log line", func() bool { return strings.Contains(buffer.String(), "\n") })
	if strings.Contains(buffer.String(), "10.0.0.10") {
		t.Fatalf("posture log carries the dial address: %s", buffer.String())
	}
	if !strings.Contains(buffer.String(), `"outcome":"unanswered"`) || !strings.Contains(buffer.String(), "unreachable") {
		t.Fatalf("posture log must name the unanswered outcome and the bounded cause: %s", buffer.String())
	}
}

func TestCachedPostureCheckKeepsTheDetailOfAnyOtherUnansweredFailure(t *testing.T) {
	var buffer syncBuffer
	check, _ := newTestCachedPostureCheck(func(context.Context) error { return errors.New("catalog read failed") },
		PostureCheckOptions{Logger: slog.New(slog.NewJSONHandler(&buffer, nil))})
	_ = check.Check(context.Background())
	waitFor(t, "the log line", func() bool { return strings.Contains(buffer.String(), "\n") })
	if !strings.Contains(buffer.String(), "catalog read failed") {
		t.Fatalf("a non-dial failure keeps its detail: %s", buffer.String())
	}
}
