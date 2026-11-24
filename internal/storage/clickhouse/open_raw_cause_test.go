package clickhouse

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
)

// exceptionServer answers the native handshake with an exception whose message
// is message. Every value is made up.
func exceptionServer(t *testing.T, message string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	text := func(value string) []byte { return append(binary.AppendUvarint(nil, uint64(len(value))), value...) }
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = conn.Read(make([]byte, 4096))
				packet := binary.LittleEndian.AppendUint32([]byte{2}, 516)
				packet = append(packet, text("DB::Exception")...)
				packet = append(packet, text(message)...)
				packet = append(packet, text("")...)
				packet = append(packet, 0)
				_, _ = conn.Write(packet)
			}()
		}
	}()
	return listener.Addr().String()
}

// Config.RawCause keeps the error class and the driver's cause text as it came;
// without it Open still redacts the DSN's credentials from the same text.
func TestRawCauseConfigKeepsTheErrorClassAndTheCauseText(t *testing.T) {
	host := exceptionServer(t, "refused for zz9q and svc%3A7kZ2")
	config := DefaultConfig(fmt.Sprintf("clickhouse://svc%%3A:zz9q@%s/d", host))
	raw := config
	raw.RawCause = true
	_, err := Open(context.Background(), raw)
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("errors.Is(err, ErrUnavailable) is false: %v", err)
	}
	if !strings.HasPrefix(err.Error(), ErrUnavailable.Error()+": ") || !strings.Contains(err.Error(), "refused for zz9q and svc%3A7kZ2") {
		t.Errorf("the raw cause text is not in the error: %q", err)
	}
	var carried RawCauseError
	if !errors.As(err, &carried) || !slices.Equal(carried.Resolved(), []string{"svc:", "zz9q"}) {
		t.Errorf("the login and password the driver resolved are not carried: %v", carried.Resolved())
	}
	_, err = Open(context.Background(), config)
	if !errors.Is(err, ErrUnavailable) || strings.Contains(err.Error(), "zz9q") {
		t.Errorf("Open must keep redacting the DSN password: %v", err)
	}
}
