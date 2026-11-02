// Package valkeyreply builds the *valkey.ValkeyError a real server reply decodes to.
package valkeyreply

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Error returns the error valkey-go yields for a server that answers every
// command with the RESP error reply text (code first, as the server sends it).
func Error(t testing.TB, text string) error {
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
			go serve(conn, text)
		}
	}()
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{listener.Addr().String()},
		DisableCache:      true,
		DisableRetry:      true,
		AlwaysRESP2:       true,
		ForceSingleClient: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = client.Do(ctx, client.B().Get().Key("k").Build()).Error()
	if err == nil {
		t.Fatal("the fake server answered without an error")
	}
	if _, ok := valkey.IsValkeyErr(err); !ok {
		t.Fatalf("not a valkey error reply: %v", err)
	}
	return err
}

func serve(conn net.Conn, text string) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "*") {
			return
		}
		n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		var words []string
		for i := 0; i < n; i++ {
			head, err := r.ReadString('\n')
			if err != nil {
				return
			}
			size, _ := strconv.Atoi(strings.TrimSpace(head[1:]))
			buf := make([]byte, size+2)
			if _, err := readFull(r, buf); err != nil {
				return
			}
			words = append(words, string(buf[:size]))
		}
		switch strings.ToUpper(words[0]) {
		case "GET":
			_, _ = conn.Write([]byte("-" + text + "\r\n"))
		case "PING":
			_, _ = conn.Write([]byte("+PONG\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
