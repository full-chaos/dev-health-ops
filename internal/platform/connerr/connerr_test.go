package connerr

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	chgoproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

// The matcher names the REAL driver error of a connection the server closed
// under an INSERT: ch-go proto.Reader.ReadByte over a loopback TCP connection
// whose peer closed ("read: EOF"), under the clickhouse-go conn_process.go
// first-block wrap (copied verbatim: the connection type is unexported). A
// ClickHouse server exception is no dropped connection.
func TestIsDroppedOnTheRealFirstBlockReadChain(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		server, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = server.Close()
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, readErr := chproto.NewReader(conn).ReadByte()
	if readErr == nil {
		t.Fatal("ReadByte on a peer-closed connection returned no error")
	}
	const connID = 1
	driverErr := fmt.Errorf("query processing: failed to read first block packet from %s (conn_id=%d): %w",
		conn.RemoteAddr(), connID, readErr)
	if !errors.Is(driverErr, io.EOF) {
		t.Fatalf("premise: driver error %q does not wrap io.EOF", driverErr)
	}
	if !IsDropped(driverErr) {
		t.Fatalf("IsDropped(%q) = false, want true", driverErr)
	}
	exception := &chgoproto.Exception{Code: 241, Name: "DB::Exception", Message: "Memory limit (total) exceeded"}
	if IsDropped(exception) || IsDropped(fmt.Errorf("insert: %w", exception)) {
		t.Fatalf("IsDropped(%v) = true, want false", exception)
	}
}

// IsDroppedWrite names a drop only when the dropped error itself is marked as
// an INSERT call (AtWrite): the mark survives %w and errors.Join, keeps the
// text and the chain, and a marked non-drop beside an unmarked dropped read is
// no dropped write.
func TestIsDroppedWriteNamesOnlyAMarkedDrop(t *testing.T) {
	drop := fmt.Errorf("query processing: failed to read first block packet: %w", fmt.Errorf("read: %w", io.EOF))
	exception := &chgoproto.Exception{Code: 241, Name: "DB::Exception", Message: "Memory limit (total) exceeded"}
	marked := AtWrite(drop)
	if AtWrite(nil) != nil {
		t.Fatal("AtWrite(nil) != nil")
	}
	if marked.Error() != drop.Error() || !errors.Is(marked, io.EOF) || !errors.Is(marked, drop) {
		t.Fatalf("AtWrite changed the error: %q errors.Is(io.EOF)=%v", marked, errors.Is(marked, io.EOF))
	}
	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"marked drop":                         {marked, true},
		"marked drop under %w":                {fmt.Errorf("send batch: %w", marked), true},
		"marked drop in a join":               {errors.Join(errors.New("abort"), marked), true},
		"marked drop in a wrapped join":       {fmt.Errorf("write: %w", errors.Join(errors.New("abort"), marked)), true},
		"unmarked drop (a read)":              {drop, false},
		"unmarked drop under %w":              {fmt.Errorf("read stored rows: %w", drop), false},
		"marked exception":                    {AtWrite(exception), false},
		"marked exception beside a read drop": {errors.Join(AtWrite(exception), fmt.Errorf("read: %w", drop)), false},
		"marked drop, any wrap text":          {fmt.Errorf("read stored rows: %w", AtWrite(drop)), true},
		"nil":                                 {nil, false},
	} {
		if got := IsDroppedWrite(tc.err); got != tc.want {
			t.Errorf("%s: IsDroppedWrite(%v) = %v, want %v", name, tc.err, got, tc.want)
		}
	}
}
