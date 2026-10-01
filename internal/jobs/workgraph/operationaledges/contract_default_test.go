package operationaledges

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// recordingConn captures the statement a reader sends and stops there.
type recordingConn struct {
	driver.Conn
	queries []string
}

func (conn *recordingConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	conn.queries = append(conn.queries, query)
	return nil, errors.New("recorded")
}

// With OPERATIONAL_ORDERING_CONTRACT unset the readers take the contract-2
// current-row read (never FINAL); "1" and any other value is refused before a
// statement is sent.
func TestReadersTakeTheCurrentRowReadWhenTheContractIsUnset(t *testing.T) {
	const orgID = "70d529e0-3c06-4597-8480-794fd02328b6"
	readers := map[string]func(context.Context, driver.Conn) error{
		"ReadIncidents": func(ctx context.Context, conn driver.Conn) error {
			_, err := ReadIncidents(ctx, conn, orgID, nil, nil)
			return err
		},
		"ReadServiceRepositoryMappings": func(ctx context.Context, conn driver.Conn) error {
			_, err := ReadServiceRepositoryMappings(ctx, conn, orgID, time.Now(), nil, nil)
			return err
		},
	}
	for name, read := range readers {
		t.Run(name+"/unset", func(t *testing.T) {
			t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "")
			if err := os.Unsetenv("OPERATIONAL_ORDERING_CONTRACT"); err != nil {
				t.Fatal(err)
			}
			conn := &recordingConn{}
			_ = read(context.Background(), conn)
			if len(conn.queries) != 1 {
				t.Fatalf("%d statements sent, want 1", len(conn.queries))
			}
			query := conn.queries[0]
			if !strings.Contains(query, "LIMIT 1 BY org_id, id") || strings.Contains(query, "FINAL") {
				t.Errorf("unset must read the contract-2 current row, got:\n%s", query)
			}
		})
		for _, value := range []string{"1", "", "3"} {
			t.Run(name+"/"+value, func(t *testing.T) {
				t.Setenv("OPERATIONAL_ORDERING_CONTRACT", value)
				conn := &recordingConn{}
				if err := read(context.Background(), conn); err == nil {
					t.Fatalf("OPERATIONAL_ORDERING_CONTRACT=%q was accepted", value)
				}
				if len(conn.queries) != 0 {
					t.Errorf("a refused contract still sent %d statement(s)", len(conn.queries))
				}
			})
		}
	}
}
