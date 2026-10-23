package health

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// alembicServer is a PostgreSQL wire server whose only data is the alembic_version table:
// it answers the version query with one row per revision (a nil revision is a NULL column).
func alembicServer(t *testing.T, revisions []*string) *pgxpool.Pool {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		connections.Wait()
	})
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				serveAlembic(connection, revisions)
			}()
		}
	}()
	config, err := pgxpool.ParseConfig("postgres://health@" + listener.Addr().String() + "/health?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func serveAlembic(connection net.Conn, revisions []*string) {
	backend := pgproto3.NewBackend(connection, connection)
	for {
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		if _, ok := message.(*pgproto3.StartupMessage); ok {
			break
		}
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		query, ok := message.(*pgproto3.Query)
		if !ok {
			return
		}
		if strings.EqualFold(strings.TrimSpace(query.String), "SELECT version_num FROM alembic_version") {
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{
				{Name: []byte("version_num"), DataTypeOID: 25, DataTypeSize: -1, TypeModifier: -1},
			}})
			for _, revision := range revisions {
				var value []byte
				if revision != nil {
					value = []byte(*revision)
				}
				backend.Send(&pgproto3.DataRow{Values: [][]byte{value}})
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
		} else {
			backend.Send(&pgproto3.EmptyQueryResponse{})
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
	}
}

func revisionRows(revisions ...string) []*string {
	rows := make([]*string, len(revisions))
	for index := range revisions {
		rows[index] = &revisions[index]
	}
	return rows
}

func TestCheckPostgresAnswersFromTheRecordedRevisions(t *testing.T) {
	cases := []struct {
		name string
		rows []*string
		want string
	}{
		{"known head", revisionRows("0152"), "ok"},
		{"minimum", revisionRows("0075"), "ok"},
		{"unknown newer head", revisionRows("0999"), "ok"},
		{"unknown newer head beside the cutover branch head", revisionRows("0066", "9999"), "ok"},
		{"below the minimum", revisionRows("0074"), "down"},
		{"cutover branch head alone", revisionRows("0066"), "down"},
		{"unparseable", revisionRows("abcd"), "down"},
		{"five digits", revisionRows("00751"), "down"},
		{"empty string", revisionRows(""), "down"},
		{"NULL column beside a satisfying head", append(revisionRows("0152"), nil), "down"},
		{"no row", nil, "down"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var logs bytes.Buffer
			deps := Deps{Pool: alembicServer(t, test.rows), Logger: slog.New(slog.NewTextHandler(&logs, nil))}
			service, got := deps.checkPostgres(context.Background())
			if service != "postgres" || got != test.want {
				t.Errorf("recorded %v: checkPostgres = %s %s, want postgres %s", derefAll(test.rows), service, got, test.want)
			}
			if strings.Contains(logs.String(), "postgres check failed") {
				t.Errorf("recorded %v: the query itself failed, so the answer is not about the revisions: %s", derefAll(test.rows), logs.String())
			}
		})
	}
}

func derefAll(rows []*string) []string {
	out := make([]string, len(rows))
	for index, row := range rows {
		if row == nil {
			out[index] = "<NULL>"
		} else {
			out[index] = *row
		}
	}
	return out
}

func TestHealthEndpointReportsPostgresOkForAnUnknownNewerHead(t *testing.T) {
	_, body := serve(t, Deps{Pool: alembicServer(t, revisionRows("0999"))}, http.MethodGet, "/health")
	if !strings.Contains(body, `"postgres":"ok"`) {
		t.Errorf("/health body %s does not report postgres ok for an unknown newer head", body)
	}
	_, body = serve(t, Deps{Pool: alembicServer(t, revisionRows("0074"))}, http.MethodGet, "/health")
	if !strings.Contains(body, `"postgres":"down"`) {
		t.Errorf("/health body %s does not report postgres down below the minimum", body)
	}
}

func TestCheckPostgresLogsTheRecordedRevisionsWhenTheSchemaIsBelowTheMinimum(t *testing.T) {
	var logs bytes.Buffer
	deps := Deps{Pool: alembicServer(t, revisionRows("0066", "0074")), Logger: slog.New(slog.NewTextHandler(&logs, nil))}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, got := deps.checkPostgres(ctx); got != "down" {
		t.Fatalf("checkPostgres = %s, want down", got)
	}
	line := logs.String()
	if !strings.Contains(line, "api health: postgres schema below the required revision") ||
		!strings.Contains(line, "0066") || !strings.Contains(line, "0074") || !strings.Contains(line, "minimum=0075") {
		t.Errorf("warn line does not name the recorded revisions and the minimum: %q", line)
	}
}
