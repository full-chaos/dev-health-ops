package postgres

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// withheldStatementPool is a pool over an in-memory PostgreSQL backend (net.Pipe, no socket) that
// answers every posture statement "f" (a refusal) and withholds the reply of the statement named by
// stage: the transaction's query ("primary"), its ROLLBACK, the identity predicate run after a
// refusal, the identity description, or the catalog diagnostics. blocked closes when the backend
// starts withholding; it answers nothing more until the client side goes away.
func withheldStatementPool(t *testing.T, stage string, queryAPI bool) (*pgxpool.Pool, <-chan struct{}) {
	t.Helper()
	client, peer := net.Pipe()
	t.Cleanup(func() { _ = peer.Close(); _ = client.Close() })
	cfg, err := pgxpool.ParseConfig("postgres://review:x@127.0.0.1/review?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	cfg.ConnConfig.DialFunc = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	blocked := make(chan struct{})
	withhold := func() {
		close(blocked)
		var one [1]byte
		_, _ = peer.Read(one[:])
	}
	go func() {
		defer peer.Close()
		backend := pgproto3.NewBackend(peer, peer)
		if _, err := backend.ReceiveStartupMessage(); err != nil {
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
		backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
		backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
		if backend.Flush() != nil {
			return
		}
		inTx, diagnostics := false, 0
		for {
			msg, err := backend.Receive()
			if err != nil {
				return
			}
			query, ok := msg.(*pgproto3.Query)
			if !ok {
				return
			}
			sql := strings.ToLower(query.String)
			switch {
			case strings.HasPrefix(sql, "begin"):
				inTx = true
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
			case strings.HasPrefix(sql, "rollback"):
				if stage == "rollback" {
					withhold()
					return
				}
				inTx = false
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
			case strings.HasPrefix(sql, "select set_config"):
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case inTx && stage == "primary",
				!inTx && stage == "identity",
				!inTx && diagnostics == 1 && (stage == "description" || stage == "catalog"),
				inTx && queryAPI && diagnostics == 1 && stage == "description":
				withhold()
				return
			default:
				if queryAPI && stage == "rollback" {
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "in-memory query error"})
				} else {
					answer := "f"
					if !inTx && stage == "catalog" {
						answer = "t"
					}
					if !inTx || queryAPI {
						diagnostics++
					}
					backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{Name: []byte("posture"), DataTypeOID: 16, DataTypeSize: 1, TypeModifier: -1}}})
					backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(answer)}})
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
				}
			}
			status := byte('I')
			if inTx {
				status = 'T'
			}
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
			if backend.Flush() != nil {
				return
			}
		}
	}()
	return pool, blocked
}

// CHAOS-8654 r1: the rollback and the diagnostics after a refusal ran on contexts detached from the
// execution, so Close cancelled the check and then waited on a statement nobody could end. Each
// production posture check is closed while the database withholds one statement's reply; Close
// must return with the reply still withheld. RunTimeout is an hour, so only Close's cancellation
// can end the statement.
func TestCachedPostureCheckCloseEndsAWithheldStatement(t *testing.T) {
	checks := map[string]func(*pgxpool.Pool, PostureCheckOptions) *CachedPostureCheck{
		"api": func(pool *pgxpool.Pool, opts PostureCheckOptions) *CachedPostureCheck {
			return NewCachedPostureCheck(pool, "review", "river", APIPosture(false), opts)
		},
		"domain": func(pool *pgxpool.Pool, opts PostureCheckOptions) *CachedPostureCheck {
			return NewCachedPostureCheck(pool, "review", "river", DomainPosture(), opts)
		},
		"coordinator": func(pool *pgxpool.Pool, opts PostureCheckOptions) *CachedPostureCheck {
			return NewCachedRunCheck("coordinator", func(ctx context.Context) error {
				return CheckCoordinatorAuthorization(ctx, pool, "review", "river")
			}, opts)
		},
		"queue": func(pool *pgxpool.Pool, opts PostureCheckOptions) *CachedPostureCheck {
			return NewCachedRunCheck("queue", func(ctx context.Context) error {
				return CheckQueueAuthorization(ctx, pool, "review", "river")
			}, opts)
		},
		"query_api": func(pool *pgxpool.Pool, opts PostureCheckOptions) *CachedPostureCheck {
			return NewCachedQueryAPIPostureCheckWithSessions(pool, "review", "river", nil, opts)
		},
	}
	for _, cell := range []struct{ stage, check string }{
		{"primary", "api"}, {"primary", "domain"}, {"primary", "coordinator"}, {"primary", "queue"}, {"primary", "query_api"},
		{"rollback", "api"}, {"rollback", "domain"}, {"rollback", "coordinator"}, {"rollback", "queue"}, {"rollback", "query_api"},
		{"identity", "api"}, {"identity", "domain"}, {"identity", "coordinator"}, {"identity", "queue"},
		{"description", "api"}, {"description", "domain"}, {"description", "coordinator"}, {"description", "queue"}, {"description", "query_api"},
		{"catalog", "api"}, {"catalog", "domain"}, {"catalog", "coordinator"},
	} {
		t.Run(cell.stage+"/"+cell.check, func(t *testing.T) {
			t.Parallel()
			pool, blocked := withheldStatementPool(t, cell.stage, cell.check == "query_api")
			check := checks[cell.check](pool, PostureCheckOptions{RunTimeout: time.Hour})
			check.Warm()
			<-blocked
			closed := make(chan struct{})
			go func() {
				check.Close()
				close(closed)
			}()
			// Failure-only guard: a passing Close returns as soon as the cancelled statement ends.
			select {
			case <-closed:
			case <-time.After(30 * time.Second):
				pool.Reset()
				t.Fatalf("Close still blocked 30 s after it cancelled the check, with the %s reply withheld", cell.stage)
			}
		})
	}
}
