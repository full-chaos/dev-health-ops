// Package ingestpg is a PostgreSQL wire server for tests of the external-ingest token gate. It answers the
// external_ingest_tokens lookup with a configured row (or none), accepts the audit_logs insert and keeps
// what it was bound to, and can fail that insert with a chosen SQLSTATE, message and detail. It is no SQL
// engine: every other statement is answered empty.
package ingestpg

import (
	"context"
	"encoding/binary"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Token is the row the lookup returns. Scopes is the JSON text of the scope list.
type Token struct {
	ID, OrgID string
	SourceID  *string
	Scopes    string
	ExpiresAt *time.Time
	RevokedAt *time.Time
}

// AuditRow is one audit_logs insert as bound; a NULL column is nil.
type AuditRow struct {
	ID, OrgID                        string
	UserID                           *string
	Action, ResourceType, ResourceID string
	Description                      *string
	Changes, RequestMetadata, Status string
	ErrorMessage                     *string
}

// Server is a running fake server.
type Server struct {
	mu       sync.Mutex
	sqls     []string
	audits   []AuditRow
	token    *Token
	failCode string
	failMsg  string
	failDet  string
	connects atomic.Int64
}

var sqlParam = regexp.MustCompile(`\$(\d+)`)

// SetToken sets the row the token lookup returns; nil is no row.
func (s *Server) SetToken(token *Token) {
	s.mu.Lock()
	s.token = token
	s.mu.Unlock()
}

// FailAuditInsert makes the audit_logs insert answer an error; an empty code restores success.
func (s *Server) FailAuditInsert(code, message, detail string) {
	s.mu.Lock()
	s.failCode, s.failMsg, s.failDet = code, message, detail
	s.mu.Unlock()
}

// Connects is the number of connections accepted.
func (s *Server) Connects() int64 { return s.connects.Load() }

// TokenSelects is the number of executed external_ingest_tokens lookups.
func (s *Server) TokenSelects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sql := range s.sqls {
		if strings.Contains(sql, "external_ingest_tokens") && strings.Contains(strings.ToUpper(sql), "SELECT") {
			n++
		}
	}
	return n
}

// AuditRows are the audit_logs inserts seen so far, failed ones included.
func (s *Server) AuditRows() []AuditRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]AuditRow(nil), s.audits...)
}

// Start runs a server and returns it with a pool on it. Both stop with the test.
func Start(t testing.TB) (*Server, *pgxpool.Pool) { return StartWith(t, nil) }

// StartWith is Start with a hook to adjust the pool configuration before the pool is built.
func StartWith(t testing.TB, adjust func(*pgxpool.Config)) (*Server, *pgxpool.Pool) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := &Server{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.connects.Add(1)
			go server.serve(conn)
		}
	}()
	config, err := pgxpool.ParseConfig("postgres://fake:fake@" + listener.Addr().String() + "/fake?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatal(err)
	}
	config.MinConns = 0
	if adjust != nil {
		adjust(config)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return server, pool
}

func isToken(sql string) bool { return strings.Contains(sql, "external_ingest_tokens") }
func isAudit(sql string) bool { return strings.Contains(sql, "INSERT INTO audit_logs") }

var auditOIDs = []uint32{2950, 2950, 2950, 25, 25, 25, 25, 114, 114, 25, 25, 1184}

func rowDescription() *pgproto3.RowDescription {
	var fields []pgproto3.FieldDescription
	for _, name := range []string{"id", "org_id", "source_id", "scopes", "expires_at", "revoked_at"} {
		oid := uint32(25)
		if strings.HasSuffix(name, "_at") {
			oid = 1184
		}
		fields = append(fields, pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1})
	}
	return &pgproto3.RowDescription{Fields: fields}
}

// timeValue renders a timestamptz result in the format the client asked for (binary: microseconds since 2000).
func timeValue(t *time.Time, binaryFormat bool) []byte {
	if t == nil {
		return nil
	}
	if !binaryFormat {
		return []byte(t.UTC().Format("2006-01-02 15:04:05.999999+00"))
	}
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(t.UnixMicro()-946684800*1_000_000))
	return out
}

func wantsBinary(codes []int16, column int) bool {
	switch len(codes) {
	case 0:
		return false
	case 1:
		return codes[0] == 1
	}
	return column < len(codes) && codes[column] == 1
}

func (s *Server) serve(conn net.Conn) {
	defer conn.Close()
	backend := pgproto3.NewBackend(conn, conn)
	for {
		message, err := backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
		if _, ok := message.(*pgproto3.SSLRequest); ok {
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		}
		break
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "17.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	statements, portals := map[string]string{}, map[string]string{}
	bound := map[string][][]byte{}
	resultFormats := map[string][]int16{}
	failed := false
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		switch m := message.(type) {
		case *pgproto3.Parse:
			statements[m.Name] = m.Query
			backend.Send(&pgproto3.ParseComplete{})
		case *pgproto3.Describe:
			sql := statements[m.Name]
			if m.ObjectType == 'S' {
				var oids []uint32
				if isAudit(sql) {
					oids = auditOIDs
				} else {
					params := 0
					for _, match := range sqlParam.FindAllStringSubmatch(sql, -1) {
						if n, _ := strconv.Atoi(match[1]); n > params {
							params = n
						}
					}
					oids = make([]uint32, params)
					for i := range oids {
						oids[i] = 25
					}
				}
				backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: oids})
			} else {
				sql = portals[m.Name]
			}
			if isToken(sql) {
				backend.Send(rowDescription())
			} else {
				backend.Send(&pgproto3.NoData{})
			}
		case *pgproto3.Bind:
			sql := statements[m.PreparedStatement]
			portals[m.DestinationPortal] = sql
			bound[m.DestinationPortal] = decodeParams(sql, m)
			resultFormats[m.DestinationPortal] = m.ResultFormatCodes
			s.mu.Lock()
			s.sqls = append(s.sqls, sql)
			s.mu.Unlock()
			backend.Send(&pgproto3.BindComplete{})
		case *pgproto3.Execute:
			if failed {
				continue
			}
			sql := portals[m.Portal]
			s.mu.Lock()
			token, code, msg, det := s.token, s.failCode, s.failMsg, s.failDet
			s.mu.Unlock()
			switch {
			case isToken(sql) && token != nil:
				backend.Send(&pgproto3.DataRow{Values: [][]byte{
					[]byte(token.ID), []byte(token.OrgID), optional(token.SourceID), []byte(token.Scopes),
					timeValue(token.ExpiresAt, wantsBinary(resultFormats[m.Portal], 4)), timeValue(token.RevokedAt, wantsBinary(resultFormats[m.Portal], 5)),
				}})
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
			case isToken(sql):
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
			case isAudit(sql):
				s.mu.Lock()
				s.audits = append(s.audits, auditRow(bound[m.Portal]))
				s.mu.Unlock()
				if code != "" {
					backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: code, Message: msg, Detail: det})
					failed = true
				} else {
					backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
				}
			default:
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 0")})
			}
		case *pgproto3.Sync:
			failed = false
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if backend.Flush() != nil {
				return
			}
		case *pgproto3.Flush:
			if backend.Flush() != nil {
				return
			}
		case *pgproto3.Terminate:
			return
		default:
			return
		}
	}
}

func optional(s *string) []byte {
	if s == nil {
		return nil
	}
	return []byte(*s)
}

// decodeParams renders each bound parameter as text: a uuid sent in binary becomes its canonical text.
func decodeParams(sql string, m *pgproto3.Bind) [][]byte {
	out := make([][]byte, len(m.Parameters))
	for i, raw := range m.Parameters {
		if raw == nil {
			continue
		}
		format := int16(0)
		switch len(m.ParameterFormatCodes) {
		case 1:
			format = m.ParameterFormatCodes[0]
		case 0:
		default:
			format = m.ParameterFormatCodes[i]
		}
		if isAudit(sql) && i < len(auditOIDs) && format == 1 {
			switch auditOIDs[i] {
			case 2950:
				if id, err := uuid.FromBytes(raw); err == nil {
					out[i] = []byte(id.String())
					continue
				}
			case 1184:
				out[i] = []byte(time.Unix(946684800, 0).Add(time.Duration(int64(binary.BigEndian.Uint64(raw))) * time.Microsecond).UTC().Format(time.RFC3339Nano))
				continue
			}
		}
		out[i] = append([]byte(nil), raw...)
	}
	return out
}

func auditRow(p [][]byte) AuditRow {
	get := func(i int) *string {
		if i >= len(p) || p[i] == nil {
			return nil
		}
		s := string(p[i])
		return &s
	}
	str := func(i int) string {
		if v := get(i); v != nil {
			return *v
		}
		return ""
	}
	return AuditRow{
		ID: str(0), OrgID: str(1), UserID: get(2), Action: str(3), ResourceType: str(4), ResourceID: str(5), Description: get(6),
		Changes: str(7), RequestMetadata: str(8), Status: str(9), ErrorMessage: get(10),
	}
}
