package publicaudit

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
)

type recordedExec struct {
	sql  string
	args []any
	err  error
}

func (r *recordedExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql, r.args = sql, args
	return pgconn.CommandTag{}, r.err
}

// Insert hands the database the argument values and Go types go-api's writer (internal/api/audit.PGWriter) hands it
// for the same entry, the id aside, for every column shape: a nil and a set user, description and error message,
// empty and set JSON columns, an empty and a set status, a created_at outside UTC.
func TestInsertWritesGoAPIsArguments(t *testing.T) {
	at := time.Date(2026, 10, 31, 9, 30, 0, 123456000, time.FixedZone("x", -7200))
	org, user := uuid.New(), uuid.New()
	description, message := "User logged in", "Invalid credentials"
	for name, entry := range map[string]audit.Entry{
		"bare": {OrgID: org, Action: "sso_login", ResourceType: "sso_provider", ResourceID: "p"},
		"full": {OrgID: org, UserID: &user, Action: "login_failed", ResourceType: "session", ResourceID: user.String(),
			Description: &description, Changes: []byte(`{"email": "a@example.com"}`),
			RequestMetadata: []byte(`{"ip_address": "203.0.113.7", "user_agent": "ua"}`), Status: "failure", ErrorMessage: &message},
	} {
		goAPI := &recordedExec{}
		if _, err := (audit.PGWriter{Now: func() time.Time { return at }}).Write(context.Background(), goAPI, entry); err != nil {
			t.Fatal(err)
		}
		got := &recordedExec{}
		row := Row{OrgID: entry.OrgID, UserID: entry.UserID, Action: string(entry.Action), ResourceType: string(entry.ResourceType),
			ResourceID: entry.ResourceID, Description: entry.Description, Changes: entry.Changes, RequestMetadata: entry.RequestMetadata,
			Status: entry.Status, ErrorMessage: entry.ErrorMessage, CreatedAt: at}
		if err := Insert(context.Background(), got, row); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(got.sql, "INSERT INTO public.audit_logs") || len(got.args) != len(goAPI.args) {
			t.Fatalf("%s: %q with %d args, go-api %d", name, got.sql, len(got.args), len(goAPI.args))
		}
		if id, ok := got.args[0].(uuid.UUID); !ok || id == uuid.Nil {
			t.Errorf("%s: id %#v, want a new uuid", name, got.args[0])
		}
		for i := 1; i < len(goAPI.args); i++ {
			if !reflect.DeepEqual(got.args[i], goAPI.args[i]) {
				t.Errorf("%s: argument %d = %#v, go-api %#v", name, i+1, got.args[i], goAPI.args[i])
			}
		}
	}
}

// InsertInAuditTx hands the same values, the two JSON columns as their text, and casts them to json in the SQL.
func TestInsertInAuditTxWritesGoAPIsValuesWithTextJSON(t *testing.T) {
	at := time.Date(2026, 10, 31, 9, 30, 0, 0, time.FixedZone("x", 3600))
	org, user := uuid.New(), uuid.New()
	description := "User registered"
	entry := audit.Entry{OrgID: org, UserID: &user, Action: "create", ResourceType: "user", ResourceID: user.String(),
		Description: &description, Changes: []byte(`{"email": "a@example.com"}`)}
	goAPI := &recordedExec{}
	if _, err := (audit.PGWriter{Now: func() time.Time { return at }}).Write(context.Background(), goAPI, entry); err != nil {
		t.Fatal(err)
	}
	got := &recordedExec{}
	if err := InsertInAuditTx(context.Background(), got, Row{OrgID: org, UserID: &user, Action: "create", ResourceType: "user",
		ResourceID: user.String(), Description: &description, Changes: entry.Changes, CreatedAt: at}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.sql, "$8::json, $9::json") || len(got.args) != len(goAPI.args) {
		t.Fatalf("%q with %d args, go-api %d", got.sql, len(got.args), len(goAPI.args))
	}
	for i := 1; i < len(goAPI.args); i++ {
		want := goAPI.args[i]
		if raw, ok := want.([]byte); ok {
			want = string(raw)
		}
		if !reflect.DeepEqual(got.args[i], want) {
			t.Errorf("argument %d = %#v, go-api %#v", i+1, got.args[i], want)
		}
	}
}

func TestInsertReturnsTheExecError(t *testing.T) {
	boom := errors.New("permission denied")
	if err := Insert(context.Background(), &recordedExec{err: boom}, Row{}); !errors.Is(err, boom) {
		t.Fatalf("Insert = %v, want the Exec error", err)
	}
}
