package ssoaudit

import (
	"context"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

type exec struct {
	sql  string
	args []any
}

// recordingTx is a transaction that records each statement; failAt makes that statement (0-based) fail.
type recordingTx struct {
	pgx.Tx
	execs      []exec
	failAt     int
	committed  bool
	rolledBack bool
}

func (r *recordingTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.execs = append(r.execs, exec{sql, args})
	if len(r.execs)-1 == r.failAt {
		return pgconn.CommandTag{}, errors.New("permission denied")
	}
	return pgconn.CommandTag{}, nil
}

func (r *recordingTx) Commit(context.Context) error {
	r.committed = true
	return nil
}

func (r *recordingTx) Rollback(context.Context) error {
	if !r.committed {
		r.rolledBack = true
	}
	return nil
}

type beginner struct {
	tx  *recordingTx
	err error
}

func (b beginner) Begin(context.Context) (pgx.Tx, error) {
	if b.err != nil {
		return nil, b.err
	}
	return b.tx, nil
}

type recordedExec struct{ exec }

func (r *recordedExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql, r.args = sql, args
	return pgconn.CommandTag{}, nil
}

var setColumns = regexp.MustCompile(`(?is)UPDATE\s+public\.sso_providers\s+SET\s+(.*?)\s+WHERE\s+id\s*=\s*\$1\s*$`)

// goAPIFailureRow is the audit_logs row go-api's recordSSOFailure (internal/api/sso/login.go) hands its writer for a
// provider failure: the protocol, no stage, the sanitized message.
func goAPIFailureRow(t *testing.T, org, provider uuid.UUID, protocol, message string, at time.Time) *recordedExec {
	t.Helper()
	sanitized := pythonparity.SanitizeErrorTextHardened(message, 4000)
	meta := pyjson.NewObject()
	meta.Set("protocol", protocol)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	goAPI := &recordedExec{}
	if _, err := (audit.PGWriter{Now: func() time.Time { return at }}).Write(context.Background(), goAPI, audit.Entry{
		OrgID: org, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSSOProvider,
		ResourceID: provider.String(), Status: "failure", ErrorMessage: &sanitized, RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	return goAPI
}

// One transaction: the provider's last_error, last_error_at and updated_at (never its status), then go-api's
// audit_logs row with the same argument values and Go types go-api's writer gets, the id aside; then the commit.
func TestAProviderFailureRecordsTheErrorAndGoAPIsAuditRow(t *testing.T) {
	at := time.Date(2026, 10, 30, 8, 0, 0, 123, time.FixedZone("x", 3600))
	org, provider := uuid.New(), uuid.New()
	for _, tc := range []struct{ protocol, message string }{
		{"oauth", "OAuth token exchange failed"},
		{"oauth", "OAuth userinfo request failed"},
		{"saml", "SAML assertion missing email attribute"},
		{"saml", "x\x00" + strings.Repeat("m", 5000)},
	} {
		tx := &recordingTx{failAt: -1}
		err := PGProviderFailure{DB: beginner{tx: tx}, Now: func() time.Time { return at }}.Failed(context.Background(),
			Row{OrgID: org, ProviderID: provider, Protocol: tc.protocol, ErrorMessage: tc.message})
		if err != nil || !tx.committed || len(tx.execs) != 2 {
			t.Fatalf("%.30q: err %v committed %v with %d statements", tc.message, err, tx.committed, len(tx.execs))
		}
		sanitized := pythonparity.SanitizeErrorTextHardened(tc.message, 4000)
		update := tx.execs[0]
		m := setColumns.FindStringSubmatch(strings.TrimSpace(update.sql))
		if m == nil {
			t.Fatalf("not an UPDATE of public.sso_providers by id: %q", update.sql)
		}
		if got, want := strings.Join(strings.Fields(m[1]), " "), "last_error = $2, last_error_at = $3, updated_at = $3"; got != want {
			t.Fatalf("SET %q, want %q", got, want)
		}
		if strings.Contains(strings.ToLower(update.sql), "status") {
			t.Fatalf("the provider record writes the status: %q", update.sql)
		}
		if want := []any{provider, sanitized, at.UTC()}; !reflect.DeepEqual(update.args, want) {
			t.Fatalf("UPDATE args %#v, want %#v", update.args, want)
		}
		goAPI := goAPIFailureRow(t, org, provider, tc.protocol, tc.message, at)
		row := tx.execs[1]
		if !strings.Contains(row.sql, "INSERT INTO public.audit_logs") || len(row.args) != len(goAPI.args) {
			t.Fatalf("audit row %q with %d args, go-api %d", row.sql, len(row.args), len(goAPI.args))
		}
		for i := 1; i < len(goAPI.args); i++ {
			if !reflect.DeepEqual(row.args[i], goAPI.args[i]) {
				t.Errorf("%.30q: argument %d = %#v, go-api %#v", tc.message, i+1, row.args[i], goAPI.args[i])
			}
		}
	}
}

// A failed statement writes nothing: the transaction is never committed, and an UPDATE that fails writes no row.
func TestAFailedProviderFailureRecordWritesNothing(t *testing.T) {
	row := Row{OrgID: uuid.New(), ProviderID: uuid.New(), Protocol: "saml", ErrorMessage: "SAML assertion missing email attribute"}
	for failAt, statements := range map[int]int{0: 1, 1: 2} {
		tx := &recordingTx{failAt: failAt}
		if err := (PGProviderFailure{DB: beginner{tx: tx}}).Failed(context.Background(), row); err == nil {
			t.Fatalf("statement %d failing: no error", failAt)
		}
		if tx.committed || !tx.rolledBack || len(tx.execs) != statements {
			t.Fatalf("statement %d failing: committed %v rolled back %v, %d statements", failAt, tx.committed, tx.rolledBack, len(tx.execs))
		}
	}
	if err := (PGProviderFailure{DB: beginner{err: errors.New("pool closed")}}).Failed(context.Background(), row); err == nil {
		t.Fatal("a failed BEGIN: no error")
	}
}

// The audit row Insert writes is go-api's for a stage too (the denial rows of the OAuth callback).
func TestInsertWritesTheStage(t *testing.T) {
	at := time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)
	got := &recordedExec{}
	row := Row{OrgID: uuid.New(), ProviderID: uuid.New(), Protocol: "oauth", Stage: "domain_check", ErrorMessage: "m"}
	if err := Insert(context.Background(), got, row, at); err != nil {
		t.Fatal(err)
	}
	if meta := got.args[8]; !reflect.DeepEqual(meta, []byte(`{"protocol": "oauth", "stage": "domain_check"}`)) {
		t.Fatalf("request_metadata %s", meta)
	}
}

// goAPISuccessRow is the audit_logs row finishSSOLogin (internal/api/sso/login.go) hands its writer.
func goAPISuccessRow(t *testing.T, org, provider, user uuid.UUID, protocol string, at time.Time) *recordedExec {
	t.Helper()
	meta := pyjson.NewObject()
	meta.Set("provider_id", provider.String())
	meta.Set("protocol", protocol)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		t.Fatal(err)
	}
	goAPI := &recordedExec{}
	if _, err := (audit.PGWriter{Now: func() time.Time { return at }}).Write(context.Background(), goAPI, audit.Entry{
		OrgID: org, UserID: &user, Action: audit.ActionSSOLogin, ResourceType: audit.ResourceSession,
		ResourceID: user.String(), RequestMetadata: []byte(metaJSON),
	}); err != nil {
		t.Fatal(err)
	}
	return goAPI
}

// One transaction: the provider's last_login_at and updated_at at one time, then go-api's audit_logs row with the same
// argument values and Go types go-api's writer gets, the id aside; then the commit.
func TestASignInRecordsLastLoginAndGoAPIsAuditRow(t *testing.T) {
	at := time.Date(2026, 10, 30, 8, 0, 0, 123, time.FixedZone("x", 3600))
	org, provider, user := uuid.New(), uuid.New(), uuid.New()
	for _, protocol := range []string{"oidc", "saml", "oauth_gitlab", "oauth_github"} {
		tx := &recordingTx{failAt: -1}
		err := PGSignIn{DB: beginner{tx: tx}, Now: func() time.Time { return at }}.Succeeded(context.Background(),
			Success{OrgID: org, ProviderID: provider, PrincipalID: user, Protocol: protocol})
		if err != nil {
			t.Fatal(err)
		}
		if len(tx.execs) != 2 || !tx.committed {
			t.Fatalf("%s: execs %d, committed %v", protocol, len(tx.execs), tx.committed)
		}
		update := tx.execs[0]
		if !strings.Contains(update.sql, "last_login_at = $2, updated_at = $2") || !strings.HasSuffix(update.sql, "WHERE id = $1") ||
			!reflect.DeepEqual(update.args, []any{provider, at.UTC()}) {
			t.Fatalf("%s: update %q %v", protocol, update.sql, update.args)
		}
		goAPI := goAPISuccessRow(t, org, provider, user, protocol, at)
		row := tx.execs[1]
		if len(row.args) != len(goAPI.args) {
			t.Fatalf("%s: %d args, go-api %d", protocol, len(row.args), len(goAPI.args))
		}
		for i := 1; i < len(row.args); i++ {
			if !reflect.DeepEqual(row.args[i], goAPI.args[i]) {
				t.Errorf("%s: arg %d = %#v, go-api %#v", protocol, i, row.args[i], goAPI.args[i])
			}
		}
	}
}

func TestAFailedSignInRecordStatementCommitsNothing(t *testing.T) {
	for failAt := range 2 {
		tx := &recordingTx{failAt: failAt}
		if err := (PGSignIn{DB: beginner{tx: tx}}).Succeeded(context.Background(), Success{}); err == nil {
			t.Fatalf("statement %d failed and the record reported success", failAt)
		}
		if tx.committed || !tx.rolledBack {
			t.Fatalf("statement %d: committed %v, rolled back %v", failAt, tx.committed, tx.rolledBack)
		}
	}
	if err := (PGSignIn{DB: beginner{err: errors.New("pool closed")}}).Succeeded(context.Background(), Success{}); err == nil {
		t.Fatal("a failed begin reported success")
	}
}
