package admin

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	wantOrgDeletionRefusedDetail = "ClickHouse is not configured, not reachable or not permitted to purge; organization deletion refused. Nothing was deleted."
	// closedClickHouseDSN names a port nothing listens on, with a password the
	// logs must never carry.
	closedClickHouseDSN    = "clickhouse://purger:pw-orgdel-unit-7d1c@127.0.0.1:1/default"
	closedClickHouseSecret = "pw-orgdel-unit-7d1c"
)

// touchRecorder is a pgDB that records every call and answers each with an
// error: an org deletion that refuses up front makes none.
type touchRecorder struct {
	mu    sync.Mutex
	calls []string
}

var errTouched = errors.New("touchRecorder: no database in this test")

func (r *touchRecorder) record(sql string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, strings.Join(strings.Fields(sql), " "))
}

func (r *touchRecorder) Begin(context.Context) (pgx.Tx, error) {
	r.record("BEGIN")
	return nil, errTouched
}

func (r *touchRecorder) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.record(sql)
	return pgconn.CommandTag{}, errTouched
}

func (r *touchRecorder) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	r.record(sql)
	return nil, errTouched
}

func (r *touchRecorder) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.record(sql)
	return errRow{}
}

type errRow struct{}

func (errRow) Scan(...any) error { return errTouched }

func (r *touchRecorder) touched() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

// An org delete (real or dry run) with no usable ClickHouse answers 503 and
// makes no Postgres call at all: nothing is counted, revoked or deleted.
func TestOrgDeletionRouteRefusesWithoutAUsableClickHouse(t *testing.T) {
	for _, tc := range []struct {
		name, dsn, query, reason string
	}{
		{"no dsn, real delete", "", "", "dsn_unset"},
		{"no dsn, dry run", "", "?dry_run=true", "dsn_unset"},
		{"closed port, real delete", closedClickHouseDSN, "", "open_failed"},
		{"closed port, dry run", closedClickHouseDSN, "?dry_run=true", "open_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			db := &touchRecorder{}
			h := &handlers{
				store:         pgStore{Pool: db},
				logger:        slog.New(slog.NewJSONHandler(&logs, nil)),
				clickHouseDSN: tc.dsn,
			}
			orgID := uuid.NewString()
			request := httptest.NewRequest(http.MethodDelete, "/api/v1/admin/orgs/"+orgID+tc.query, nil)
			request.SetPathValue("org_id", orgID)
			recorder := httptest.NewRecorder()
			func() {
				defer func() {
					if panicked := recover(); panicked != nil {
						t.Errorf("the handler panicked: %v", panicked)
					}
				}()
				h.deleteOrganization(recorder, request)
			}()
			if recorder.Code != http.StatusServiceUnavailable {
				t.Errorf("status = %d, want 503; body %s", recorder.Code, recorder.Body.String())
			}
			if want := fmt.Sprintf(`{"detail":%q}`, wantOrgDeletionRefusedDetail); strings.TrimSpace(recorder.Body.String()) != want {
				t.Errorf("body = %s, want %s", recorder.Body.String(), want)
			}
			if calls := db.touched(); len(calls) > 0 {
				t.Errorf("the refused delete made %d Postgres call(s), want 0: %v", len(calls), calls)
			}
			line := logs.String()
			for _, want := range []string{`"level":"ERROR"`, `"msg":"admin: org deletion refused"`, `"org_id":"` + orgID + `"`, `"reason":"` + tc.reason + `"`} {
				if !strings.Contains(line, want) {
					t.Errorf("log %q lacks %s", line, want)
				}
			}
			if strings.Contains(line, closedClickHouseSecret) || strings.Contains(recorder.Body.String(), closedClickHouseSecret) {
				t.Errorf("the ClickHouse password leaked: log %q body %q", line, recorder.Body.String())
			}
		})
	}
}

// `dho admin orgs delete` runs Operator.DeleteOrg: with no usable ClickHouse it
// refuses (the verb prints "Error: <message>" and exits 1) before any
// Postgres call.
func TestOperatorDeleteOrgRefusesWithoutAUsableClickHouse(t *testing.T) {
	for _, tc := range []struct {
		name, dsn string
		dryRun    bool
	}{
		{"no dsn", "", false},
		{"no dsn, dry run", "", true},
		{"closed port", closedClickHouseDSN, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			db := &touchRecorder{}
			operator := Operator{db: db, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
			var err error
			func() {
				defer func() {
					if panicked := recover(); panicked != nil {
						err = fmt.Errorf("panicked: %v", panicked)
					}
				}()
				_, err = operator.DeleteOrg(context.Background(), uuid.NewString(), tc.dryRun, DeleteOrgConfig{ClickHouseDSN: tc.dsn})
			}()
			var refusal *OperatorError
			if !errors.As(err, &refusal) || refusal.Message != wantOrgDeletionRefusedDetail {
				t.Errorf("err = %v, want the refusal %q", err, wantOrgDeletionRefusedDetail)
			}
			if calls := db.touched(); len(calls) > 0 {
				t.Errorf("the refused delete made %d Postgres call(s), want 0: %v", len(calls), calls)
			}
			if !strings.Contains(logs.String(), `"msg":"admin: org deletion refused"`) {
				t.Errorf("no refusal log line: %q", logs.String())
			}
			if strings.Contains(logs.String(), closedClickHouseSecret) {
				t.Errorf("the ClickHouse password leaked into the log: %q", logs.String())
			}
		})
	}
}
