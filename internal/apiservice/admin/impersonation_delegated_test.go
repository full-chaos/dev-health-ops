package admin

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

type delegatedRow struct{ vals []any }

func (r delegatedRow) Scan(dest ...any) error {
	for i, d := range dest {
		switch p := d.(type) {
		case *uuid.UUID:
			*p = r.vals[i].(uuid.UUID)
		case *string:
			*p = r.vals[i].(string)
		case *bool:
			*p = r.vals[i].(bool)
		case *time.Time:
			*p = r.vals[i].(time.Time)
		case **string:
			email := r.vals[i].(string)
			*p = &email
		default:
			return fmt.Errorf("unexpected scan destination %T", d)
		}
	}
	return nil
}

// delegatedDB scripts the reads of the impersonation routes and records every begin and statement.
type delegatedDB struct {
	pgDB
	target       *delegatedTarget
	org          *uuid.UUID
	open         *uuid.UUID
	begins       int
	statements   []string
	txStatements []string

	// openBy, when set, keys the open impersonation row by the admin the lookup names; lookups and ended record
	// the admin of each lookup and the id of each row the stop ends.
	openBy  map[uuid.UUID]uuid.UUID
	lookups []uuid.UUID
	ended   []uuid.UUID
}

type delegatedTarget struct {
	id                uuid.UUID
	active, superuser bool
}

func (d *delegatedDB) Begin(context.Context) (pgx.Tx, error) {
	d.begins++
	return &delegatedTx{db: d}, nil
}

func (d *delegatedDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	d.statements = append(d.statements, sql)
	switch {
	case strings.Contains(sql, "FROM memberships"):
		if d.org == nil {
			return delegatedRow{nil}.noRows()
		}
		return delegatedRow{[]any{*d.org, "member"}}
	case strings.Contains(sql, "FROM impersonation_sessions"):
		if d.openBy != nil {
			admin, _ := args[0].(uuid.UUID)
			d.lookups = append(d.lookups, admin)
			if id, ok := d.openBy[admin]; ok {
				d.open = &id
			} else {
				d.open = nil
			}
		}
		if d.open == nil {
			return delegatedRow{nil}.noRows()
		}
		row := []any{*d.open, uuid.New(), uuid.New(), "member", "t@example.com", time.Unix(1_800_000_900, 0)}
		if strings.Contains(sql, "JOIN users") {
			return delegatedRow{row}
		}
		return delegatedRow{append(row[:4:4], row[5])}
	}
	if d.target == nil {
		return delegatedRow{nil}.noRows()
	}
	return delegatedRow{[]any{d.target.id, "t@example.com", d.target.active, d.target.superuser}}
}

type noRowsRow struct{}

func (noRowsRow) Scan(...any) error { return pgx.ErrNoRows }

func (delegatedRow) noRows() pgx.Row { return noRowsRow{} }

type delegatedTx struct {
	pgx.Tx
	db *delegatedDB
}

func (x *delegatedTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	x.db.txStatements = append(x.db.txStatements, sql)
	if strings.HasPrefix(sql, "UPDATE impersonation_sessions") {
		id, _ := args[0].(uuid.UUID)
		x.db.ended = append(x.db.ended, id)
	}
	return pgconn.NewCommandTag("INSERT 1"), nil
}

func (x *delegatedTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	x.db.txStatements = append(x.db.txStatements, sql)
	return x.db.QueryRow(ctx, sql, args...)
}
func (x *delegatedTx) Commit(context.Context) error   { return nil }
func (x *delegatedTx) Rollback(context.Context) error { return nil }

type fakeDelegator struct {
	started []sessionissue.DelegatedRequest
	ended   []uuid.UUID
	session sessionissue.DelegatedSession
	err     error
	// onStart, when set, runs as auth-service is asked to start a session.
	onStart func()
}

func (f *fakeDelegator) StartDelegated(_ context.Context, _ *http.Request, req sessionissue.DelegatedRequest) (sessionissue.DelegatedSession, error) {
	if f.onStart != nil {
		f.onStart()
	}
	f.started = append(f.started, req)
	return f.session, f.err
}

func (f *fakeDelegator) EndDelegated(_ context.Context, _ *http.Request, id uuid.UUID) error {
	f.ended = append(f.ended, id)
	return f.err
}

type delegatedCase struct {
	h          *handlers
	db         *delegatedDB
	delegator  *fakeDelegator
	admin      uuid.UUID
	target     uuid.UUID
	org        uuid.UUID
	sid        uuid.UUID
	expiresAt  time.Time
	sessionRow uuid.UUID
}

func newDelegatedCase(withDelegator bool) *delegatedCase {
	c := &delegatedCase{admin: uuid.New(), target: uuid.New(), org: uuid.New(), sid: uuid.New(), sessionRow: uuid.New(), expiresAt: time.Unix(1_800_000_900, 0).UTC()}
	c.db = &delegatedDB{target: &delegatedTarget{id: c.target, active: true}, org: &c.org}
	c.delegator = &fakeDelegator{session: sessionissue.DelegatedSession{
		SessionID: c.sessionRow, ExpiresAt: c.expiresAt, Access: "a.b.c", SessionAccess: "s.t.u", ExpiresIn: 900,
	}}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c.h = &handlers{
		store:  pgStore{Pool: c.db, Now: func() time.Time { return time.Unix(1_800_000_000, 0) }},
		audit:  audit.PGWriter{},
		cache:  newImpersonationCache(nil, nil, logger),
		logger: logger,
	}
	if withDelegator {
		c.h.delegator = c.delegator
	}
	return c
}

func (c *delegatedCase) do(handler http.Handler, target uuid.UUID, withSession bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/impersonate", strings.NewReader("{}"))
	body, _, _, _ := pybody.Read(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"target_user_id":"`+target.String()+`"}`)))
	user := &policy.User{UserID: c.admin.String()}
	if withSession {
		user.SessionID = &c.sid
	}
	ctx := policy.WithBody(policy.WithUser(req.Context(), user), body)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req.WithContext(ctx))
	return rec
}

func (c *delegatedCase) allStatements() string {
	return strings.Join(append(append([]string(nil), c.db.statements...), c.db.txStatements...), "\n")
}

func TestDelegatedStartAsksAuthServiceAndWritesNoRowOrAudit(t *testing.T) {
	c, _ := newHandoffCase(t)
	rec := c.do(c.h.impersonateStart(), c.target, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(c.delegator.started) != 1 {
		t.Fatalf("auth-service starts = %d", len(c.delegator.started))
	}
	got := c.delegator.started[0]
	if got.BaseSessionID != c.sid || got.TargetPrincipalID != c.target || got.OrganizationID != c.org || got.Reason != "admin_impersonation" {
		t.Fatalf("request = %+v", got)
	}
	if c.db.begins != 0 || strings.Contains(c.allStatements(), "INSERT") || strings.Contains(c.allStatements(), "UPDATE") {
		t.Fatalf("go-api wrote: begins=%d statements=%s", c.db.begins, c.allStatements())
	}
	body := rec.Body.String()
	for _, want := range []string{`"status":"active"`, `"access_token":"a.b.c"`, `"token_type":"bearer"`, `"expires_in":900`, `"session_access_token":"s.t.u"`, `"target_user"`, `"expires_at"`, `"handoff_id":"`} {
		if !strings.Contains(body, want) {
			t.Errorf("answer lacks %s: %s", want, body)
		}
	}
}

func TestDelegatedStartWithoutSessionTokenIsRefusedAndAsksNothing(t *testing.T) {
	c := newDelegatedCase(true)
	rec := c.do(c.h.impersonateStart(), c.target, false)
	if rec.Code != http.StatusConflict || len(c.delegator.started) != 0 {
		t.Fatalf("status %d, starts %d: %s", rec.Code, len(c.delegator.started), rec.Body.String())
	}
}

func TestDelegatedStartFailsClosedWhenAuthServiceFails(t *testing.T) {
	for name, err := range map[string]error{
		"unavailable": sessionissue.ErrUnavailable,
		"refused":     &sessionissue.RefusedError{Status: http.StatusForbidden, Reason: "not_entitled"},
		"plain":       errors.New("boom"),
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := newHandoffCase(t)
			c.delegator.err = err
			rec := c.do(c.h.impersonateStart(), c.target, true)
			if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), sessionissue.UnavailableText) || len(c.delegator.started) != 1 {
				t.Fatalf("status %d, %d auth-service starts: %s", rec.Code, len(c.delegator.started), rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "access_token") || c.db.begins != 0 || strings.Contains(c.allStatements(), "INSERT") {
				t.Fatalf("fallback or token leaked: begins=%d %s", c.db.begins, rec.Body.String())
			}
		})
	}
}

// With the flag on the target checks answer as go-api's own start answers them.
func TestDelegatedStartRefusesATargetAsTheLegacyStartDoes(t *testing.T) {
	type mutate func(c *delegatedCase) uuid.UUID
	for name, m := range map[string]mutate{
		"self":      func(c *delegatedCase) uuid.UUID { return c.admin },
		"missing":   func(c *delegatedCase) uuid.UUID { c.db.target = nil; return c.target },
		"inactive":  func(c *delegatedCase) uuid.UUID { c.db.target.active = false; return c.target },
		"superuser": func(c *delegatedCase) uuid.UUID { c.db.target.superuser = true; return c.target },
		"no_member": func(c *delegatedCase) uuid.UUID { c.db.org = nil; return c.target },
	} {
		t.Run(name, func(t *testing.T) {
			legacy, flagged := newDelegatedCase(false), newDelegatedCase(true)
			flagged.admin, flagged.target, flagged.org = legacy.admin, legacy.target, legacy.org
			flagged.db.target, flagged.db.org = &delegatedTarget{id: legacy.target, active: true}, &legacy.org
			legacy.db.target, legacy.db.org = &delegatedTarget{id: legacy.target, active: true}, &legacy.org
			legacyTarget, flaggedTarget := m(legacy), m(flagged)
			want := legacy.do(legacy.h.impersonateStart(), legacyTarget, true)
			got := flagged.do(flagged.h.impersonateStart(), flaggedTarget, true)
			if want.Code == http.StatusOK {
				t.Fatalf("legacy start accepted %s", name)
			}
			if got.Code != want.Code || got.Body.String() != want.Body.String() {
				t.Fatalf("flag on: %d %s; legacy: %d %s", got.Code, got.Body.String(), want.Code, want.Body.String())
			}
			if len(flagged.delegator.started) != 0 {
				t.Fatalf("auth-service asked for a refused target")
			}
		})
	}
}

// Flag off (no Delegator): go-api's own start, unchanged: one transaction with the end of prior rows, the new row and the audit row.
func TestStartWithoutDelegatorIsGoAPIsOwnStart(t *testing.T) {
	c := newDelegatedCase(false)
	rec := c.do(c.h.impersonateStart(), c.target, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if c.db.begins != 1 || len(c.delegator.started) != 0 {
		t.Fatalf("begins=%d delegated=%d", c.db.begins, len(c.delegator.started))
	}
	joined := strings.Join(c.db.txStatements, "\n")
	for _, want := range []string{"UPDATE impersonation_sessions", "INSERT INTO impersonation_sessions", "INSERT INTO audit_logs"} {
		if !strings.Contains(joined, want) {
			t.Errorf("transaction lacks %q:\n%s", want, joined)
		}
	}
	for _, key := range []string{"access_token", "token_type", "expires_in"} {
		if strings.Contains(rec.Body.String(), key) {
			t.Errorf("flag-off answer holds %s: %s", key, rec.Body.String())
		}
	}
	keys := rec.Body.String()
	if !strings.HasPrefix(keys, `{"status":"active","target_user":{`) || !strings.Contains(keys, `"expires_at":`) {
		t.Errorf("answer members changed: %s", keys)
	}
}

func TestDelegatedStopEndsTheOpenSessionAtAuthServiceAndWritesNothing(t *testing.T) {
	c := newDelegatedCase(true)
	open := uuid.New()
	c.db.open = &open
	rec := c.do(c.h.impersonateStop(), c.target, true)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status":"stopped"`) {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if len(c.delegator.ended) != 1 || c.delegator.ended[0] != open {
		t.Fatalf("ended = %v, want %s", c.delegator.ended, open)
	}
	if c.db.begins != 0 || strings.Contains(c.allStatements(), "UPDATE") || strings.Contains(c.allStatements(), "INSERT") {
		t.Fatalf("go-api wrote: begins=%d %s", c.db.begins, c.allStatements())
	}
}

func TestDelegatedStopFailsClosed(t *testing.T) {
	c := newDelegatedCase(true)
	open := uuid.New()
	c.db.open = &open
	c.delegator.err = sessionissue.ErrUnavailable
	rec := c.do(c.h.impersonateStop(), c.target, true)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), sessionissue.UnavailableText) {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	if c.db.begins != 0 {
		t.Fatalf("go-api began a transaction")
	}
}

func TestDelegatedStopWithNoOpenSessionAsksNothing(t *testing.T) {
	c := newDelegatedCase(true)
	rec := c.do(c.h.impersonateStop(), c.target, true)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "No active impersonation session") || len(c.delegator.ended) != 0 {
		t.Fatalf("status %d, ends %d: %s", rec.Code, len(c.delegator.ended), rec.Body.String())
	}
}

func TestStopWithoutDelegatorIsGoAPIsOwnStop(t *testing.T) {
	c := newDelegatedCase(false)
	open := uuid.New()
	c.db.open = &open
	rec := c.do(c.h.impersonateStop(), c.target, true)
	if rec.Code != http.StatusOK || c.db.begins != 1 || len(c.delegator.ended) != 0 {
		t.Fatalf("status %d begins %d: %s", rec.Code, c.db.begins, rec.Body.String())
	}
	joined := strings.Join(c.db.txStatements, "\n")
	if !strings.Contains(joined, "UPDATE impersonation_sessions SET ended_at") || !strings.Contains(joined, "INSERT INTO audit_logs") {
		t.Errorf("stop transaction:\n%s", joined)
	}
}

// A caller with no auth-service session (a go-api token) cannot stop a delegated session either: 409, the row stays open and
// auth-service is not asked. With no open row the answer stays 400.
func TestDelegatedStopWithoutSessionTokenIsRefusedAndEndsNothing(t *testing.T) {
	c := newDelegatedCase(true)
	open := uuid.New()
	c.db.open = &open
	rec := c.do(c.h.impersonateStop(), c.target, false)
	if rec.Code != http.StatusConflict || len(c.delegator.ended) != 0 {
		t.Fatalf("status %d, ends %d: %s", rec.Code, len(c.delegator.ended), rec.Body.String())
	}
	none := newDelegatedCase(true)
	rec = none.do(none.h.impersonateStop(), none.target, false)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "No active impersonation session") {
		t.Fatalf("no open row: status %d: %s", rec.Code, rec.Body.String())
	}
}
