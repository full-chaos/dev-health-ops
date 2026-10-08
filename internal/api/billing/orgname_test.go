package billing

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type nameRow struct {
	name *string
	err  error
}

func (r nameRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*(dest[0].(**string)) = r.name
	return nil
}

// nameDB answers `SELECT name FROM organizations WHERE id = $1` from a map
// keyed by the id argument, and records every id it was asked for.
type nameDB struct {
	names     map[uuid.UUID]*string
	failRead  error
	failBegin error
	refundOrg uuid.UUID
	asked     []uuid.UUID
	begun     int
	rolled    int
	committed int
}

func (d *nameDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return noRows{}, nil
}

type noRows struct{ pgx.Rows }

func (noRows) Next() bool { return false }
func (noRows) Err() error { return nil }
func (noRows) Close()     {}

func (d *nameDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.New("unused")
}

func (d *nameDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "FROM refunds") {
		return fakeRefundRow{org: d.refundOrg}
	}
	id := args[0].(uuid.UUID)
	d.asked = append(d.asked, id)
	if d.failRead != nil {
		return nameRow{err: d.failRead}
	}
	name, found := d.names[id]
	if !found {
		return nameRow{err: pgx.ErrNoRows}
	}
	return nameRow{name: name}
}

// fakeRefundRow is a stored refunds row of one org.
type fakeRefundRow struct{ org uuid.UUID }

func (r fakeRefundRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = uuid.New()
	*(dest[1].(*uuid.UUID)) = r.org
	*(dest[7].(*int32)) = 100
	*(dest[8].(*string)) = "usd"
	*(dest[9].(*string)) = "succeeded"
	return nil
}

func (d *nameDB) Begin(context.Context) (pgx.Tx, error) {
	if d.failBegin != nil {
		return nil, d.failBegin
	}
	d.begun++
	return &nameSavepoint{db: d}, nil
}

type nameSavepoint struct {
	pgx.Tx
	db *nameDB
}

func (s *nameSavepoint) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return s.db.QueryRow(ctx, sql, args...)
}
func (s *nameSavepoint) Rollback(context.Context) error { s.db.rolled++; return nil }
func (s *nameSavepoint) Commit(context.Context) error   { s.db.committed++; return nil }

func strp(s string) *string { return &s }

func payloadOf(org uuid.UUID) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", "row-1")
	out.Set("org_id", org.String())
	out.Set("status", "open")
	return out
}

func orgNameOf(t *testing.T, payload *pyjson.Object) pyjson.Value {
	t.Helper()
	value, present := payload.Get("org_name")
	if !present {
		t.Fatal("org_name key missing: the key must always be present (null when unknown)")
	}
	return value
}

func logged() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, nil)), &buf
}

func TestWithOrgNameServesTheRowsOwnOrgName(t *testing.T) {
	own, other := uuid.New(), uuid.New()
	db := &nameDB{names: map[uuid.UUID]*string{own: strp("Acme"), other: strp("Other Org")}}
	h := handlers{logger: slog.Default()}
	payload := payloadOf(own)
	h.withOrgName(context.Background(), db, payload)
	if got := orgNameOf(t, payload); got != "Acme" {
		t.Fatalf("org_name = %v, want Acme", got)
	}
	if len(db.asked) != 1 || db.asked[0] != own {
		t.Fatalf("lookup asked for %v, want only the row's own org %v", db.asked, own)
	}
	if db.committed != 1 || db.rolled != 0 {
		t.Fatalf("savepoint committed=%d rolled=%d, want 1/0", db.committed, db.rolled)
	}
}

func TestWithOrgNameIsNullWhenThereIsNoName(t *testing.T) {
	own := uuid.New()
	cases := map[string]map[uuid.UUID]*string{
		"no row":     {},
		"null name":  {own: nil},
		"blank name": {own: strp("   ")},
	}
	for label, names := range cases {
		db := &nameDB{names: names}
		h := handlers{logger: slog.Default()}
		payload := payloadOf(own)
		h.withOrgName(context.Background(), db, payload)
		if got := orgNameOf(t, payload); got != nil {
			t.Errorf("%s: org_name = %v, want null", label, got)
		}
		if strings.Contains(mustDump(t, payload), own.String()+`", "org_name": "`+own.String()) {
			t.Errorf("%s: the id was served as the name", label)
		}
	}
}

func TestWithOrgNameIsNullForAPayloadWithoutAUsableOrgID(t *testing.T) {
	db := &nameDB{names: map[uuid.UUID]*string{}}
	h := handlers{logger: slog.Default()}
	for _, org := range []pyjson.Value{nil, "not-a-uuid", 7} {
		payload := pyjson.NewObject()
		payload.Set("org_id", org)
		h.withOrgName(context.Background(), db, payload)
		if got := orgNameOf(t, payload); got != nil {
			t.Errorf("org_id %v: org_name = %v, want null", org, got)
		}
	}
	if len(db.asked) != 0 {
		t.Fatalf("lookup ran for an unusable org_id: %v", db.asked)
	}
}

func TestWithOrgNameLookupFailureKeepsTheRow(t *testing.T) {
	own := uuid.New()
	for label, db := range map[string]*nameDB{
		"read fails":      {names: map[uuid.UUID]*string{own: strp("Acme")}, failRead: errors.New("boom")},
		"savepoint fails": {names: map[uuid.UUID]*string{own: strp("Acme")}, failBegin: errors.New("no savepoint")},
	} {
		logger, buf := logged()
		h := handlers{logger: logger}
		payload := payloadOf(own)
		h.withOrgName(context.Background(), db, payload)
		if got := orgNameOf(t, payload); got != nil {
			t.Errorf("%s: org_name = %v, want null", label, got)
		}
		if id, _ := payload.Get("id"); id != "row-1" {
			t.Errorf("%s: row lost its fields", label)
		}
		if !strings.Contains(buf.String(), "org name lookup") {
			t.Errorf("%s: failure was not logged: %q", label, buf.String())
		}
		if label == "read fails" && db.rolled != 1 {
			t.Errorf("%s: savepoint rolled back %d times, want 1 (the transaction must stay usable)", label, db.rolled)
		}
	}
}

func mustDump(t *testing.T, payload *pyjson.Object) string {
	t.Helper()
	text, err := pyjson.Dumps(payload)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

// The three billing payload builders each carry org_name for their own row's
// org, and none serves another org's name.
func TestBillingPayloadsCarryTheirOwnOrgName(t *testing.T) {
	own, other := uuid.New(), uuid.New()
	db := &nameDB{refundOrg: own, names: map[uuid.UUID]*string{own: strp("Acme"), other: strp("Other Org")}}
	h := handlers{logger: slog.Default()}
	ctx := context.Background()

	invoice, err := h.invoiceJSON(ctx, db, invoiceRow{ID: uuid.New(), OrgID: own, Status: "open", Currency: "usd"}, false)
	if err != nil {
		t.Fatal(err)
	}
	view, err := h.subscriptionView(ctx, db, subscriptionRow{ID: uuid.New(), OrgID: own, Status: "active"})
	if err != nil {
		t.Fatal(err)
	}
	refundAnswer, err := h.refundReply(ctx, db, uuid.New(), nil)
	if err != nil {
		t.Fatal(err)
	}
	refund, isObject := refundAnswer.body.(*pyjson.Object)
	if !isObject {
		t.Fatalf("refund reply body is %T", refundAnswer.body)
	}
	for label, payload := range map[string]*pyjson.Object{"invoice": invoice, "subscription": view, "refund": refund} {
		if got := orgNameOf(t, payload); got != "Acme" {
			t.Errorf("%s: org_name = %v, want Acme", label, got)
		}
		if strings.Contains(mustDump(t, payload), "Other Org") {
			t.Errorf("%s: another org's name was served", label)
		}
	}
	for _, id := range db.asked {
		if id != own {
			t.Errorf("a lookup read org %v, want only %v", id, own)
		}
	}
}
