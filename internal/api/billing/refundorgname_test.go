package billing

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stripe/stripe-go/v86"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// refundTx is a transaction fake for the refund paths that serve a refund:
// it answers each statement the path runs and delegates the organisation
// name read (and its savepoint) to nameDB.
type refundTx struct {
	pgx.Tx
	db      *nameDB
	invoice uuid.UUID
}

func (t *refundTx) Begin(ctx context.Context) (pgx.Tx, error) { return t.db.Begin(ctx) }

func (t *refundTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (t *refundTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "FROM invoices WHERE id"):
		return invoiceOwnerRow{org: t.db.refundOrg}
	case strings.Contains(sql, "idempotency_key FROM refunds WHERE"):
		return keyedRefundRow{invoice: t.invoice}
	case strings.Contains(sql, "status, failure_reason FROM refunds"):
		return priorRefundRow{}
	case strings.Contains(sql, "count(s.id)"):
		return countRow{}
	}
	return t.db.QueryRow(ctx, sql, args...)
}

func (t *refundTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &oneRefundRows{org: t.db.refundOrg, left: 1}, nil
}

type oneRefundRows struct {
	pgx.Rows
	org  uuid.UUID
	left int
}

func (r *oneRefundRows) Next() bool {
	if r.left == 0 {
		return false
	}
	r.left--
	return true
}
func (r *oneRefundRows) Scan(dest ...any) error { return fakeRefundRow{org: r.org}.Scan(dest...) }
func (r *oneRefundRows) Err() error             { return nil }
func (r *oneRefundRows) Close()                 {}

type countRow struct{}

func (countRow) Scan(dest ...any) error { *(dest[0].(*int64)) = 1; return nil }

type priorRefundRow struct{}

func (priorRefundRow) Scan(dest ...any) error { *(dest[0].(*string)) = "pending"; return nil }

type invoiceOwnerRow struct{ org uuid.UUID }

func (r invoiceOwnerRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = r.org
	*(dest[2].(*string)) = "paid"
	*(dest[3].(*int64)) = 1000
	*(dest[4].(*string)) = "usd"
	*(dest[6].(*string)) = "in_1"
	return nil
}

type keyedRefundRow struct{ invoice uuid.UUID }

func (r keyedRefundRow) Scan(dest ...any) error {
	*(dest[0].(*uuid.UUID)) = uuid.New()
	invoice := r.invoice
	*(dest[1].(**uuid.UUID)) = &invoice
	*(dest[2].(*int64)) = 100
	*(dest[3].(*string)) = "succeeded"
	requested := "balance"
	*(dest[9].(**string)) = &requested
	return nil
}

func refundTxFor(own uuid.UUID, names map[uuid.UUID]*string) (*refundTx, handlers) {
	db := &nameDB{refundOrg: own, names: names}
	h := handlers{logger: slog.Default(), now: time.Now}
	return &refundTx{db: db, invoice: uuid.New()}, h
}

func refundOrgNames(own uuid.UUID) map[string]map[uuid.UUID]*string {
	other := uuid.New()
	return map[string]map[uuid.UUID]*string{
		"served": {own: strp("Acme"), other: strp("Other Org")},
		"absent": {other: strp("Other Org")},
	}
}

func checkRefundOrgName(t *testing.T, path, label string, payload pyjson.Value) {
	t.Helper()
	obj, isObject := payload.(*pyjson.Object)
	if !isObject {
		t.Fatalf("%s %s: body is %T", path, label, payload)
	}
	got := orgNameOf(t, obj)
	switch label {
	case "served":
		if got != "Acme" {
			t.Errorf("%s: org_name = %v, want Acme", path, got)
		}
	default:
		if got != nil {
			t.Errorf("%s: org_name = %v, want null when the org has no name", path, got)
		}
	}
	if strings.Contains(mustDump(t, obj), "Other Org") {
		t.Errorf("%s %s: another org's name was served", path, label)
	}
}

func TestRefundListServesTheRowsOrgName(t *testing.T) {
	own := uuid.New()
	for label, names := range refundOrgNames(own) {
		tx, h := refundTxFor(own, names)
		answer, err := h.refundListReply(context.Background(), tx, nil, pageQuery{limit: 20})
		if err != nil {
			t.Fatal(err)
		}
		page, isObject := answer.body.(*pyjson.Object)
		if !isObject {
			t.Fatalf("list body is %T", answer.body)
		}
		items, _ := page.Get("items")
		list, isList := items.([]pyjson.Value)
		if !isList || len(list) != 1 {
			t.Fatalf("list items = %v", items)
		}
		checkRefundOrgName(t, "list", label, list[0])
	}
}

func TestRefundCreateServesTheRowsOrgName(t *testing.T) {
	own := uuid.New()
	for label, names := range refundOrgNames(own) {
		tx, h := refundTxFor(own, names)
		answer, err := h.completeRefund(context.Background(), tx, pendingRefund{id: uuid.New(), org: own},
			&stripe.Refund{ID: "re_1", Status: stripe.RefundStatusSucceeded})
		if err != nil {
			t.Fatal(err)
		}
		checkRefundOrgName(t, "create", label, answer.body)
	}
}

func TestRefundIdempotentReplayServesTheRowsOrgName(t *testing.T) {
	own := uuid.New()
	for label, names := range refundOrgNames(own) {
		tx, h := refundTxFor(own, names)
		_, early, err := h.reserveRefund(context.Background(), tx, refundRequest{}, tx.invoice, uuid.New(), "key-1")
		if err != nil {
			t.Fatal(err)
		}
		if early == nil {
			t.Fatal("a repeated key must answer the first refund")
		}
		checkRefundOrgName(t, "replay", label, early.body)
	}
}
