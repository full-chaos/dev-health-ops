package goapiproof

import (
	"context"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// failNthExecQuerier is a minimal Querier (no testcontainers, no
// TxBeginner -- WriteAtomic falls back to a direct Write) whose Exec call
// number failOn returns an error. Write does exactly two Exec calls per
// receipt (the candidate-build upsert, then the proof-run insert), so a
// caller writing N receipts sequentially can fail exactly one of them by
// picking the right 1-indexed call number.
type failNthExecQuerier struct {
	execCount int
	failOn    int
}

func (f *failNthExecQuerier) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	f.execCount++
	if f.execCount == f.failOn {
		return pgconn.CommandTag{}, fmt.Errorf("simulated write failure on exec #%d", f.execCount)
	}
	return pgconn.CommandTag{}, nil
}

func (f *failNthExecQuerier) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, fmt.Errorf("failNthExecQuerier: Query is not implemented (Write never calls it)")
}

func (f *failNthExecQuerier) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return nil
}

// variantReceipt is wellFormedReceipt with a Variant name and its own
// RequestIdentity, mirroring how two OperationSpec.Variants entries for
// the SAME Operation produce two distinct Receipts (run.go's seal/
// ReceiptsFor).
func variantReceipt(variant, identity string) Receipt {
	receipt := wellFormedReceipt()
	receipt.Variant = variant
	receipt.RequestIdentity = identity
	return receipt
}

// CHAOS-5623: cmd/go-api-prove's `written[operation]` map was keyed by
// operation name alone, so with OperationSpec.Variants (flowMatrix's
// WORK_TYPE/TEAM/REPO) a partial receipt failure could not say WHICH
// variant wrote its receipt -- one variant's successful write set the
// shared operation-only key, and every sibling variant sharing that
// operation read as written too, including one whose write genuinely
// failed.
//
// This drives WriteReceipts directly (no Postgres needed: failNthExecQuerier
// is not a TxBeginner, so WriteAtomic falls back to a plain Write) with two
// receipts for the SAME operation under different variants: TEAM's write
// succeeds, REPO's fails on ITS first Exec (call #3 of 4: TEAM's own two
// calls are #1-#2).
func TestWriteReceiptsTracksReceiptWrittenPerVariant(t *testing.T) {
	team := variantReceipt("TEAM", "sha256:identity-team")
	repo := variantReceipt("REPO", "sha256:identity-repo")

	db := &failNthExecQuerier{failOn: 3}
	written, err := WriteReceipts(context.Background(), db, []Receipt{team, repo})
	if err == nil {
		t.Fatal("expected the REPO variant's write to fail")
	}

	if !written[ReceiptKey(team.SelectedOperation, team.Variant)] {
		t.Fatalf("TEAM variant's receipt was written but not recorded: %v", written)
	}
	if written[ReceiptKey(repo.SelectedOperation, repo.Variant)] {
		t.Fatalf("REPO variant's write FAILED but was recorded as written: %v", written)
	}
	// Pin against the pre-fix bug directly: a bare operation-name key
	// must never appear at all here, since neither receipt has an empty
	// Variant -- if it did, it would mean TEAM's success and REPO's
	// failure had collapsed onto one shared key, exactly the defect this
	// change fixes.
	if written[team.SelectedOperation] {
		t.Fatalf("a bare operation-name key must not appear when every receipt carries a Variant: %v", written)
	}
}

// The base-request form (Variant == "") keeps using a bare operation-name
// key, so every pre-CHAOS-5623 caller and test (WriteReceipts callers
// that never set Variant) is unaffected.
func TestReceiptKeyIsBareOperationNameWhenVariantIsEmpty(t *testing.T) {
	if got, want := ReceiptKey("featureFlags", ""), "featureFlags"; got != want {
		t.Fatalf("ReceiptKey(op, \"\") = %q, want %q", got, want)
	}
	if got := ReceiptKey("flowMatrix", "TEAM"); got == "flowMatrix" || got == "" {
		t.Fatalf("ReceiptKey(op, variant) must differ from the bare operation name, got %q", got)
	}
	if ReceiptKey("flowMatrix", "TEAM") == ReceiptKey("flowMatrix", "REPO") {
		t.Fatal("two different variants of the same operation must produce different keys")
	}
}

// The control: two variants of the same operation that BOTH write
// successfully must BOTH be recorded, distinctly.
func TestWriteReceiptsRecordsBothVariantsWhenBothSucceed(t *testing.T) {
	team := variantReceipt("TEAM", "sha256:identity-team-ok")
	repo := variantReceipt("REPO", "sha256:identity-repo-ok")

	db := &failNthExecQuerier{failOn: 0} // never fails
	written, err := WriteReceipts(context.Background(), db, []Receipt{team, repo})
	if err != nil {
		t.Fatalf("WriteReceipts: %v", err)
	}
	if !written[ReceiptKey(team.SelectedOperation, team.Variant)] || !written[ReceiptKey(repo.SelectedOperation, repo.Variant)] {
		t.Fatalf("both variants must be recorded as written: %v", written)
	}
	if len(written) != 2 {
		t.Fatalf("expected exactly 2 distinct keys, got %v", written)
	}
}
