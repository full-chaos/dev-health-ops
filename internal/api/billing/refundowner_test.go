package billing

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// TestRefundOwnerDecisionGrid runs decideRefundOwner over its whole input
// grid (payment invoices {0, 1, 2} x metadata org {none, the payment's,
// another} x waiting row {none, same org and invoice, same org and another
// invoice, same org and no invoice, another org}), each cell against an
// expectation written out by hand, and checks the properties the rule
// exists for on every cell:
//
//   - a refund is never recorded under an org other than its payment's
//     invoice's, whatever the metadata says;
//   - a payment held by several invoices never records;
//   - a waiting row is completed only when it fits the owner;
//   - every skip carries a reason and every decision that leaves Python's
//     rule carries a note.
func TestRefundOwnerDecisionGrid(t *testing.T) {
	orgA, orgB := uuid.MustParse("aaaaaaaa-0000-4000-8000-00000000000a"), uuid.MustParse("bbbbbbbb-0000-4000-8000-00000000000b")
	invoice1, invoice2, invoiceOther := uuid.New(), uuid.New(), uuid.New()
	known := map[string]bool{skipSeveralInvoices: true, skipNoOwner: true, skipWaitingRowMisfit: true}
	notes := map[string]bool{"": true, noteOwnerFromPayment: true, noteOverriddenByPayment: true}

	for count := 0; count <= 2; count++ {
		for _, meta := range []string{"none", "payment", "another"} {
			for _, waiting := range []string{"none", "same org and invoice", "same org, another invoice", "same org, no invoice", "another org"} {
				name := fmt.Sprintf("invoices=%d meta=%s waiting=%s", count, meta, waiting)
				t.Run(name, func(t *testing.T) {
					in := refundOwnerInput{}
					switch count {
					case 1:
						in.Invoices = []paymentInvoice{{id: invoice1, org: orgA}}
					case 2:
						in.Invoices = []paymentInvoice{{id: invoice1, org: orgA}, {id: invoice2, org: orgA}}
					}
					switch meta {
					case "payment":
						in.MetaOrg = &orgA
					case "another":
						in.MetaOrg = &orgB
					}
					switch waiting {
					case "same org and invoice":
						in.Waiting = &storedRefund{org: orgA, invoice: &invoice1}
					case "same org, another invoice":
						in.Waiting = &storedRefund{org: orgA, invoice: &invoiceOther}
					case "same org, no invoice":
						in.Waiting = &storedRefund{org: orgA}
					case "another org":
						in.Waiting = &storedRefund{org: orgB}
					}
					got := decideRefundOwner(in)

					// The expectation, cell by cell, written from the rule's
					// statement and not from its code.
					want := ""
					switch {
					case count == 2:
						want = "skip " + skipSeveralInvoices
					case count == 1 && waiting == "none":
						want = "record A, invoice 1"
					case count == 1 && (waiting == "same org and invoice" || waiting == "same org, no invoice"):
						want = "record A, invoice 1, adopt"
					case count == 1: // another invoice of the org, or another org
						want = "skip " + skipWaitingRowMisfit
					case count == 0 && meta == "none" && waiting == "none":
						want = "skip " + skipNoOwner
					case count == 0 && meta == "none":
						want = "record no org, adopt" // the row keeps its own org
					case count == 0 && meta == "payment" && waiting == "none":
						want = "record A"
					case count == 0 && meta == "payment" && (waiting == "another org"):
						want = "skip " + skipWaitingRowMisfit
					case count == 0 && meta == "payment":
						want = "record A, adopt" // a waiting row of org A, whichever invoice it names
					case count == 0 && meta == "another" && waiting == "none":
						want = "record B"
					case count == 0 && meta == "another" && waiting == "another org":
						want = "record B, adopt"
					case count == 0 && meta == "another":
						want = "skip " + skipWaitingRowMisfit
					default:
						t.Fatalf("no expectation for %s", name)
					}
					if have := describeOwner(got, orgA, orgB, invoice1); have != want {
						t.Errorf("decision = %q, want %q", have, want)
					}

					// Properties, on every cell.
					if got.Skip != "" && !known[got.Skip] {
						t.Errorf("skip reason %q is not a named one", got.Skip)
					}
					if !notes[got.Note] {
						t.Errorf("note %q is not a named one", got.Note)
					}
					if count == 1 && got.Skip == "" && got.Org != nil && *got.Org != orgA {
						t.Errorf("a refund paid on org A's invoice is recorded under %v", *got.Org)
					}
					if count == 2 && (got.Skip == "" || got.Org != nil || got.Adopt) {
						t.Errorf("a payment held by several invoices was not skipped: %+v", got)
					}
					if got.Adopt {
						if in.Waiting == nil {
							t.Errorf("adopts a waiting row that does not exist")
						} else if got.Org != nil && in.Waiting.org != *got.Org {
							t.Errorf("adopts a waiting row of another org")
						} else if got.Invoice != nil && in.Waiting.invoice != nil && *in.Waiting.invoice != *got.Invoice {
							t.Errorf("adopts a waiting row linked to another invoice")
						}
					}
					if got.Skip == "" && got.Org == nil && !got.Adopt {
						t.Errorf("records for nobody: %+v", got)
					}
					if got.Skip != "" && (got.Org != nil || got.Adopt || got.Invoice != nil) {
						t.Errorf("a skip carries an owner: %+v", got)
					}
				})
			}
		}
	}
}

func describeOwner(d refundOwnerDecision, orgA, orgB, invoice1 uuid.UUID) string {
	if d.Skip != "" {
		return "skip " + d.Skip
	}
	out := "record "
	switch {
	case d.Org == nil:
		out += "no org"
	case *d.Org == orgA:
		out += "A"
	case *d.Org == orgB:
		out += "B"
	default:
		out += "?"
	}
	if d.Invoice != nil {
		if *d.Invoice == invoice1 {
			out += ", invoice 1"
		} else {
			out += ", another invoice"
		}
	}
	if d.Adopt {
		out += ", adopt"
	}
	return out
}
