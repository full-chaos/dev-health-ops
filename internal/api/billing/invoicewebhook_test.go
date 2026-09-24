package billing

import "testing"

// TestInvoiceStatusFollows pins the forward-only status rule.
func TestInvoiceStatusFollows(t *testing.T) {
	cases := []struct {
		stored, status string
		want           bool
	}{
		{"draft", "open", true}, {"open", "draft", false}, {"open", "payment_failed", true}, {"payment_failed", "open", true},
		{"open", "paid", true}, {"paid", "open", false}, {"paid", "paid", true}, {"paid", "void", false},
		{"void", "paid", false}, {"void", "void", true}, {"uncollectible", "paid", true}, {"paid", "uncollectible", false},
		{"uncollectible", "open", false}, {"draft", "draft", true},
	}
	for _, c := range cases {
		if got := invoiceStatusFollows(c.stored, c.status); got != c.want {
			t.Errorf("%s -> %s: got %t, want %t", c.stored, c.status, got, c.want)
		}
	}
}
