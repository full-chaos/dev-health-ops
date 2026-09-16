package operational

import "testing"

// TestDecodeBillingAttributesIntFieldMatchesPythonIntString pins intField's
// documented claim -- "Python's int(str) tolerates surrounding whitespace and
// a sign, and rejects everything else" -- against cases where that claim was
// incomplete: CPython's int(str) also accepts PEP 515 underscores strictly
// between digits and Unicode Nd digits, both reachable in a stored
// `billing_notifications.attributes` string field ("the other arms exist so
// a hand-edited row renders the way it used to", this file's own stringField
// doc). strconv.ParseInt rejects both, so a hand-edited amount_cents of
// "1_000" silently became a permanent drop instead of 1000 -- for a billing
// notification, a wrong or dropped dollar amount, not a cosmetic gap.
func TestDecodeBillingAttributesIntFieldMatchesPythonIntString(t *testing.T) {
	tests := []struct {
		name string
		json string
		want int64
	}{
		{name: "underscore_grouped_thousands", json: `{"amount_cents":"1_000"}`, want: 1000},
		{name: "underscore_between_sign_and_digit_body", json: `{"amount_cents":"-1_000"}`, want: -1000},
		{name: "fullwidth_digits", json: `{"amount_cents":"１２３"}`, want: 123},
		{name: "surrounding_whitespace_still_works", json: `{"amount_cents":" 42 "}`, want: 42},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeBillingAttributes([]byte(tc.json))
			if err != nil {
				t.Fatalf("DecodeBillingAttributes(%s) error: %v", tc.json, err)
			}
			if got.AmountCents != tc.want {
				t.Fatalf("AmountCents = %d, want %d", got.AmountCents, tc.want)
			}
		})
	}
}

// TestDecodeBillingAttributesIntFieldStillRejectsADecimalPoint pins the
// negative direction: a value with a decimal point in the STRING branch is
// still not int(str) -- it must go through intField's separate float-then-
// truncate branch (a bare JSON number, not a quoted string) or be refused,
// never silently accepted by whatever underscore/Nd handling this gains.
func TestDecodeBillingAttributesIntFieldStillRejectsADecimalPoint(t *testing.T) {
	_, err := DecodeBillingAttributes([]byte(`{"amount_cents":"1.5"}`))
	if err == nil {
		t.Fatalf("expected an error for a quoted decimal-point string, got nil")
	}
}
