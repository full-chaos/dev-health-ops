package pythonparity

import (
	"errors"
	"math/big"
	"strings"
	"testing"
)

// Every case here was measured against a live CPython 3.12 int(s): see the
// PR body for the exact `python3 -c` transcript. This is not a golden JSON
// corpus the way ParseFloat's is (no generator exists for one yet), but
// every accept/reject boundary the grammar comment claims is exercised by an
// actual case below, not merely asserted.
func TestParseIntMatchesCPython(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string // decimal text of the expected *big.Int, "" if want is an error
		wantErr bool
	}{
		{name: "zero", input: "0", want: "0"},
		{name: "plain", input: "1", want: "1"},
		{name: "negative", input: "-1", want: "-1"},
		{name: "explicit_plus", input: "+1", want: "1"},
		{name: "surrounding_space", input: " 3 ", want: "3"},
		{name: "underscore_between_digits", input: "1_0", want: "10"},
		{name: "underscore_grouped", input: "1_000_000", want: "1000000"},
		{name: "leading_underscore", input: "_1", wantErr: true},
		{name: "trailing_underscore", input: "1_", wantErr: true},
		{name: "doubled_underscore", input: "1__0", wantErr: true},
		{name: "underscore_next_to_dot_like_noise", input: "1_.5", wantErr: true},
		{name: "decimal_point_rejected", input: "1.5", wantErr: true},
		{name: "interior_space_rejected", input: "1 5", wantErr: true},
		{name: "doubled_sign_minus", input: "--1", wantErr: true},
		{name: "doubled_sign_mixed", input: "+-1", wantErr: true},
		{name: "empty", input: "", wantErr: true},
		{name: "blank", input: "   ", wantErr: true},
		{name: "trailing_letter", input: "1a", wantErr: true},
		{name: "leading_zero_kept_as_value", input: "01", want: "1"},
		{name: "negative_zero", input: "-0", want: "0"},
		{name: "many_leading_zeros", input: "000123", want: "123"},
		// Numeric whitespace is Py_UNICODE_ISSPACE (tab/newline/NBSP fold to
		// space), NOT str.strip()'s wider set -- 0x1c is REJECTED by int(),
		// which is why this must go through transformDecimalAndSpaceToASCII's
		// isPythonIntSpace table rather than strings.TrimSpace or
		// pythonparity.Strip.
		{name: "tab_and_newline", input: "\t5\n", want: "5"},
		{name: "nbsp", input: "\xc2\xa05", want: "5"},
		{name: "file_separator_rejected", input: "\x1c5", wantErr: true},
		// Unicode Nd digits transform to ASCII before the grammar runs.
		{name: "fullwidth_digits", input: "１２３", want: "123"},
		{name: "arabic_indic_digits", input: "١٢٣", want: "123"},
		{name: "devanagari_digit", input: "५", want: "5"},
		// The digit cap is exactly 4300 DIGITS; underscores are free.
		{name: "at_digit_cap", input: strings.Repeat("9", MaxIntStringDigits), want: strings.Repeat("9", MaxIntStringDigits)},
		{name: "one_over_digit_cap", input: strings.Repeat("9", MaxIntStringDigits+1), wantErr: true},
		{
			name:  "digit_cap_with_free_underscores",
			input: "1_" + strings.Repeat("2", MaxIntStringDigits-1),
			want:  "1" + strings.Repeat("2", MaxIntStringDigits-1),
		},
		{
			name:    "one_over_cap_with_underscore_still_rejected",
			input:   strings.Repeat("9", MaxIntStringDigits) + "_1",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInt(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ParseInt(%q) = %v, nil; want an error (CPython raises ValueError)", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseInt(%q) unexpected error: %v", tc.input, err)
			}
			want, ok := new(big.Int).SetString(tc.want, 10)
			if !ok {
				t.Fatalf("test bug: %q is not a valid decimal literal", tc.want)
			}
			if got.Cmp(want) != 0 {
				t.Fatalf("ParseInt(%q) = %s, want %s", tc.input, got.String(), want.String())
			}
		})
	}
}

// TestParseIntOverlongIsErrIntStringTooLong pins the SPECIFIC error CPython's
// digit-cap ValueError maps to, not just "some error" -- a caller deciding
// whether to fall back to a default vs. treat the value as a hard
// misconfiguration needs to tell "too long" apart from "not a number at
// all".
func TestParseIntOverlongIsErrIntStringTooLong(t *testing.T) {
	_, err := ParseInt(strings.Repeat("9", MaxIntStringDigits+1))
	if !errors.Is(err, ErrIntStringTooLong) {
		t.Fatalf("err = %v, want ErrIntStringTooLong", err)
	}
}
