package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

// decimalFormTokens are refused on the TEXT of the token, whatever value a
// float64 parse would give them. The first group is integral in value, the
// second rounds to an integer, the third is plainly fractional.
var decimalFormTokens = []string{
	"1.0", "1e0", "1E0", "1.0e0", "403.0", "9007199254740992.0", "9007199254740993.0", "1e300",
	"1.0000000000000000001", "9007199254740990.1", "9007199254740990.5", "1.0000000000000001",
	"1.5", "-1.0", "-0.0",
}

func TestRefuseDecimalFormRefusesEveryDecimalOrExponentToken(t *testing.T) {
	for _, token := range decimalFormTokens {
		if err := RefuseDecimalForm(token); err == nil || !strings.Contains(err.Error(), "decimal or exponent form") {
			t.Errorf("%s: err = %v, want a decimal-form refusal", token, err)
		}
	}
}

func TestRefuseDecimalFormPassesIntegerTokens(t *testing.T) {
	for _, token := range []string{"0", "1", "-1", "9007199254740993", "9223372036854775807"} {
		if err := RefuseDecimalForm(token); err != nil {
			t.Errorf("%s: unexpected refusal %v", token, err)
		}
	}
}

func TestRevisionRefusesEveryDecimalFormTokenAndKeepsIntegerTokensExact(t *testing.T) {
	for _, token := range decimalFormTokens {
		var r Revision
		if err := json.Unmarshal([]byte(token), &r); err == nil {
			t.Errorf("Revision accepted %s as %d", token, r)
		}
	}
	// 2^53+1 as an INTEGER token stays exact: the reason integer tokens are
	// the only accepted form.
	var r Revision
	if err := json.Unmarshal([]byte("9007199254740993"), &r); err != nil || r != 9007199254740993 {
		t.Fatalf("integer token: %v %d", err, r)
	}
}

func TestDecodingIntoAnIntegerFieldRefusesAZeroFractionDecimal(t *testing.T) {
	// The Go half of the wire-integer type guard for ordinary fields is the
	// decoder itself; RefuseDecimalForm's doc comment relies on it.
	var envelope struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal([]byte(`{"status": 403.0}`), &envelope); err == nil {
		t.Fatal("encoding/json accepted 403.0 into an int field")
	}
	if err := json.Unmarshal([]byte(`{"status": 403}`), &envelope); err != nil || envelope.Status != 403 {
		t.Fatalf("integer token: %v %d", err, envelope.Status)
	}
}
