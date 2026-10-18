package contracts

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestRefuseInexactDecimalFiresAtAndPastTheBound(t *testing.T) {
	// 9007199254740992.0 is exactly 2^53 and must be refused: 2^53+1 parses to
	// the same double, so the two are indistinguishable once a float is held.
	for _, token := range []string{"9007199254740992.0", "9007199254740993.0", "-9007199254740992.0", "1e300"} {
		f, err := strconv.ParseFloat(token, 64)
		if err != nil {
			t.Fatal(err)
		}
		if err := RefuseInexactDecimal(token, f); err == nil || !strings.Contains(err.Error(), "past 2^53") {
			t.Errorf("%s: err = %v, want a 2^53 refusal", token, err)
		}
	}
}

func TestRefuseInexactDecimalPassesBelowTheBound(t *testing.T) {
	for _, token := range []string{"9007199254740991.0", "0.0", "-9007199254740991.0"} {
		f, _ := strconv.ParseFloat(token, 64)
		if err := RefuseInexactDecimal(token, f); err != nil {
			t.Errorf("%s: unexpected refusal %v", token, err)
		}
	}
}

func TestTheExactFloatBoundIsTwoToTheFiftyThird(t *testing.T) {
	if MaxExactFloatInteger != math.Pow(2, 53) {
		t.Fatalf("MaxExactFloatInteger = %v", MaxExactFloatInteger)
	}
}

func TestDecodingIntoAnIntegerFieldRefusesAZeroFractionDecimal(t *testing.T) {
	// The Go half of the wire-integer type guard is the decoder itself;
	// RefuseInexactDecimal's doc comment relies on it, so it is pinned here.
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
