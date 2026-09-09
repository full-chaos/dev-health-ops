package goapiproof

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
)

// ErrNonFiniteNumber reports a response body carrying a bare NaN,
// Infinity or -Infinity literal.
//
// Why this is its own error rather than a parse failure. Python's json
// module accepts those literals by default (that is how the Python plane
// can emit them at all); Go's encoding/json rejects them as a syntax
// error, which is indistinguishable at the call site from a truncated or
// corrupt body. Parity rule 3 makes a non-finite value ALWAYS a mismatch,
// so the two outcomes must not collapse: a body with NaN in it is a
// comparable response whose verdict is `mismatch`, while an unparseable
// body is `proof_failed` -- one is a finding about the planes, the other
// is a finding about the measurement.
var ErrNonFiniteNumber = errors.New("goapiproof: response body carries a non-finite JSON number literal")

// nonFiniteLiteral matches a bare NaN/Infinity/-Infinity token in a value
// position -- i.e. preceded by ':' or ',' or '[' and optional whitespace,
// so a STRING containing the word "Infinity" is not mistaken for one.
var nonFiniteLiteral = regexp.MustCompile(`[:,\[]\s*-?(NaN|Infinity)\b`)

// Snapshotter turns one plane's raw HTTP response body into a Snapshot.
//
// DecodeSnapshot decodes with UseNumber so an integer is never widened to
// a float before comparison: parity rule 3 makes Tier-A comparison exact,
// and a decoder that turns 9007199254740993 into a float64 has already
// destroyed the difference the rule exists to catch. asFloat re-parses
// the literal only when both sides are numbers.
func DecodeSnapshot(body []byte) (Snapshot, error) {
	if nonFiniteLiteral.Match(body) {
		return Snapshot{}, ErrNonFiniteNumber
	}

	// Decode the envelope into raw messages first so "data": null and an
	// absent "data" key stay distinguishable -- both decode to a nil `any`
	// through a plain map, and parity rule 2 needs them apart.
	var envelope map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return Snapshot{}, fmt.Errorf("goapiproof: decode response envelope: %w", err)
	}

	snapshot := Snapshot{}
	if raw, ok := envelope["data"]; ok {
		snapshot.DataPresent = true
		if err := decodeWithNumbers(raw, &snapshot.Data); err != nil {
			return Snapshot{}, fmt.Errorf("goapiproof: decode response data: %w", err)
		}
	}
	if raw, ok := envelope["errors"]; ok {
		if err := decodeWithNumbers(raw, &snapshot.Errors); err != nil {
			return Snapshot{}, fmt.Errorf("goapiproof: decode response errors: %w", err)
		}
	}
	return snapshot, nil
}

func decodeWithNumbers(raw json.RawMessage, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}
