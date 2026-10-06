package providersync

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

// Both size bounds of the write contract report the typed error, which still
// matches the recovery-unsafe sentinel; every other refusal stays the bare
// sentinel, so the unit handler can tell a size bound from the rest.
func TestBuildEffectBatchSizeBoundsReportTheTypedError(t *testing.T) {
	small := json.RawMessage(`{"a":1}`)
	rows := make([]json.RawMessage, maxEffectRows+1)
	for index := range rows {
		rows[index] = small
	}
	big := json.RawMessage(`{"a":"` + strings.Repeat("x", 22<<20) + `"}`)
	cases := []struct {
		name      string
		rows      []json.RawMessage
		dest      string
		wantBound string
	}{
		{"rows above bound", rows, "work_items", "rows"},
		{"rows at bound", rows[:maxEffectRows], "work_items", ""},
		{"bytes above bound", []json.RawMessage{big, big, big}, "work_items", "payload_bytes"},
		{"empty destination", []json.RawMessage{small}, "", "-bare"},
		{"malformed row", []json.RawMessage{json.RawMessage(`[1]`)}, "work_items", "-bare"},
	}
	for _, testCase := range cases {
		_, err := BuildEffectBatch(testCase.dest, EffectReplaySafe, testCase.rows)
		var bound *EffectBoundExceededError
		isBound := errors.As(err, &bound)
		switch testCase.wantBound {
		case "":
			if err != nil {
				t.Errorf("%s: err=%v, want nil", testCase.name, err)
			}
		case "-bare":
			if isBound || !errors.Is(err, ErrEffectRecoveryUnsafe) {
				t.Errorf("%s: err=%v, want the bare sentinel", testCase.name, err)
			}
		default:
			if !isBound || bound.Limit != testCase.wantBound || bound.Table != "work_items" ||
				(testCase.wantBound == "rows" && bound.Rows != maxEffectRows+1) ||
				(testCase.wantBound == "payload_bytes" && bound.Bytes <= maxEffectPayloadBytes) || !errors.Is(err, ErrEffectRecoveryUnsafe) {
				t.Errorf("%s: err=%v, want bound %q wrapping the sentinel", testCase.name, err, testCase.wantBound)
			}
			if strings.ContainsAny(err.Error(), "0123456789") {
				t.Errorf("%s: error text carries a number: %q", testCase.name, err.Error())
			}
		}
	}
}
