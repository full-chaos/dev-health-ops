package contracts

import (
	"fmt"
	"testing"
	"time"
)

const offsetTestLocal = "2020-09-03T06:00:00.123456789"

func TestTimestampOffsetBound(t *testing.T) {
	cases := []struct {
		offset string
		accept bool
	}{
		{"Z", true},
		{"+00:00", true},
		{"+23:59", true},
		{"-23:59", true},
		{"+14:00", true},
		{"+24:00", false},
		{"-24:00", false},
		{"+23:60", false},
		{"+12:60", false},
		{"-24:60", false},
		{"+24:59", false},
		{"+24:60", false},
	}
	for _, tc := range cases {
		raw := offsetTestLocal + tc.offset
		t.Run(tc.offset, func(t *testing.T) {
			var stamp Timestamp
			stampErr := stamp.UnmarshalJSON([]byte(`"` + raw + `"`))
			_, wireErr := ParseWireTimestamp(ProvisioningEventSurface, "created_at", raw)
			if tc.accept && (stampErr != nil || wireErr != nil) {
				t.Fatalf("%s refused: Timestamp=%v ParseWireTimestamp=%v", raw, stampErr, wireErr)
			}
			if !tc.accept && (stampErr == nil || wireErr == nil) {
				t.Fatalf("%s accepted: Timestamp=%v ParseWireTimestamp=%v", raw, stampErr, wireErr)
			}
		})
	}
}

// Every offset the parsers accept must marshal to text the same parser
// accepts, at an instant no later than the parsed one.
func TestTimestampAcceptedOffsetsRoundTrip(t *testing.T) {
	for _, sign := range []string{"+", "-"} {
		for hour := 0; hour <= 99; hour++ {
			for minute := 0; minute <= 99; minute++ {
				raw := fmt.Sprintf("%s%s%02d:%02d", offsetTestLocal, sign, hour, minute)
				inBound := hour <= 23 && minute <= 59

				var stamp Timestamp
				err := stamp.UnmarshalJSON([]byte(`"` + raw + `"`))
				if (err == nil) != inBound {
					t.Fatalf("Timestamp %s: accepted=%t, want %t (%v)", raw, err == nil, inBound, err)
				}
				if err == nil {
					encoded, err := stamp.MarshalJSON()
					if err != nil {
						t.Fatalf("marshal %s: %v", raw, err)
					}
					var again Timestamp
					if err := again.UnmarshalJSON(encoded); err != nil {
						t.Fatalf("re-parse %s from %s: %v", encoded, raw, err)
					}
					if again.After(stamp.Time) {
						t.Fatalf("round trip moved %s forward to %s", stamp.Time, again.Time)
					}
				}

				parsed, err := ParseWireTimestamp(ProvisioningEventSurface, "created_at", raw)
				if (err == nil) != inBound {
					t.Fatalf("ParseWireTimestamp %s: accepted=%t, want %t (%v)", raw, err == nil, inBound, err)
				}
				if err == nil {
					text := parsed.Format(time.RFC3339Nano)
					again, err := ParseWireTimestamp(ProvisioningEventSurface, "created_at", text)
					if err != nil {
						t.Fatalf("re-parse %s from %s: %v", text, raw, err)
					}
					if !again.Equal(parsed) {
						t.Fatalf("round trip moved %s to %s", parsed, again)
					}
				}
			}
		}
	}
}
