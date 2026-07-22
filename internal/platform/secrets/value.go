// Package secrets loads and contains sensitive configuration values.
package secrets

import (
	"encoding/json"
	"log/slog"
)

const redacted = "[REDACTED]"

// Value makes accidental formatting and structured logging safe. Call Reveal
// only at the adapter boundary that needs the underlying credential or DSN.
type Value struct {
	raw string
}

// NewValue wraps a sensitive value.
func NewValue(value string) Value {
	return Value{raw: value}
}

// Reveal returns the underlying value. It must not be logged or included in an
// error message.
func (v Value) Reveal() string {
	return v.raw
}

// Configured reports whether the value is non-empty without exposing it.
func (v Value) Configured() bool {
	return v.raw != ""
}

func (Value) String() string {
	return redacted
}

func (Value) GoString() string {
	return redacted
}

// LogValue prevents slog from reflecting the private field or printing a DSN.
func (Value) LogValue() slog.Value {
	return slog.StringValue(redacted)
}

// MarshalJSON keeps diagnostic config dumps from exposing the raw value.
func (Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(redacted)
}
