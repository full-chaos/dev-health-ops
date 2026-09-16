// Package checkedcast is the one narrowing boundary between a Go
// arbitrary-width int and a ClickHouse fixed-width unsigned column
// (UInt8/UInt16/UInt32/UInt64).
//
// A bare uint32(x) conversion in Go wraps silently on overflow or on a
// negative input -- a corrupted-but-plausible number reaches the wire, and
// nothing about the write looks wrong. ClickHouse's own Python client
// raises on the equivalent narrowing instead, so a native-family writer that
// wants the same fail-loud behavior narrows through this package rather than
// converting directly.
//
// Every function here takes the metrics family and destination field name so
// a refusal, and the ERROR-level log line it emits alongside the returned
// error, both name exactly which value failed and where -- never just the
// row it came from. Callers add any further row-identifying context by
// wrapping the returned error.
//
// This package is for values whose SOURCE has no structural ceiling below
// the destination column's width -- an accumulated sum, LOC churn, a
// duration, or any other value built from provider data with nothing
// clamping it. A value that is structurally bounded below the column's
// width (a per-query row count with no LIMIT, an enum, a small clamp) may
// keep a direct conversion, with a comment at the call site stating the
// bound instead of a call into this package.
package checkedcast

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
)

// ErrOutOfRange is returned, wrapped, when a value does not fit the width of
// its destination column.
var ErrOutOfRange = errors.New("checkedcast: value does not fit its destination column width")

// Uint8 narrows value for a UInt8 destination column named field in family,
// refusing rather than wrapping when value is negative or exceeds
// math.MaxUint8.
func Uint8(value int, family, field string) (uint8, error) {
	if value < 0 || value > math.MaxUint8 {
		return 0, outOfRange(family, field, value, "uint8")
	}
	return uint8(value), nil
}

// Uint16 narrows value for a UInt16 destination column named field in
// family, refusing rather than wrapping when value is negative or exceeds
// math.MaxUint16.
func Uint16(value int, family, field string) (uint16, error) {
	if value < 0 || value > math.MaxUint16 {
		return 0, outOfRange(family, field, value, "uint16")
	}
	return uint16(value), nil
}

// Uint32 narrows value for a UInt32 destination column named field in
// family, refusing rather than wrapping when value is negative or exceeds
// math.MaxUint32.
func Uint32(value int, family, field string) (uint32, error) {
	if value < 0 || uint64(value) > math.MaxUint32 {
		return 0, outOfRange(family, field, value, "uint32")
	}
	return uint32(value), nil
}

// Uint64 narrows value for a UInt64 destination column named field in
// family. Go's int is 64-bit signed, so a value built from it can only fail
// to fit a UInt64 by being negative -- it can never exceed math.MaxUint64.
func Uint64(value int, family, field string) (uint64, error) {
	if value < 0 {
		return 0, outOfRange(family, field, value, "uint64")
	}
	return uint64(value), nil
}

// outOfRange logs the refusal at ERROR with the family, field and value that
// failed, and returns a matching error wrapping ErrOutOfRange.
func outOfRange(family, field string, value int, width string) error {
	slog.Error("checked cast refused a value outside its destination column's width",
		"family", family, "field", field, "value", value, "width", width)
	return fmt.Errorf("%w: %s.%s value %d does not fit %s", ErrOutOfRange, family, field, value, width)
}
