package pythonparity

import (
	"encoding/hex"
	"unicode/utf8"
)

// DecodeClickHouseString reproduces how the Python plane reads a ClickHouse
// `String` column, which is NOT how the Go driver reads one.
//
// # THE MEASURED POLICY
//
// clickhouse-connect decodes String columns as UTF-8 and, on failure,
// substitutes the lowercase hex of the raw bytes
// (clickhouse_connect/driver/buffer.py:135-138):
//
//	try:
//	    app(x.decode(encoding))
//	except UnicodeDecodeError:
//	    app(x.hex())
//
// Verified end to end against a real ClickHouse container, with the stored
// bytes confirmed server-side via hex():
//
//	stored     clickhouse-connect   clickhouse-go
//	E4BFAE     '修'                  "修"              <- the only agreement
//	FF         'ff'                 "\xff"
//	E4BF       'e4bf'               "\xe4\xbf"
//	EDA080     'eda080'             "\xed\xa0\x80"
//	61FF62     '61ff62'             "a\xffb"
//
// # TWO THINGS THAT ARE EASY TO GET WRONG
//
// The substitution is per VALUE, not per byte. `61FF62` becomes the six-
// character string "61ff62" -- the valid 'a' and 'b' are hexed too. A port
// that replaced only the offending byte would produce "a<something>b" and
// diverge on every mixed value.
//
// And the result is pure ASCII, which is why this belongs at the READER and
// not in MarshalPythonJSON. By the time Python's JSON encoder sees such a
// value it is ordinary ASCII text; there is nothing left for the encoder to
// do differently. Plan section 5d originally put this in the encoder and
// assumed an errors="replace" policy. Both were wrong -- the layer and the
// policy -- and the corrected version is what this function implements.
//
// # WHAT THE CALLER MUST DO
//
// The Go driver hands back the raw bytes, so chquery must call this on every
// String column whose value reaches a hash or a comparison with the Python
// plane. Applying it is not optional for those columns: skipping it leaves the
// two planes with different strings for the same row, and therefore different
// input_hash values, and therefore a re-categorization that costs money.
//
// A value that already looks like lowercase hex is indistinguishable from a
// substituted one afterwards. That ambiguity exists in Python too and is
// reproduced rather than resolved.
func DecodeClickHouseString(raw []byte) string {
	if utf8.Valid(raw) {
		return string(raw)
	}
	return hex.EncodeToString(raw)
}

// DecodeClickHouseStringValue is DecodeClickHouseString for a value the Go
// driver already scanned into a string.
//
// clickhouse-go scans a String column into a Go string without validating it,
// so the result may hold arbitrary bytes. This is the form most call sites
// need, because scanning into []byte instead would change every fetcher's
// signature.
func DecodeClickHouseStringValue(scanned string) string {
	if utf8.ValidString(scanned) {
		return scanned
	}
	return hex.EncodeToString([]byte(scanned))
}
