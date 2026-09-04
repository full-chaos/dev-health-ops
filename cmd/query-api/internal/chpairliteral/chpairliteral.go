// Package chpairliteral is the CHAOS-4745 fix, lifted to a shared helper
// (CHAOS-4977) instead of a second hand copy.
//
// dev-health-go@v0.6.2's clickhouse.Binding wire format has no tuple-array
// encoding (clickhouse/bindings.go's clickHouseParameter handles only
// string/[]string/time.Time/ints -- confirmed directly against the pinned
// module version), so a typed Binding cannot carry a set of (string, string)
// pairs directly. Neither can a hand-rolled Array(Tuple(String,String))
// literal survive arbitrary field content: quoted/escaped forms were tried
// and each broke differently against the real native-driver protocol (see
// workgraph/membership.go's batchResolveMembership doc comment for the full
// three-iteration history that found this).
//
// Encode sidesteps string escaping entirely instead: each pair is rendered
// as hex(a)+":"+hex(b). Hex digits and ':' can never require escaping, for
// ANY input byte sequence (quotes, backslashes, unicode, control bytes,
// empty strings), because the character set hex encoding produces has no
// meaning to ClickHouse's string-literal grammar at all.
package chpairliteral

import (
	"encoding/hex"
	"strings"
)

// Encode renders pairs as a ClickHouse Array(String) literal of
// hex(pair[0])+":"+hex(pair[1]) tokens. Bind the result as the STRING value
// of an {name:Array(String)} native-protocol parameter (NOT Array(Tuple)):
//
//	bindings := []clickhouse.Binding{{Name: "pairs", Value: chpairliteral.Encode(pairs)}}
//
// Decode server-side and match against the raw columns via a tuple IN
// subquery -- this is what keeps both columns eligible for ClickHouse's
// PrimaryKey range analysis (`EXPLAIN indexes=1`), unlike a computed
// hex()/concat() match on the column itself:
//
//	WHERE (col_a, col_b) IN (
//	    SELECT unhex(splitByChar(':', p)[1]), unhex(splitByChar(':', p)[2])
//	    FROM (SELECT arrayJoin({pairs:Array(String)}) AS p)
//	)
func Encode(pairs [][2]string) string {
	encoded := make([]string, len(pairs))
	for i, pair := range pairs {
		encoded[i] = "'" + hex.EncodeToString([]byte(pair[0])) + ":" + hex.EncodeToString([]byte(pair[1])) + "'"
	}
	return "[" + strings.Join(encoded, ",") + "]"
}
