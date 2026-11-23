package testopsrisk

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// CHAOS-13814: a request with no repository or team scope must send the same two
// statements as before the scope existed. The digests are of the statement text at
// 84d6c394e2, taken before the scope clause was written.
func TestStatementsWithNoScopeKeepTheirBaseText(t *testing.T) {
	for name, tc := range map[string]struct{ text, want string }{
		"daily":    {dailyQuery, "0113ed07d926f05f049f7a0901c65cf0bad29c4efa20b085e4880e76ffdd4241"},
		"quadrant": {quadrantQuery, "b9e78238cc58064c8aa5fabbdbd928d4cc95566c11eda743f4c1cbac0f9cde7e"},
	} {
		sum := sha256.Sum256([]byte(tc.text))
		if got := hex.EncodeToString(sum[:]); got != tc.want {
			t.Errorf("%s statement digest = %s, want %s", name, got, tc.want)
		}
	}
}
