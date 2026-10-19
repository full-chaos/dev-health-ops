package authapi

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

// FuzzParseRefreshRequest holds the refresh body parser to the schema: what it
// accepts is one object of exactly the two members, each once, with the
// credential in its alphabet; a refusal never quotes the body.
func FuzzParseRefreshRequest(f *testing.F) {
	good := "dhr1_" + strings.Repeat("A", 43)
	for _, seed := range []string{
		`{"schema_version":"refresh-request.v1","refresh_credential":"` + good + `"}`,
		`{"refresh_credential":"` + good + `","schema_version":"refresh-request.v1"}`,
		`{"schema_version":"refresh-request.v1","refresh_credential":"` + good + `","refresh_credential":"x"}`,
		`{"schema_version":"refresh-request.v1","refresh_credential":"a.b.c"}`,
		`{"schema_version":"refresh-request.v1","refresh_credential":"` + good + `"} {}`,
		`{"schema_version":"refresh-request.v1","refresh_credential":"A` + good[1:] + `"}`,
		``, `null`, `[]`, `{`, `{"schema_version":1}`,
	} {
		f.Add([]byte(seed))
	}
	alphabet := regexp.MustCompile(`^[A-Za-z0-9_-]{43,256}$`)
	f.Fuzz(func(t *testing.T, body []byte) {
		credential, err := parseRefreshRequest(bytes.NewReader(body))
		if err != nil {
			if credential.Reveal() != "" {
				t.Fatal("a refusal returned a credential")
			}
			if err != errRefreshRequest {
				t.Fatalf("a refusal is not the fixed error: %v", err)
			}
			return
		}
		if !alphabet.MatchString(credential.Reveal()) {
			t.Fatalf("accepted a credential outside the alphabet")
		}
		var members map[string]json.RawMessage
		if json.Unmarshal(body, &members) != nil || len(members) != 2 {
			t.Fatalf("accepted a body that is not a two-member object")
		}
		var version, value string
		if json.Unmarshal(members["schema_version"], &version) != nil || version != "refresh-request.v1" ||
			json.Unmarshal(members["refresh_credential"], &value) != nil || value != credential.Reveal() {
			t.Fatalf("accepted body disagrees with what encoding/json reads")
		}
	})
}
