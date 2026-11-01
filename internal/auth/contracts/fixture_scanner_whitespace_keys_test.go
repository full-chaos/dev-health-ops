package contracts

import (
	"encoding/base64"
	"testing"
)

// Credential-shaped halves are built at run time from fragments; no literal secret sits in this file.
func splitCells() (prefixed, random string) {
	return "AKIA" + "QZ4TW7K2M9XPL3RB", "Qm8vK2xZp4LdR9tW" + "c7NbY3hF5jS6gH1u"
}

func tokenOf(header, payload string) string {
	b64 := func(text string) string { return base64.RawURLEncoding.EncodeToString([]byte(text)) }
	return `{"access_token":"` + b64(header) + "." + b64(payload) + `.EXAMPLESIGNATURE"}`
}

func TestScannerReadsJSONTextWithLeadingWhitespace(t *testing.T) {
	_, _, _, punctuated := plainCells()
	for name, value := range map[string]string{
		"leading space":           ` {"k":"` + punctuated + `"}`,
		"leading tab and newline": "\n\t" + `{"k":"` + punctuated + `"}`,
		"leading CRLF and array":  "\r\n" + `["` + punctuated + `"]`,
		"leading space, two docs": ` {"a":"x"}["` + punctuated + `"]`,
	} {
		if findings := scanFixtureContent(name, stringDoc(t, value)); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
	for name, value := range map[string]string{
		"leading space, benign JSON":          " " + requestBody(),
		"leading new line, benign JSON":       "\n  " + requestBody(),
		"text that only looks like JSON":      "  hello {world}",
		"form feed before benign JSON":        "\f" + `{"a":"b"}`,
		"leading space, benign JSON and tail": " " + requestBody() + "{}",
	} {
		if findings := scanFixtureContent(name, stringDoc(t, value)); len(findings) != 0 {
			t.Errorf("%s: refused: %v", name, findings)
		}
	}
}

// Only JSON whitespace is skipped. A value led by a form feed, a vertical tab or a no-break space is not JSON text and
// is judged whole; a finding cell cannot pin that, because such a value is not printable ASCII.
func TestEmbeddedDocumentsSkipOnlyJSONWhitespace(t *testing.T) {
	for _, lead := range []string{" ", "\t", "\r", "\n", " \t\r\n"} {
		if _, ok, _ := embeddedDocuments(lead + "{}"); !ok {
			t.Errorf("%q before a document: not read", lead)
		}
	}
	for _, lead := range []string{"\f", "\v", " ", " ", "x"} {
		if _, ok, _ := embeddedDocuments(lead + "{}"); ok {
			t.Errorf("%q before a document: read as JSON text", lead)
		}
	}
	if _, ok, _ := embeddedDocuments(" \t\r\n"); ok {
		t.Error("whitespace alone was read as a document")
	}
}

func TestScannerJoinsHeaderAndPayloadMemberNames(t *testing.T) {
	prefixed, random := splitCells()
	for name, content := range map[string]string{
		"prefix split across a header name and a payload value": tokenOf(`{"`+prefixed[:3]+`":1}`, `{"v":"`+prefixed[3:]+`"}`),
		"prefix split across a header value and a payload name": tokenOf(`{"v":"`+prefixed[:3]+`"}`, `{"`+prefixed[3:]+`":1}`),
		"prefix split across a header name and a payload name":  tokenOf(`{"`+prefixed[:3]+`":1}`, `{"`+prefixed[3:]+`":1}`),
		"random secret split across a header name and a value":  tokenOf(`{"`+random[:16]+`":1}`, `{"v":"`+random[16:]+`"}`),
		"random secret split across a header value and a name":  tokenOf(`{"v":"`+random[:16]+`"}`, `{"`+random[16:]+`":1}`),
	} {
		if findings := scanFixtureContent(name, content); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
}

// Pairs stay cross-segment: halves that sit together in one segment are not joined with each other.
func TestScannerDoesNotJoinStringsOfOneSegment(t *testing.T) {
	prefixed, random := splitCells()
	for name, content := range map[string]string{
		"header name and value of one header":    tokenOf(`{"`+prefixed[:3]+`":"`+prefixed[3:]+`"}`, `{"n":1}`),
		"payload name and value of one payload":  tokenOf(`{"n":1}`, `{"`+prefixed[:3]+`":"`+prefixed[3:]+`"}`),
		"random halves as two header names":      tokenOf(`{"`+random[:16]+`":1,"`+random[16:]+`":2}`, `{"n":1}`),
		"random halves as two payload values":    tokenOf(`{"n":1}`, `{"a":"`+random[:16]+`","b":"`+random[16:]+`"}`),
		"benign header and payload with members": tokenOf(`{"alg":"EdDSA","typ":"at+jwt"}`, `{"iss":"https://auth.example.test","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA"}`),
	} {
		if findings := scanFixtureContent(name, content); len(findings) != 0 {
			t.Errorf("%s: refused: %v", name, findings)
		}
	}
}
