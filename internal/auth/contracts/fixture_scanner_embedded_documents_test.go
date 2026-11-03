package contracts

import (
	"strings"
	"testing"
)

// requestBody has the shape of a decision-golden request_raw: compact JSON text with mixed case, digits and
// punctuation, which is not one secret.
func requestBody() string {
	return `{"schema_version":"authorization_request.v1","principal_reference":"prf_EXAMPLE0000000000000001",` +
		`"action":"acr.context.read","resource":{"kind":"organization","id":"org_EXAMPLE0000000000000001"},` +
		`"context":{"requesting_service":"dev-health-web"}}`
}

func TestScannerReadsJSONTextWithSeveralDocumentsTokenByToken(t *testing.T) {
	_, _, _, punctuated := plainCells()
	body := requestBody()
	clean := map[string]string{
		"request body with a trailing object":        body + `{}`,
		"two request bodies separated by a space":    body + ` ` + body,
		"two request bodies separated by a new line": body + "\n" + body,
	}
	for name, value := range clean {
		if findings := scanFixtureContent(name, stringDoc(t, value)); len(findings) != 0 {
			t.Errorf("%s: refused: %v", name, findings)
		}
	}
	hidden := map[string]string{
		"punctuated secret in a trailing object":                    body + `{"k":"` + punctuated + `"}`,
		"punctuated secret in a trailing array":                     body + `["` + punctuated + `"]`,
		"second document is an array holding the punctuated secret": `{"a":"x"}` + `["` + punctuated + `"]`,
		"malformed JSON text holding a punctuated secret":           `{"k":"` + punctuated,
		"garbage after a document":                                  body + `xyz` + punctuated,
	}
	for name, value := range hidden {
		if findings := scanFixtureContent(name, stringDoc(t, value)); len(findings) == 0 {
			t.Errorf("%s: no finding", name)
		}
	}
}

func TestScannerManyDocumentsAreAFindingNotAHang(t *testing.T) {
	value := strings.Repeat("{}", 20000)
	findings := scanFixtureContent("many", stringDoc(t, value))
	if !strings.Contains(strings.Join(findings, "\n"), "JSON documents") {
		t.Errorf("20000 documents in one value must be refused, got: %v", findings)
	}
}
