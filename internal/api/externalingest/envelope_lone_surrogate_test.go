package externalingest

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func loneSurrogateEnvelope(idem, instance, externalID, payload string) string {
	return `{"schemaVersion":"external-ingest.v1","idempotencyKey":"` + idem + `","source":{"system":"github","instance":"` + instance +
		`"},"records":[{"kind":"repository.v1","externalId":"` + externalID + `","payload":` + payload + `}]}`
}

func parseAnswer(t *testing.T, body string) (int, string) {
	t.Helper()
	_, err := parseEnvelope([]byte(body))
	if err == nil {
		return http.StatusOK, ""
	}
	recorder := httptest.NewRecorder()
	writeIngestError(recorder, err.(*ingestError))
	return recorder.Code, strings.TrimSpace(recorder.Body.String())
}

func TestParseEnvelopeAnswersALoneSurrogateWith422StringUnicode(t *testing.T) {
	const ok = `{"externalId":"acme/repo","sourceSystem":"github"}`
	cases := map[string]string{
		"payload string":         loneSurrogateEnvelope("c1", "acme/repo", "acme/repo", `{"externalId":"acme/\ud800repo","sourceSystem":"github"}`),
		"payload extra field":    loneSurrogateEnvelope("c2", "acme/repo", "acme/repo", `{"externalId":"acme/repo","sourceSystem":"github","name":"a\ud800b"}`),
		"idempotencyKey":         loneSurrogateEnvelope(`k\ud800`, "acme/repo", "acme/repo", ok),
		"record externalId":      loneSurrogateEnvelope("c4", "acme/repo", `a\udc00`, ok),
		"source.instance":        loneSurrogateEnvelope("c5", `a\ud800`, "acme/repo", ok),
		"high then plain text":   loneSurrogateEnvelope("c6", "acme/repo", "acme/repo", `{"externalId":"\ud800x"}`),
		"high then other escape": loneSurrogateEnvelope("c7", "acme/repo", "acme/repo", `{"externalId":"\ud800A"}`),
		"surrogate in a key":     loneSurrogateEnvelope("c8", "acme/repo", "acme/repo", `{"k\ud800":1}`),
		"low surrogate first":    loneSurrogateEnvelope("c9", "acme/repo", "acme/repo", `{"externalId":"\udc00\ud800"}`),
	}
	for name, body := range cases {
		status, text := parseAnswer(t, body)
		lower := strings.ToLower(text)
		if status != http.StatusUnprocessableEntity || !strings.Contains(text, `"code":"string_unicode"`) ||
			!strings.Contains(text, `"message":"Request body contains an invalid Unicode string"`) ||
			!strings.Contains(text, `"errors":[{"type":"string_unicode","loc":["body"`) {
			t.Errorf("%s: %d %s, want 422 string_unicode in the error envelope", name, status, text)
		}
		if strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(text, `"input"`) || strings.Count(text, `"type"`) != 1 {
			t.Errorf("%s: the answer echoes the input or carries more than one entry: %s", name, text)
		}
	}
}

func TestParseEnvelopeKeepsEveryOtherAnswerWithALoneSurrogateRule(t *testing.T) {
	const ok = `{"externalId":"acme/repo","sourceSystem":"github"}`
	if status, text := parseAnswer(t, loneSurrogateEnvelope("p1", "acme/repo", "acme/repo", `{"externalId":"😀"}`)); status != http.StatusOK {
		t.Errorf("a valid surrogate pair: %d %s, want it accepted", status, text)
	}
	if status, text := parseAnswer(t, loneSurrogateEnvelope("p2", "acme/repo", "acme/repo", ok)); status != http.StatusOK {
		t.Errorf("control: %d %s", status, text)
	}
	status, text := parseAnswer(t, `{"schemaVersion":"external-ingest.v1","idempotencyKey":"m1","records":[]}`)
	if status != http.StatusBadRequest || !strings.Contains(text, `"code":"invalid_envelope"`) {
		t.Errorf("missing source: %d %s, want 400 invalid_envelope", status, text)
	}
	for name, body := range map[string]string{
		"syntax error":                  `{"schemaVersion":`,
		"syntax error before surrogate": `{"a":tru,"b":"\ud800"}`,
		"surrogate after syntax error":  `{"b":"\ud800","a":tru}`,
	} {
		if status, text := parseAnswer(t, body); status != http.StatusInternalServerError || !strings.Contains(text, `"code":"internal_error"`) {
			t.Errorf("%s: %d %s, want the frozen 500", name, status, text)
		}
	}
}
