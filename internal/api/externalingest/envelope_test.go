package externalingest

import (
	"strings"
	"testing"
)

func validEnvelopeJSON() string {
	return `{
		"schemaVersion": "external-ingest.v1",
		"idempotencyKey": "key-1",
		"source": {"system": "github", "instance": "acme/repo"},
		"records": [{"kind": "repository.v1", "externalId": "acme/repo", "payload": {}}]
	}`
}

func TestParseEnvelopeAcceptsTheValidShape(t *testing.T) {
	envelope, err := parseEnvelope([]byte(validEnvelopeJSON()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if envelope.Source.Type != "customer_push" {
		t.Errorf("source.type default = %q, want customer_push", envelope.Source.Type)
	}
	if envelope.Source.EntityFamily != legacyEntityFamily {
		t.Errorf("entityFamily default = %q, want legacy", envelope.Source.EntityFamily)
	}
}

func TestParseEnvelopeRejects(t *testing.T) {
	cases := map[string]string{
		"malformed json": `{not json`,
		"unknown top-level key": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i"},"records":[{"kind":"repository.v1","externalId":"i","payload":{}}],
			"extra":"nope"}`,
		"missing schemaVersion": `{"idempotencyKey":"k","source":{"system":"github","instance":"i"},
			"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"empty idempotencyKey": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"",
			"source":{"system":"github","instance":"i"},"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"unknown source.system": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"bitbucket","instance":"i"},"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"empty source.instance": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":""},"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"invalid entityFamily": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i","entityFamily":"nope"},
			"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"wrong source.type": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"type":"webhook","system":"github","instance":"i"},
			"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"window ended before started": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i"},
			"window":{"startedAt":"2026-01-02T00:00:00Z","endedAt":"2026-01-01T00:00:00Z"},
			"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`,
		"empty records": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i"},"records":[]}`,
		"missing record kind": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i"},"records":[{"externalId":"i","payload":{}}]}`,
		"empty record externalId": `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
			"source":{"system":"github","instance":"i"},"records":[{"kind":"repository.v1","externalId":"","payload":{}}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseEnvelope([]byte(body)); err == nil {
				t.Fatalf("expected an error")
			}
		})
	}
}

func TestParseEnvelopeAcceptsAWindowWhereEndedEqualsStarted(t *testing.T) {
	body := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"k",
		"source":{"system":"github","instance":"i"},
		"window":{"startedAt":"2026-01-01T00:00:00Z","endedAt":"2026-01-01T00:00:00Z"},
		"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`
	if _, err := parseEnvelope([]byte(body)); err != nil {
		t.Fatalf("endedAt == startedAt must be accepted: %v", err)
	}
}

func TestParseEnvelopeIdempotencyKeyTooLong(t *testing.T) {
	long := strings.Repeat("a", 256)
	body := `{"schemaVersion":"external-ingest.v1","idempotencyKey":"` + long + `",
		"source":{"system":"github","instance":"i"},
		"records":[{"kind":"repository.v1","externalId":"i","payload":{}}]}`
	if _, err := parseEnvelope([]byte(body)); err == nil {
		t.Fatal("expected a length error")
	}
}
