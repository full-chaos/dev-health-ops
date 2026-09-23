package webhookintake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func pagerdutySign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "v1=" + hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyPagerDutySignature(t *testing.T) {
	body := []byte(`{"x":1}`)
	valid := pagerdutySign("secret", body)
	cases := []struct {
		name, header string
		want         bool
	}{
		{"valid single candidate", valid, true},
		{"valid among several candidates (rotation)", "v1=deadbeef," + valid, true},
		{"wrong secret", pagerdutySign("other", body), false},
		{"missing header", "", false},
		{"malformed candidate (no v1= prefix)", hex.EncodeToString([]byte("x")), false},
		{"too many candidates", strings.Repeat("v1=aa,", maxSignatureCandidates+1), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := verifyPagerDutySignature(body, c.header, []byte("secret")); got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// TestPagerdutyReplayIdentityAndKeyAreDeterministic pins the same key shape
// pagerduty.py's _replay_key/_replay_identity build -- the oracle condition
// team-lead set (a replayed delivery must give the identical outcome on
// both planes) starts with both planes deriving the identical Redis key
// from the identical inputs.
func TestPagerdutyReplayIdentityAndKeyAreDeterministic(t *testing.T) {
	body := []byte(`{"event":{"id":"e1"}}`)
	identity := pagerdutyReplayIdentity("sub-1", "e1", body)
	if identity != "sub-1\x1fe1" {
		t.Fatalf("identity = %q", identity)
	}
	key := pagerdutyReplayKey("binding-1", identity)
	again := pagerdutyReplayKey("binding-1", identity)
	if key != again {
		t.Fatal("replay key must be a pure function of its inputs")
	}
	if !strings.HasPrefix(key, "pagerduty-webhook-replay:binding-1:") {
		t.Fatalf("key = %q", key)
	}
}

// TestPagerdutyReplayIdentityFallsBackToBodyHash mirrors
// _replay_identity's own fallback: an event with no usable id hashes the
// raw body instead, so two deliveries with the SAME body from the SAME
// subscription collide (the correct behavior -- Python's own documented
// replay-protection shape), and two different bodies never collide.
func TestPagerdutyReplayIdentityFallsBackToBodyHash(t *testing.T) {
	identityA := pagerdutyReplayIdentity("sub-1", "", []byte("body-a"))
	identityA2 := pagerdutyReplayIdentity("sub-1", "  ", []byte("body-a"))
	identityB := pagerdutyReplayIdentity("sub-1", "", []byte("body-b"))
	if identityA != identityA2 {
		t.Fatal("whitespace-only event id must fall back the same as an empty one")
	}
	if identityA == identityB {
		t.Fatal("different bodies must not collide")
	}
}

func TestPagerDutyV3WebhookMarshalCanonicalShape(t *testing.T) {
	data, err := pyjson.DecodeString(`{"incident":{"id":"I1"}}`)
	if err != nil {
		t.Fatal(err)
	}
	webhook := pagerDutyV3Webhook{
		Event: pagerDutyEvent{
			ID:         "E1",
			EventType:  "incident.triggered",
			OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		},
		data: data,
	}
	got, err := webhook.marshalCanonical()
	if err != nil {
		t.Fatal(err)
	}
	// Field order matches PagerDutyEvent's own declaration order
	// (id, event_type, occurred_at, data) -- model_dump_json() emits fields
	// in declaration order, not sorted (unlike pyjson.MarshalCanonical, used
	// only for delivery-key hashing elsewhere in this package).
	want := `{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"2026-01-02T03:04:05Z","data":{"incident":{"id":"I1"}}}}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}

func TestParsePagerDutyWebhookRejectsMalformedAndInvalidShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"not JSON", `{`},
		{"not an object", `[1,2,3]`},
		{"missing event", `{"foo":1}`},
		{"missing event.id", `{"event":{"event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z"}}`},
		{"missing event.event_type", `{"event":{"id":"E1","occurred_at":"2026-01-01T00:00:00Z"}}`},
		{"bad occurred_at", `{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"not-a-date"}}`},
		// Go previously accepted these two shapes and enqueued them, while
		// Python's pydantic model (PagerDutyEventType | Literal["pagey.ping"],
		// data: dict[str, JsonValue] with no default) rejects both with a 400
		// -- confirmed live against real Postgres+Valkey.
		{"unknown event_type", `{"event":{"id":"E1","event_type":"incident.made_up","occurred_at":"2026-01-01T00:00:00Z","data":{}}}`},
		{"missing data", `{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z"}}`},
		{"data is not an object", `{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"2026-01-01T00:00:00Z","data":[1,2]}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := parsePagerDutyWebhook([]byte(c.body)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestParsePagerDutyWebhookAcceptsAWellFormedEvent(t *testing.T) {
	body := `{"event":{"id":"E1","event_type":"incident.triggered","occurred_at":"2026-01-02T03:04:05Z","data":{"x":1}}}`
	webhook, err := parsePagerDutyWebhook([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if webhook.Event.ID != "E1" || webhook.Event.EventType != "incident.triggered" {
		t.Fatalf("%+v", webhook.Event)
	}
	if !webhook.Event.OccurredAt.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Fatalf("occurred_at = %v", webhook.Event.OccurredAt)
	}
}

// TestPythonMicrosecondFractionMatchesPythonIsoformat pins the fix: Go's
// ".999999" time layout trims trailing zeros (123400us -> ".1234"), but
// Python's datetime.isoformat()/model_dump_json() never does -- it omits
// the fraction entirely at zero microseconds, else always emits exactly 6
// digits. Confirmed live against the interpreter:
// `datetime(2026,1,1,tzinfo=UTC).replace(microsecond=123400).isoformat()`
// returns "...123400+00:00", not "...1234+00:00".
func TestPythonMicrosecondFractionMatchesPythonIsoformat(t *testing.T) {
	cases := []struct {
		name        string
		microsecond int
		want        string
	}{
		{"zero microseconds: no fraction", 0, ""},
		{"trailing-zero microseconds: not trimmed", 123400, ".123400"},
		{"single nonzero digit: still 6 digits", 1, ".000001"},
		{"max microseconds", 999999, ".999999"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			instant := time.Date(2026, 1, 1, 0, 0, 0, c.microsecond*1000, time.UTC)
			if got := pythonMicrosecondFraction(instant); got != c.want {
				t.Fatalf("pythonMicrosecondFraction(microsecond=%d) = %q, want %q", c.microsecond, got, c.want)
			}
		})
	}
}

func TestPythonIsoformatOmitsFractionAtZeroAndUsesNumericOffset(t *testing.T) {
	instant := time.Date(2026, 1, 2, 3, 4, 5, 123400*1000, time.UTC)
	if got, want := pythonIsoformat(instant), "2026-01-02T03:04:05.123400+00:00"; got != want {
		t.Fatalf("pythonIsoformat = %q, want %q", got, want)
	}
	zero := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if got, want := pythonIsoformat(zero), "2026-01-02T03:04:05+00:00"; got != want {
		t.Fatalf("pythonIsoformat = %q, want %q", got, want)
	}
}
