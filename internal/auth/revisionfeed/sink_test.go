package revisionfeed

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

func deliverable(revision string) audit.Event {
	return audit.Event{
		ID: 3, AggregateType: "membership", AggregateID: "mbr_EXAMPLE0000000000000001", EventType: "membership.changed",
		Payload:        []byte(`{"revision":` + revision + `,"change":{"note":"SENTINEL_PAYLOAD_VALUE"}}`),
		IdempotencyKey: "membership:mbr_EXAMPLE0000000000000001:" + revision,
		CreatedAt:      time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC),
	}
}

func reasonOf(err error) string {
	var r interface{ DeliveryReason() string }
	if errors.As(err, &r) {
		return r.DeliveryReason()
	}
	return ""
}

func TestTheKeyAndChannelNamesAreAStableContract(t *testing.T) {
	if HighWaterKey != "dho:rl:auth_svc_rev_hwm" {
		t.Fatalf("HighWaterKey = %q", HighWaterKey)
	}
	if Channel != "auth_svc_rev" {
		t.Fatalf("Channel = %q", Channel)
	}
	// The ACL of the auth login reaches keys under dho:rl:auth_svc_ and channels under auth_svc_ and nothing else.
	if !strings.HasPrefix(HighWaterKey, "dho:rl:auth_svc_") || !strings.HasPrefix(Channel, "auth_svc_") {
		t.Fatal("the key or the channel is outside the prefixes the auth ACL user may reach")
	}
}

func TestSinkRunsOneScriptWithTheHashKeyTheAggregateFieldTheRevisionAndTheChannel(t *testing.T) {
	client := &fakeClient{evalResult: 1}
	sink, err := NewSink(client)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Deliver(context.Background(), deliverable("4")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	calls := client.calls()
	if len(calls) != 1 {
		t.Fatalf("%d script calls, want exactly one (one Lua call does the HSET max and the PUBLISH)", len(calls))
	}
	call := calls[0]
	if call.script != highWaterScript {
		t.Fatal("the call did not run the high-water script")
	}
	if len(call.keys) != 1 || call.keys[0] != HighWaterKey {
		t.Fatalf("keys %q", call.keys)
	}
	if len(call.args) != 3 || call.args[0] != "membership:mbr_EXAMPLE0000000000000001" || call.args[1] != "4" || call.args[2] != Channel {
		t.Fatalf("args %q, want [type:id 4 channel]", call.args)
	}
}

func TestSinkReturnsNilOnlyAfterTheScriptSucceeded(t *testing.T) {
	for name, tc := range map[string]struct {
		err    error
		reason string
	}{
		"valkey down":            {ErrUnavailable, "consumer_unavailable"},
		"server refused (ACL)":   {ErrRejected, "consumer_rejected"},
		"an unclassified error":  {errors.New("dial tcp 10.0.0.1:6379 SENTINEL_URI"), "consumer_unavailable"},
		"context deadline":       {context.DeadlineExceeded, "consumer_unavailable"},
		"wrapped unavailability": {errors.Join(errors.New("x"), ErrUnavailable), "consumer_unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			sink, _ := NewSink(&fakeClient{evalErr: tc.err})
			err := sink.Deliver(context.Background(), deliverable("4"))
			if err == nil {
				t.Fatal("Deliver returned nil although the script failed")
			}
			if got := reasonOf(err); got != tc.reason {
				t.Fatalf("reason %q, want %q", got, tc.reason)
			}
			known := false
			for _, r := range audit.AllFailureReasons() {
				known = known || r == reasonOf(err)
			}
			if !known {
				t.Fatalf("reason %q is outside the publisher's closed set", reasonOf(err))
			}
			if strings.Contains(err.Error(), "SENTINEL") || strings.Contains(err.Error(), "10.0.0.1") {
				t.Fatalf("the error text carries the cause: %v", err)
			}
		})
	}
}

func TestSinkRefusesAnEventThatDoesNotMapWithoutTouchingValkey(t *testing.T) {
	client := &fakeClient{evalResult: 1}
	sink, _ := NewSink(client)
	bad := deliverable("4")
	bad.IdempotencyKey = "membership:mbr_EXAMPLE0000000000000001:5"
	err := sink.Deliver(context.Background(), bad)
	if err == nil || reasonOf(err) != "consumer_rejected" {
		t.Fatalf("err = %v, reason %q, want consumer_rejected", err, reasonOf(err))
	}
	if n := len(client.calls()); n != 0 {
		t.Fatalf("%d script calls for an event that does not map", n)
	}
	if strings.Contains(err.Error(), "SENTINEL") {
		t.Fatalf("the error carries the payload: %v", err)
	}
}

func TestNewSinkRefusesANilClient(t *testing.T) {
	if _, err := NewSink(nil); err == nil {
		t.Fatal("a sink without a client")
	}
}

// The module set has no Lua interpreter, so the script's semantics are pinned two ways: this test pins its shape (the
// max comparison guards the only HSET, and the PUBLISH follows), and the integration cells run it on a real Valkey.
func TestTheHighWaterScriptTakesTheMaxBeforeItWritesAndPublishesAfterwards(t *testing.T) {
	s := highWaterScript
	hget := strings.Index(s, "'HGET'")
	hset := strings.Index(s, "'HSET'")
	publish := strings.Index(s, "'PUBLISH'")
	if hget < 0 || hset < 0 || publish < 0 {
		t.Fatalf("the script lacks HGET, HSET or PUBLISH:\n%s", s)
	}
	if !(hget < hset && hset < publish) {
		t.Fatalf("order must be HGET, HSET, PUBLISH:\n%s", s)
	}
	if strings.Count(s, "'HSET'") != 1 || strings.Count(s, "'PUBLISH'") != 1 {
		t.Fatalf("one HSET and one PUBLISH expected:\n%s", s)
	}
	// the HSET sits inside an if whose condition compares the incoming revision with the current one
	ifIndex := strings.LastIndex(s[:hset], "if ")
	if ifIndex < 0 || !strings.Contains(s[ifIndex:hset], "incoming > current") || !strings.Contains(s[ifIndex:hset], "then") {
		t.Fatalf("the HSET is not guarded by incoming > current:\n%s", s)
	}
	// Codex r3 P3: the guard is EXACTLY "current == nil or incoming > current": a constant, an extra `or`, a reversed or
	// widened comparison, or a second branch around the HSET fails here (the module set has no Lua interpreter to run it).
	block := regexp.MustCompile(`(?m)^if current == nil or incoming > current then\n  redis\.call\('HSET', KEYS\[1\], ARGV\[1\], ARGV\[2\]\)\n  raised = 1\nend\nredis\.call\('PUBLISH', ARGV\[3\], ARGV\[1\] \.\. ':' \.\. ARGV\[2\]\)\n`)
	if !block.MatchString(s) {
		t.Fatalf("the guarded HSET block is not exactly the reviewed one:\n%s", s)
	}
	if strings.Count(s, "if ") != 1 || strings.Count(s, " or ") != 1 || strings.Contains(s, "true") || strings.Contains(s, "else") || strings.Contains(s, "not ") {
		t.Fatalf("the script has a branch, a constant or an operator the reviewed one does not:\n%s", s)
	}
	if !regexp.MustCompile(`(?m)^local current = tonumber\(redis\.call\('HGET', KEYS\[1\], ARGV\[1\]\)\)\nlocal incoming = tonumber\(ARGV\[2\]\)\n`).MatchString(s) {
		t.Fatalf("current and incoming are not read as numbers from the hash and the revision:\n%s", s)
	}
	if strings.Contains(s[publish:], "'HSET'") {
		t.Fatal("a write after the publish")
	}
	if !strings.Contains(s, "tonumber(") {
		t.Fatal("revisions are compared as numbers, not as strings")
	}
}
