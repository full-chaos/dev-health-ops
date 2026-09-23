package webhookintake

import "testing"

// TestWebhookResponseBodyKeyOrderMatchesPythonsFieldDeclarationOrder pins
// key order: policy.WriteJSON on a plain map[string]any sorts keys
// alphabetically (encoding/json's own behavior), which silently reordered
// this response against models.py's WebhookResponse field declaration
// order (status, event_id, message) -- masked in the venue oracle until
// its own normalizer stopped alphabetizing every response it touched
// (event_id appears in every case here).
func TestWebhookResponseBodyKeyOrderMatchesPythonsFieldDeclarationOrder(t *testing.T) {
	body := webhookResponseBody("accepted", "E1", "Processing push event")
	want := []string{"status", "event_id", "message"}
	got := body.Keys()
	if len(got) != len(want) {
		t.Fatalf("keys = %v, want %v", got, want)
	}
	for i, key := range want {
		if got[i] != key {
			t.Fatalf("keys = %v, want %v", got, want)
		}
	}
}
