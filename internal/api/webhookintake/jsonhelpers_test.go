package webhookintake

import (
	"net/http/httptest"
	"testing"
)

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

// TestRequiredHeaderErrorsDistinguishesAbsentFromEmpty pins the fix: a
// header sent with an empty value is still PRESENT (Python's declared type
// is a plain str with no length constraint, so "" satisfies it); only a
// header key with no value at all is "missing".
func TestRequiredHeaderErrorsDistinguishesAbsentFromEmpty(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	req.Header.Set("X-GitHub-Event", "")
	errs := requiredHeaderErrors(req,
		[2]string{"X-GitHub-Event", "x-github-event"},
		[2]string{"X-GitHub-Delivery", "x-github-delivery"},
	)
	if len(errs) != 1 {
		t.Fatalf("errs = %+v, want exactly one (X-GitHub-Delivery)", errs)
	}
	loc := errs[0].Loc
	if len(loc) != 2 || loc[1] != "x-github-delivery" {
		t.Fatalf("errs[0].Loc = %v, want [header x-github-delivery]", loc)
	}
	if errs[0].Type != "missing" || errs[0].Msg != "Field required" || errs[0].Input != nil {
		t.Fatalf("errs[0] = %+v", errs[0])
	}
}

func TestRequiredHeaderErrorsEmptyWhenAllPresent(t *testing.T) {
	req := httptest.NewRequest("POST", "/", nil)
	req.Header.Set("X-Gitlab-Event", "Push Hook")
	if errs := requiredHeaderErrors(req, [2]string{"X-Gitlab-Event", "x-gitlab-event"}); len(errs) != 0 {
		t.Fatalf("errs = %+v, want none", errs)
	}
}
