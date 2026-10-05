package goapiproof

import (
	"testing"
	"time"
)

// testopsJobFailures refuses a window of more than 90 days, and the proof window is 92: its request must be a
// window the operation serves, or every proof run would measure a refusal.
func TestTestopsJobFailuresRequestIsAWindowTheOperationServes(t *testing.T) {
	spec, ok := operationSpecs["testopsJobFailures"]
	if !ok {
		t.Fatal("testopsJobFailures has no corpus entry")
	}
	w := DefaultWindow()
	input, ok := spec.Variables("org-1", w)["input"].(map[string]any)
	if !ok {
		t.Fatal("the request has no input object")
	}
	since, err := time.Parse("2006-01-02", input["sinceDate"].(string))
	if err != nil {
		t.Fatal(err)
	}
	until, err := time.Parse("2006-01-02", input["untilDate"].(string))
	if err != nil {
		t.Fatal(err)
	}
	if input["untilDate"] != w.UntilDate {
		t.Errorf("untilDate = %v, want the window's own end %s", input["untilDate"], w.UntilDate)
	}
	if days := int(until.Sub(since).Hours() / 24); days != 30 {
		t.Errorf("the request spans %d days, want 30 (the operation refuses more than 90)", days)
	}
	if got := testopsJobFailuresSince("not-a-date"); got != "not-a-date" {
		t.Errorf("an unreadable date became %q", got)
	}
}
