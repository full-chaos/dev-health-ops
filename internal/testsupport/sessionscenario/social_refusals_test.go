package sessionscenario

import (
	"net/http"
	"testing"
)

// Every replay override names a golden step it really changes and says why:
// an override with no matching step, no reason, or the golden's own answer is
// a silent bypass.
func TestSocialLinkRefusalsEachMatchAGoldenStepAndChangeIt(t *testing.T) {
	golden := map[string]RecordedAnswer{}
	for _, answer := range LoadGolden(t).Answers {
		golden[answer.Name] = answer
	}
	for name, refusal := range socialLinkRefusals {
		recorded, ok := golden[name]
		if !ok {
			t.Errorf("override %q matches no golden step", name)
			continue
		}
		if refusal.status == 0 {
			t.Errorf("override %q has no status", name)
		}
		if refusal.reason == "" {
			t.Errorf("override %q has no reason", name)
		}
		if recorded.Status == refusal.status && recorded.Body == refusal.body {
			t.Errorf("override %q equals the golden answer: it changes nothing", name)
		}
	}
}
