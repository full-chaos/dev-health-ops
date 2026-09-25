package goapiproof

import (
	"context"
	"errors"
	"testing"
)

// CHAOS-6803: a two-plane proof sends the document to BOTH planes, so a
// registered mutation would apply its write on each. It is refused before any
// request leaves the runner, in every serving mode.
func TestRunRefusesADocumentThatIsNotAQueryBeforeSendingAnything(t *testing.T) {
	body := `{"data":{"featureFlags":[]}}`
	for _, tc := range []struct{ name, document string }{
		{"mutation", "mutation Touch { featureFlags { key } }"},
		{"subscription", "subscription Watch { featureFlags { key } }"},
		{"unparseable", "query Broken {"},
		{"two operations", "query A { featureFlags { key } } mutation M { featureFlags { key } }"},
	} {
		for _, mode := range []string{"canary", "primary", "shadow"} {
			t.Run(tc.name+"/mode="+mode, func(t *testing.T) {
				edge := &fakeEdge{goBody: body, pythonBody: body}
				runner := newRunner(t, edge, mode)
				runner.Documents["featureFlags"] = tc.document
				runner.Config.GoProofURL = "http://proof.invalid"

				outcomes, _, err := runner.Run(context.Background())
				if !errors.Is(err, ErrNothingMeasured) {
					t.Fatalf("expected ErrNothingMeasured, got %v", err)
				}
				if outcomes[0].RefusalReason != RefusalNotAQueryDocument {
					t.Fatalf("expected %s, got %s (%s)", RefusalNotAQueryDocument, outcomes[0].RefusalReason, outcomes[0].RefusalDetail)
				}
				if outcomes[0].RefusalDetail == "" {
					t.Fatal("a refusal must carry a detail an operator can act on")
				}
				if len(edge.seen) != 0 {
					t.Fatalf("a refused document must send nothing, but the edge saw %v", edge.seen)
				}
			})
		}
	}
}

func TestRunStillProvesAQueryDocument(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	edge := &fakeEdge{goBody: body, pythonBody: body, goBuild: "b18e56fa79cfe20ce0f75df148144b832d92be36"}
	runner := newRunner(t, edge, "canary")
	outcomes, _, err := runner.Run(context.Background())
	if err != nil {
		t.Fatalf("a query document must still be measured: %v", err)
	}
	if outcomes[0].RefusalReason == RefusalNotAQueryDocument || len(edge.seen) == 0 {
		t.Fatalf("a query document was refused as not-a-query or never sent: %+v", outcomes[0])
	}
}
