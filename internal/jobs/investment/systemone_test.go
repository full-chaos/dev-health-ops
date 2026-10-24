package investment

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
)

type fakeSystemOne struct {
	asked, finished, skipped int
	keeps, low               map[int]bool
}

func (f *fakeSystemOne) categorize(context.Context, Config, preprocessed) (categorize.CategorizationOutcome, error) {
	f.asked++
	return categorize.CategorizationOutcome{LLMModel: "fake-model"}, nil
}
func (f *fakeSystemOne) Stamp() string                                   { return "fake-stamp" }
func (f *fakeSystemOne) Identity() (string, string)                      { return "fake-provider", "fake-model" }
func (f *fakeSystemOne) keepsLastRow(i int) bool                         { return f.keeps[i] }
func (f *fakeSystemOne) isLowQuality(i int) bool                         { return f.low[i] }
func (f *fakeSystemOne) noteSkippedTerminal(n int)                       { f.skipped = n }
func (f *fakeSystemOne) finish(context.Context, *chwrite.Writer, Config) { f.finished++ }
func (f *fakeSystemOne) SetObserver(ServedObserver)                      {}
func (f *fakeSystemOne) Close() error                                    { return nil }

func TestTheMaterializerCategorizesThroughTheSystemOneInterface(t *testing.T) {
	fake := &fakeSystemOne{keeps: map[int]bool{1: true}, low: map[int]bool{2: true}}
	m := &Materializer{systemOne: fake}
	cfg := Config{}
	outcome, err := m.categorizeEntry(context.Background(), cfg, preprocessed{})
	if err != nil || outcome.LLMModel != "fake-model" || fake.asked != 1 {
		t.Fatalf("the call did not go through the interface: %+v %v asked=%d", outcome, err, fake.asked)
	}
	if got := m.modelVersion(cfg); got != "fake-stamp" {
		t.Fatalf("modelVersion = %q", got)
	}
	if p, mod := m.usageIdentity(cfg); p != "fake-provider" || mod != "fake-model" {
		t.Fatalf("usageIdentity = %q/%q", p, mod)
	}
	if !m.keepsLastRow(1) || m.keepsLastRow(0) || !m.isLowQuality(2) || m.isLowQuality(0) {
		t.Fatal("row flags did not come from the interface")
	}
	m.noteSkippedTerminal(3)
	m.finishServed(context.Background(), cfg)
	if fake.skipped != 3 || fake.finished != 1 {
		t.Fatalf("skipped=%d finished=%d", fake.skipped, fake.finished)
	}
}

func TestSetServedNilLeavesNoSystemOne(t *testing.T) {
	m := &Materializer{}
	m.SetServed(nil)
	if m.backend() != nil || m.keepsLastRow(0) || m.isLowQuality(0) {
		t.Fatal("a nil served backend must leave the generative path with no SystemOne")
	}
	served, err := newServedDecision(&recordingSender{}, debugLogger(&syncBuffer{}))
	if err != nil {
		t.Fatal(err)
	}
	m.SetServed(served)
	if m.backend() == nil {
		t.Fatal("SetServed did not set the SystemOne")
	}
}
