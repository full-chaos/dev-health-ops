package investment

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize/decision"
)

type recordingSender struct{ bodies [][]byte }

func (sender *recordingSender) PostSystemOneDetailed(_ context.Context, body []byte) (categorize.SystemOneResult, error) {
	sender.bodies = append(sender.bodies, append([]byte(nil), body...))
	return categorize.SystemOneResult{}, errors.New("recorded")
}
func (sender *recordingSender) Model() string { return decision.DefaultModel }
func (sender *recordingSender) Close() error  { return nil }

func TestServedRequestBytesAndStampAreGolden(t *testing.T) {
	sender := &recordingSender{}
	served, err := newServedDecision(sender, debugLogger(&syncBuffer{}))
	if err != nil {
		t.Fatal(err)
	}
	entry := shadowEntry(0, "golden-unit", shadowTestBundle(t, "golden-unit", 6))
	_, _ = served.categorize(context.Background(), shadowTestConfig(), entry)
	if len(sender.bodies) == 0 {
		t.Fatal("the served backend sent no request")
	}
	m := &Materializer{}
	m.SetServed(served)
	provider, model := m.usageIdentity(Config{})
	got := append([]byte(nil), sender.bodies[0]...)
	got = append(got, []byte("\n--\nstamp="+m.modelVersion(Config{})+"\nusage="+provider+"/"+model+"\n")...)

	path := filepath.Join("testdata", "systemone_request.golden")
	if os.Getenv("UPDATE_SYSTEMONE_GOLDEN") == "1" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("the served request bytes, stamp or usage identity changed (%d bytes, golden %d)", len(got), len(want))
	}
}
