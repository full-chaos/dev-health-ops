package prove

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// CHAOS-7440: the help text of -dry-run said "execute and compare, but write NO receipts" while the run opens no
// database, reads no routing row, refuses every operation and measures nothing. This test runs -dry-run end to
// end and pins what it does (no pool is opened, no operation request reaches the edge, nothing executed, no
// receipt written, the run ends in ErrNothingMeasured), then pins that the help text says exactly that and does
// not claim execution or comparison.
func TestDryRunHelpTextAgreesWithWhatTheRunDoes(t *testing.T) {
	var graphqlHits atomic.Int32
	args, reportPath := exitFixture(t, func(w http.ResponseWriter, _ bool) {
		graphqlHits.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	var kept []string
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-postgres-uri") {
			kept = append(kept, arg)
		}
	}
	kept = append(kept, "-dry-run")
	var opened atomic.Int32
	original := openPostgresPool
	t.Cleanup(func() { openPostgresPool = original })
	openPostgresPool = func(context.Context, string) (dbPool, error) {
		opened.Add(1)
		return nil, errors.New("a dry run must open no database")
	}

	err := runCLI(t, kept)
	if !errors.Is(err, goapiproof.ErrNothingMeasured) {
		t.Fatalf("a dry run ended with %v, want the measured-nothing error", err)
	}
	if opened.Load() != 0 {
		t.Fatalf("a dry run opened %d database connection(s)", opened.Load())
	}
	if graphqlHits.Load() != 0 {
		t.Fatalf("a dry run sent %d operation request(s) to the edge", graphqlHits.Load())
	}
	raw, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var report struct {
		Summary struct {
			Attempted       int `json:"attempted"`
			Executed        int `json:"executed"`
			Refused         int `json:"refused"`
			ReceiptsWritten int `json:"receipts_written"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("the report does not decode: %v", err)
	}
	if report.Summary.Attempted == 0 || report.Summary.Refused != report.Summary.Attempted || report.Summary.Executed != 0 || report.Summary.ReceiptsWritten != 0 {
		t.Fatalf("a dry run's summary = %+v, want every attempted operation refused, none executed, no receipt", report.Summary)
	}

	fs, _ := registerFlags()
	flag := fs.Lookup("dry-run")
	if flag == nil {
		t.Fatal("no -dry-run flag")
	}
	usage := strings.ToLower(flag.Usage)
	for _, claim := range []string{"execute and compare"} {
		if strings.Contains(usage, claim) {
			t.Errorf("the -dry-run help text claims %q, the run executes nothing", claim)
		}
	}
	for _, fact := range []string{"no database", "no receipts", "refused", "nothing is executed or compared", "measured-nothing error"} {
		if !strings.Contains(usage, fact) {
			t.Errorf("the -dry-run help text does not say %q, which is what the run does: %q", fact, flag.Usage)
		}
	}
}
