package workerservice

import (
	"context"
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
)

type quietDiscoverer struct{}

func (quietDiscoverer) RepositoryIDs(context.Context, string) ([]daily.RepositoryID, error) {
	return nil, nil
}

type reportingQuietDiscoverer struct{ quietDiscoverer }

func (reportingQuietDiscoverer) ReportRepositoriesNotDiscovered(context.Context, string) {}

type noDrain struct{}

func (noDrain) DrainTouchedDays(context.Context, string, string) {}

// The worker's daily dispatcher asks its discoverer, through an optional
// interface, which stored rows the discovery leaves out. The worker refuses a
// discoverer that cannot answer, with its own error: otherwise a wrapper
// around the real discoverer would drop the report and every run would be
// silent. A discoverer that can answer gets past that check (and then fails
// on the missing store of this test, with another error).
func TestTheDailyDispatcherRefusesADiscovererThatCannotReport(t *testing.T) {
	if _, err := newDrainingDailyDispatcher(nil, nil, quietDiscoverer{}, noDrain{}); !errors.Is(err, errDailyDiscovererCannotReport) {
		t.Fatalf("a discoverer with no report: error %v, want %v", err, errDailyDiscovererCannotReport)
	}
	if _, err := newDrainingDailyDispatcher(nil, nil, reportingQuietDiscoverer{}, noDrain{}); err == nil || errors.Is(err, errDailyDiscovererCannotReport) {
		t.Fatalf("a discoverer with the report: error %v, want the error of the missing store only", err)
	}
	// The discoverer the worker builds is one that can report.
	var built daily.RepositoryDiscoverer = (*daily.ClickHouseRepositoryDiscoverer)(nil)
	if _, reports := built.(daily.RepositoriesNotDiscoveredReporter); !reports {
		t.Fatal("the ClickHouse discoverer of the worker cannot report")
	}
}
