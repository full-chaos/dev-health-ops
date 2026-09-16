package meta

import (
	"context"
	"errors"
	"reflect"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
)

// erroringClient fails every Query call -- ports main.py's inner
// try/except swallowing a version-query failure (main.py:438-445).
type erroringClient struct{}

func (erroringClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return nil, errors.New("clickhouse unavailable")
}

// emptyRowsClient returns zero rows -- ports main.py:442's
// `if result else "unknown"` branch.
type emptyRowsClient struct{}

func (emptyRowsClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &versionRowScanner{served: true}, nil // Next() returns false immediately
}

// scanErrorRowScanner has a row to serve but fails to Scan it.
type scanErrorRowScanner struct{ served bool }

func (s *scanErrorRowScanner) Next() bool {
	if s.served {
		return false
	}
	s.served = true
	return true
}
func (s *scanErrorRowScanner) Scan(...any) error { return errors.New("scan failed") }
func (s *scanErrorRowScanner) Err() error        { return nil }
func (s *scanErrorRowScanner) Close() error      { return nil }

type scanErrorClient struct{}

func (scanErrorClient) Query(context.Context, string, []dhclickhouse.Binding) (dhclickhouse.RowScanner, error) {
	return &scanErrorRowScanner{}, nil
}

func wantStaticFields(t *testing.T, resp Response) {
	t.Helper()
	if resp.Backend != "clickhouse" {
		t.Errorf("Backend = %q, want %q", resp.Backend, "clickhouse")
	}
	if resp.LastIngestAt != nil {
		t.Errorf("LastIngestAt = %v, want nil", resp.LastIngestAt)
	}
	if resp.Coverage == nil || len(resp.Coverage) != 0 {
		t.Errorf("Coverage = %v, want non-nil empty map", resp.Coverage)
	}
	wantLimits := map[string]int{"max_days": 365, "max_repos": 1000}
	if !reflect.DeepEqual(resp.Limits, wantLimits) {
		t.Errorf("Limits = %v, want %v", resp.Limits, wantLimits)
	}
	wantEndpoints := []string{
		"/api/v1/home", "/api/v1/quadrant", "/api/v1/flame", "/api/v1/heatmap",
		"/api/v1/work-units", "/api/v1/sankey", "/api/v1/investment",
		"/api/v1/opportunities", "/graphql",
	}
	if !reflect.DeepEqual(resp.SupportedEndpoints, wantEndpoints) {
		t.Errorf("SupportedEndpoints = %v, want %v", resp.SupportedEndpoints, wantEndpoints)
	}
}

func TestBuildResponseWithWorkingClient(t *testing.T) {
	resp := BuildResponse(context.Background(), fixedVersionClient{version: "24.3.1.2672"})
	if resp.Version != "24.3.1.2672" {
		t.Errorf("Version = %q, want %q", resp.Version, "24.3.1.2672")
	}
	wantStaticFields(t, resp)
}

// TestBuildResponseDegradesToUnknownOnQueryError ports main.py's
// documented behaviour ("Silently ignore version query failures - not
// critical for meta endpoint", main.py:443-444): a ClickHouse query
// failure never surfaces as an error, it degrades the version field and
// the response is still complete.
func TestBuildResponseDegradesToUnknownOnQueryError(t *testing.T) {
	resp := BuildResponse(context.Background(), erroringClient{})
	if resp.Version != unknownVersion {
		t.Errorf("Version = %q, want %q", resp.Version, unknownVersion)
	}
	wantStaticFields(t, resp)
}

func TestBuildResponseDegradesToUnknownOnEmptyResult(t *testing.T) {
	resp := BuildResponse(context.Background(), emptyRowsClient{})
	if resp.Version != unknownVersion {
		t.Errorf("Version = %q, want %q", resp.Version, unknownVersion)
	}
}

func TestBuildResponseDegradesToUnknownOnScanError(t *testing.T) {
	resp := BuildResponse(context.Background(), scanErrorClient{})
	if resp.Version != unknownVersion {
		t.Errorf("Version = %q, want %q", resp.Version, unknownVersion)
	}
}

func TestBuildResponseDegradesToUnknownOnNilClient(t *testing.T) {
	resp := BuildResponse(context.Background(), nil)
	if resp.Version != unknownVersion {
		t.Errorf("Version = %q, want %q", resp.Version, unknownVersion)
	}
	wantStaticFields(t, resp)
}

func TestBuildResponseNeverReturnsAnError(t *testing.T) {
	// BuildResponse has no error return at all -- this test exists so a
	// future signature change (adding one back) is a compile-time-visible
	// diff here, not a silent contract change. See the package doc
	// comment's "Declared divergence" section for why Python's
	// construction-failure 503 branch has no per-request analog in this
	// port's client model.
	var resp Response = BuildResponse(context.Background(), erroringClient{})
	_ = resp
}
