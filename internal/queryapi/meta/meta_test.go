package meta

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestBuildResponseStaticFields(t *testing.T) {
	resp := BuildResponse()
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

func TestResponseNamesNoServerVersion(t *testing.T) {
	body, err := json.Marshal(BuildResponse())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(strings.ToLower(string(body)), "version") {
		t.Fatalf("meta response mentions a version: %s", body)
	}
}
