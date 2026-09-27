package admin

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

// @require_feature checks the process license (has_feature) before the
// org's own license: a process license granting the feature allows without
// consulting the org at all (no database here, so reaching the org check
// with a real org id would panic on the nil pool).
func TestRequireFeatureConsultsTheProcessLicenseFirst(t *testing.T) {
	t.Cleanup(func() { licensing.SetProcessLicense(nil) })
	h := &handlers{logger: slog.Default()}
	const org = "8d0b9f0e-0000-4000-8000-000000000001"

	licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{auditLogFeature: true}})
	recorder := httptest.NewRecorder()
	if !h.requireFeature(context.Background(), recorder, auditLogFeature, org) {
		t.Fatalf("process license granting %s denied: %d %s", auditLogFeature, recorder.Code, recorder.Body)
	}

	// Not granted by the process license, and no org to evaluate: 402 with
	// the process tier as current_tier.
	licensing.SetProcessLicense(&licensing.ProcessLicense{Tier: "enterprise", Features: map[string]bool{}})
	recorder = httptest.NewRecorder()
	if h.requireFeature(context.Background(), recorder, auditLogFeature, "not-a-uuid") {
		t.Fatal("allowed without a grant")
	}
	if recorder.Code != http.StatusPaymentRequired || !strings.Contains(recorder.Body.String(), `"current_tier":"enterprise"`) {
		t.Fatalf("got %d %s", recorder.Code, recorder.Body)
	}
}
