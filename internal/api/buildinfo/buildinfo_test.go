package buildinfo

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

func serve(t *testing.T, info version.Info, status int) http.Header {
	t.Helper()
	handler := Stamp(info)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	return rec.Result().Header
}

func TestStampNamesPlaneAndBuildOnEveryStatus(t *testing.T) {
	for _, status := range []int{200, 401, 500} {
		header := serve(t, version.Info{Commit: " abc123 "}, status)
		if got := header.Get(PlaneHeader); got != "go" {
			t.Fatalf("status %d plane = %q, want go", status, got)
		}
		if got := header.Get(BuildHeader); got != "abc123" {
			t.Fatalf("status %d build = %q, want abc123", status, got)
		}
	}
}

func TestStampOmitsBuildWhenCommitUnknown(t *testing.T) {
	for _, commit := range []string{"", "unknown"} {
		header := serve(t, version.Info{Commit: commit}, 200)
		if got := header.Get(BuildHeader); got != "" {
			t.Fatalf("commit %q produced build header %q; an unidentified build must not be named", commit, got)
		}
		if header.Get(PlaneHeader) != "go" {
			t.Fatalf("plane header missing for commit %q", commit)
		}
	}
}

func TestRoutesMountNothingWithoutAGuard(t *testing.T) {
	if got := Routes(nil, version.Info{}); len(got) != 0 {
		t.Fatalf("got %d routes with no guard, want 0", len(got))
	}
}
