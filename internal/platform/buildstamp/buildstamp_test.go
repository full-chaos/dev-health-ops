package buildstamp

import (
	"net/http"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

func TestSetProvenanceNamesPlaneAlwaysAndBuildOnlyWhenKnown(t *testing.T) {
	for commit, wantBuild := range map[string]string{" abc ": "abc", "": "", "unknown": ""} {
		header := http.Header{}
		SetProvenance(header, commit)
		if got := header.Get(PlaneHeader); got != "go" {
			t.Fatalf("commit %q: plane = %q", commit, got)
		}
		if got := header.Get(BuildHeader); got != wantBuild {
			t.Fatalf("commit %q: build = %q, want %q", commit, got, wantBuild)
		}
	}
}

func TestBodyIsTheInfoJSONWithATrailingNewline(t *testing.T) {
	body, err := Body(version.Info{Service: "api", Commit: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"service":"api","version":"","commit":"abc","build_time":"","go_version":"","modified":false}` + "\n"
	if string(body) != want {
		t.Fatalf("body = %q, want %q", body, want)
	}
}
