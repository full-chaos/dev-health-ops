package providersync

import (
	"archive/zip"
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-13193: every member that is skipped as not a report is counted, directory entries (top level and nested)
// and members with an unsafe name included, and the Info line carries counts only: no repository and no run.
func TestGitHubTestsSkippedDirectoryAndUnsafeMembersAreCountedAndLoggedWithoutIdentifiers(t *testing.T) {
	var zbuf bytes.Buffer
	zw := zip.NewWriter(&zbuf)
	for _, m := range []struct{ name, body string }{
		{"junit.xml", githubTestsMultiSuiteJUnit(1)},
		{"dir/", ""},
		{"reports/subdir/", ""},
		{"../escape.json", "{}"},
	} {
		w, err := zw.Create(m.name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(m.body))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := parseGitHubTestsArtifact(zbuf.Bytes(), "7", "repo", "9", "org", nil, nil, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if rows.NonReportJSON != 0 || rows.NonReportOther != 3 || rows.ReportsParsed != 1 {
		t.Fatalf("json=%d other=%d parsed=%d, want 0/3/1", rows.NonReportJSON, rows.NonReportOther, rows.ReportsParsed)
	}

	var buf bytes.Buffer
	swapDefaultLogger(t, logging.NewJSON(&buf, slog.LevelInfo))
	client := &providerfoundation.HTTPClient{Provider: "github", Metrics: providerfoundation.NewMetrics()}
	recordGitHubTestsNonReportMembers(client, Claim{Provider: "github", Dataset: "cicd"}, rows)
	log := buf.String()
	if !strings.Contains(log, `"skipped_other":3`) {
		t.Fatalf("missing skipped_other=3 in log: %s", log)
	}
	for _, banned := range []string{`"repository"`, `"run"`, "acme", "escape.json", "subdir"} {
		if strings.Contains(log, banned) {
			t.Fatalf("log carries %s: %s", banned, log)
		}
	}
}
