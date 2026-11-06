package providersync

import (
	"archive/zip"
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"

	"log/slog"
)

// CHAOS-13193: members that are not reports (.xml/.info) are counted by a
// closed extension class, logged as counts only, and never become an
// incomplete cause (reports_complete and the watermark stay as they are).
func TestGitHubTestsNonReportMembersAreCountedNotIncomplete(t *testing.T) {
	var buf bytes.Buffer
	var zbuf bytes.Buffer
	zw := zip.NewWriter(&zbuf)
	for name, body := range map[string]string{
		"junit.xml":  githubTestsMultiSuiteJUnit(1),
		"shard.json": `{"Action":"pass"}`,
		"sbom.json":  `{}`,
		"notes.txt":  "x",
		"dir/":       "",
	} {
		w, err := zw.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = w.Write([]byte(body))
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := parseGitHubTestsArtifact(zbuf.Bytes(), "7", "repo", "9", "org", nil, nil, time.Unix(0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if rows.NonReportJSON != 2 || rows.NonReportOther != 1 || rows.ReportsParsed != 1 {
		t.Fatalf("json=%d other=%d parsed=%d, want 2/1/1", rows.NonReportJSON, rows.NonReportOther, rows.ReportsParsed)
	}
	if incomplete, optional := rows.optionalIncomplete(); len(incomplete) != 0 || !optional {
		t.Fatalf("incomplete=%v optional=%v, want none: non-report members are not an incomplete cause", incomplete, optional)
	}

	swapDefaultLogger(t, logging.NewJSON(&buf, slog.LevelInfo))
	client := &providerfoundation.HTTPClient{Provider: "github", Metrics: providerfoundation.NewMetrics()}
	recordGitHubTestsNonReportMembers(client, Claim{Provider: "github", Dataset: "cicd"}, "acme/api", "9", rows)
	var out bytes.Buffer
	if err := client.Metrics.WritePrometheus(&out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`dev_health_provider_non_report_members_skipped_total{provider="github",dataset="cicd",class="json"} 2`,
		`dev_health_provider_non_report_members_skipped_total{provider="github",dataset="cicd",class="other"} 1`,
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("missing %q in:\n%s", want, out.String())
		}
	}
	log := buf.String()
	for _, want := range []string{`"skipped_json":2`, `"skipped_other":1`, `"reports_parsed":1`} {
		if !strings.Contains(log, want) {
			t.Fatalf("missing %s in log: %s", want, log)
		}
	}
	if strings.Contains(log, "shard.json") || strings.Contains(log, "sbom.json") {
		t.Fatalf("log carries member names: %s", log)
	}
}
