package providersync

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// The skip summary line names the list each prefix exclusion came from (closed set config | code; the configuration's
// list is checked first). The source is in the log line only: no cursor field, result key or served row carries it.
func TestGitHubTestsSkipSummaryNamesThePrefixExclusionSource(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	image := githubTestsPrefixGuardArtifact{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}
	timings := githubTestsPrefixGuardArtifact{name: "go-test-timings-integration-packages-1", members: map[string]string{"t.tsv.gz": "pkg\t1.0"}}
	codeList := []string{"go-images-", "go-test-timings-", "go-junit-part-"}
	// A 40-byte configured prefix (Parse allows up to artifactexclude.MaxEntryBytes = 100): the sample cuts names at
	// githubTestsMaxArtifactNameBytes (24), so the source is taken at match time on the full name.
	const longConfig = "ci-long-artifact-family-for-a-forty-byte"
	long := githubTestsPrefixGuardArtifact{name: longConfig + "-1", members: map[string]string{"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/l", "TestL") + "</testsuites>"}}
	// A 41-byte configured prefix that starts with a code prefix: both lists match, so it reports config (a cut name would
	// match the code prefix only).
	const longBoth = "go-images-amd64-for-a-long-config-prefix-"
	longImage := githubTestsPrefixGuardArtifact{name: longBoth + "9-1", members: map[string]string{"images.tar": "not-a-report"}}
	for _, cell := range []struct {
		name           string
		artifacts      []githubTestsPrefixGuardArtifact
		configPrefixes []string
		want           []string
	}{
		{name: "A: only the configuration's list matches", artifacts: []githubTestsPrefixGuardArtifact{extra, report}, configPrefixes: []string{"ci-extra-"}, want: []string{"config"}},
		{name: "B: only the code list matches (a name cut at the sample bound too)", artifacts: []githubTestsPrefixGuardArtifact{image, timings, report}, want: []string{"code"}},
		{name: "C: a prefix in both lists reports config", artifacts: []githubTestsPrefixGuardArtifact{image, report}, configPrefixes: []string{"GO-IMAGES-"}, want: []string{"config"}},
		{name: "D: one exclusion from each list", artifacts: []githubTestsPrefixGuardArtifact{image, extra, report}, configPrefixes: []string{"ci-extra-"}, want: []string{"code", "config"}},
		{name: "E: a 40-byte configured prefix", artifacts: []githubTestsPrefixGuardArtifact{long, report}, configPrefixes: []string{longConfig}, want: []string{"config"}},
		// No code prefix is longer than 16 bytes ("go-test-timings-"); F is the longest code-only match: a 38-byte name.
		{name: "F: a long name matched only by the code list", artifacts: []githubTestsPrefixGuardArtifact{timings, report}, want: []string{"code"}},
		{name: "G: a 41-byte configured prefix that starts with a code prefix", artifacts: []githubTestsPrefixGuardArtifact{longImage, report}, configPrefixes: []string{longBoth}, want: []string{"config"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			records := captureMembershipLogs(t)
			served, _ := githubTestsPrefixGuardRunWith(t, cell.artifacts, codeList, cell.configPrefixes)
			if served.excludedPrefix == 0 {
				t.Fatalf("premise: no prefix exclusion, got %+v", served)
			}
			summaries := 0
			for _, record := range *records {
				if record.Message != "provider artifacts skipped this unit; inventory continued" {
					continue
				}
				summaries++
				attrs := membershipLogAttrs(record)
				got, ok := attrs["excluded_non_report_artifact_prefix_source"].([]string)
				if !ok || !slices.Equal(got, cell.want) {
					t.Errorf("excluded_non_report_artifact_prefix_source = %v (ok=%t), want %v", attrs["excluded_non_report_artifact_prefix_source"], ok, cell.want)
				}
				if record.Level != slog.LevelWarn {
					t.Errorf("level = %s, want the summary's WARN", record.Level)
				}
			}
			if summaries == 0 {
				t.Fatalf("no skip summary line; records: %+v", *records)
			}
		})
	}
}

// The field name and the value exactly as the production JSON handler (logging.NewJSON) renders them on the summary line:
// a config-only unit, a code-only unit and a unit with both.
func TestGitHubTestsSkipSummaryRendersThePrefixExclusionSourceLiterals(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	image := githubTestsPrefixGuardArtifact{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}
	codeList := []string{"go-images-", "go-test-timings-", "go-junit-part-"}
	for _, cell := range []struct {
		name           string
		artifacts      []githubTestsPrefixGuardArtifact
		configPrefixes []string
		literal        string
	}{
		{name: "config only", artifacts: []githubTestsPrefixGuardArtifact{image, report}, configPrefixes: codeList, literal: `"excluded_non_report_artifact_prefix_source":["config"]`},
		{name: "code only", artifacts: []githubTestsPrefixGuardArtifact{image, report}, literal: `"excluded_non_report_artifact_prefix_source":["code"]`},
		{name: "both", artifacts: []githubTestsPrefixGuardArtifact{image, extra, report}, configPrefixes: []string{"ci-extra-"}, literal: `"excluded_non_report_artifact_prefix_source":["code","config"]`},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var out bytes.Buffer
			restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelDebug))
			githubTestsPrefixGuardRunWith(t, cell.artifacts, codeList, cell.configPrefixes)
			restore()
			lines := 0
			for _, line := range strings.Split(out.String(), "\n") {
				if !strings.Contains(line, `"msg":"provider artifacts skipped this unit; inventory continued"`) {
					continue
				}
				lines++
				if !strings.Contains(line, cell.literal) {
					t.Errorf("summary line does not hold %s: %s", cell.literal, line)
				}
			}
			if lines == 0 {
				t.Fatalf("no summary line in the JSON log: %s", out.String())
			}
		})
	}
}
