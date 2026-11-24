package providersync

import (
	"log/slog"
	"slices"
	"testing"
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
