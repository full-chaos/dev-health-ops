package providersync

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

const githubTestsPrefixSourceNameKey = "excluded_non_report_artifact_prefix_source_name"

// The per-chunk prefix exclusion line carries the source as a STRING attribute (SigNoz does not index an array), under a
// key no earlier line used, so one key never holds two types over a roll.
func TestGitHubTestsChunkLineCarriesThePrefixExclusionSourceAsAString(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1") + "</testsuites>"}}
	extra := githubTestsPrefixGuardArtifact{name: "ci-extra-1", members: map[string]string{
		"x.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/x", "TestX") + "</testsuites>"}}
	artifacts := []githubTestsPrefixGuardArtifact{extra, report}

	records := captureMembershipLogs(t)
	githubTestsPrefixGuardRunWith(t, artifacts, []string{"ci-extra-"})
	lines := 0
	for _, record := range *records {
		if record.Message != githubTestsPrefixSourceChunkMessage {
			continue
		}
		lines++
		value, present := membershipLogAttrs(record)[githubTestsPrefixSourceNameKey]
		if source, ok := value.(string); !present || !ok || source != "config" {
			t.Errorf("%s = %#v (present=%t), want the string \"config\"", githubTestsPrefixSourceNameKey, value, present)
		}
	}
	if lines == 0 {
		t.Fatalf("no per-chunk prefix exclusion line; records: %+v", *records)
	}

	var out bytes.Buffer
	restore := logging.InstallDefault(logging.NewJSON(&out, slog.LevelDebug))
	githubTestsPrefixGuardRunWith(t, artifacts, []string{"ci-extra-"})
	restore()
	jsonLines := 0
	for _, line := range strings.Split(out.String(), "\n") {
		if !strings.Contains(line, `"msg":"`+githubTestsPrefixSourceChunkMessage+`"`) {
			continue
		}
		jsonLines++
		if !strings.Contains(line, `"`+githubTestsPrefixSourceNameKey+`":"config"`) {
			t.Errorf("chunk line lacks %s as a string: %s", githubTestsPrefixSourceNameKey, line)
		}
	}
	if jsonLines == 0 {
		t.Fatalf("no chunk line in the JSON log: %s", out.String())
	}
}
