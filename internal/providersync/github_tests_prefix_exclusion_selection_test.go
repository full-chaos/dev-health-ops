package providersync

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
)

// githubTestsBaseArtifactSelection is the selection rule of ops 7fb09c11a049 (before CHAOS-14559's source order): the
// suffixes, then one prefix match over the code list OR the configuration's list.
func githubTestsBaseArtifactSelection(name string, configPrefixes []string) (bool, string) {
	lower := strings.ToLower(name)
	for _, suffix := range githubTestsNonReportArtifactSuffixes {
		if strings.HasSuffix(lower, suffix) {
			return false, githubTestsExclusionReasonSuffix
		}
	}
	if artifactexclude.Matches(lower, githubTestsNonReportArtifactPrefixes) || artifactexclude.Matches(lower, configPrefixes) {
		return false, githubTestsExclusionReasonPrefix
	}
	return true, ""
}

// Checking the configuration's list first only names the source: for a name matched by both lists, by one alone or by
// neither, excluded yes/no and the reason token equal the base rule's code || config answer.
func TestGitHubTestsPrefixSourceOrderLeavesTheSelectionUnchanged(t *testing.T) {
	config := []string{"ci-extra-", "GO-IMAGES-", "go-test-timings-integration-"}
	for _, row := range []struct {
		name, shape string
		excluded    bool
	}{
		{"go-images-amd64-1", "both lists", true},
		{"GO-IMAGES-arm64-1", "both lists, upper case", true},
		{"go-test-timings-integration-packages-1", "both lists, the config prefix longer", true},
		{"ci-extra-1", "the config list alone", true},
		{"CI-Extra-2", "the config list alone, mixed case", true},
		{"go-junit-part-3", "the code list alone", true},
		{"go-test-timings-unit-1", "the code list alone, beside a longer config prefix", true},
		{"test-results", "neither", false},
		{"junit-go-images-1", "neither, a prefix inside the name", false},
		{"go-images-x.dockerbuild", "both lists and a suffix", true},
		{"ci-extra-1.spdx.json", "the config list and a suffix", true},
	} {
		for _, prefixes := range [][]string{config, nil} {
			wantSelected, wantReason := githubTestsBaseArtifactSelection(row.name, prefixes)
			gotSelected, gotReason := githubTestsArtifactSelectionSeam(row.name, prefixes)
			selected, reason, _ := githubTestsArtifactSelection(row.name, prefixes)
			if gotSelected != wantSelected || gotReason != wantReason || selected != wantSelected || reason != wantReason {
				t.Errorf("%s (%s, config %v): seam (%t, %q), selection (%t, %q); base (%t, %q)",
					row.name, row.shape, prefixes, gotSelected, gotReason, selected, reason, wantSelected, wantReason)
			}
			if prefixes != nil && wantSelected == row.excluded {
				t.Errorf("premise: %s (%s) excluded by base = %t, want %t", row.name, row.shape, !wantSelected, row.excluded)
			}
		}
	}
}
