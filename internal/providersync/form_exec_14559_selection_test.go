package providersync

import (
	"reflect"
	"testing"
)

// DO NOT MERGE. CHAOS-14559 config-write card: the write leaves the planned artifact set unchanged. The integration
// config text the card's write stores (byte-equal to what TestCard14559WriteArgvExecutes reads back after the verb)
// goes through the claim decoder and the config-prefix reader into the selection seam; every synthetic name gets the
// same (selected, reason) with the code list only and with the code list joined with the written list.
func TestCard14559WriteLeavesTheArtifactSelectionUnchanged(t *testing.T) {
	cardArgv := []string{"go-images-", "go-test-timings-", "go-junit-part-"}
	if !reflect.DeepEqual(cardArgv, githubTestsNonReportArtifactPrefixes) {
		t.Fatalf("the card's --prefix values %q are not the code list %q", cardArgv, githubTestsNonReportArtifactPrefixes)
	}
	before := `{"fetch_comments": true}`
	after := `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["go-images-", "go-test-timings-", "go-junit-part-"]}`
	read := func(stored string) []string {
		t.Helper()
		var claim Claim
		if err := decodeClaimDocuments(&claim, []byte(`{}`), []byte(`{}`), []byte(`{}`), []byte(stored)); err != nil {
			t.Fatalf("decode %s: %v", stored, err)
		}
		prefixes, invalid := githubTestsConfigExcludePrefixesChecked(claim)
		if invalid {
			t.Fatalf("the reader refused %s", stored)
		}
		return prefixes
	}
	if got := read(before); got != nil {
		t.Fatalf("before the write the reader gives %q, want none", got)
	}
	written := read(after)
	if !reflect.DeepEqual(written, cardArgv) {
		t.Fatalf("after the write the reader gives %q, want %q", written, cardArgv)
	}
	names := []string{
		// each prefix, and the bare prefix
		"go-images-linux-amd64", "go-images-", "go-test-timings-3", "go-test-timings-", "go-junit-part-17", "go-junit-part-",
		// case variants
		"GO-IMAGES-linux-arm64", "Go-Test-Timings-1", "go-JUNIT-part-2",
		// near misses
		"go-images", "go-image-x", "go_images-x", "xgo-images-1", " go-images-1", "go-test-timing-1", "go-test-timings",
		"go-junit-parts-1", "go-junit-part", "go-junit", "go-junit-merged", "junit-part-1", "images-go-1",
		// report artifacts, suffix exclusions, empty
		"test-results", "junit-report.xml", "coverage", "x.dockerbuild", "sbom.spdx.json", "go-images-x.spdx.json", "",
	}
	excludedBefore := map[string]bool{}
	for _, name := range names {
		codeSelected, codeReason := githubTestsArtifactSelectionSeam(name, read(before))
		joinedSelected, joinedReason := githubTestsArtifactSelectionSeam(name, written)
		if codeSelected != joinedSelected || codeReason != joinedReason {
			t.Errorf("%q: code list only (%v, %q), with the written list (%v, %q)", name, codeSelected, codeReason, joinedSelected, joinedReason)
		}
		if !codeSelected {
			excludedBefore[name] = true
		}
	}
	want := map[string]bool{
		"go-images-linux-amd64": true, "go-images-": true, "go-test-timings-3": true, "go-test-timings-": true,
		"go-junit-part-17": true, "go-junit-part-": true, "GO-IMAGES-linux-arm64": true, "Go-Test-Timings-1": true,
		"go-JUNIT-part-2": true, "x.dockerbuild": true, "sbom.spdx.json": true, "go-images-x.spdx.json": true,
	}
	if !reflect.DeepEqual(excludedBefore, want) {
		t.Errorf("excluded with the code list = %v, want %v", excludedBefore, want)
	}
	// Control: a written list with one more name does change the set, so the compare above can fail.
	if selected, _ := githubTestsArtifactSelectionSeam("test-results", append(append([]string{}, written...), "test-")); selected {
		t.Errorf("control: a config prefix test- did not exclude test-results")
	}
}
