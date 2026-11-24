package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsPrefixSourceAttrs returns the prefix exclusion source attribute of every skip summary line in records.
func githubTestsPrefixSourceAttrs(t *testing.T, run func()) []any {
	t.Helper()
	records := captureMembershipLogs(t)
	run()
	var values []any
	for _, record := range *records {
		if record.Message == "provider artifacts skipped this unit; inventory continued" {
			values = append(values, membershipLogAttrs(record)["excluded_non_report_artifact_prefix_source"])
		}
	}
	if len(values) == 0 {
		t.Fatalf("no skip summary line")
	}
	return values
}

// githubTestsPrefixSourceCursor is the terminal cursor of a unit that excluded image by the code list.
func githubTestsPrefixSourceCursor(t *testing.T) (Claim, githubTestsChunkCursor) {
	t.Helper()
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1") + "</testsuites>"}}
	image := githubTestsPrefixGuardArtifact{name: "go-images-amd64-9-1", members: map[string]string{"images.tar": "not-a-report"}}
	_, walk := githubTestsPrefixGuardRunWith(t, []githubTestsPrefixGuardArtifact{image, report}, []string{"go-images-", "go-test-timings-", "go-junit-part-"}, nil)
	if walk.cursor.ExcludedNonReportPrefix == 0 || walk.cursor.ExcludedNonReportPrefixSources != githubTestsPrefixExclusionSourceBit(githubTestsPrefixExclusionSourceCode) {
		t.Fatalf("premise: a code-list exclusion, got count %d sources %d", walk.cursor.ExcludedNonReportPrefix, walk.cursor.ExcludedNonReportPrefixSources)
	}
	return nativeTestClaim("github", "cicd"), walk.cursor
}

// githubTestsPrefixSourceKey is the cursor JSON key of ExcludedNonReportPrefixSources: the one key a cursor gains when
// only that field is set.
func githubTestsPrefixSourceKey(t *testing.T) string {
	t.Helper()
	keys := func(cursor githubTestsChunkCursor) map[string]any {
		encoded, err := json.Marshal(cursor)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		return fields
	}
	without, with := keys(githubTestsChunkCursor{}), keys(githubTestsChunkCursor{ExcludedNonReportPrefixSources: 3})
	var added []string
	for key := range with {
		if _, ok := without[key]; !ok {
			added = append(added, key)
		}
	}
	if len(added) != 1 {
		t.Fatalf("premise: the field adds exactly one key, got %v", added)
	}
	return added[0]
}

// Mixed roll, new -> old -> new: a cursor written by this code and written back by a worker without the field (it decodes
// the JSON and drops the unknown key) resumes here with no error, keeps its counts, and its exclusions are UNSOURCED on
// the summary line (no attribute), never "code".
func TestGitHubTestsPrefixSourceCursorWrittenBackWithoutTheFieldIsUnsourced(t *testing.T) {
	claim, cursor := githubTestsPrefixSourceCursor(t)
	cursor.ExcludedNonReportPrefixSources |= githubTestsPrefixExclusionSourceBit(githubTestsPrefixExclusionSourceConfig)
	encoded, err := encodeGitHubTestsChunkCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	var old map[string]any
	if err := json.Unmarshal([]byte(encoded), &old); err != nil {
		t.Fatal(err)
	}
	key := githubTestsPrefixSourceKey(t)
	if _, ok := old[key]; !ok {
		t.Fatalf("premise: the new cursor carries the field %q: %s", key, encoded)
	}
	delete(old, key)
	writtenBack, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := decodeGitHubTestsChunkCursor(string(writtenBack))
	if err != nil {
		t.Fatalf("the written-back cursor does not resume: %v", err)
	}
	if resumed.ExcludedNonReportPrefix != cursor.ExcludedNonReportPrefix || resumed.ExcludedNonReportPrefixSources != 0 {
		t.Fatalf("resumed count %d sources %d, want %d and 0", resumed.ExcludedNonReportPrefix, resumed.ExcludedNonReportPrefixSources, cursor.ExcludedNonReportPrefix)
	}
	for _, value := range githubTestsPrefixSourceAttrs(t, func() {
		if _, err := githubTestsFinalMetadataBatch(claim, resumed); err != nil {
			t.Fatal(err)
		}
	}) {
		if value != nil {
			t.Errorf("excluded_non_report_artifact_prefix_source = %v, want no attribute (unsourced)", value)
		}
	}
}

// Mixed roll, old -> new: a cursor without the field (written by a worker before it) read here leaves the earlier
// exclusions unsourced; a later chunk's config-list exclusion reads as config only, never code.
func TestGitHubTestsPrefixSourceCursorWithoutTheFieldIsNeverCode(t *testing.T) {
	claim, cursor := githubTestsPrefixSourceCursor(t)
	cursor.ExcludedNonReportPrefixSources = 0
	encoded, err := encodeGitHubTestsChunkCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	resumed, err := decodeGitHubTestsChunkCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range githubTestsPrefixSourceAttrs(t, func() {
		if _, err := githubTestsFinalMetadataBatch(claim, resumed); err != nil {
			t.Fatal(err)
		}
	}) {
		if value != nil {
			t.Errorf("earlier chunks only: excluded_non_report_artifact_prefix_source = %v, want no attribute", value)
		}
	}
	resumed.ExcludedNonReportPrefixSources |= githubTestsPrefixExclusionSourceBit(githubTestsPrefixExclusionSourceConfig)
	for _, value := range githubTestsPrefixSourceAttrs(t, func() {
		if _, err := githubTestsFinalMetadataBatch(claim, resumed); err != nil {
			t.Fatal(err)
		}
	}) {
		if got, ok := value.([]string); !ok || !slices.Equal(got, []string{githubTestsPrefixExclusionSourceConfig}) {
			t.Errorf("with a later config exclusion: excluded_non_report_artifact_prefix_source = %v, want [config]", value)
		}
	}
}

// The set stays inside the closed set whatever the chunk count: a walk that yields after every emission (many
// resumes), with repeated exclusions from both lists, ends with exactly both bits and the two names.
func TestGitHubTestsPrefixSourceSetStaysClosedOverManyChunks(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1") + "</testsuites>"}}
	artifacts := []githubTestsPrefixGuardArtifact{report}
	for _, name := range []string{"go-images-a-1", "ci-extra-1", "go-images-b-1", "ci-extra-2", "go-test-timings-1", "ci-extra-3", "go-junit-part-1"} {
		artifacts = append(artifacts, githubTestsPrefixGuardArtifact{name: name, members: map[string]string{"x.bin": "not-a-report"}})
	}
	claim := nativeTestClaim("github", "cicd")
	claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": []any{"ci-extra-"}}
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	client := githubTestsClient(t, fakehttp.Client(&githubTestsPrefixGuardDoer{t: t, artifacts: artifacts}))
	resume, passes, final := "", 0, false
	both := githubTestsPrefixExclusionSourceBit(githubTestsPrefixExclusionSourceConfig) | githubTestsPrefixExclusionSourceBit(githubTestsPrefixExclusionSourceCode)
	values := githubTestsPrefixSourceAttrs(t, func() {
		for !final {
			passes++
			if passes > 200 {
				t.Fatal("the walk never finished")
			}
			err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
				func(emission ChunkRouteEmission) error {
					cursor, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
					if err != nil {
						return err
					}
					if cursor.ExcludedNonReportPrefixSources&^both != 0 {
						t.Errorf("pass %d: the source set %d holds a bit outside the closed set %d", passes, cursor.ExcludedNonReportPrefixSources, both)
					}
					resume = emission.CursorAfter
					if emission.Final {
						final = true
						if cursor.ExcludedNonReportPrefix != 7 || cursor.ExcludedNonReportPrefixSources != both {
							t.Errorf("final cursor: count %d sources %d, want 7 and %d", cursor.ExcludedNonReportPrefix, cursor.ExcludedNonReportPrefixSources, both)
						}
						return nil
					}
					return errGitHubTestsWalkContinuation
				})
			if err != nil && !errors.Is(err, errGitHubTestsWalkContinuation) {
				t.Fatalf("pass %d: %v", passes, err)
			}
		}
	})
	if passes < 2 {
		t.Fatalf("premise: the walk resumed %d times, want several chunks", passes)
	}
	for _, value := range values {
		if got, ok := value.([]string); !ok || !slices.Equal(got, []string{"code", "config"}) {
			t.Errorf("excluded_non_report_artifact_prefix_source = %v, want [code config]", value)
		}
	}
	for _, mask := range []int{0, 1, 2, 3} {
		if names := githubTestsPrefixExclusionSourceNames(mask); len(names) > 2 {
			t.Errorf("mask %d names %v: more than the closed set", mask, names)
		}
	}
}
