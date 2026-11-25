package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// githubTestsPrefixSourceBytesSHA256 is the sha256 of the resume state below as main encodes it (ops 7fb09c11a049, before
// CHAOS-14559's source logging): naming the source of a prefix exclusion must not change one byte of the chunk cursor or
// of the prepared chunk sidecar.
const githubTestsPrefixSourceBytesSHA256 = "65377015ca835365ece6b5ece88b54f62796652421226bbb8888519575e063fd"

// A unit with prefix exclusions from both lists, walked so every emission resumes (each chunk a new pass), plus the
// realistic widest cursor stamp: every emission's cursors and prepared chunk sidecar payload, and the stamp's encoding,
// are byte-equal to main.
func TestGitHubTestsPrefixSourceLeavesTheResumeStateByteEqual(t *testing.T) {
	report := githubTestsPrefixGuardArtifact{name: "test-results", members: map[string]string{
		"junit.xml": "<testsuites>" + githubTestsPrefixGuardSuite("pkg/a", "TestA1", "TestA2") + "</testsuites>"}}
	artifacts := []githubTestsPrefixGuardArtifact{report}
	for _, name := range []string{"go-images-a-1", "ci-extra-1", "go-test-timings-1", "ci-extra-2"} {
		artifacts = append(artifacts, githubTestsPrefixGuardArtifact{name: name, members: map[string]string{"x.bin": "not-a-report"}})
	}
	claim := nativeTestClaim("github", "cicd")
	claim.IntegrationConfig = map[string]any{"ci_artifact_exclude_prefixes": []any{"ci-extra-", "go-images-", "go-test-timings-"}}
	normalizedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	client := githubTestsClient(t, fakehttp.Client(&githubTestsPrefixGuardDoer{t: t, artifacts: artifacts}))
	digest := sha256.New()
	add := func(label string, data []byte) {
		digest.Write([]byte(label))
		digest.Write([]byte{0})
		digest.Write(data)
		digest.Write([]byte{0})
	}
	resume, ordinal, final, excluded := "", 0, false, 0
	for passes := 0; !final; passes++ {
		if passes > 200 {
			t.Fatal("the walk never finished")
		}
		err := GitHubTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt, resume,
			func(emission ChunkRouteEmission) error {
				payload, err := encodedPreparedChunkPayload(PreparedProviderChunk{
					SchemaVersion: chunkPayloadSchemaVersion, RouteVersion: chunkRouteVersion, Ordinal: ordinal,
					CursorBefore: emission.CursorBefore, CursorAfter: emission.CursorAfter, InventoryComplete: emission.Final,
					Effects: emission.Batch.Effects, Result: emission.Batch.Result, Watermark: emission.Batch.Watermark,
				})
				if err != nil {
					return err
				}
				add("cursor_before", []byte(emission.CursorBefore))
				add("cursor_after", []byte(emission.CursorAfter))
				add("prepared_chunk", payload)
				ordinal++
				resume = emission.CursorAfter
				if emission.Final {
					final = true
					cursor, err := decodeGitHubTestsChunkCursor(emission.CursorAfter)
					if err != nil {
						return err
					}
					excluded = cursor.ExcludedNonReportPrefix
					return nil
				}
				return errGitHubTestsWalkContinuation
			})
		if err != nil && !errors.Is(err, errGitHubTestsWalkContinuation) {
			t.Fatalf("pass %d: %v", passes, err)
		}
	}
	if excluded != 4 || ordinal < 2 {
		t.Fatalf("premise: 4 prefix exclusions over several chunks, got %d over %d emissions", excluded, ordinal)
	}
	widest := githubTestsRealisticCursorBase()
	sort.Slice(widest.Incomplete, func(i, j int) bool {
		a, b := widest.Incomplete[i], widest.Incomplete[j]
		return a.Component < b.Component || a.Component == b.Component && a.Cause < b.Cause
	})
	stamp, err := json.Marshal(widest)
	if err != nil {
		t.Fatal(err)
	}
	add("widest_stamp", stamp)
	if got := hex.EncodeToString(digest.Sum(nil)); got != githubTestsPrefixSourceBytesSHA256 {
		t.Errorf("resume state sha256 = %s, want main's %s: the cursor or the prepared chunk changed", got, githubTestsPrefixSourceBytesSHA256)
	}
}
