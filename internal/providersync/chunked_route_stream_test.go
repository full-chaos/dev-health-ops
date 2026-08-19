package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitHubTestsChunkRouteEmitsBoundedPagesAndFinalMetadata(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	doer := &githubTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{
		"junit.xml": githubTestsJUnitFixture, "lcov.info": githubTestsLCOVFixture,
	})}
	claim := nativeTestClaim("github", "tests")
	var emissions int
	var finals int
	if err := (GitHubTestsRouteHandler{}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, githubTestsClient(t, doer), now, "",
		func(emission ChunkRouteEmission) error {
			emissions++
			if emission.Final {
				finals++
			}
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) > 100 {
					t.Fatalf("effect %s rows=%d", effect.Destination, len(effect.Rows))
				}
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if emissions < 2 || finals != 1 {
		t.Fatalf("emissions=%d finals=%d", emissions, finals)
	}
}

func TestGitLabTestsChunkRouteEmitsBoundedPagesAndFinalMetadata(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	doer := &gitLabTestsRouteDoer{t: t, archive: githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})}
	claim := nativeTestClaim("gitlab", "tests")
	var emissions int
	var finals int
	if err := (GitLabTestsRouteHandler{}).CollectChunks(
		context.Background(), claim, providerfoundation.Credential{}, gitLabRepositoryClient(t, doer, "https://gitlab.example"), now, "",
		func(emission ChunkRouteEmission) error {
			emissions++
			if emission.Final {
				finals++
			}
			for _, effect := range emission.Batch.Effects {
				if len(effect.Rows) > 100 {
					t.Fatalf("effect %s rows=%d", effect.Destination, len(effect.Rows))
				}
			}
			return nil
		},
	); err != nil {
		t.Fatal(err)
	}
	if emissions < 2 || finals != 1 {
		t.Fatalf("emissions=%d finals=%d", emissions, finals)
	}
}
