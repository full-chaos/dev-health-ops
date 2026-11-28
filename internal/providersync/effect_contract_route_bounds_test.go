package providersync

import (
	"errors"
	"testing"
)

// CHAOS-14723 raised the write contract to 200,000 rows and 128 MiB for the GitHub tests route. The routes below keep the
// bounds they had under the 100,000-row / 64 MiB contract.

func TestGitHubBlameWriteBoundsKeepTheirValueAfterTheContractRaise(t *testing.T) {
	limits := defaultGitHubBlameLimits()
	if limits.maxRows != 100_000 || limits.maxPayloadBytes != (64<<20)/10*9 {
		t.Fatalf("github blame limits %d rows / %d bytes, want 100,000 / %d", limits.maxRows, limits.maxPayloadBytes, (64<<20)/10*9)
	}
	if gitHubBlameMaxRows != 100_000 || gitHubBlameMaxPayloadBytes != 64<<20 {
		t.Fatalf("github blame write bounds %d rows / %d bytes, want 100,000 / %d", gitHubBlameMaxRows, gitHubBlameMaxPayloadBytes, 64<<20)
	}
}

func TestGitLabBlameWriteBoundsKeepTheirValueAfterTheContractRaise(t *testing.T) {
	limits := defaultGitLabBlameLimits()
	if limits.maxRows != 100_000 || limits.maxPayloadBytes != (64<<20)/10*9 {
		t.Fatalf("gitlab blame limits %d rows / %d bytes, want 100,000 / %d", limits.maxRows, limits.maxPayloadBytes, (64<<20)/10*9)
	}
}

func TestGitHubWorkItemsIncompleteBoundKeepsItsValueAfterTheContractRaise(t *testing.T) {
	entries := func(count int) map[string]any {
		incomplete := make([]GitHubWorkItemsIncomplete, count)
		for index := range incomplete {
			incomplete[index] = GitHubWorkItemsIncomplete{Component: "milestones", Cause: "transient"}
		}
		return map[string]any{githubWorkItemsIncompleteResultKey: incomplete}
	}
	if _, _, err := applyGitHubWorkItemsIncompletePolicy("github", "work-items", entries(100_000), nil); err != nil {
		t.Fatalf("100,000 incomplete entries refused: %v", err)
	}
	if _, _, err := applyGitHubWorkItemsIncompletePolicy("github", "work-items", entries(100_001), nil); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("100,001 incomplete entries gave %v, want ErrInvalidConfiguration", err)
	}
}
