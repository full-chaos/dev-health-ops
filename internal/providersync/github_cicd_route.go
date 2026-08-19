package providersync

import (
	"encoding/json"
	"time"
)

// gitHubWorkflowRunPayload is shared by the complete CICD/tests alias and
// deployment correlation. No partial CICD handler or effect writer exists.
type gitHubWorkflowRunPayload struct {
	ID           json.Number `json:"id"`
	Name         any         `json:"name"`
	Conclusion   any         `json:"conclusion"`
	Status       any         `json:"status"`
	CreatedAt    *string     `json:"created_at"`
	RunStartedAt *string     `json:"run_started_at"`
	UpdatedAt    *string     `json:"updated_at"`
	RunAttempt   any         `json:"run_attempt"`
	Event        any         `json:"event"`
	HeadSHA      any         `json:"head_sha"`
	HeadBranch   any         `json:"head_branch"`
	HTMLURL      any         `json:"html_url"`
	PullRequests []struct {
		Number any `json:"number"`
		Base   struct {
			Ref any `json:"ref"`
		} `json:"base"`
	} `json:"pull_requests"`
}

func parseGitHubWorkflowTime(value *string) *time.Time {
	return parseGitHubPullTime(value)
}

func workflowRetryCount(value any) uint32 {
	attempt, err := json.Number(stringValue(value)).Int64()
	if err != nil || attempt < 2 {
		return 0
	}
	return uint32(attempt - 1)
}

func ciPipelineRunOutsideWindow(startedAt time.Time, claim Claim) bool {
	return (claim.SinceAt != nil && startedAt.Before(claim.SinceAt.UTC())) ||
		(claim.BeforeAt != nil && startedAt.After(claim.BeforeAt.UTC()))
}
