package providersync

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

const (
	gitLabApproveNote   = "approved this merge request"
	gitLabUnapproveNote = "unapproved this merge request"
	gitLabDiffNoteType  = "DiffNote"
)

// mapGitLabPullRequestReviews is the Go-side port of
// processors.gitlab.map_gitlab_mr_reviews. GitLab has no discrete review
// resource, so the canonical review rows are reconstructed from timestamped
// system notes plus the approvals endpoint. Discussion notes and an MR
// author's own diff notes remain ordinary comments and never become review
// rows.
func mapGitLabPullRequestReviews(
	claim Claim,
	repoID string,
	number int,
	approvals map[string]any,
	notes []json.RawMessage,
	fallbackAt *time.Time,
	normalizedAt time.Time,
	authorData map[string]any,
) ([]pullRequestReviewRow, *time.Time, int) {
	rows := make([]pullRequestReviewRow, 0)
	seenIDs := make(map[string]struct{})
	author := strings.ToLower(strings.TrimSpace(stringValue(authorData["username"])))
	approvedReviewers := make(map[string]struct{})
	var firstReviewAt *time.Time
	changesRequested := 0

	add := func(reviewID, reviewer, state string, at *time.Time) {
		if reviewID == "" {
			return
		}
		if _, exists := seenIDs[reviewID]; exists {
			return
		}
		seenIDs[reviewID] = struct{}{}
		if at != nil && (firstReviewAt == nil || at.Before(*firstReviewAt)) {
			value := at.UTC()
			firstReviewAt = &value
		}
		submittedAt := normalizedAt
		if fallbackAt != nil {
			submittedAt = *fallbackAt
		}
		if at != nil {
			submittedAt = *at
		}
		row := pullRequestReviewRow{
			RepoID: repoID, Number: number, ReviewID: reviewID,
			Reviewer: firstNonEmpty(reviewer, "Unknown"), State: state,
			SubmittedAt: submittedAt.UTC().Truncate(time.Millisecond),
			LastSynced:  normalizedAt, OrgID: claim.OrgID,
		}
		if row.State == "CHANGES_REQUESTED" {
			changesRequested++
		}
		rows = append(rows, row)
	}

	for _, raw := range notes {
		var note map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if decoder.Decode(&note) != nil || note == nil {
			continue
		}
		noteAuthor, _ := note["author"].(map[string]any)
		username := firstNonEmpty(stringValue(noteAuthor["username"]), "Unknown")
		createdAt := parseGitLabPullTime(note["created_at"])
		noteID := stringValue(note["id"])
		if noteID == "" {
			noteID = "None"
		}
		body := strings.ToLower(strings.TrimSpace(stringValue(note["body"])))
		if system, ok := note["system"].(bool); ok && system {
			switch {
			case strings.HasPrefix(body, gitLabApproveNote):
				approvedReviewers[username] = struct{}{}
				add("note-"+noteID, username, "APPROVED", createdAt)
			case strings.HasPrefix(body, gitLabUnapproveNote):
				add("note-"+noteID, username, "DISMISSED", createdAt)
			}
			continue
		}
		if stringValue(note["type"]) != gitLabDiffNoteType {
			continue
		}
		if author != "" && strings.ToLower(strings.TrimSpace(username)) == author {
			continue
		}
		add("note-"+noteID, username, "COMMENTED", createdAt)
	}

	approved, _ := approvals["approved_by"].([]any)
	for _, raw := range approved {
		entry, _ := raw.(map[string]any)
		user, _ := entry["user"].(map[string]any)
		if user == nil {
			continue
		}
		username := firstNonEmpty(stringValue(user["username"]), "Unknown")
		if _, exists := approvedReviewers[username]; exists {
			continue
		}
		approvedReviewers[username] = struct{}{}
		userID := stringValue(user["id"])
		reviewID := "approval-" + username
		if userID != "" {
			reviewID = "approval-" + userID
		}
		add(reviewID, username, "APPROVED", nil)
	}
	return rows, firstReviewAt, changesRequested
}
