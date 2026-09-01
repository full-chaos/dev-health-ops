package providersync

import (
	"bytes"
	"encoding/json"
	"math/big"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubWorkItemPRSocialUser mirrors GitHubGraphQLUser, the exact object the
// active Python GitHubWorkClient adapter gives the semantic normalizers.
type githubWorkItemPRSocialUser struct {
	Login *string `json:"login"`
	Email *string `json:"email"`
	Name  *string `json:"name"`
}

// githubWorkItemPRSocialComment mirrors GitHubGraphQLComment after Python's
// _comment_from_graphql conversion. Keeping this typed intermediate makes the
// camelCase GraphQL-to-semantic boundary independently oracle-testable.
type githubWorkItemPRSocialComment struct {
	ID        any                        `json:"id"`
	CreatedAt *time.Time                 `json:"created_at"`
	User      githubWorkItemPRSocialUser `json:"user"`
	Body      string                     `json:"body"`
}

// githubWorkItemPRSocialEvent mirrors GitHubGraphQLEvent after Python's
// _event_from_graphql conversion.
type githubWorkItemPRSocialEvent struct {
	CreatedAt *time.Time                 `json:"created_at"`
	Event     string                     `json:"event"`
	Actor     githubWorkItemPRSocialUser `json:"actor"`
	Label     any                        `json:"label"`
}

// githubWorkItemPRSocialSemanticPayload is the explicit composition seam
// between the raw GraphQL fetch foundation and normalizeGitHubPullRequestBundle.
type githubWorkItemPRSocialSemanticPayload struct {
	Comments []json.RawMessage
	Events   []json.RawMessage
	// ClosingIssueRefs (CHAOS-4757) passes the raw closingIssuesReferences
	// nodes straight through: unlike Comments/Events there is no Python
	// conversion this needs to mirror byte-for-byte (no Python producer exists
	// for this field — Go-only per the standing sync-ownership rule), so
	// extractGitHubClosingIssueReferences parses the GraphQL node shape
	// directly rather than through an intermediate semantic struct.
	ClosingIssueRefs []json.RawMessage
}

type githubWorkItemPRSocialRawComment struct {
	ID             any     `json:"id"`
	DatabaseID     any     `json:"databaseId"`
	FullDatabaseID any     `json:"fullDatabaseId"`
	Body           *string `json:"body"`
	CreatedAt      *string `json:"createdAt"`
	Author         *struct {
		Login *string `json:"login"`
	} `json:"author"`
}

type githubWorkItemPRSocialRawEvent struct {
	TypeName  string  `json:"__typename"`
	CreatedAt *string `json:"createdAt"`
	Actor     *struct {
		Login *string `json:"login"`
	} `json:"actor"`
}

func adaptGitHubWorkItemPRSocialPayload(
	payload GitHubWorkItemPRSocialPayload,
) (githubWorkItemPRSocialSemanticPayload, error) {
	adapted := githubWorkItemPRSocialSemanticPayload{
		Comments:         make([]json.RawMessage, 0, len(payload.Comments)),
		Events:           make([]json.RawMessage, 0, len(payload.Events)),
		ClosingIssueRefs: append([]json.RawMessage(nil), payload.ClosingIssueRefs...),
	}
	for _, raw := range payload.Comments {
		comment, err := adaptGitHubWorkItemPRSocialComment(raw)
		if err != nil {
			return githubWorkItemPRSocialSemanticPayload{}, err
		}
		encoded, err := json.Marshal(comment)
		if err != nil {
			return githubWorkItemPRSocialSemanticPayload{}, providerfoundation.ErrNormalizationInvalid
		}
		adapted.Comments = append(adapted.Comments, encoded)
	}
	for _, raw := range payload.Events {
		event, err := adaptGitHubWorkItemPRSocialEvent(raw)
		if err != nil {
			return githubWorkItemPRSocialSemanticPayload{}, err
		}
		encoded, err := json.Marshal(event)
		if err != nil {
			return githubWorkItemPRSocialSemanticPayload{}, providerfoundation.ErrNormalizationInvalid
		}
		adapted.Events = append(adapted.Events, encoded)
	}
	return adapted, nil
}

func adaptGitHubWorkItemPRSocialComment(
	raw json.RawMessage,
) (githubWorkItemPRSocialComment, error) {
	var node githubWorkItemPRSocialRawComment
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if decoder.Decode(&node) != nil {
		return githubWorkItemPRSocialComment{}, providerfoundation.ErrNormalizationInvalid
	}
	comment := githubWorkItemPRSocialComment{
		ID:        githubWorkItemPRSocialPythonTruthyID(node.DatabaseID, node.FullDatabaseID, node.ID),
		CreatedAt: parseGitHubWorkItemTime(node.CreatedAt),
	}
	if node.Body != nil {
		comment.Body = *node.Body
	}
	if node.Author != nil {
		comment.User.Login = node.Author.Login
	}
	return comment, nil
}

func adaptGitHubWorkItemPRSocialEvent(
	raw json.RawMessage,
) (githubWorkItemPRSocialEvent, error) {
	var node githubWorkItemPRSocialRawEvent
	if json.Unmarshal(raw, &node) != nil {
		return githubWorkItemPRSocialEvent{}, providerfoundation.ErrNormalizationInvalid
	}
	event := githubWorkItemPRSocialEvent{
		CreatedAt: parseGitHubWorkItemTime(node.CreatedAt),
		Event: map[string]string{
			"MergedEvent": "merged", "ClosedEvent": "closed", "ReopenedEvent": "reopened",
		}[node.TypeName],
	}
	if node.Actor != nil {
		event.Actor.Login = node.Actor.Login
	}
	return event, nil
}

// githubWorkItemPRSocialPythonTruthyID mirrors Python's
// databaseId or fullDatabaseId or id precedence without converting a
// precision-sensitive integer to float64 or a string.
func githubWorkItemPRSocialPythonTruthyID(candidates ...any) any {
	for _, candidate := range candidates {
		switch value := candidate.(type) {
		case nil:
			continue
		case string:
			if value != "" {
				return value
			}
		case json.Number:
			integer := new(big.Int)
			if _, ok := integer.SetString(value.String(), 10); ok {
				if integer.Sign() != 0 {
					return integer
				}
				continue
			}
			if strings.TrimSpace(value.String()) != "" && value.String() != "0" {
				return value.String()
			}
		case bool:
			if value {
				return value
			}
		default:
			return value
		}
	}
	return nil
}
