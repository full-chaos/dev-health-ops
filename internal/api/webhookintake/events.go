package webhookintake

// eventType is models.py's WebhookEventType. "unknown" events are answered
// 202 accepted without ever reaching _persist_webhook_delivery/the outbox
// (router.py's own early-return branch for each provider).
type eventType string

const (
	eventPush                eventType = "push"
	eventPullRequest         eventType = "pull_request"
	eventMergeRequest        eventType = "merge_request"
	eventIssueCreated        eventType = "issue_created"
	eventIssueUpdated        eventType = "issue_updated"
	eventIssueClosed         eventType = "issue_closed"
	eventIssueDeleted        eventType = "issue_deleted"
	eventPipeline            eventType = "pipeline"
	eventDeployment          eventType = "deployment"
	eventCheckRun            eventType = "check_run"
	eventInstallation        eventType = "installation"
	eventMarketplacePurchase eventType = "marketplace_purchase"
	eventUnknown             eventType = "unknown"
)

var githubEventMap = map[string]eventType{
	"push":                      eventPush,
	"pull_request":              eventPullRequest,
	"issues":                    eventIssueUpdated,
	"issue_comment":             eventIssueUpdated,
	"deployment":                eventDeployment,
	"deployment_status":         eventDeployment,
	"check_run":                 eventCheckRun,
	"check_suite":               eventCheckRun,
	"installation":              eventInstallation,
	"installation_repositories": eventInstallation,
	"marketplace_purchase":      eventMarketplacePurchase,
}

var gitlabEventMap = map[string]eventType{
	"Push Hook":          eventPush,
	"Tag Push Hook":      eventPush,
	"Merge Request Hook": eventMergeRequest,
	"Issue Hook":         eventIssueUpdated,
	"Pipeline Hook":      eventPipeline,
	"Deployment Hook":    eventDeployment,
}

var jiraEventMap = map[string]eventType{
	"jira:issue_created": eventIssueCreated,
	"jira:issue_updated": eventIssueUpdated,
	"jira:issue_deleted": eventIssueDeleted,
}

// mapGithubEvent ports models.py's map_github_event.
func mapGithubEvent(eventName string, action string) eventType {
	if eventName == "issues" {
		switch action {
		case "opened":
			return eventIssueCreated
		case "closed":
			return eventIssueClosed
		case "deleted":
			return eventIssueDeleted
		default:
			return eventIssueUpdated
		}
	}
	if mapped, ok := githubEventMap[eventName]; ok {
		return mapped
	}
	return eventUnknown
}

// mapGitlabEvent ports models.py's map_gitlab_event.
func mapGitlabEvent(eventName string, action string) eventType {
	if eventName == "Issue Hook" {
		switch action {
		case "open":
			return eventIssueCreated
		case "close":
			return eventIssueClosed
		default:
			return eventIssueUpdated
		}
	}
	if mapped, ok := gitlabEventMap[eventName]; ok {
		return mapped
	}
	return eventUnknown
}

// mapJiraEvent ports models.py's map_jira_event.
func mapJiraEvent(webhookEvent string) eventType {
	if mapped, ok := jiraEventMap[webhookEvent]; ok {
		return mapped
	}
	return eventUnknown
}
