package webhookintake

// eventType is models.py's WebhookEventType. "unknown" events are answered
// 202 accepted without ever reaching _persist_webhook_delivery/the outbox
// (router.py's own early-return branch for each provider).
type eventType string

const (
	eventPush                eventType = "push"
	eventPullRequest         eventType = "pull_request"
	eventIssueCreated        eventType = "issue_created"
	eventIssueUpdated        eventType = "issue_updated"
	eventIssueClosed         eventType = "issue_closed"
	eventIssueDeleted        eventType = "issue_deleted"
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
