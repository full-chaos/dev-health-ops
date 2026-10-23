package operational

// otherWebhookEventType is the one label value every event type outside
// knownWebhookEventTypes is recorded as.
const otherWebhookEventType = "other"

// knownWebhookEventTypes is the one set of event types the webhook handlers
// know: the types the intake persists (internal/api/webhookintake/events.go)
// plus workflow_run, which the github sync routing names but the intake never
// persists. A metric label takes only these values; the pin tests fail when a
// handler or the intake adds a type that is missing here.
var knownWebhookEventTypes = map[string]bool{
	"push":                 true,
	"pull_request":         true,
	"merge_request":        true,
	"issue_created":        true,
	"issue_updated":        true,
	"issue_closed":         true,
	"issue_deleted":        true,
	"pipeline":             true,
	"deployment":           true,
	"check_run":            true,
	"installation":         true,
	"marketplace_purchase": true,
	"workflow_run":         true,
}

// webhookEventTypeLabel is the value of the event_type label: the event type
// itself when a handler knows it, "other" for anything else.
func webhookEventTypeLabel(eventType string) string {
	if knownWebhookEventTypes[eventType] {
		return eventType
	}
	return otherWebhookEventType
}
