package webhookintake

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// verifyJiraSignature ports auth.py's verify_jira_signature: HMAC-SHA256 over
// the raw body in X-Hub-Signature, an optional "sha256=" prefix either way.
func verifyJiraSignature(body []byte, signatureHeader, secret string) bool {
	return verifyJiraWithKey(body, signatureHeader, newVerifyingKey(secret))
}

func verifyJiraWithKey(body []byte, signatureHeader string, key verifyingKey) bool {
	if signatureHeader == "" {
		return false
	}
	mac := hmac.New(sha256.New, key.hmacKey)
	mac.Write(body)
	computed := hex.EncodeToString(mac.Sum(nil))
	expected := strings.TrimPrefix(signatureHeader, "sha256=")
	return hmac.Equal([]byte(computed), []byte(expected))
}

// handleJiraWebhook ports router.py's jira_webhook.
func (d Deps) handleJiraWebhook() http.HandlerFunc {
	key := newVerifyingKey(d.secrets().Jira)
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		if !verifyJiraWithKey(body, r.Header.Get("X-Hub-Signature"), key) {
			d.logger().Warn("Jira webhook signature validation failed")
			policy.WriteDetail(w, http.StatusUnauthorized, "Invalid signature", nil)
			return
		}

		payload, err := decodeJSONBody(body)
		if err != nil {
			if writeIntLimit(w, err) {
				return
			}
			d.logger().Warn("Invalid JSON in Jira webhook", "error", err)
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid JSON payload", nil)
			return
		}
		object, _ := payload.(*pyjson.Object)
		webhookEventName := stringField(object, "webhookEvent")
		eType := mapJiraEvent(webhookEventName)

		issue := objectField(object, "issue")
		issueKey := stringField(issue, "key")
		fields := objectField(issue, "fields")
		project := objectField(fields, "project")
		projectKey := stringField(project, "key")
		var orgID, repoID *string
		if projectKey != "" {
			key := projectKey
			orgID = &key
			repoID = &key
		}

		timestamp := numericOrStringID(object, "timestamp")
		var deliveryID string
		if issueKey != "" {
			deliveryID = webhookEventName + ":" + issueKey + ":" + timestamp
		}

		event := webhookEvent{
			Provider:      "jira",
			EventType:     eType,
			RawEventType:  webhookEventName,
			DeliveryID:    deliveryID,
			OrgID:         orgID,
			RepoID:        repoID,
			Payload:       body,
			PayloadParsed: payload,
		}
		respondForEvent(w, r, d, event, webhookEventName)
	}
}
