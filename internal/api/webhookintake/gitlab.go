package webhookintake

import (
	"crypto/hmac"
	"io"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// verifyGitLabToken ports auth.py's verify_gitlab_token: a constant-time
// compare of X-Gitlab-Token against the configured token (not HMAC -- GitLab
// sends the shared secret itself, not a signature over the body).
func verifyGitLabToken(tokenHeader, secret string) bool {
	if tokenHeader == "" {
		return false
	}
	return hmac.Equal([]byte(tokenHeader), []byte(secret))
}

// handleGitLabWebhook ports router.py's gitlab_webhook.
func (d Deps) handleGitLabWebhook() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secret := d.secrets().GitLab
		if secret == "" {
			d.logger().Warn("GITLAB_WEBHOOK_TOKEN not configured - rejecting webhook")
			policy.WriteDetail(w, http.StatusInternalServerError, "Webhook token not configured", nil)
			return
		}
		if !verifyGitLabToken(r.Header.Get("X-Gitlab-Token"), secret) {
			d.logger().Warn("GitLab webhook token validation failed")
			policy.WriteDetail(w, http.StatusUnauthorized, "Invalid token", nil)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		xGitlabEvent := r.Header.Get("X-Gitlab-Event")

		payload, err := decodeJSONBody(body)
		if err != nil {
			d.logger().Warn("Invalid JSON in GitLab webhook", "error", err)
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid JSON payload", nil)
			return
		}
		object, _ := payload.(*pyjson.Object)
		objectAttrs := objectField(object, "object_attributes")
		action := stringField(objectAttrs, "action")
		if action == "" {
			action = stringField(objectAttrs, "state")
		}
		eType := mapGitlabEvent(xGitlabEvent, action)

		project := objectField(object, "project")
		repoName := stringPtrField(project, "path_with_namespace")
		orgID := stringPtrField(project, "namespace")

		objectKind := stringField(object, "object_kind")
		objectID := numericOrStringID(objectAttrs, "id")
		if objectID == "" {
			objectID = objectKind
		}
		deliveryID := xGitlabEvent
		if objectID != "" {
			deliveryID = xGitlabEvent + ":" + objectID
		}
		rawEventType := xGitlabEvent
		if action != "" {
			rawEventType = xGitlabEvent + ":" + action
		}

		event := webhookEvent{
			Provider:      "gitlab",
			EventType:     eType,
			RawEventType:  rawEventType,
			DeliveryID:    deliveryID,
			OrgID:         orgID,
			RepoName:      repoName,
			Payload:       body,
			PayloadParsed: payload,
		}
		respondForEvent(w, r, d, event, xGitlabEvent)
	}
}

// numericOrStringID reads object_attributes.id, which GitLab sends as a
// JSON number (router.py reads it with plain dict access, so any JSON
// scalar becomes str(...) once it reaches an f-string).
func numericOrStringID(object *pyjson.Object, name string) string {
	if object == nil {
		return ""
	}
	value, ok := object.Get(name)
	if !ok || value == nil {
		return ""
	}
	switch v := value.(type) {
	case string:
		return v
	case pyjson.Int:
		return v.String()
	default:
		return ""
	}
}
