package webhookintake

import (
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// verifyGitLabToken ports auth.py's verify_gitlab_token: a constant-time
// compare of X-Gitlab-Token against the configured token (not HMAC -- GitLab
// sends the shared secret itself, not a signature over the body).
func verifyGitLabToken(tokenHeader, secret string) bool {
	return verifyGitLabWithKey(tokenHeader, newVerifyingKey(secret))
}

func verifyGitLabWithKey(tokenHeader string, key verifyingKey) bool {
	if tokenHeader == "" {
		return false
	}
	// Digests, so the work does not depend on the length of either side.
	given := sha256.Sum256([]byte(tokenHeader))
	return subtle.ConstantTimeCompare(given[:], key.digest[:]) == 1
}

// handleGitLabWebhook ports router.py's gitlab_webhook.
func (d Deps) handleGitLabWebhook() http.HandlerFunc {
	key := newVerifyingKey(d.secrets().GitLab)
	return func(w http.ResponseWriter, r *http.Request) {
		// Python declares X-Gitlab-Event as a required FastAPI Header()
		// param, resolved (and 422'd) ahead of the token-checking body
		// dependency -- same confirmed-live ordering as GitHub's own two
		// headers (see github.go).
		if errs := requiredHeaderErrors(r, [2]string{"X-Gitlab-Event", "x-gitlab-event"}); len(errs) > 0 {
			policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
			return
		}
		if !verifyGitLabWithKey(r.Header.Get("X-Gitlab-Token"), key) {
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
			if writeIntLimit(w, err) {
				return
			}
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
