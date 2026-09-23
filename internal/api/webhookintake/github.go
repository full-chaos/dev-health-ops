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

// verifyGitHubSignature ports auth.py's verify_github_signature: HMAC-SHA256
// over the raw body, "sha256=<hex>" in X-Hub-Signature-256.
func verifyGitHubSignature(body []byte, signatureHeader, secret string) bool {
	if signatureHeader == "" || !strings.HasPrefix(signatureHeader, "sha256=") {
		return false
	}
	expected := signatureHeader[len("sha256="):]
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	computed := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(computed), []byte(expected))
}

// handleGitHubWebhook ports router.py's github_webhook.
func (d Deps) handleGitHubWebhook() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		secret := d.secrets().GitHub
		if secret == "" {
			d.logger().Warn("GITHUB_WEBHOOK_SECRET not configured - rejecting webhook")
			policy.WriteDetail(w, http.StatusInternalServerError, "Webhook secret not configured", nil)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid request body", nil)
			return
		}
		if !verifyGitHubSignature(body, r.Header.Get("X-Hub-Signature-256"), secret) {
			d.logger().Warn("GitHub webhook signature validation failed")
			policy.WriteDetail(w, http.StatusUnauthorized, "Invalid signature", nil)
			return
		}
		// NAMED LIMIT: Python declares X-GitHub-Event/X-GitHub-Delivery as
		// required FastAPI Header() params, so a request missing either gets
		// FastAPI's own 422 parameter-validation shape before any handler
		// code runs. That shape is not ported here (a real GitHub delivery
		// always sends both); a missing header degrades to an empty string
		// instead, which still dispatches (delivery_key falls back to the
		// payload hash when DeliveryID is "").
		xGithubEvent := r.Header.Get("X-GitHub-Event")
		xGithubDelivery := r.Header.Get("X-GitHub-Delivery")

		payload, err := decodeJSONBody(body)
		if err != nil {
			d.logger().Warn("Invalid JSON in GitHub webhook", "error", err)
			policy.WriteDetail(w, http.StatusBadRequest, "Invalid JSON payload", nil)
			return
		}
		object, _ := payload.(*pyjson.Object)
		action := stringField(object, "action")
		eType := mapGithubEvent(xGithubEvent, action)
		repoObject := objectField(object, "repository")
		repoName := stringPtrField(repoObject, "full_name")
		orgObject := objectField(object, "organization")
		var orgID *string
		if orgObject != nil && orgObject.Len() > 0 {
			orgID = stringPtrField(orgObject, "login")
		} else {
			orgID = stringPtrField(objectField(repoObject, "owner"), "login")
		}
		rawEventType := xGithubEvent
		if action != "" {
			rawEventType = xGithubEvent + "." + action
		}

		event := webhookEvent{
			Provider:      "github",
			EventType:     eType,
			RawEventType:  rawEventType,
			DeliveryID:    xGithubDelivery,
			OrgID:         orgID,
			RepoName:      repoName,
			Payload:       body,
			PayloadParsed: payload,
		}
		respondForEvent(w, r, d, event, xGithubEvent)
	}
}
