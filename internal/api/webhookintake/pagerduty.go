package webhookintake

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

const (
	maxWebhookBodyBytes          = 1_048_576
	maxSignatureCandidates       = 8
	replayRetentionSeconds       = 30 * 24 * 60 * 60
	pendingReplayClaimTTLSeconds = 5 * 60
	canonicalIncidentFeatureKey  = "canonical_incident_ingestion"
	featureDisabledDetail        = "Canonical incident ingestion is not enabled for this organization"
	queueUnavailableDetail       = "Webhook queue unavailable"
	invalidSignatureDetail       = "Invalid signature"
	bindingNotFoundDetail        = "Webhook binding not found"
)

// pagerdutyBinding is the row this route reads (id, status, secret) --
// receivable_by_id's scope: active/candidate/ready only, a revoked or
// deleted binding answers 404 exactly like one that never existed. Binding
// admin CRUD (create/rotate/activate/revoke) is CHAOS-6255, not this ticket.
type pagerdutyBinding struct {
	ID                     string
	OrgID                  string
	Status                 string
	ProviderSubscriptionID string
	SigningSecretEncrypted string
}

func loadReceivableBinding(ctx context.Context, pool *pgxpool.Pool, bindingID string) (pagerdutyBinding, error) {
	var binding pagerdutyBinding
	err := pool.QueryRow(ctx, `
SELECT id::text, org_id::text, status, provider_subscription_id, signing_secret_encrypted
FROM public.pagerduty_webhook_bindings
WHERE id = $1::uuid AND status IN ('active', 'candidate', 'ready')`, bindingID,
	).Scan(&binding.ID, &binding.OrgID, &binding.Status, &binding.ProviderSubscriptionID, &binding.SigningSecretEncrypted)
	if errors.Is(err, pgx.ErrNoRows) {
		return pagerdutyBinding{}, errBindingNotFound
	}
	if err != nil {
		return pagerdutyBinding{}, err
	}
	return binding, nil
}

var errBindingNotFound = errors.New("pagerduty webhook binding not found")

// markCandidateReadyFromVerifiedPing ports
// PagerDutyWebhookBindingService.mark_candidate_ready_from_verified_ping:
// advance a candidate binding to "ready" once its receiver verifies one
// pagey.ping. A no-op (not an error) for a binding already past candidate.
func markCandidateReadyFromVerifiedPing(ctx context.Context, pool *pgxpool.Pool, bindingID, orgID string) error {
	command, err := pool.Exec(ctx, `
UPDATE public.pagerduty_webhook_bindings
SET status = 'ready', updated_at = now()
WHERE id = $1::uuid AND org_id = $2::uuid AND status = 'candidate'`, bindingID, orgID)
	if err != nil {
		return err
	}
	if command.RowsAffected() == 0 {
		// Either already ready/active (no-op, matches Python returning the
		// unchanged row) or genuinely absent -- loadReceivableBinding already
		// proved the row exists moments earlier, so treat this as a benign
		// race rather than a hard failure.
		return nil
	}
	return nil
}

func verifyPagerDutySignature(body []byte, header string, secret []byte) bool {
	if header == "" {
		return false
	}
	candidates := strings.Split(header, ",")
	if len(candidates) > maxSignatureCandidates {
		return false
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	valid := false
	for _, candidate := range candidates {
		trimmed := strings.TrimSpace(candidate)
		if !strings.HasPrefix(trimmed, "v1=") {
			continue
		}
		signature := strings.TrimPrefix(trimmed, "v1=")
		if len(signature) != 64 || !isHex(signature) {
			continue
		}
		if hmac.Equal([]byte(expected), []byte(strings.ToLower(signature))) {
			valid = true
		}
	}
	return valid
}

func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}

func pagerdutyStreamName(bindingID string) string {
	return "pagerduty-webhooks:" + bindingID
}

func pagerdutyReplayKey(bindingID, replayIdentity string) string {
	sum := sha256.Sum256([]byte(replayIdentity))
	return "pagerduty-webhook-replay:" + bindingID + ":" + hex.EncodeToString(sum[:])
}

func pagerdutyReplayIdentity(providerSubscriptionID, eventID string, body []byte) string {
	identity := strings.TrimSpace(eventID)
	if identity == "" {
		sum := sha256.Sum256(body)
		identity = hex.EncodeToString(sum[:])
	}
	return providerSubscriptionID + "\x1f" + identity
}

type replayClaimOutcome int

const (
	replayClaimed replayClaimOutcome = iota
	replayPending
	replayReplayed
)

func claimDelivery(ctx context.Context, client valkeygo.Client, bindingID, providerSubscriptionID, eventID string, body []byte) (replayClaimOutcome, error) {
	if client == nil {
		return 0, errQueueUnavailable
	}
	replayKey := pagerdutyReplayKey(bindingID, pagerdutyReplayIdentity(providerSubscriptionID, eventID, body))
	bodyHash := sha256.Sum256(body)
	bodyHashHex := hex.EncodeToString(bodyHash[:])
	pendingState := "pending:" + bodyHashHex
	claimed, err := client.Do(ctx, client.B().Set().Key(replayKey).Value(pendingState).
		Nx().Ex(pendingReplayClaimTTLSeconds*time.Second).Build()).ToString()
	if err == nil && claimed == "OK" {
		return replayClaimed, nil
	}
	if err != nil && !valkeygo.IsValkeyNil(err) {
		return 0, fmt.Errorf("%w: %v", errQueueUnavailable, err)
	}
	stored, err := client.Do(ctx, client.B().Get().Key(replayKey).Build()).ToString()
	if err != nil {
		return 0, fmt.Errorf("%w: %v", errQueueUnavailable, err)
	}
	acceptedState := "accepted:" + bodyHashHex
	switch stored {
	case acceptedState:
		return replayReplayed, nil
	case pendingState:
		return replayPending, nil
	default:
		return 0, errReplayBodyConflict
	}
}

var errQueueUnavailable = errors.New("webhook queue unavailable")
var errReplayBodyConflict = errors.New("webhook delivery body conflict")

func releaseReplayClaim(ctx context.Context, client valkeygo.Client, bindingID, providerSubscriptionID, eventID string, body []byte) {
	if client == nil {
		return
	}
	replayKey := pagerdutyReplayKey(bindingID, pagerdutyReplayIdentity(providerSubscriptionID, eventID, body))
	_ = client.Do(ctx, client.B().Del().Key(replayKey).Build()).Error()
}

// promoteReplayClaimLua ports pagerduty.py's _PROMOTE_REPLAY_CLAIM_SCRIPT
// exactly: same compare-and-swap shape, same key, so a replayed delivery
// gives the identical outcome on both planes (team-lead's oracle condition).
const promoteReplayClaimLua = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3], 'XX')
end
return false
`

func acceptReplayClaim(ctx context.Context, client valkeygo.Client, bindingID, providerSubscriptionID, eventID string, body []byte) error {
	if client == nil {
		return errQueueUnavailable
	}
	bodyHash := sha256.Sum256(body)
	bodyHashHex := hex.EncodeToString(bodyHash[:])
	replayKey := pagerdutyReplayKey(bindingID, pagerdutyReplayIdentity(providerSubscriptionID, eventID, body))
	result := valkeygo.NewLuaScriptNoSha(promoteReplayClaimLua).Exec(ctx, client,
		[]string{replayKey},
		[]string{"pending:" + bodyHashHex, "accepted:" + bodyHashHex, strconv.Itoa(replayRetentionSeconds)},
	)
	accepted, err := result.ToString()
	if err != nil && !valkeygo.IsValkeyNil(err) {
		return fmt.Errorf("%w: %v", errQueueUnavailable, err)
	}
	if accepted != "OK" {
		return errQueueUnavailable
	}
	return nil
}

func enqueuePagerDutyEvent(ctx context.Context, client valkeygo.Client, bindingID string, webhook pagerDutyV3Webhook, rawBodySHA256 string, now time.Time) (string, error) {
	if client == nil {
		return "", errQueueUnavailable
	}
	payloadJSON, err := webhook.marshalCanonical()
	if err != nil {
		return "", err
	}
	fields := map[string]string{
		"binding_id":      bindingID,
		"event_id":        webhook.Event.ID,
		"event_type":      webhook.Event.EventType,
		"occurred_at":     pythonIsoformat(webhook.Event.OccurredAt),
		"received_at":     pythonIsoformat(now),
		"raw_body_sha256": rawBodySHA256,
		"payload":         string(payloadJSON),
	}
	command := client.B().Xadd().Key(pagerdutyStreamName(bindingID)).
		Maxlen().Almost().Threshold(strconv.Itoa(receiverStreamMaxlen)).Id("*").FieldValue()
	for key, value := range fields {
		command = command.FieldValue(key, value)
	}
	id, err := client.Do(ctx, command.Build()).ToString()
	if err != nil {
		return "", fmt.Errorf("%w: %v", errQueueUnavailable, err)
	}
	return id, nil
}

const receiverStreamMaxlen = 10_000

// pythonMicrosecondFraction is Python's datetime.isoformat()/model_dump_json()
// fractional-second text: OMITTED when microsecond is exactly zero, else
// ALWAYS exactly 6 digits, zero-padded, never trailing-zero-trimmed.
// Go's ".999999" format layout trims trailing zeros (123400us -> ".1234"),
// which Python's own formatting never does (123400us -> ".123400") --
// confirmed against the live interpreter:
// `datetime(2026,1,1,tzinfo=UTC).replace(microsecond=123400).isoformat()`
// returns "...+00:00" with ".123400", not ".1234". The earlier oracle run's
// fixtures all carried zero microseconds, so this never surfaced there.
func pythonMicrosecondFraction(instant time.Time) string {
	microseconds := instant.Nanosecond() / 1000
	if microseconds == 0 {
		return ""
	}
	return fmt.Sprintf(".%06d", microseconds)
}

// pythonIsoformat renders instant like Python's datetime.isoformat() on a
// UTC-aware datetime (pagerduty.py's _enqueue_event: occurred_at.astimezone
// (UTC).isoformat(), received_at = datetime.now(UTC).isoformat()): the
// offset is always the numeric "+00:00" and never a "Z" suffix.
// VENUE-ORACLE-CAUGHT: the previous time.RFC3339 formatting rendered "Z" for
// a UTC offset, which time.Parse (internal/jobs/pagerduty/stream.go's own
// consumer) accepts either way, but which failed byte-for-byte against the
// real Python stream entry.
func pythonIsoformat(instant time.Time) string {
	utc := instant.UTC()
	return utc.Format("2006-01-02T15:04:05") + pythonMicrosecondFraction(utc) + "+00:00"
}

// pagerDutyV3Webhook is pagerduty_models.py's PagerDutyV3Webhook: a frozen,
// extra="ignore" model with exactly one field (event). model_dump_json()
// therefore reserializes ONLY {"event": {"id", "event_type", "occurred_at",
// "data"}} -- any other top-level or event-level key in the raw request is
// dropped, and "data" is carried through structurally as received (it is
// declared dict[str, JsonValue], not reshaped).
//
// occurred_at's re-serialized text inside "payload" (pydantic's own
// datetime-to-JSON formatting) is a "Z"-suffixed UTC instant, with the
// fractional-second text omitted at zero microseconds and otherwise a fixed
// 6 digits (pythonMicrosecondFraction) -- confirmed byte-for-byte against
// the real Python stream entry by the venue oracle
// (TestWebhookIntakeVenueOraclePagerDuty) for a whole-second UTC-offset
// input, which is what every real PagerDuty v3 webhook sends; the
// fractional-second fix itself is proven by unit test
// (TestPythonMicrosecondFractionMatchesPythonIsoformat) and adversarial
// review's own executed repro against the live interpreter, not yet by an
// extended oracle fixture carrying nonzero microseconds. NAMED LIMIT: a
// non-UTC offset input is normalized to UTC here before re-serializing
// (time.Parse then .UTC()); whether pydantic instead preserves the original
// offset on such an input is unverified -- the oracle's fixture never
// exercises one.
type pagerDutyV3Webhook struct {
	Event pagerDutyEvent
	data  pyjson.Value
}

type pagerDutyEvent struct {
	ID         string
	EventType  string
	OccurredAt time.Time
}

func (w pagerDutyV3Webhook) marshalCanonical() ([]byte, error) {
	event := pyjson.NewObject()
	event.Set("id", w.Event.ID)
	event.Set("event_type", w.Event.EventType)
	occurredAtUTC := w.Event.OccurredAt.UTC()
	event.Set("occurred_at", occurredAtUTC.Format("2006-01-02T15:04:05")+pythonMicrosecondFraction(occurredAtUTC)+"Z")
	if w.data != nil {
		event.Set("data", w.data)
	} else {
		event.Set("data", pyjson.NewObject())
	}
	envelope := pyjson.NewObject()
	envelope.Set("event", event)
	return pyjson.Marshal(envelope)
}

// pagerDutyEventTypes mirrors pagerduty_models.py's PagerDutyEventType
// StrEnum plus the "pagey.ping" Literal pagerDutyEvent.event_type accepts
// alongside it (event_type: PagerDutyEventType | Literal["pagey.ping"]).
// VENUE-ORACLE-CAUGHT: parsePagerDutyWebhook previously accepted ANY
// non-empty event_type string -- Python's pydantic model rejects an unknown
// one with a 400, so Go was silently accepting and enqueuing a malformed
// event the Python original refuses.
var pagerDutyEventTypes = map[string]bool{
	"pagey.ping":                       true,
	"incident.triggered":               true,
	"incident.acknowledged":            true,
	"incident.unacknowledged":          true,
	"incident.escalated":               true,
	"incident.reassigned":              true,
	"incident.delegated":               true,
	"incident.priority_updated":        true,
	"incident.resolved":                true,
	"incident.reopened":                true,
	"incident.annotated":               true,
	"incident.responder.added":         true,
	"incident.responder.replied":       true,
	"incident.service_updated":         true,
	"incident.status_update_published": true,
	"service.created":                  true,
	"service.deleted":                  true,
	"service.updated":                  true,
}

func parsePagerDutyWebhook(body []byte) (pagerDutyV3Webhook, error) {
	value, err := decodeJSONBody(body)
	if err != nil {
		return pagerDutyV3Webhook{}, errMalformedJSON
	}
	object, _ := value.(*pyjson.Object)
	if object == nil {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	eventObject := objectField(object, "event")
	if eventObject == nil {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	// id: str = Field(max_length=512) in Python -- REQUIRED (no default) but
	// carries no min_length, so an empty string is a VALID id once present;
	// _replay_identity (pagerduty.py) explicitly falls back to a body hash
	// for exactly that case. stringField alone can't tell "absent" from
	// "present as \"\"", which is why the earlier id == "" check rejected a
	// well-formed empty-id event Python accepts -- Get's own ok distinguishes
	// them.
	idValue, idPresent := eventObject.Get("id")
	id, idIsString := idValue.(string)
	if !idPresent || !idIsString || len(id) > 512 {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	eventType := stringField(eventObject, "event_type")
	if !pagerDutyEventTypes[eventType] {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	// occurred_at: datetime is pydantic's lax datetime (an ISO string, a unix
	// timestamp as a number or numeric string), then the model's validator
	// refuses a naive value.
	occurredAtRaw, _ := eventObject.Get("occurred_at")
	occurredAtParsed, failure := pytime.ParseDatetime(pydanticDatetimeInput(occurredAtRaw))
	if failure != nil || !occurredAtParsed.Aware {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	occurredAt := occurredAtParsed.Time
	// data: dict[str, JsonValue] is REQUIRED in Python (no default) and must
	// be a JSON object -- objectField returns nil for both "absent" and
	// "present but not an object", exactly pydantic's rejection surface.
	dataObject := objectField(eventObject, "data")
	if dataObject == nil {
		return pagerDutyV3Webhook{}, errInvalidEvent
	}
	data, _ := eventObject.Get("data")
	return pagerDutyV3Webhook{
		Event: pagerDutyEvent{ID: id, EventType: eventType, OccurredAt: occurredAt.UTC()},
		data:  data,
	}, nil
}

// pydanticDatetimeInput maps a decoded JSON value onto the shapes
// pytime.ParseDatetime reads: a string, a float64, an exact integer; anything
// else (bool, null, array, object) reaches it as itself and is refused as
// datetime_type.
func pydanticDatetimeInput(value pyjson.Value) any {
	switch typed := value.(type) {
	case pyjson.Int:
		return typed.Int
	case pyjson.Float:
		return float64(typed)
	}
	return value
}

// Exact casing pinned to pagerduty.py's own HTTPException detail strings
// ("Malformed JSON", "Invalid PagerDuty V3 event") -- these become the
// {"detail": ...} response body verbatim (err.Error() at the call site).
var errMalformedJSON = errors.New("Malformed JSON")
var errInvalidEvent = errors.New("Invalid PagerDuty V3 event")

func readBodyLimited(r *http.Request) ([]byte, *pagerdutyHTTPError) {
	if raw := r.Header.Get("Content-Length"); raw != "" {
		declared, err := strconv.Atoi(raw)
		if err != nil {
			return nil, &pagerdutyHTTPError{http.StatusBadRequest, "Invalid Content-Length"}
		}
		if declared < 0 || declared > maxWebhookBodyBytes {
			return nil, &pagerdutyHTTPError{http.StatusRequestEntityTooLarge, "Payload too large"}
		}
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxWebhookBodyBytes+1))
	if err != nil {
		return nil, &pagerdutyHTTPError{http.StatusBadRequest, "Invalid request body"}
	}
	if len(body) > maxWebhookBodyBytes {
		return nil, &pagerdutyHTTPError{http.StatusRequestEntityTooLarge, "Payload too large"}
	}
	return body, nil
}

type pagerdutyHTTPError struct {
	Status int
	Detail string
}

// handlePagerDutyWebhook ports pagerduty.py's pagerduty_webhook end to end.
func (d Deps) handlePagerDutyWebhook() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !d.limiters.pagerduty.Allow(forwardedIP(r)) {
			policy.WriteDetail(w, http.StatusTooManyRequests, policy.ErrorDetail("Rate limit exceeded. Please try again later."), nil)
			return
		}
		bindingID := r.PathValue("binding_id")
		parsed, err := uuid.Parse(bindingID)
		if err != nil {
			policy.WriteDetail(w, http.StatusNotFound, bindingNotFoundDetail, nil)
			return
		}
		canonicalBindingID := parsed.String()
		subscriptionHeader := r.Header.Get("X-Webhook-Subscription")
		if subscriptionHeader == "" {
			policy.WriteDetail(w, http.StatusUnauthorized, invalidSignatureDetail, nil)
			return
		}
		if d.Pool == nil {
			policy.WriteDetail(w, http.StatusInternalServerError, "webhook store is unavailable", nil)
			return
		}
		binding, err := loadReceivableBinding(r.Context(), d.Pool, canonicalBindingID)
		if errors.Is(err, errBindingNotFound) {
			policy.WriteDetail(w, http.StatusNotFound, bindingNotFoundDetail, nil)
			return
		}
		if err != nil {
			d.logger().Error("pagerduty_webhook load binding failed", "binding_id", canonicalBindingID, "error", err)
			policy.WriteDetail(w, http.StatusInternalServerError, "webhook store is unavailable", nil)
			return
		}
		if binding.ID != canonicalBindingID || subscriptionHeader != binding.ProviderSubscriptionID {
			d.logger().Warn("pagerduty_webhook.audit rejected", "binding_id", canonicalBindingID, "reason", "subscription_mismatch")
			policy.WriteDetail(w, http.StatusUnauthorized, invalidSignatureDetail, nil)
			return
		}
		body, httpErr := readBodyLimited(r)
		if httpErr != nil {
			policy.WriteDetail(w, httpErr.Status, httpErr.Detail, nil)
			return
		}
		secretBytes, err := d.Decryptor.Decrypt(secrets.NewValue(binding.SigningSecretEncrypted))
		if err != nil {
			d.logger().Error("pagerduty_webhook decrypt binding secret failed", "binding_id", canonicalBindingID, "error", err)
			policy.WriteDetail(w, http.StatusInternalServerError, "webhook store is unavailable", nil)
			return
		}
		if !verifyPagerDutySignature(body, r.Header.Get("X-PagerDuty-Signature"), secretBytes) {
			policy.WriteDetail(w, http.StatusUnauthorized, invalidSignatureDetail, nil)
			return
		}
		webhook, err := parsePagerDutyWebhook(body)
		if err != nil {
			status := http.StatusBadRequest
			policy.WriteDetail(w, status, err.Error(), nil)
			return
		}

		switch binding.Status {
		case "active":
			if webhook.Event.EventType == "pagey.ping" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
		case "candidate", "ready":
			if webhook.Event.EventType == "pagey.ping" {
				decision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), binding.OrgID, canonicalIncidentFeatureKey)
				if err != nil || !decision.Allowed {
					policy.WriteDetail(w, http.StatusForbidden, featureDisabledDetail, nil)
					return
				}
				if err := markCandidateReadyFromVerifiedPing(r.Context(), d.Pool, canonicalBindingID, binding.OrgID); err != nil {
					d.logger().Error("pagerduty_webhook mark candidate ready failed", "binding_id", canonicalBindingID, "error", err)
					policy.WriteDetail(w, http.StatusInternalServerError, "webhook store is unavailable", nil)
					return
				}
				w.WriteHeader(http.StatusNoContent)
				return
			}
			d.logger().Warn("pagerduty_webhook.audit rejected", "binding_id", canonicalBindingID, "reason", "candidate_event")
			policy.WriteDetail(w, http.StatusForbidden, "Candidate webhook binding accepts only pagey.ping", nil)
			return
		default:
			policy.WriteDetail(w, http.StatusNotFound, bindingNotFoundDetail, nil)
			return
		}

		decision, err := (licensing.PostgresStore{Pool: d.Pool}).Decide(r.Context(), binding.OrgID, canonicalIncidentFeatureKey)
		if err != nil || !decision.Allowed {
			policy.WriteDetail(w, http.StatusForbidden, featureDisabledDetail, nil)
			return
		}

		outcome, err := claimDelivery(r.Context(), d.Valkey, canonicalBindingID, binding.ProviderSubscriptionID, webhook.Event.ID, body)
		if errors.Is(err, errReplayBodyConflict) {
			d.logger().Warn("pagerduty_webhook.audit rejected", "binding_id", canonicalBindingID, "reason", "replay_body_conflict")
			policy.WriteDetail(w, http.StatusConflict, "Webhook delivery body conflict", nil)
			return
		}
		if err != nil {
			policy.WriteDetail(w, http.StatusServiceUnavailable, queueUnavailableDetail, nil)
			return
		}
		switch outcome {
		case replayReplayed:
			d.logger().Info("pagerduty_webhook.audit replayed", "binding_id", canonicalBindingID, "event_id", webhook.Event.ID)
			w.WriteHeader(http.StatusAccepted)
			return
		case replayPending:
			policy.WriteDetail(w, http.StatusServiceUnavailable, queueUnavailableDetail, nil)
			return
		}

		streamEntryID, err := enqueuePagerDutyEvent(r.Context(), d.Valkey, canonicalBindingID, webhook, sha256Hex(body), d.now())
		if err != nil {
			releaseReplayClaim(r.Context(), d.Valkey, canonicalBindingID, binding.ProviderSubscriptionID, webhook.Event.ID, body)
			policy.WriteDetail(w, http.StatusServiceUnavailable, queueUnavailableDetail, nil)
			return
		}
		d.logger().Info("pagerduty_webhook.enqueued", "binding_id", canonicalBindingID, "event_id", webhook.Event.ID, "stream_entry_id", streamEntryID)

		if err := acceptReplayClaim(r.Context(), d.Valkey, canonicalBindingID, binding.ProviderSubscriptionID, webhook.Event.ID, body); err != nil {
			policy.WriteDetail(w, http.StatusServiceUnavailable, queueUnavailableDetail, nil)
			return
		}
		d.logger().Info("pagerduty_webhook.audit accepted", "binding_id", canonicalBindingID, "event_id", webhook.Event.ID)
		w.WriteHeader(http.StatusAccepted)
	}
}

func sha256Hex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
