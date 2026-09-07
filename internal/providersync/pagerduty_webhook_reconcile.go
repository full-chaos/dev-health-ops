package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PagerDuty webhook reconciliation, ported from Python's
// providers/pagerduty/webhooks.py reconcile_pagerduty_webhook (CHAOS-4105).
//
// It lives in this package, not in internal/jobs/pagerduty, because the row
// types, normalizers and ordering derivations it must reuse are unexported
// here. Duplicating them next to the stream handler would have produced a
// second, independently-drifting definition of the same eight operational_*
// tables -- exactly what the pull side already owns and what the eleven
// live-Python oracle pairs already pin.
//
// The port is deliberately thin: every field value comes from the SAME
// normalizePagerDuty* function the pull sync uses, so a webhook-sourced row
// and a pull-sourced row for one entity share an id and merge in ClickHouse
// rather than racing as two rows. Only three behaviours are webhook-specific,
// and each is a documented delta from the pull path:
//
//  1. the _versioned() override -- the webhook's occurred_at and event id
//     replace the payload-derived source version/event fields, and
//     received_at replaces the sync clock;
//  2. the service.deleted tombstone, which marks the row deleted AT the event
//     time rather than bumping past it the way a snapshot tombstone must;
//  3. the REST hydration fallback for a webhook payload too sparse to
//     normalize.

// PagerDutyWebhookEvent is one delivered v3 webhook, already authenticated,
// admitted and receipt-claimed by the stream handler.
type PagerDutyWebhookEvent struct {
	EventID    string
	EventType  string
	OccurredAt time.Time
	ReceivedAt time.Time
	// Data is the webhook's event.data object verbatim.
	Data json.RawMessage
}

// PagerDutyIncidentHydrator fills in an incident whose webhook payload is too
// sparse to normalize. It mirrors Python's PagerDutyClient.get_incident and
// returns the raw API object so this file keeps exactly one incident-shaped
// decode path.
type PagerDutyIncidentHydrator interface {
	HydrateIncident(ctx context.Context, incidentID string) (json.RawMessage, error)
}

// PagerDutyWebhookSink is the one method this file needs from an effect
// writer. It is an interface rather than the concrete sink types so a test
// can record what would be written without standing up ClickHouse; the
// production wiring passes the very same PagerDuty*ClickHouseEffects values
// cmd/dev-health-worker builds for the pull route.
type PagerDutyWebhookSink interface {
	WriteEffect(context.Context, Claim, EffectBatch) error
}

// PagerDutyWebhookSinks are the already-native effect writers a webhook
// reuses, constructed from the same ClickHouse connection as the pull route.
type PagerDutyWebhookSinks struct {
	IncidentFamily PagerDutyWebhookSink
	Services       PagerDutyWebhookSink
	Users          PagerDutyWebhookSink
	// Responders is deliberately its own sink rather than a fifth
	// destination on IncidentFamily -- see
	// pagerduty_incident_responders.go for why widening the family sink
	// was the wrong shape.
	Responders PagerDutyWebhookSink
}

// PagerDutyWebhookWrite reports one committed destination write so the caller
// can log rows-per-table without re-deriving them.
type PagerDutyWebhookWrite struct {
	Destination string
	Rows        int
}

// PagerDutyWebhookOutcome is the observable result of one reconciliation.
type PagerDutyWebhookOutcome struct {
	Writes   []PagerDutyWebhookWrite
	Hydrated bool
}

// Rows totals every row committed across destinations.
func (outcome PagerDutyWebhookOutcome) Rows() int {
	total := 0
	for _, write := range outcome.Writes {
		total += write.Rows
	}
	return total
}

// ErrPagerDutyWebhookUnsupported is returned for an event type this
// reconciler does not handle. It is a terminal condition, not a retryable
// one: replaying the same delivery cannot produce a different verdict.
var ErrPagerDutyWebhookUnsupported = errors.New("pagerduty webhook event type is not reconciled")

// ErrPagerDutyWebhookMalformed is returned for a payload that cannot be
// decoded or normalized. Also terminal, and the direct analogue of Python's
// pydantic ValidationError branch.
var ErrPagerDutyWebhookMalformed = errors.New("pagerduty webhook payload is malformed")

// PagerDuty v3 event types, ported verbatim from
// api/webhooks/pagerduty_models.py PagerDutyEventType. The literal strings
// are the wire contract; a rename here silently stops reconciling an event.
const (
	pagerDutyEventIncidentTriggered       = "incident.triggered"
	pagerDutyEventIncidentAcknowledged    = "incident.acknowledged"
	pagerDutyEventIncidentUnacknowledged  = "incident.unacknowledged"
	pagerDutyEventIncidentEscalated       = "incident.escalated"
	pagerDutyEventIncidentReassigned      = "incident.reassigned"
	pagerDutyEventIncidentDelegated       = "incident.delegated"
	pagerDutyEventIncidentPriorityUpdated = "incident.priority_updated"
	pagerDutyEventIncidentResolved        = "incident.resolved"
	pagerDutyEventIncidentReopened        = "incident.reopened"
	pagerDutyEventIncidentAnnotated       = "incident.annotated"
	pagerDutyEventResponderAdded          = "incident.responder.added"
	pagerDutyEventResponderReplied        = "incident.responder.replied"
	pagerDutyEventIncidentServiceUpdated  = "incident.service_updated"
	pagerDutyEventStatusUpdatePublished   = "incident.status_update_published"
	pagerDutyEventServiceCreated          = "service.created"
	pagerDutyEventServiceDeleted          = "service.deleted"
	pagerDutyEventServiceUpdated          = "service.updated"
)

// pagerDutyIncidentFamilyEvents are the event types whose data object carries
// (or nests) an incident and therefore always write an incident row first.
// Ported from the single combined case arm in webhooks.py.
func pagerDutyIncidentFamilyEvent(eventType string) bool {
	switch eventType {
	case pagerDutyEventIncidentTriggered, pagerDutyEventIncidentAcknowledged,
		pagerDutyEventIncidentUnacknowledged, pagerDutyEventIncidentEscalated,
		pagerDutyEventIncidentReassigned, pagerDutyEventIncidentDelegated,
		pagerDutyEventIncidentPriorityUpdated, pagerDutyEventIncidentResolved,
		pagerDutyEventIncidentReopened, pagerDutyEventIncidentAnnotated,
		pagerDutyEventResponderAdded, pagerDutyEventResponderReplied,
		pagerDutyEventStatusUpdatePublished, pagerDutyEventIncidentServiceUpdated:
		return true
	}
	return false
}

// PagerDutyWebhookEventTypes lists every event type this reconciler handles,
// so the stream handler can reject an unknown type before it takes a receipt
// claim rather than after.
func PagerDutyWebhookEventTypes() []string {
	return []string{
		pagerDutyEventIncidentTriggered, pagerDutyEventIncidentAcknowledged,
		pagerDutyEventIncidentUnacknowledged, pagerDutyEventIncidentEscalated,
		pagerDutyEventIncidentReassigned, pagerDutyEventIncidentDelegated,
		pagerDutyEventIncidentPriorityUpdated, pagerDutyEventIncidentResolved,
		pagerDutyEventIncidentReopened, pagerDutyEventIncidentAnnotated,
		pagerDutyEventResponderAdded, pagerDutyEventResponderReplied,
		pagerDutyEventIncidentServiceUpdated, pagerDutyEventStatusUpdatePublished,
		pagerDutyEventServiceCreated, pagerDutyEventServiceDeleted,
		pagerDutyEventServiceUpdated,
	}
}

// ReconcilePagerDutyWebhook writes the canonical rows one webhook implies.
//
// claim is the webhook's synthetic sync claim: the caller mints it from the
// locked binding graph and fences it with the receipt store, so the effect
// sinks' tenant/lease checks apply to a webhook exactly as they apply to a
// scheduled unit. Its Dataset is overwritten per destination, because the
// sinks pair one dataset with one table on purpose.
func ReconcilePagerDutyWebhook(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	sinks PagerDutyWebhookSinks,
	hydrator PagerDutyIncidentHydrator,
) (PagerDutyWebhookOutcome, error) {
	if ctx == nil || len(event.Data) == 0 ||
		event.OccurredAt.IsZero() || event.ReceivedAt.IsZero() ||
		strings.TrimSpace(event.EventID) == "" ||
		strings.TrimSpace(providerInstance) == "" {
		return PagerDutyWebhookOutcome{}, ErrPagerDutyWebhookMalformed
	}
	occurredAt := event.OccurredAt.UTC().Truncate(time.Microsecond)
	receivedAt := event.ReceivedAt.UTC().Truncate(time.Microsecond)
	switch {
	case event.EventType == pagerDutyEventServiceCreated ||
		event.EventType == pagerDutyEventServiceUpdated ||
		event.EventType == pagerDutyEventServiceDeleted:
		return reconcilePagerDutyServiceEvent(
			ctx, claim, providerInstance, event, occurredAt, receivedAt, sinks,
		)
	case pagerDutyIncidentFamilyEvent(event.EventType):
		return reconcilePagerDutyIncidentEvent(
			ctx, claim, providerInstance, event, occurredAt, receivedAt, sinks, hydrator,
		)
	default:
		return PagerDutyWebhookOutcome{}, fmt.Errorf(
			"%w: %q", ErrPagerDutyWebhookUnsupported, event.EventType,
		)
	}
}

// reconcilePagerDutyServiceEvent handles the three service.* events. Their
// data object IS the service, unlike the incident family's nesting.
func reconcilePagerDutyServiceEvent(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	occurredAt, receivedAt time.Time,
	sinks PagerDutyWebhookSinks,
) (PagerDutyWebhookOutcome, error) {
	var payload pagerDutyServicePayload
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	serviceClaim, err := pagerDutyWebhookClaim(claim, "services")
	if err != nil {
		return PagerDutyWebhookOutcome{}, err
	}
	row, err := normalizePagerDutyService(serviceClaim, providerInstance, payload, receivedAt)
	if err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	pagerDutyWebhookVersionService(&row, event.EventID, occurredAt, receivedAt)
	if event.EventType == pagerDutyEventServiceDeleted {
		// Python's _tombstone_service: deleted AT the event's own version, not
		// one microsecond past it. The snapshot tombstone
		// (pagerDutyServiceTombstone) deliberately bumps the version to win a
		// ReplacingMergeTree race against a row it is reconciling away; a
		// webhook is not reconciling a snapshot and must not move the clock.
		row.IsDeleted = true
		deletedAt := row.SourceVersionAt
		row.DeletedAt = &deletedAt
	}
	if err := fillPagerDutyServiceOrdering(&row); err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	if err := pagerDutyWebhookWrite(
		ctx, sinks.Services, serviceClaim, "operational_services", []pagerDutyServiceRow{row},
	); err != nil {
		return PagerDutyWebhookOutcome{}, err
	}
	return PagerDutyWebhookOutcome{
		Writes: []PagerDutyWebhookWrite{{Destination: "operational_services", Rows: 1}},
	}, nil
}

// reconcilePagerDutyIncidentEvent handles the fourteen incident-family
// events: always an incident row, plus a note, a user and responder pair, or
// a timeline event depending on the type.
func reconcilePagerDutyIncidentEvent(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	occurredAt, receivedAt time.Time,
	sinks PagerDutyWebhookSinks,
	hydrator PagerDutyIncidentHydrator,
) (PagerDutyWebhookOutcome, error) {
	incidentData := pagerDutyWebhookIncidentData(event.Data)
	var payload pagerDutyIncidentPayload
	if err := json.Unmarshal(incidentData, &payload); err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	outcome := PagerDutyWebhookOutcome{}
	if pagerDutyWebhookNeedsHydration(payload) {
		if hydrator == nil {
			return PagerDutyWebhookOutcome{}, fmt.Errorf(
				"%w: incident %q needs hydration and no client is configured",
				ErrPagerDutyWebhookMalformed, payload.ID,
			)
		}
		hydrated, err := hydrator.HydrateIncident(ctx, payload.ID)
		if err != nil {
			// A hydration failure is TRANSIENT: the REST call can succeed on
			// the next delivery. Returning it unwrapped keeps it out of the
			// malformed/unsupported terminal classes.
			return PagerDutyWebhookOutcome{}, fmt.Errorf("hydrate pagerduty incident: %w", err)
		}
		payload = pagerDutyIncidentPayload{}
		if err := json.Unmarshal(hydrated, &payload); err != nil {
			return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
		}
		outcome.Hydrated = true
	}
	incidentClaim, err := pagerDutyWebhookClaim(claim, "incidents")
	if err != nil {
		return PagerDutyWebhookOutcome{}, err
	}
	incident, err := normalizePagerDutyIncident(incidentClaim, providerInstance, payload, receivedAt)
	if err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	pagerDutyWebhookVersionIncident(&incident, event.EventID, occurredAt, receivedAt)
	if err := fillPagerDutyIncidentOrdering(&incident); err != nil {
		return PagerDutyWebhookOutcome{}, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	if err := pagerDutyWebhookWrite(
		ctx, sinks.IncidentFamily, incidentClaim, "operational_incidents",
		[]pagerDutyIncidentRow{incident},
	); err != nil {
		return PagerDutyWebhookOutcome{}, err
	}
	outcome.Writes = append(outcome.Writes, PagerDutyWebhookWrite{
		Destination: "operational_incidents", Rows: 1,
	})

	switch event.EventType {
	case pagerDutyEventIncidentAnnotated:
		writes, err := pagerDutyWebhookWriteNote(
			ctx, claim, providerInstance, event, occurredAt, receivedAt, incident.ID, sinks,
		)
		if err != nil {
			return PagerDutyWebhookOutcome{}, err
		}
		outcome.Writes = append(outcome.Writes, writes...)
	case pagerDutyEventResponderAdded, pagerDutyEventResponderReplied:
		writes, err := pagerDutyWebhookWriteResponder(
			ctx, claim, providerInstance, event, occurredAt, receivedAt, incident.ID, sinks,
		)
		if err != nil {
			return PagerDutyWebhookOutcome{}, err
		}
		outcome.Writes = append(outcome.Writes, writes...)
	case pagerDutyEventStatusUpdatePublished:
		writes, err := pagerDutyWebhookWriteStatusUpdate(
			ctx, claim, providerInstance, event, occurredAt, receivedAt, incident.ID, sinks,
		)
		if err != nil {
			return PagerDutyWebhookOutcome{}, err
		}
		outcome.Writes = append(outcome.Writes, writes...)
	}
	return outcome, nil
}

func pagerDutyWebhookWriteNote(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	occurredAt, receivedAt time.Time,
	incidentID string,
	sinks PagerDutyWebhookSinks,
) ([]PagerDutyWebhookWrite, error) {
	var payload pagerDutyNotePayload
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	noteClaim, err := pagerDutyWebhookClaim(claim, "incident-notes")
	if err != nil {
		return nil, err
	}
	row, err := normalizePagerDutyNote(noteClaim, providerInstance, payload, incidentID, receivedAt)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	pagerDutyWebhookVersionNote(&row, event.EventID, occurredAt, receivedAt)
	if err := fillPagerDutyNoteOrdering(&row); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	if err := pagerDutyWebhookWrite(
		ctx, sinks.IncidentFamily, noteClaim, "operational_incident_notes",
		[]pagerDutyNoteRow{row},
	); err != nil {
		return nil, err
	}
	return []PagerDutyWebhookWrite{{Destination: "operational_incident_notes", Rows: 1}}, nil
}

// pagerDutyWebhookWriteStatusUpdate ports the STATUS_UPDATE_PUBLISHED arm.
// Python overrides the normalized log entry's event_type with the webhook's
// own event type and its occurred_at with the event time, because a status
// update is not a PagerDuty log entry and carries neither.
func pagerDutyWebhookWriteStatusUpdate(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	occurredAt, receivedAt time.Time,
	incidentID string,
	sinks PagerDutyWebhookSinks,
) ([]PagerDutyWebhookWrite, error) {
	var payload pagerDutyLogEntryPayload
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	entryClaim, err := pagerDutyWebhookClaim(claim, "incident-log-entries")
	if err != nil {
		return nil, err
	}
	row, err := normalizePagerDutyLogEntry(
		entryClaim, providerInstance, payload, incidentID, receivedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	pagerDutyWebhookVersionLogEntry(&row, event.EventID, occurredAt, receivedAt)
	row.EventType = pagerDutyEventStatusUpdatePublished
	entryOccurredAt := occurredAt
	row.OccurredAt = &entryOccurredAt
	if err := fillPagerDutyLogEntryOrdering(&row); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	if err := pagerDutyWebhookWrite(
		ctx, sinks.IncidentFamily, entryClaim, "operational_incident_timeline_events",
		[]pagerDutyLogEntryRow{row},
	); err != nil {
		return nil, err
	}
	return []PagerDutyWebhookWrite{
		{Destination: "operational_incident_timeline_events", Rows: 1},
	}, nil
}

// pagerDutyWebhookWriteResponder ports the RESPONDER_ADDED/REPLIED arm: an
// optional user row, then the responder row itself. Python appends the user
// first and inserts in list order, so a reader that follows user_id always
// finds the user row already written.
func pagerDutyWebhookWriteResponder(
	ctx context.Context,
	claim Claim,
	providerInstance string,
	event PagerDutyWebhookEvent,
	occurredAt, receivedAt time.Time,
	incidentID string,
	sinks PagerDutyWebhookSinks,
) ([]PagerDutyWebhookWrite, error) {
	var responder pagerDutyWebhookResponderPayload
	if err := json.Unmarshal(event.Data, &responder); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	writes := make([]PagerDutyWebhookWrite, 0, 2)
	userID := ""
	if responder.User != nil {
		userClaim, err := pagerDutyWebhookClaim(claim, "users")
		if err != nil {
			return nil, err
		}
		row, err := normalizePagerDutyUser(userClaim, providerInstance, *responder.User, receivedAt)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
		}
		pagerDutyWebhookVersionUser(&row, event.EventID, occurredAt, receivedAt)
		if err := fillPagerDutyUserOrdering(&row); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
		}
		if err := pagerDutyWebhookWrite(
			ctx, sinks.Users, userClaim, "operational_users", []pagerDutyUserRow{row},
		); err != nil {
			return nil, err
		}
		userID = row.ID
		writes = append(writes, PagerDutyWebhookWrite{Destination: "operational_users", Rows: 1})
	}
	// Python keys the responder row on the WEBHOOK event id, not on the
	// responder assignment id: a responder assignment has no stable external
	// identity across deliveries, so the event is the identity. Preserved
	// verbatim -- changing it would re-key every existing row.
	responderClaim, err := pagerDutyWebhookClaim(claim, "incidents")
	if err != nil {
		return nil, err
	}
	row := pagerDutyResponderRow{
		OrgID: responderClaim.OrgID, Provider: "pagerduty",
		ProviderInstanceID: providerInstance, SourceEntityType: "responder",
		ExternalID: event.EventID, SourceVersionAt: occurredAt,
		SourceEventAt: &occurredAt, ObservedAt: receivedAt, LastSynced: receivedAt,
		IncidentID: incidentID, ResponderName: responder.Name, Role: responder.Role,
		ResponderAssignmentID: responder.ID, RequestedAt: &occurredAt,
	}
	eventID := event.EventID
	row.SourceEventID = &eventID
	if userID != "" {
		row.UserID = &userID
	}
	if err := fillPagerDutyResponderOrdering(&row); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPagerDutyWebhookMalformed, err)
	}
	if err := pagerDutyWebhookWrite(
		ctx, sinks.Responders, responderClaim, "operational_incident_responders",
		[]pagerDutyResponderRow{row},
	); err != nil {
		return nil, err
	}
	return append(writes, PagerDutyWebhookWrite{
		Destination: "operational_incident_responders", Rows: 1,
	}), nil
}

// pagerDutyWebhookResponderPayload is the responder-shaped view of an
// incident.responder.* data object.
type pagerDutyWebhookResponderPayload struct {
	ID   *string               `json:"id"`
	Name *string               `json:"name"`
	Role *string               `json:"role"`
	User *pagerDutyUserPayload `json:"user"`
}

// pagerDutyWebhookIncidentData ports _incident_payload: the incident-family
// events nest the incident under "incident" when the data object is something
// else (a note, a responder, a status update), and ARE the incident
// otherwise.
func pagerDutyWebhookIncidentData(data json.RawMessage) json.RawMessage {
	var envelope struct {
		Incident json.RawMessage `json:"incident"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return data
	}
	trimmed := strings.TrimSpace(string(envelope.Incident))
	if trimmed == "" || trimmed == "null" || !strings.HasPrefix(trimmed, "{") || trimmed == "{}" {
		return data
	}
	return envelope.Incident
}

// pagerDutyWebhookNeedsHydration ports _needs_incident_hydration.
func pagerDutyWebhookNeedsHydration(payload pagerDutyIncidentPayload) bool {
	return payload.Title == nil || *payload.Title == "" ||
		payload.Status == nil || *payload.Status == "" ||
		payload.CreatedAt == nil || *payload.CreatedAt == ""
}

// pagerDutyWebhookClaim re-scopes the webhook's synthetic claim to one
// dataset. The sinks pair dataset with destination, and Unit.Validate pins
// CostClass to the dataset's registered capability, so both must move
// together.
func pagerDutyWebhookClaim(claim Claim, dataset string) (Claim, error) {
	capability, ok := Capability("pagerduty", dataset)
	if !ok {
		return Claim{}, ErrInvalidConfiguration
	}
	claim.Dataset = dataset
	claim.CostClass = capability.CostClass
	if err := claim.Validate(); err != nil {
		return Claim{}, err
	}
	return claim, nil
}

// pagerDutyWebhookWrite builds the one-row effect batch and commits it
// through the pull path's own sink, so tenant scope, lease fence, incident
// entitlement, row validation and the ClickHouse statement are all the
// already-proven ones.
func pagerDutyWebhookWrite[T any](
	ctx context.Context,
	sink PagerDutyWebhookSink,
	claim Claim,
	destination string,
	rows []T,
) error {
	if sink == nil {
		return ErrInvalidConfiguration
	}
	effect, err := effectBatchFromValues(destination, EffectReadbackRequired, rows)
	if err != nil {
		return err
	}
	if err := sink.WriteEffect(ctx, claim, effect); err != nil {
		return fmt.Errorf("write %s: %w", destination, err)
	}
	return nil
}

// The _versioned() port, one function per row type because the rows are
// distinct structs rather than a shared base. Each sets exactly the five
// fields webhooks.py's replace() sets, and each is followed by a re-derivation
// of the ordering fields: source_conflict_key and source_revision are computed
// FROM these values, so overriding them without re-deriving would ship a row
// whose ordering key describes the pre-override state -- which the sinks'
// validate*Rows would reject, but only after the fact.
func pagerDutyWebhookVersionIncident(
	row *pagerDutyIncidentRow, eventID string, occurredAt, receivedAt time.Time,
) {
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = occurredAt, receivedAt, receivedAt
	sourceEventAt, sourceEventID := occurredAt, eventID
	row.SourceEventAt, row.SourceEventID = &sourceEventAt, &sourceEventID
}

func pagerDutyWebhookVersionService(
	row *pagerDutyServiceRow, eventID string, occurredAt, receivedAt time.Time,
) {
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = occurredAt, receivedAt, receivedAt
	sourceEventAt, sourceEventID := occurredAt, eventID
	row.SourceEventAt, row.SourceEventID = &sourceEventAt, &sourceEventID
}

func pagerDutyWebhookVersionUser(
	row *pagerDutyUserRow, eventID string, occurredAt, receivedAt time.Time,
) {
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = occurredAt, receivedAt, receivedAt
	sourceEventAt, sourceEventID := occurredAt, eventID
	row.SourceEventAt, row.SourceEventID = &sourceEventAt, &sourceEventID
}

func pagerDutyWebhookVersionNote(
	row *pagerDutyNoteRow, eventID string, occurredAt, receivedAt time.Time,
) {
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = occurredAt, receivedAt, receivedAt
	sourceEventAt, sourceEventID := occurredAt, eventID
	row.SourceEventAt, row.SourceEventID = &sourceEventAt, &sourceEventID
}

func pagerDutyWebhookVersionLogEntry(
	row *pagerDutyLogEntryRow, eventID string, occurredAt, receivedAt time.Time,
) {
	row.SourceVersionAt, row.ObservedAt, row.LastSynced = occurredAt, receivedAt, receivedAt
	sourceEventAt, sourceEventID := occurredAt, eventID
	row.SourceEventAt, row.SourceEventID = &sourceEventAt, &sourceEventID
}
