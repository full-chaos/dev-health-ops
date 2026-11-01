package authapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/outboxprojection"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushlifecycle"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// PatternAcrCredentialEvents takes one acr credential lifecycle event per POST
// from the acr-api workload. It is mounted only when the embedded endpoint
// profile document holds its row.
const PatternAcrCredentialEvents = "/internal/v1/acr/credential-events"

const (
	opAcrCredentialEvent = "acr_credential_event"

	// acrEventsAudience is the audience the route's workload token carries, and
	// acrEventsAction the action its live workload row must list: the token
	// carries no action claim, so the action is registry data.
	acrEventsAudience = issueWorkloadAudience
	acrEventsAction   = "credentials.project"

	// maxAcrEventBody bounds one event.
	maxAcrEventBody = 16 << 10
)

// The route's closed refusals of a decided event.
const (
	reasonProjectionRetry    = "projection_retry"
	reasonProjectionConflict = "projection_conflict"
	reasonProjectionRefused  = "projection_refused"
)

// AcrWorkloads reads a platform workload row (*workload.Store).
type AcrWorkloads interface {
	LookupPrincipal(ctx context.Context, principal uuid.UUID) (*workload.Info, error)
}

// CredentialProjector projects one event (*outboxprojection.Engine). Its error
// is an infrastructure fault; every decision is a Result.
type CredentialProjector interface {
	Project(ctx context.Context, in outboxprojection.Input) (outboxprojection.Result, error)
}

// AcrEventDeps are the acr events route's collaborators.
type AcrEventDeps struct {
	// Workload admits the one acr-api workload subject, as the session issue
	// route admits go-api's.
	Workload  *SessionIssueWorkload
	Workloads AcrWorkloads
	Projector CredentialProjector
	Logger    *slog.Logger
	// Meter registers the outcome counter; nil uses the process meter.
	Meter metric.Meter
}

type acrEventHandlers struct {
	AcrEventDeps
	rec recorder
}

// AcrEventRoutes mounts the route when the embedded endpoint-profile document
// holds its row.
func AcrEventRoutes(deps AcrEventDeps) ([]httpapi.Route, error) {
	return acrEventRoutes(deps, authv1.AuthServiceEndpointProfiles)
}

// acrEventRoutes is AcrEventRoutes over a given document. The row must list the
// workload token class alone: this route takes no other class.
func acrEventRoutes(deps AcrEventDeps, rows []byte) ([]httpapi.Route, error) {
	if deps.Workload == nil || deps.Workloads == nil || deps.Projector == nil || deps.Logger == nil {
		return nil, errors.New("authapi: the acr events route needs a workload path, a workload store, a projector and a logger")
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	h := acrEventHandlers{AcrEventDeps: deps, rec: rec}
	if !rowListsOnlyClass(rows, PatternAcrCredentialEvents, workloadTokenClass) {
		deps.Logger.Info("auth acr events route not mounted: no endpoint-profile row", "route", PatternAcrCredentialEvents)
		return nil, nil
	}
	return []httpapi.Route{{Method: http.MethodPost, Pattern: PatternAcrCredentialEvents, Handler: http.HandlerFunc(h.acrCredentialEvent)}}, nil
}

func rowListsOnlyClass(raw []byte, pattern, class string) bool {
	var document struct {
		Rows []struct {
			Service         string   `json:"service"`
			Method          string   `json:"method"`
			Route           string   `json:"route"`
			AcceptedClasses []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return false
	}
	for _, row := range document.Rows {
		if row.Service == AccessService && row.Method == http.MethodPost && row.Route == pattern {
			return slices.Equal(row.AcceptedClasses, []string{class})
		}
	}
	return false
}

// acrEventCaller admits the acr-api workload before the body is read. Each
// refusal comes before the next read: the class from the bearer's text, the
// audience and subject from the unverified payload, then the signature, and
// then the live workload row.
func (h acrEventHandlers) acrEventCaller(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	denied := unauthorized(reasonInvalidCredentials)
	bearer, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, opAcrCredentialEvent, denied, nil)
		return uuid.Nil, false
	}
	if class, _ := classifyIssueBearer(bearer); class != issueClassWorkload {
		h.rec.refuse(w, r, opAcrCredentialEvent, denied, nil)
		return uuid.Nil, false
	}
	caller, reason, err := h.Workload.authorize(bearer)
	if err != nil {
		h.rec.refuse(w, r, opAcrCredentialEvent, unavailable(reasonDependency), err)
		return uuid.Nil, false
	}
	if reason != "" {
		h.rec.refuse(w, r, opAcrCredentialEvent, denied, nil)
		return uuid.Nil, false
	}
	row, err := h.Workloads.LookupPrincipal(r.Context(), *caller.workload)
	if err != nil && !errors.Is(err, workload.ErrNotFound) {
		h.rec.refuse(w, r, opAcrCredentialEvent, unavailable(reasonStoreUnavailable), err)
		return uuid.Nil, false
	}
	if err != nil || row.Disabled || !slices.Contains(row.Audiences, acrEventsAudience) || !slices.Contains(row.AllowedActions, acrEventsAction) {
		h.rec.refuse(w, r, opAcrCredentialEvent, refusal{http.StatusForbidden, reasonNotEntitled}, nil)
		return uuid.Nil, false
	}
	return *caller.workload, true
}

// acrEventInput parses one posted event of actor. The digest is over the posted
// bytes: a same-revision event with any other member is a conflict, whatever the
// mapping keeps.
func acrEventInput(raw []byte, actor uuid.UUID) (outboxprojection.Input, error) {
	event, err := contracts.ParseEmbeddedAcrCredentialLifecycleEvent(raw)
	if err != nil {
		return outboxprojection.Input{}, err
	}
	organization, err := outboxprojection.ParseOrganization(event.Payload.OrganizationID)
	if err != nil {
		return outboxprojection.Input{}, err
	}
	if strings.TrimPrefix(event.Payload.OrganizationID, "org_") != organization.String() {
		return outboxprojection.Input{}, fmt.Errorf("%w: organization id is not canonical", outboxprojection.ErrMalformed)
	}
	return outboxprojection.Input{
		Domain: outboxprojection.DomainAcr, Event: pushlifecycle.FromAcr(event),
		AcrClass: event.Payload.Change.AcrClass, OwnerSubject: event.Payload.Change.OwnerSubject,
		BodyDigest: sha256.Sum256(raw), Actor: actor,
	}, nil
}

// projectionRefusal is the answer of a decided event that did not end in done.
func projectionRefusal(class outboxprojection.Class) refusal {
	switch class {
	case outboxprojection.ClassRetry:
		return refusal{http.StatusConflict, reasonProjectionRetry}
	case outboxprojection.ClassConflict:
		return refusal{http.StatusConflict, reasonProjectionConflict}
	case outboxprojection.ClassUnfenced:
		return unavailable(reasonStoreUnavailable)
	}
	return refusal{http.StatusConflict, reasonProjectionRefused}
}

// acrCredentialEvent is POST /internal/v1/acr/credential-events: one event, one
// transaction. 204 is applied, duplicate or stale. The publisher holds on a
// retry, and marks a conflict or a refusal final.
func (h acrEventHandlers) acrCredentialEvent(w http.ResponseWriter, r *http.Request) {
	actor, ok := h.acrEventCaller(w, r)
	if !ok {
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxAcrEventBody))
	if err != nil {
		h.rec.refuse(w, r, opAcrCredentialEvent, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	in, err := acrEventInput(raw, actor)
	if err != nil {
		h.rec.refuse(w, r, opAcrCredentialEvent, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	result, err := h.Projector.Project(r.Context(), in)
	if err != nil {
		h.rec.refuse(w, r, opAcrCredentialEvent, unavailable(reasonStoreUnavailable), err)
		return
	}
	if result.Class != outboxprojection.ClassDone {
		h.rec.refuse(w, r, opAcrCredentialEvent, projectionRefusal(result.Class), nil)
		return
	}
	h.rec.record(r.Context(), opAcrCredentialEvent, "success", reasonAccepted, nil)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNoContent)
}
