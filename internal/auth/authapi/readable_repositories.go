package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// PatternReadableRepositories is the read listing go-api's Ask Dev principal access read calls: for one user principal
// and one organization, the repositories that principal may read through acr.context.read (acp rulings Q1 and Q2 on
// CHAOS-13561). It takes only the go-api workload token, and mounts only when the endpoint-profile document holds its
// row: without the row it answers 404 and the service starts.
const PatternReadableRepositories = "/internal/v1/principals/readable-repositories"

const opReadableRepositories = "readable_repositories"

// ReadableRepositoriesAction is the one action the listing answers for (acp ruling Q2 on CHAOS-13561). Every
// organization role that holds it also holds acr.evidence.read (policycatalog's pair census), so the listing of one is
// the listing of both.
const ReadableRepositoriesAction = "acr.context.read"

const readableRepositoriesSchema = "readable_repositories.v1"

// The scope member of the answer.
const (
	ReadableScopeOrganization = "organization"
	ReadableScopeRepositories = "repositories"
	ReadableScopeDenied       = "denied"
)

// maxReadableRepositoriesBody bounds the request: one object of two canonical UUIDs.
const maxReadableRepositoriesBody = 1 << 10

// readableRepositoriesAnswer is the readable_repositories.v1 document: scope organization (every repository of the
// organization), repositories with the principal's own canonical keys (sorted, unique, at most 256, [] for none), or
// denied with the read decision's reason_code (repository_allowlist_too_large over 256 keys that are the live path of a
// repository identity row, never a truncated list).
type readableRepositoriesAnswer struct {
	SchemaVersion string    `json:"schema_version"`
	Scope         string    `json:"scope"`
	Repositories  *[]string `json:"repositories,omitempty"`
	ReasonCode    string    `json:"reason_code,omitempty"`
}

// ReadableRepositoriesDeps are the listing route's collaborators.
type ReadableRepositoriesDeps struct {
	State     DecisionState
	Evaluator *authorization.Evaluator
	// Workload is the go-api workload path (the session issue route's); nil refuses every bearer.
	Workload *SessionIssueWorkload
	// ProfileRows is the endpoint-profile document the route's row is looked up in; nil reads the embedded document.
	ProfileRows []byte
	Logger      *slog.Logger
	Meter       metric.Meter
}

type readableRepositoriesHandlers struct {
	ReadableRepositoriesDeps
	rec recorder
}

// ReadableRepositoriesRoutes returns the listing route when the go-api workload path is configured and the
// endpoint-profile document holds a row that lists exactly the workload token class, else none and one Info line.
func ReadableRepositoriesRoutes(deps ReadableRepositoriesDeps) ([]httpapi.Route, error) {
	if deps.State == nil || deps.Evaluator == nil {
		return nil, errors.New("authapi: the readable repositories route needs decision state and an evaluator")
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	rows := deps.ProfileRows
	if rows == nil {
		rows = authv1.AuthServiceEndpointProfiles
	}
	if deps.Workload == nil || rowsExactFor(rows, []string{PatternReadableRepositories}, []string{workloadTokenClass}) != nil {
		deps.Logger.Info("auth readable repositories route not mounted: no go-api workload path or no endpoint-profile row",
			"route", PatternReadableRepositories)
		return nil, nil
	}
	h := readableRepositoriesHandlers{ReadableRepositoriesDeps: deps, rec: rec}
	return []httpapi.Route{{Method: http.MethodPost, Pattern: PatternReadableRepositories, Handler: http.HandlerFunc(h.serve),
		MaxBodyBytes: maxReadableRepositoriesBody}}, nil
}

// serve is POST /internal/v1/principals/readable-repositories: one strict object {"principal_id","organization_id"}
// in, the principal's read listing out. The bearer is checked before the body is read; every state read runs in one
// read-only transaction of the call, and nothing is cached: the listing is re-read live on every call.
func (h readableRepositoriesHandlers) serve(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, opReadableRepositories, unauthorized(reasonInvalidCredentials), nil)
		return
	}
	if !h.caller(w, r, token) {
		return
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxReadableRepositoriesBody), "principal_id", "organization_id")
	if err != nil {
		h.rec.refuse(w, r, opReadableRepositories, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	principal, perr := body.uuid("principal_id")
	org, oerr := body.uuid("organization_id")
	if perr != nil || oerr != nil {
		h.rec.refuse(w, r, opReadableRepositories, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	answer, err := h.listing(ctx, principal, org)
	if err != nil {
		h.rec.refuse(w, r, opReadableRepositories, unavailable(reasonStoreUnavailable), err)
		return
	}
	h.rec.record(ctx, opReadableRepositories, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, answer)
}

// caller admits only the go-api workload token, as the session state route does (SessionIssueWorkload: audience
// auth-service and the configured go-api subject). Every other bearer, another workload's token and a user token
// included, is the one 401; the reason is only logged.
func (h readableRepositoriesHandlers) caller(w http.ResponseWriter, r *http.Request, token string) bool {
	ctx := r.Context()
	class, reason := classifyIssueBearer(token)
	switch class {
	case issueClassWorkload:
		_, why, err := h.Workload.authorize(token)
		if err != nil {
			h.rec.refuse(w, r, opReadableRepositories, unavailable(reasonDependency), err)
			return false
		}
		if why == "" {
			return true
		}
		reason = why
	case issueClassCompat:
		reason = "retired_credential_class"
	}
	h.rec.logger.WarnContext(ctx, "readable repositories refused: bearer", "refusal", reason, "operation", opReadableRepositories)
	h.rec.refuse(w, r, opReadableRepositories, unauthorized(reasonInvalidCredentials), nil)
	return false
}

var errReadableStateFailure = errors.New("authapi: the readable repositories listing could not read its state")

// listing is Evaluator.ReadableRepositoriesInInventory for ReadableRepositoriesAction over the principal's own
// snapshot (no session, so no delegation): a set over 256 keys counts only the keys with a live repository identity
// row (CHAOS-13618). A state failure (a store or clock failure, or a decision reason that is a state failure) is an
// error, never a deny and never a list.
func (h readableRepositoriesHandlers) listing(ctx context.Context, principal, org uuid.UUID) (readableRepositoriesAnswer, error) {
	orgID := org.String()
	var d authorization.Decision
	err := h.State.Read(ctx, func(tx DecisionTx) error {
		snap, err := tx.Snapshot(ctx, principal, orgID)
		if err != nil {
			return err
		}
		if snap == nil || snap.NowUnixNano() <= 0 {
			return errReadableStateFailure
		}
		d = h.Evaluator.ReadableRepositoriesInInventory(authorization.Request{
			Action: ReadableRepositoriesAction, ResourceKind: "repository", ResourceKey: "*", OrganizationID: orgID,
		}, snap)
		return nil
	})
	if err != nil {
		return readableRepositoriesAnswer{}, err
	}
	code, onWire := wireReasonCode(d.Reason)
	if !onWire {
		return readableRepositoriesAnswer{}, errReadableStateFailure
	}
	answer := readableRepositoriesAnswer{SchemaVersion: readableRepositoriesSchema}
	switch {
	case !d.Allowed:
		answer.Scope, answer.ReasonCode = ReadableScopeDenied, code
	case d.Obligations == nil || d.Obligations.TenantScope != orgID:
		return readableRepositoriesAnswer{}, errReadableStateFailure
	case d.Obligations.RepositoryScope == authorization.RepositoryScopeOrganization:
		answer.Scope = ReadableScopeOrganization
	default:
		keys := append([]string{}, d.Obligations.RepositoryAllowlist...)
		answer.Scope, answer.Repositories = ReadableScopeRepositories, &keys
	}
	return answer, nil
}
