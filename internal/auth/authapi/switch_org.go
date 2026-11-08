package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// PatternCompatSwitchOrg is go-api's organization switch, POST
// /api/v1/auth/switch-org (internal/api/session switchOrg), served here as a
// session exchange: the caller's own session (the bearer's sid) is superseded
// by a session in the requested organization. It is mounted only when the
// embedded endpoint-profile document holds its row.
const PatternCompatSwitchOrg = "/api/v1/auth/switch-org"

const opSwitchOrg = "switch_org"

// go-api's switch-org texts (internal/api/session routes.go switchOrg), and
// this route's own 403 for a session that may not switch.
const (
	textSwitchNotMember  = "User is not a member of the selected organization"
	textSwitchInvalidOrg = "Invalid organization ID"
	textSwitchNotAllowed = "Organization switch is not allowed for this session"
	textInvalidToken     = "Invalid or expired token"
)

// switchOrgLimit is the switch budget per signed-in principal: a switch is
// user-paced.
var switchOrgLimit = httpapi.Limit{ID: "auth_svc_switch_org", Count: 30, Window: 15 * time.Minute}

// compatSwitchOrgRow is go-api's switch row (routes.go switchOrg): action
// login, resource session/<user>, in the new session's organization.
var compatSwitchOrgRow = compatSessionRow("login", "User switched active organization")

// SwitchOrgSessions is the session domain of the switch (*session.Store).
type SwitchOrgSessions interface {
	CreateSupersedingGuarded(ctx context.Context, principalID uuid.UUID, organizationID *uuid.UUID, amr []string,
		predecessor uuid.UUID, event audit.AuditEvent, guard session.Guard) (session.Issued, error)
}

// SwitchOrgReads are the reads the switch makes inside the session
// transaction (PGUsers).
type SwitchOrgReads interface {
	StateOn(ctx context.Context, q session.Queryer, userID uuid.UUID) (UserState, bool, error)
	// OrganizationActiveOn is false for an inactive or unknown organization.
	OrganizationActiveOn(ctx context.Context, q session.Queryer, orgID uuid.UUID) (bool, error)
	// MemberRoleLockedOn is RoleOn holding the membership row against a
	// concurrent removal until the transaction ends.
	MemberRoleLockedOn(ctx context.Context, q session.Queryer, userID, orgID uuid.UUID) (*string, bool, error)
}

// SwitchOrgConfig mounts POST /api/v1/auth/switch-org (CompatDeps.SwitchOrg).
type SwitchOrgConfig struct {
	Sessions SwitchOrgSessions
	Reads    SwitchOrgReads
}

// OrganizationActiveOn reads public.organizations.is_active as go-api's switch
// does (o.is_active IS true) on q.
func (p PGUsers) OrganizationActiveOn(ctx context.Context, q session.Queryer, orgID uuid.UUID) (bool, error) {
	var active bool
	err := q.QueryRow(ctx, `SELECT is_active IS true FROM public.organizations WHERE id = $1`, orgID).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("%w: reading public.organizations: %w", errUsersStore, err)
	}
	return active, nil
}

// MemberRoleLockedOn is RoleOn with the row held FOR SHARE while the users are
// owned, when the runtime role holds the UPDATE a row lock needs and the
// removal it waits for is auth-service's. Without users owned, go-api removes
// members and ends no auth session, so the read is plain.
func (p PGUsers) MemberRoleLockedOn(ctx context.Context, q session.Queryer, userID, orgID uuid.UUID) (*string, bool, error) {
	if !p.UsersOwned {
		return p.RoleOn(ctx, q, userID, orgID)
	}
	var role *string
	err := q.QueryRow(ctx, `SELECT role FROM public.memberships WHERE user_id = $1 AND org_id = $2 LIMIT 1 FOR SHARE`, userID, orgID).Scan(&role)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("%w: reading public.memberships: %w", errUsersStore, err)
	}
	return role, true, nil
}

// withSwitchOrgRoute mounts the switch route when rows holds its row with the
// user access token class (the retired HS256 class is on no row). Without it
// it logs one line naming the route and mounts nothing.
func (h compatHandlers) withSwitchOrgRoute(routes []httpapi.Route, rows []byte) ([]httpapi.Route, error) {
	if h.SwitchOrg == nil {
		return routes, nil
	}
	if h.SwitchOrg.Sessions == nil || h.SwitchOrg.Reads == nil {
		return nil, errors.New("authapi: the switch-org route needs its sessions and its reads")
	}
	if h.Issuer == nil || h.Issuer.access == nil {
		h.rec.logger.Info("auth switch-org route not mounted: AUTH_TOKEN_ISSUER_URL is unset (no access-token verifier)", "route", PatternCompatSwitchOrg)
		return routes, nil
	}
	if rowsAcceptFor(rows, []string{PatternCompatSwitchOrg}, []string{accessverify.ClassUser}) != nil {
		h.rec.logger.Info("auth switch-org route not mounted: no endpoint-profile row", "route", PatternCompatSwitchOrg)
		return routes, nil
	}
	s := switchOrgHandler{compatHandlers: h, limiter: h.Limits.limiter(switchOrgLimit)}
	return append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternCompatSwitchOrg, Handler: http.HandlerFunc(s.compatSwitchOrg)}), nil
}

type switchOrgHandler struct {
	compatHandlers
	limiter *httpapi.KeyedLimiter
}

// switchRefusal is one refused switch: its status, reason code and go-api text.
type switchRefusal struct {
	status       int
	reason, text string
}

var (
	refuseNotMember  = switchRefusal{http.StatusForbidden, reasonNotEntitled, textSwitchNotMember}
	refuseNotAllowed = switchRefusal{http.StatusForbidden, reasonNotEntitled, textSwitchNotAllowed}
)

func switchTokenRefusal(reason string) switchRefusal {
	return switchRefusal{http.StatusUnauthorized, reason, textInvalidToken}
}

func (h switchOrgHandler) refuse(w http.ResponseWriter, r *http.Request, answer switchRefusal) {
	h.rec.record(r.Context(), opSwitchOrg, "denied", answer.reason, nil)
	if answer.status == http.StatusUnauthorized {
		policy.WriteAuthFailure(w, answer.text, nil)
		return
	}
	policy.WriteDetail(w, answer.status, policy.ErrorDetail(answer.text), nil)
}

// bearerRefusal answers a refused bearer as go-api's guard does
// (writeGoAPIBearerRefusal); a delegated session is this route's 403.
func (h switchOrgHandler) bearerRefusal(w http.ResponseWriter, r *http.Request, operation string, answer refusal, err error) {
	result := "denied"
	if answer.status != http.StatusUnauthorized && answer.status != http.StatusForbidden {
		result = "error"
	}
	h.rec.record(r.Context(), operation, result, answer.reason, err)
	writeGoAPIBearerRefusal(w, r, answer, textSwitchNotAllowed)
}

// compatSwitchOrg is POST /api/v1/auth/switch-org with go-api's wire: the body
// read first and the bearer then resolved (go-api's BodyFirst(Authenticated)),
// then the strict {org_id} body, the per-principal budget, and the switch. The
// organization is only ever the request's: whether the caller may enter it is
// decided by fresh reads of the user, the organization and the membership,
// inside the transaction that supersedes the bearer's own session. Nothing
// else of the request names a session or an organization.
func (h switchOrgHandler) compatSwitchOrg(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var raw bytes.Buffer
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.TeeReader(r.Body, &raw), r.Body}
	body, ok := h.readCompatBody(w, r, opSwitchOrg, nil)
	if !ok {
		return
	}
	caller := handlers{Deps: Deps{Sessions: h.Sessions, Users: h.Users, Issuer: h.Issuer, Logger: h.Logger}, rec: h.rec}
	bearer, record, ok := caller.authenticateWith(w, r, opSwitchOrg, h.bearerRefusal)
	if !ok {
		return
	}
	if bearer.Impersonated {
		h.refuse(w, r, refuseNotAllowed)
		return
	}
	org, ok := h.readSwitchBody(w, r, body, raw.Bytes())
	if !ok {
		return
	}
	principal := bearer.PrincipalID
	key := "switch-org:" + h.Limits.digest("principal", principal.String())
	if !admit(w, r, goAPIRefusal, PatternCompatSwitchOrg, charge{h.limiter, key}) {
		return
	}
	if err := h.Issuer.Preflight(ctx); err != nil {
		h.unavailableCompat(w, r, opSwitchOrg, err)
		return
	}
	// A first read on the pool: an organization the caller holds no
	// membership in is refused before it is projected. The decision is the
	// guard's, on the transaction.
	if _, member, err := h.Users.Role(ctx, principal, org); err != nil {
		h.fail(w, r, opSwitchOrg, err)
		return
	} else if !member {
		h.refuse(w, r, refuseNotMember)
		return
	}
	err := h.Projector.Project(ctx, principal)
	if err == nil {
		err = h.Projector.ProjectOrganization(ctx, org)
	}
	switch {
	case errors.Is(err, identity.ErrUserNotFound):
		h.refuse(w, r, switchTokenRefusal(reasonPrincipalNotFound))
		return
	case errors.Is(err, identity.ErrOrganizationNotFound):
		h.refuse(w, r, refuseNotMember)
		return
	case err != nil:
		h.fail(w, r, opSwitchOrg, err)
		return
	}
	judged := switchJudgement{reads: h.SwitchOrg.Reads, principal: principal, org: org, logger: h.rec.logger}
	issued, err := h.SwitchOrg.Sessions.CreateSupersedingGuarded(session.WithPublicRow(ctx, compatSwitchOrgRow),
		principal, &org, record.AMR, bearer.SessionID, switchEvent(ctx, principal, org, record.AMR), judged.guard)
	switch {
	case errors.Is(err, session.ErrGuardRefused) && judged.refused != nil:
		h.refuse(w, r, *judged.refused)
		return
	case errors.Is(err, session.ErrPredecessorNotLive):
		h.refuse(w, r, switchTokenRefusal(reasonSessionRevoked))
		return
	case errors.Is(err, session.ErrPredecessorOtherPrincipal), errors.Is(err, session.ErrPredecessorDelegated):
		h.refuse(w, r, refuseNotAllowed)
		return
	case errors.Is(err, session.ErrPrincipalUnavailable):
		h.refuse(w, r, switchTokenRefusal(reasonDeactivated))
		return
	case errors.Is(err, session.ErrOrganizationUnavailable):
		h.refuse(w, r, refuseNotAllowed)
		return
	case err != nil:
		h.fail(w, r, opSwitchOrg, err)
		return
	}
	tokens, err := h.Issuer.Tokens(ctx, Grant{
		PrincipalID: principal, OrganizationID: &org, Role: roleText(judged.role, true), SessionID: issued.Session.ID, User: judged.user,
	})
	if err != nil {
		h.failMint(w, r, opSwitchOrg, err)
		return
	}
	h.rec.record(ctx, opSwitchOrg, "success", reasonAccepted, nil)
	writeCompatModel(w, loginResponse(tokens, h.Issuer.ExpiresIn(), issued.Credential, false,
		userInfo(principal, judged.user, &Membership{OrgID: org, Role: judged.role}), nil))
}

// readSwitchBody is go-api's SwitchOrgRequest, strict: one member, org_id, a
// string (422 otherwise, pydantic's errors), named once, holding a canonical
// lowercase hyphenated UUID (go-api's 400 otherwise). raw is the body as
// read, for the duplicate member a decoded object no longer shows.
func (h switchOrgHandler) readSwitchBody(w http.ResponseWriter, r *http.Request, body pybody.Body, raw []byte) (uuid.UUID, bool) {
	var errs pybody.Errors
	var text string
	if object, ok := errs.Object(body); ok {
		text, _ = errs.RequiredString(object, "org_id", 0, 0)
		errs.ForbidExtra(object, "org_id")
	}
	if len(errs) > 0 {
		h.rec.record(r.Context(), opSwitchOrg, "denied", reasonMalformed, nil)
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return uuid.UUID{}, false
	}
	id, err := uuid.Parse(text)
	if duplicateMember(raw) || err != nil || text != id.String() {
		h.rec.record(r.Context(), opSwitchOrg, "denied", reasonMalformed, nil)
		policy.WriteDetail(w, http.StatusBadRequest, policy.ErrorDetail(textSwitchInvalidOrg), nil)
		return uuid.UUID{}, false
	}
	return id, true
}

// duplicateMember reports a JSON object body that names a member twice. A body
// the standard decoder cannot walk counts as one: the check fails closed.
func duplicateMember(raw []byte) bool {
	text, err := pyjson.DecodeBody(raw)
	if err != nil {
		return true
	}
	dec := json.NewDecoder(strings.NewReader(text))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return true
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return true
		}
		name, _ := tok.(string)
		if seen[name] {
			return true
		}
		seen[name] = true
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return true
		}
	}
	return false
}

// switchJudgement is the guard of one switch and what it decided: the user
// and the role it read, or the refusal.
type switchJudgement struct {
	reads          SwitchOrgReads
	principal, org uuid.UUID
	logger         *slog.Logger

	user    UserState
	role    *string
	refused *switchRefusal
}

// guard decides the switch under the predecessor's lock: the user exists, is
// active, is no service account, and holds the version the predecessor is
// pinned to; the organization is active; the user is its member. Superusers
// need the membership too, as on go-api.
func (j *switchJudgement) guard(ctx context.Context, q session.Queryer, predecessor session.Record) error {
	refuse := func(answer switchRefusal) error {
		j.refused = &answer
		return session.ErrGuardRefused
	}
	user, found, err := j.reads.StateOn(ctx, q, j.principal)
	switch {
	case err != nil:
		return err
	case !found:
		return refuse(switchTokenRefusal(reasonPrincipalNotFound))
	case !user.IsActive:
		return refuse(switchTokenRefusal(reasonDeactivated))
	case checkPin(ctx, j.logger, predecessor, user) != pinCurrent:
		return refuse(switchTokenRefusal(reasonRevisionStale))
	case identity.IsServiceProvider(user.AuthProvider):
		return refuse(refuseNotAllowed)
	}
	active, err := j.reads.OrganizationActiveOn(ctx, q, j.org)
	if err != nil {
		return err
	}
	if !active {
		return refuse(refuseNotMember)
	}
	role, member, err := j.reads.MemberRoleLockedOn(ctx, q, j.principal, j.org)
	if err != nil {
		return err
	}
	if !member {
		return refuse(refuseNotMember)
	}
	j.user, j.role = user, role
	return nil
}

// switchEvent is the successor's auth.session.issued row: the user as actor
// and subject, the organization, and the predecessor's amr.
func switchEvent(ctx context.Context, principal, org uuid.UUID, amr []string) audit.AuditEvent {
	actor, organization := principal.String(), org.String()
	return audit.AuditEvent{
		EventType: EventSessionIssued, Outcome: audit.OutcomeAllowed,
		ActorPrincipalID: &actor, SubjectPrincipalID: &actor, OrganizationID: &organization, RequestID: requestid.Audit(ctx),
		Attributes: map[string]any{"reason_code": reasonAccepted, "target_kind": "session", "amr": append([]string(nil), amr...)},
	}
}
