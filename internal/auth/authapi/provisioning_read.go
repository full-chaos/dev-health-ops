package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/publicread"
)

// The versioned read routes: POST get and list of principals, organizations, memberships and grants. Each is mounted only
// when the embedded endpoint-profile document holds its row, so without the rows they answer 404.
const (
	PatternPrincipalsGet     = "/internal/v1/principals/get"
	PatternPrincipalsList    = "/internal/v1/principals/list"
	PatternOrganizationsGet  = "/internal/v1/organizations/get"
	PatternOrganizationsList = "/internal/v1/organizations/list"
	PatternMembershipsGet    = "/internal/v1/memberships/get"
	PatternMembershipsList   = "/internal/v1/memberships/list"
	PatternGrantsGet         = "/internal/v1/grants/get"
	PatternGrantsList        = "/internal/v1/grants/list"
)

// ReadStore is the read side of organizations, memberships and user principals (*publicread.Reader): the public tables
// are its record.
type ReadStore interface {
	Organization(ctx context.Context, id uuid.UUID) (publicread.Organization, error)
	OrganizationsOf(ctx context.Context, principal uuid.UUID, after *uuid.UUID, limit int) ([]publicread.OrganizationOf, bool, error)
	Membership(ctx context.Context, org, principal uuid.UUID) (publicread.Membership, error)
	Memberships(ctx context.Context, org uuid.UUID, after *uuid.UUID, limit int) ([]publicread.Membership, bool, error)
	Principal(ctx context.Context, id uuid.UUID) (publicread.Principal, error)
	Principals(ctx context.Context, org uuid.UUID, kind string, after *uuid.UUID, limit int) ([]publicread.Principal, bool, error)
}

// GrantReads is the read side of the resource grant aggregate (*grant.Store).
type GrantReads interface {
	Get(ctx context.Context, orgID, id uuid.UUID) (*grant.Grant, error)
	ListForOrg(ctx context.Context, orgID uuid.UUID, f grant.ListFilter) ([]grant.Grant, error)
}

// readRoute is one read route: its operation in the contract, pattern and handler.
type readRoute struct {
	op      string
	pattern string
	handle  func(h provisioningHandlers, w http.ResponseWriter, r *http.Request, op contracts.ReadOperation)
	grants  bool
}

var readRoutes = []readRoute{
	{"principals_get", PatternPrincipalsGet, provisioningHandlers.principalsGet, false},
	{"principals_list", PatternPrincipalsList, provisioningHandlers.principalsList, false},
	{"organizations_get", PatternOrganizationsGet, provisioningHandlers.organizationsGet, false},
	{"organizations_list", PatternOrganizationsList, provisioningHandlers.organizationsList, false},
	{"memberships_get", PatternMembershipsGet, provisioningHandlers.membershipsGet, false},
	{"memberships_list", PatternMembershipsList, provisioningHandlers.membershipsList, false},
	{"grants_get", PatternGrantsGet, provisioningHandlers.grantsGet, true},
	{"grants_list", PatternGrantsList, provisioningHandlers.grantsList, true},
}

// withReadRoutes mounts each read route whose store is wired and whose endpoint-profile row lists the session issuer credential
// class, else logs one line naming the route.
func (h provisioningHandlers) withReadRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	for _, rr := range readRoutes {
		stored := h.Reads != nil
		if rr.grants {
			stored = h.GrantReads != nil
		}
		if !stored {
			continue
		}
		if rowsAcceptFor(rows, []string{rr.pattern}, []string{serviceSessionIssuerClass}) != nil {
			h.Logger.Info("auth read route left dark: no endpoint-profile row", "route", rr.pattern)
			continue
		}
		op, ok := contracts.ReadOperationByName(rr.op)
		if !ok || op.Route != rr.pattern {
			panic("authapi: read route " + rr.pattern + " is not an operation of the read contract")
		}
		rr := rr
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: rr.pattern,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { rr.handle(h, w, r, op) })})
	}
	return routes
}

// readAdmit checks the bearer is a session issuer credential holding the operation's read scope (a write scope never
// satisfies it) and reads the body as one strict object of the allowed members. A refusal is already written.
func (h provisioningHandlers) readAdmit(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation, allowed ...string) (strictObject, bool) {
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, op.Name, unauthorized(reasonInvalidCredentials), nil)
		return nil, false
	}
	ctx := r.Context()
	class, reason := classifyIssueBearer(token)
	if class != issueClassCompat {
		h.rec.logger.WarnContext(ctx, "read route refused: bearer", "operation", op.Name, "refusal", reason)
		h.rec.refuse(w, r, op.Name, unauthorized(reasonInvalidCredentials), nil)
		return nil, false
	}
	_, refused, err := h.Credentials.CheckScope(ctx, contracts.NewSecretString(token), op.Scope)
	if err != nil {
		h.rec.refuse(w, r, op.Name, unavailable(reasonStoreUnavailable), err)
		return nil, false
	}
	if refused != "" {
		h.rec.logger.WarnContext(ctx, "read route refused: service credential", "operation", op.Name, "refusal", string(refused))
		h.rec.refuse(w, r, op.Name, unauthorized(reasonInvalidCredentials), nil)
		return nil, false
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxProvisioningBody), append([]string{"schema_version"}, allowed...)...)
	if err != nil || !body.hasString("schema_version", op.RequestSurface) {
		h.rec.refuse(w, r, op.Name, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return nil, false
	}
	return body, true
}

func (o strictObject) hasString(name, want string) bool {
	var text string
	return json.Unmarshal(o[name], &text) == nil && text == want
}

// wireUUID reads a required wire id: the prefix and a canonical UUID.
func (o strictObject) wireUUID(name, prefix string) (uuid.UUID, error) {
	var text string
	if json.Unmarshal(o[name], &text) != nil {
		return uuid.UUID{}, errProvisioningBody
	}
	return parseWireUUID(text, prefix)
}

func parseWireUUID(text, prefix string) (uuid.UUID, error) {
	rest, ok := strings.CutPrefix(text, prefix)
	if !ok {
		return uuid.UUID{}, errProvisioningBody
	}
	id, err := uuid.Parse(rest)
	if err != nil || rest != id.String() {
		return uuid.UUID{}, errProvisioningBody
	}
	return id, nil
}

// optionalWireUUID reads an absent member as nil; a member that is present is never null.
func (o strictObject) optionalWireUUID(name, prefix string) (*uuid.UUID, error) {
	if _, present := o[name]; !present {
		return nil, nil
	}
	id, err := o.wireUUID(name, prefix)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// optionalEnum reads an absent member as "", else one of the values.
func (o strictObject) optionalEnum(name string, values ...string) (string, error) {
	if _, present := o[name]; !present {
		return "", nil
	}
	var text string
	if json.Unmarshal(o[name], &text) != nil {
		return "", errProvisioningBody
	}
	for _, v := range values {
		if text == v {
			return text, nil
		}
	}
	return "", errProvisioningBody
}

// pageRequest reads limit and cursor: limit 1 to 100 as an integer token, absent is the default; the cursor must be the canonical
// encoding of an id of the operation's kind.
func (o strictObject) pageRequest(op contracts.ReadOperation) (limit int, after *uuid.UUID, err error) {
	limit = contracts.ReadDefaultLimit
	if raw, present := o["limit"]; present {
		if string(raw) == "null" {
			return 0, nil, errProvisioningBody
		}
		n, err := o.optionalInt("limit", 1, contracts.ReadMaxLimit)
		if err != nil || n == nil {
			return 0, nil, errProvisioningBody
		}
		limit = int(*n)
	}
	if raw, present := o["cursor"]; present {
		var cursor string
		if json.Unmarshal(raw, &cursor) != nil {
			return 0, nil, errProvisioningBody
		}
		id, err := contracts.DecodeKeysetCursor(cursor, op.IDPrefix)
		if err != nil {
			return 0, nil, errProvisioningBody
		}
		uid, err := parseWireUUID(id, op.IDPrefix)
		if err != nil {
			return 0, nil, errProvisioningBody
		}
		after = &uid
	}
	return limit, after, nil
}

func (o strictObject) presentBool(name string) (bool, error) {
	raw, present := o[name]
	if !present {
		return false, nil
	}
	switch string(raw) {
	case "true":
		return true, nil
	case "false":
		return false, nil
	}
	return false, errProvisioningBody
}

func (h provisioningHandlers) malformed(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	h.rec.refuse(w, r, op.Name, refusal{http.StatusBadRequest, reasonMalformed}, nil)
}

// readFailure answers a failed read: the named not-found for a missing row, else 503 with the cause logged and never answered.
func (h provisioningHandlers) readFailure(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation, err error, missing error, notFound string) {
	answer := unavailable(reasonStoreUnavailable)
	if errors.Is(err, missing) {
		answer = refusal{http.StatusNotFound, notFound}
	}
	h.rec.refuse(w, r, op.Name, answer, failure(answer, err))
}

func (h provisioningHandlers) answered(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation, body any) {
	h.rec.record(r.Context(), op.Name, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, body)
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

type principalReadWire struct {
	SchemaVersion string `json:"schema_version"`
	PrincipalID   string `json:"principal_id"`
	Kind          string `json:"kind"`
	Status        string `json:"status"`
	Revision      int64  `json:"revision"`
}

func principalWire(p publicread.Principal) principalReadWire {
	status := "active"
	if !p.Active {
		status = "disabled"
	}
	return principalReadWire{"principal-read.v1", "prn_" + p.ID.String(), p.Kind, status, p.Revision}
}

type organizationReadWire struct {
	SchemaVersion  string `json:"schema_version"`
	OrganizationID string `json:"organization_id"`
	Slug           string `json:"slug"`
	Name           string `json:"name"`
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
}

func organizationWire(o publicread.Organization) organizationReadWire {
	status := "active"
	if !o.Active {
		status = "suspended"
	}
	return organizationReadWire{"organization-read.v1", "org_" + o.ID.String(), o.Slug, o.Name, status, o.Revision, stamp(o.CreatedAt), stamp(o.UpdatedAt)}
}

type membershipReadWire struct {
	SchemaVersion  string `json:"schema_version"`
	MembershipID   string `json:"membership_id"`
	OrganizationID string `json:"organization_id"`
	PrincipalID    string `json:"principal_id"`
	Role           string `json:"role"`
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
	CreatedAt      string `json:"created_at"`
	UpdatedAt      string `json:"updated_at"`
	JoinedAt       string `json:"joined_at"`
}

// membershipWire: public.memberships holds active rows only. An active membership has joined; a row without joined_at reads as
// joined when it was created, as login does.
func membershipWire(m publicread.Membership) membershipReadWire {
	joined := m.CreatedAt
	if m.JoinedAt != nil {
		joined = *m.JoinedAt
	}
	return membershipReadWire{"membership-read.v1", "mbr_" + m.ID.String(), "org_" + m.OrgID.String(), "prn_" + m.PrincipalID.String(), m.Role,
		"active", m.Revision, stamp(m.CreatedAt), stamp(m.UpdatedAt), stamp(joined)}
}

type grantReadWire struct {
	SchemaVersion  string  `json:"schema_version"`
	GrantID        string  `json:"grant_id"`
	OrganizationID string  `json:"organization_id"`
	PrincipalID    string  `json:"principal_id"`
	Role           string  `json:"role"`
	ResourceKey    string  `json:"resource_key"`
	ExpiresAt      *string `json:"expires_at"`
	Status         string  `json:"status"`
	Revision       int64   `json:"revision"`
	CreatedAt      string  `json:"created_at"`
}

func grantWire(g grant.Grant) grantReadWire {
	a := answerOf(&g)
	return grantReadWire{"grant-read.v1", "grt_" + g.ID.String(), "org_" + g.OrgID.String(), "prn_" + g.PrincipalID.String(), g.Role, g.Key,
		a.ExpiresAt, a.Status, g.Revision, stamp(g.CreatedAt)}
}

type listWire struct {
	SchemaVersion string `json:"schema_version"`
	Items         any    `json:"items"`
	NextCursor    string `json:"next_cursor,omitempty"`
}

func nextCursor(more bool, prefix string, last uuid.UUID) string {
	if !more {
		return ""
	}
	return contracts.EncodeKeysetCursor(prefix + last.String())
}

func (h provisioningHandlers) principalsGet(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "principal_id")
	if !ok {
		return
	}
	id, err := body.wireUUID("principal_id", "prn_")
	if err != nil {
		h.malformed(w, r, op)
		return
	}
	found, err := h.Reads.Principal(r.Context(), id)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonPrincipalNotFound)
		return
	}
	h.answered(w, r, op, principalWire(found))
}

func (h provisioningHandlers) principalsList(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id", "kind", "limit", "cursor")
	if !ok {
		return
	}
	org, err1 := body.wireUUID("organization_id", "org_")
	kind, err2 := body.optionalEnum("kind", "user", "service_account", "workload")
	limit, after, err3 := body.pageRequest(op)
	if errors.Join(err1, err2, err3) != nil {
		h.malformed(w, r, op)
		return
	}
	found, more, err := h.Reads.Principals(r.Context(), org, kind, after, limit)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonPrincipalNotFound)
		return
	}
	items := make([]principalReadWire, 0, len(found))
	for _, p := range found {
		items = append(items, principalWire(p))
	}
	var last uuid.UUID
	if len(found) > 0 {
		last = found[len(found)-1].ID
	}
	h.answered(w, r, op, listWire{"principal-list.v1", items, nextCursor(more && len(found) > 0, op.IDPrefix, last)})
}

func (h provisioningHandlers) organizationsGet(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id")
	if !ok {
		return
	}
	id, err := body.wireUUID("organization_id", "org_")
	if err != nil {
		h.malformed(w, r, op)
		return
	}
	found, err := h.Reads.Organization(r.Context(), id)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonOrganizationNotFound)
		return
	}
	h.answered(w, r, op, organizationWire(found))
}

type organizationItemWire struct {
	Organization organizationReadWire `json:"organization"`
	Membership   membershipReadWire   `json:"membership"`
}

func (h provisioningHandlers) organizationsList(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "principal_id", "membership_status", "limit", "cursor")
	if !ok {
		return
	}
	principal, err1 := body.wireUUID("principal_id", "prn_")
	status, err2 := body.optionalEnum("membership_status", "invited", "active", "suspended", "removed")
	limit, after, err3 := body.pageRequest(op)
	if errors.Join(err1, err2, err3) != nil {
		h.malformed(w, r, op)
		return
	}
	items := []organizationItemWire{}
	if status != "" && status != "active" {
		h.answered(w, r, op, listWire{"organization-list.v1", items, ""})
		return
	}
	found, more, err := h.Reads.OrganizationsOf(r.Context(), principal, after, limit)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonPrincipalNotFound)
		return
	}
	for _, f := range found {
		items = append(items, organizationItemWire{organizationWire(f.Organization), membershipWire(f.Membership)})
	}
	var last uuid.UUID
	if len(found) > 0 {
		last = found[len(found)-1].Organization.ID
	}
	h.answered(w, r, op, listWire{"organization-list.v1", items, nextCursor(more && len(found) > 0, op.IDPrefix, last)})
}

func (h provisioningHandlers) membershipsGet(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id", "principal_id")
	if !ok {
		return
	}
	org, err1 := body.wireUUID("organization_id", "org_")
	principal, err2 := body.wireUUID("principal_id", "prn_")
	if errors.Join(err1, err2) != nil {
		h.malformed(w, r, op)
		return
	}
	found, err := h.Reads.Membership(r.Context(), org, principal)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonMembershipNotFound)
		return
	}
	h.answered(w, r, op, membershipWire(found))
}

func (h provisioningHandlers) membershipsList(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id", "status", "limit", "cursor")
	if !ok {
		return
	}
	org, err1 := body.wireUUID("organization_id", "org_")
	status, err2 := body.optionalEnum("status", "invited", "active", "suspended", "removed")
	limit, after, err3 := body.pageRequest(op)
	if errors.Join(err1, err2, err3) != nil {
		h.malformed(w, r, op)
		return
	}
	items := []membershipReadWire{}
	if status != "" && status != "active" {
		h.answered(w, r, op, listWire{"membership-list.v1", items, ""})
		return
	}
	found, more, err := h.Reads.Memberships(r.Context(), org, after, limit)
	if err != nil {
		h.readFailure(w, r, op, err, publicread.ErrNotFound, reasonMembershipNotFound)
		return
	}
	for _, m := range found {
		items = append(items, membershipWire(m))
	}
	var last uuid.UUID
	if len(found) > 0 {
		last = found[len(found)-1].ID
	}
	h.answered(w, r, op, listWire{"membership-list.v1", items, nextCursor(more && len(found) > 0, op.IDPrefix, last)})
}

func (h provisioningHandlers) grantsGet(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id", "grant_id")
	if !ok {
		return
	}
	org, err1 := body.wireUUID("organization_id", "org_")
	id, err2 := body.wireUUID("grant_id", "grt_")
	if errors.Join(err1, err2) != nil {
		h.malformed(w, r, op)
		return
	}
	found, err := h.GrantReads.Get(r.Context(), org, id)
	if err != nil {
		h.readFailure(w, r, op, err, grant.ErrNotFound, reasonGrantNotFound)
		return
	}
	h.answered(w, r, op, grantWire(*found))
}

func (h provisioningHandlers) grantsList(w http.ResponseWriter, r *http.Request, op contracts.ReadOperation) {
	body, ok := h.readAdmit(w, r, op, "organization_id", "principal_id", "include_revoked", "limit", "cursor")
	if !ok {
		return
	}
	org, err1 := body.wireUUID("organization_id", "org_")
	principal, err2 := body.optionalWireUUID("principal_id", "prn_")
	revoked, err3 := body.presentBool("include_revoked")
	limit, after, err4 := body.pageRequest(op)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.malformed(w, r, op)
		return
	}
	all, err := h.GrantReads.ListForOrg(r.Context(), org, grant.ListFilter{PrincipalID: principal, IncludeRevoked: revoked})
	if err != nil {
		h.readFailure(w, r, op, err, grant.ErrNotFound, reasonGrantNotFound)
		return
	}
	items := []grantReadWire{}
	more := false
	var last uuid.UUID
	for _, g := range all {
		if after != nil && g.ID.String() <= after.String() {
			continue
		}
		if len(items) == limit {
			more = true
			break
		}
		items = append(items, grantWire(g))
		last = g.ID
	}
	h.answered(w, r, op, listWire{"grant-list.v1", items, nextCursor(more, op.IDPrefix, last)})
}
