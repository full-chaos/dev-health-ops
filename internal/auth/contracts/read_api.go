package contracts

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"
)

// The scopes of the versioned read API. A read holds a read scope; a write
// scope never satisfies one.
const (
	ScopePrincipalsRead    = "principals:read"
	ScopeOrganizationsRead = "organizations:read"
	ScopeMembershipsRead   = "memberships:read"
	ScopeGrantsRead        = "grants:read"
)

// Page size of a list read: an absent limit is ReadDefaultLimit, a limit over
// ReadMaxLimit is refused and never clamped.
const (
	ReadDefaultLimit = 50
	ReadMaxLimit     = 100
)

// ReadOperation is one read of the versioned read API. The route is where a
// later change mounts it; nothing serves it here.
type ReadOperation struct {
	Name            string
	Route           string
	RequestSurface  string
	ResponseSurface string
	Scope           string
	List            bool
	// IDPrefix is the prefix of the id a get names or a list orders by, and of
	// the id inside a list cursor.
	IDPrefix string
}

var readOperations = []ReadOperation{
	{"principals_get", "/internal/v1/principals/get", "principal-get-request.v1", "principal-read.v1", ScopePrincipalsRead, false, "prn_"},
	{"principals_list", "/internal/v1/principals/list", "principal-list-request.v1", "principal-list.v1", ScopePrincipalsRead, true, "prn_"},
	{"organizations_get", "/internal/v1/organizations/get", "organization-get-request.v1", "organization-read.v1", ScopeOrganizationsRead, false, "org_"},
	{"organizations_list", "/internal/v1/organizations/list", "organization-list-request.v1", "organization-list.v1", ScopeOrganizationsRead, true, "org_"},
	{"memberships_get", "/internal/v1/memberships/get", "membership-get-request.v1", "membership-read.v1", ScopeMembershipsRead, false, "mbr_"},
	{"memberships_list", "/internal/v1/memberships/list", "membership-list-request.v1", "membership-list.v1", ScopeMembershipsRead, true, "mbr_"},
	{"grants_get", "/internal/v1/grants/get", "grant-get-request.v1", "grant-read.v1", ScopeGrantsRead, false, "grt_"},
	{"grants_list", "/internal/v1/grants/list", "grant-list-request.v1", "grant-list.v1", ScopeGrantsRead, true, "grt_"},
}

func init() {
	// A read response may carry a repository key (grant-read, grant-list) or
	// a list of such bodies: no refusal of one repeats input text.
	for _, op := range readOperations {
		credentialCarryingSurfaces[op.ResponseSurface] = true
	}
}

// ReadOperations returns the operations of the read API.
func ReadOperations() []ReadOperation { return slices.Clone(readOperations) }

// ReadOperationByName returns one operation by name.
func ReadOperationByName(name string) (ReadOperation, bool) {
	for _, op := range readOperations {
		if op.Name == name {
			return op, true
		}
	}
	return ReadOperation{}, false
}

// ReadScopes returns the read scopes, sorted.
func ReadScopes() []string {
	set := map[string]bool{}
	for _, op := range readOperations {
		set[op.Scope] = true
	}
	out := make([]string, 0, len(set))
	for scope := range set {
		out = append(out, scope)
	}
	sort.Strings(out)
	return out
}

// ReadScopeAllows is true when held carries the read scope of the operation. A
// write scope, another read scope and an unknown operation never allow it.
func ReadScopeAllows(held []string, operation string) bool {
	op, ok := ReadOperationByName(operation)
	return ok && slices.Contains(held, op.Scope)
}

const cursorPrefix = "k1_"

// EncodeKeysetCursor is the cursor of a page that ends at id: the version
// prefix and the unpadded base64url of the id.
func EncodeKeysetCursor(id string) string {
	return cursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(id))
}

var wireIDBody = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// DecodeKeysetCursor is the id a cursor names. The id must carry idPrefix and
// a wire id body, and the cursor must be the canonical encoding of it.
func DecodeKeysetCursor(cursor, idPrefix string) (string, error) {
	body, ok := strings.CutPrefix(cursor, cursorPrefix)
	if !ok {
		return "", fmt.Errorf("read cursor: no %s prefix", cursorPrefix)
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(body)
	if err != nil || len(raw) == 0 {
		return "", fmt.Errorf("read cursor: not the unpadded base64url of an id")
	}
	id := string(raw)
	rest, ok := strings.CutPrefix(id, idPrefix)
	if !ok || !wireIDBody.MatchString(rest) {
		return "", fmt.Errorf("read cursor: not an id of kind %s", idPrefix)
	}
	return id, nil
}

// ReadRequest is a parsed read request. Limit is the effective page size.
type ReadRequest struct {
	Operation        ReadOperation
	OrganizationID   string
	PrincipalID      string
	GrantID          string
	Kind             string
	Status           string
	MembershipStatus string
	IncludeRevoked   bool
	Limit            int
	Cursor           string
	// AfterID is the id the request cursor names; empty for the first page.
	AfterID string
}

type readRequestWire struct {
	OrganizationID   string `json:"organization_id"`
	PrincipalID      string `json:"principal_id"`
	GrantID          string `json:"grant_id"`
	Kind             string `json:"kind"`
	Status           string `json:"status"`
	MembershipStatus string `json:"membership_status"`
	IncludeRevoked   bool   `json:"include_revoked"`
	Limit            *int   `json:"limit"`
	Cursor           string `json:"cursor"`
}

func readRule(surface, rule string) error {
	return &CredentialRefusal{Surface: surface, Stage: StageClientRule, Rule: rule}
}

// ParseReadRequest validates and parses the request body of an operation.
// Beyond the schema it refuses a cursor that does not decode to an id of the
// operation's kind.
func ParseReadRequest(root, operation string, raw []byte) (*ReadRequest, error) {
	op, ok := ReadOperationByName(operation)
	if !ok {
		return nil, fmt.Errorf("read api: no operation %q", operation)
	}
	var wire readRequestWire
	if err := decodeWire(root, op.RequestSurface, raw, &wire); err != nil {
		return nil, err
	}
	req := &ReadRequest{Operation: op, OrganizationID: wire.OrganizationID, PrincipalID: wire.PrincipalID, GrantID: wire.GrantID,
		Kind: wire.Kind, Status: wire.Status, MembershipStatus: wire.MembershipStatus, IncludeRevoked: wire.IncludeRevoked,
		Limit: ReadDefaultLimit, Cursor: wire.Cursor}
	if wire.Limit != nil {
		req.Limit = *wire.Limit
	}
	if wire.Cursor != "" {
		after, err := DecodeKeysetCursor(wire.Cursor, op.IDPrefix)
		if err != nil {
			rule := "cursor_undecodable"
			if body, ok := strings.CutPrefix(wire.Cursor, cursorPrefix); ok {
				if raw, derr := base64.RawURLEncoding.Strict().DecodeString(body); derr == nil && len(raw) > 0 && !strings.HasPrefix(string(raw), op.IDPrefix) {
					rule = "cursor_wrong_kind"
				}
			}
			return nil, readRule(op.RequestSurface, rule)
		}
		req.AfterID = after
	}
	return req, nil
}

// ReadResponse is a parsed read response: the ids of its items in order, and
// the cursor of the next page when there is one.
type ReadResponse struct {
	Operation  string
	IDs        []string
	NextCursor string
}

type readResourceWire struct {
	PrincipalID    string `json:"principal_id"`
	OrganizationID string `json:"organization_id"`
	MembershipID   string `json:"membership_id"`
	GrantID        string `json:"grant_id"`
	Kind           string `json:"kind"`
	Status         string `json:"status"`
	Revision       int64  `json:"revision"`
}

type readItemWire struct {
	readResourceWire
	Organization *readResourceWire `json:"organization"`
	Membership   *readResourceWire `json:"membership"`
}

type readPageWire struct {
	Items      []readItemWire `json:"items"`
	NextCursor *string        `json:"next_cursor"`
}

// ParseReadResponse validates and parses the response to a parsed request.
// Beyond the schema it refuses a response that is not the answer to the
// request: another id, another organization or grantee, a filter that was not
// honoured, a page that is over the limit, out of order or does not start after
// the request cursor, and a next cursor that is not the last item's. Every
// refusal is a CredentialRefusal: a surface, a stage and a rule, never a value,
// because a grant carries a repository key.
func ParseReadResponse(root string, req *ReadRequest, raw []byte) (*ReadResponse, error) {
	op := req.Operation
	if !op.List {
		var res readResourceWire
		if err := decodeWire(root, op.ResponseSurface, raw, &res); err != nil {
			return nil, err
		}
		if rule := checkReadGet(op, req, res); rule != "" {
			return nil, readRule(op.ResponseSurface, rule)
		}
		return &ReadResponse{Operation: op.Name, IDs: []string{readItemID(op, readItemWire{readResourceWire: res})}}, nil
	}
	var page readPageWire
	if err := decodeWire(root, op.ResponseSurface, raw, &page); err != nil {
		return nil, err
	}
	out := &ReadResponse{Operation: op.Name}
	if len(page.Items) > req.Limit {
		return nil, readRule(op.ResponseSurface, "items_over_limit")
	}
	previous := req.AfterID
	for _, item := range page.Items {
		id := readItemID(op, item)
		if id <= previous {
			rule := "items_not_ascending"
			if len(out.IDs) == 0 {
				rule = "item_not_after_cursor"
			}
			return nil, readRule(op.ResponseSurface, rule)
		}
		if rule := checkReadListItem(op, req, item); rule != "" {
			return nil, readRule(op.ResponseSurface, rule)
		}
		out.IDs = append(out.IDs, id)
		previous = id
	}
	if page.NextCursor != nil {
		if len(out.IDs) == 0 {
			return nil, readRule(op.ResponseSurface, "cursor_on_empty_page")
		}
		last, err := DecodeKeysetCursor(*page.NextCursor, op.IDPrefix)
		if err != nil || last != out.IDs[len(out.IDs)-1] {
			return nil, readRule(op.ResponseSurface, "next_cursor_not_last_item")
		}
		out.NextCursor = *page.NextCursor
	}
	return out, nil
}

func readItemID(op ReadOperation, item readItemWire) string {
	switch op.IDPrefix {
	case "prn_":
		return item.PrincipalID
	case "mbr_":
		return item.MembershipID
	case "grt_":
		return item.GrantID
	}
	if item.Organization != nil {
		return item.Organization.OrganizationID
	}
	return item.OrganizationID
}

func checkReadGet(op ReadOperation, req *ReadRequest, res readResourceWire) string {
	switch op.Name {
	case "principals_get":
		if res.PrincipalID != req.PrincipalID {
			return "id_mismatch"
		}
	case "organizations_get":
		if res.OrganizationID != req.OrganizationID {
			return "id_mismatch"
		}
	case "memberships_get":
		if res.OrganizationID != req.OrganizationID {
			return "organization_mismatch"
		}
		if res.PrincipalID != req.PrincipalID {
			return "principal_mismatch"
		}
	case "grants_get":
		if res.GrantID != req.GrantID {
			return "id_mismatch"
		}
		if res.OrganizationID != req.OrganizationID {
			return "organization_mismatch"
		}
	}
	return ""
}

func checkReadListItem(op ReadOperation, req *ReadRequest, item readItemWire) string {
	switch op.Name {
	case "principals_list":
		if req.Kind != "" && item.Kind != req.Kind {
			return "kind_filter_mismatch"
		}
	case "memberships_list":
		if item.OrganizationID != req.OrganizationID {
			return "item_wrong_organization"
		}
		if req.Status != "" && item.Status != req.Status {
			return "status_filter_mismatch"
		}
	case "grants_list":
		if item.OrganizationID != req.OrganizationID {
			return "item_wrong_organization"
		}
		if req.PrincipalID != "" && item.PrincipalID != req.PrincipalID {
			return "item_wrong_principal"
		}
		if !req.IncludeRevoked && item.Status == "revoked" {
			return "revoked_item_not_requested"
		}
	case "organizations_list":
		if item.Organization == nil || item.Membership == nil {
			return "item_pair_mismatch"
		}
		if item.Membership.PrincipalID != req.PrincipalID {
			return "item_wrong_principal"
		}
		if item.Membership.OrganizationID != item.Organization.OrganizationID {
			return "item_pair_mismatch"
		}
		if req.MembershipStatus != "" && item.Membership.Status != req.MembershipStatus {
			return "status_filter_mismatch"
		}
	}
	return ""
}

// readErrorStatuses are the statuses a read answers an error with: a read has
// no state conflict.
var readErrorStatuses = map[int]bool{400: true, 401: true, 403: true, 404: true, 429: true, 503: true}

// ParseReadError validates the error body of a read received on an HTTP
// response whose status line said httpStatus. error.v1 has no member that could
// name a repository, and a refusal never repeats input text.
func ParseReadError(root string, raw []byte, httpStatus int, now time.Time) error {
	if !readErrorStatuses[httpStatus] {
		return readRule(ErrorSurface, "status_not_for_reads")
	}
	if _, err := ParseErrorEnvelope(root, raw, httpStatus, now); err != nil {
		return &CredentialRefusal{Surface: ErrorSurface, Stage: StageSchema}
	}
	return nil
}
