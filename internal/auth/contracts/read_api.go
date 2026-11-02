package contracts

import (
	"errors"
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

// Page size of a list read.
const (
	ReadDefaultLimit = 50
	ReadMaxLimit     = 100
)

// ReadOperation is one read of the versioned read API.
type ReadOperation struct {
	Name            string
	Route           string
	RequestSurface  string
	ResponseSurface string
	Scope           string
	List            bool
	IDPrefix        string
}

// ReadRequest is a parsed read request.
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
}

// ReadResponse is a parsed read response.
type ReadResponse struct {
	Operation  string
	IDs        []string
	NextCursor string
}

var errReadNotImplemented = errors.New("read api: not implemented")

// ReadOperations returns the operations of the read API.
func ReadOperations() []ReadOperation { return nil }

// ReadOperationByName returns one operation by name.
func ReadOperationByName(name string) (ReadOperation, bool) { return ReadOperation{}, false }

// ReadScopes returns the read scopes, sorted.
func ReadScopes() []string { return nil }

// ReadScopeAllows is true when held carries the read scope of the operation.
func ReadScopeAllows(held []string, operation string) bool { return true }

// EncodeKeysetCursor is the cursor of a page that ends at id.
func EncodeKeysetCursor(id string) string { return "" }

// DecodeKeysetCursor is the id a cursor names, which must carry idPrefix.
func DecodeKeysetCursor(cursor, idPrefix string) (string, error) {
	return "", errReadNotImplemented
}

// ParseReadRequest validates and parses the request body of an operation.
func ParseReadRequest(root, operation string, raw []byte) (*ReadRequest, error) {
	return nil, errReadNotImplemented
}

// ParseReadResponse validates and parses the response to a parsed request.
func ParseReadResponse(root string, req *ReadRequest, raw []byte) (*ReadResponse, error) {
	return nil, errReadNotImplemented
}

// ParseReadError validates and parses the error body of a read.
func ParseReadError(root string, raw []byte, httpStatus int, now time.Time) error {
	return errReadNotImplemented
}
