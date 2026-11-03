package authapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
)

const (
	authorizationRequestSchema      = "authorization_request.v1"
	authorizationBatchRequestSchema = "authorization_batch_request.v1"
	maxAuthorizationBatchItems      = 100
)

// The patterns and closed sets below are authorization_request.v1's, with the
// members it takes by reference (principal.v1 action, error.v1 request_id,
// endpoint-profile service). authorize_request_test.go holds the decoder and
// the schema to the same verdicts.
var (
	authorizeActionPattern        = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	authorizeRepositoryKeyPattern = regexp.MustCompile(`^([a-z0-9][a-z0-9._-]{0,99}/[a-z0-9][a-z0-9._-]{0,99}|\*)$`)
	authorizeResourceKeyPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,255}$`)
	authorizeRequestIDPattern     = regexp.MustCompile(`^req_[A-Za-z0-9_-]{26}$`)

	authorizeResourceTypes = []string{
		"organization", "repository", "membership", "service_account", "grant",
		"credential", "session", "signing_key", "subscription", "platform",
	}
	authorizeClientServices = []string{
		"dev-health-ops-api", "dev-health-ops-billing-edge", "dev-health-web", "dev-health-acr-api",
		"dev-health-acr-mcp", "dev-health-query-api", "dev-health-auth-service",
	}
)

// ErrAuthorizationRequestInvalid is wrapped by every refusal of the decoders.
// The error text names the member, never its value.
var ErrAuthorizationRequestInvalid = errors.New("authapi: invalid authorization request")

func invalidAuthorization(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrAuthorizationRequestInvalid, fmt.Sprintf(format, args...))
}

// AuthorizationQuestion is one decoded authorization_request.v1. It carries no
// identity: PrincipalReference is still to be resolved, and OrganizationID is
// the resource's organization as a hint the decision checks against the
// principal's memberships.
type AuthorizationQuestion struct {
	PrincipalReference string
	Action             string
	ResourceType       string
	ResourceKey        string
	OrganizationID     string
	RequestID          string
	ClientService      string
}

// DecodeAuthorizationRequest decodes one authorization_request.v1 body. Any
// member the contract does not define (a principal, an organization, roles,
// scopes or an actor the caller asserts), a repeated member, a member in
// another case, a wrong type or trailing data is refused.
func DecodeAuthorizationRequest(body []byte) (AuthorizationQuestion, error) {
	members, err := strictMembers(body)
	if err != nil {
		return AuthorizationQuestion{}, err
	}
	return questionFromMembers(members, "")
}

// DecodeAuthorizationBatchRequest decodes one authorization_batch_request.v1
// body: 1 to 100 requests, each held to DecodeAuthorizationRequest's rules.
func DecodeAuthorizationBatchRequest(body []byte) ([]AuthorizationQuestion, error) {
	members, err := strictMembers(body)
	if err != nil {
		return nil, err
	}
	if err := closedMembers(members, "", []string{"schema_version", "requests"}, []string{"schema_version", "requests"}); err != nil {
		return nil, err
	}
	if err := constString(members["schema_version"], "/schema_version", authorizationBatchRequestSchema); err != nil {
		return nil, err
	}
	items, err := strictArray(members["requests"], "/requests")
	if err != nil {
		return nil, err
	}
	if len(items) < 1 || len(items) > maxAuthorizationBatchItems {
		return nil, invalidAuthorization("/requests holds %d items, want 1 to %d", len(items), maxAuthorizationBatchItems)
	}
	out := make([]AuthorizationQuestion, 0, len(items))
	for i, item := range items {
		path := fmt.Sprintf("/requests/%d", i)
		itemMembers, err := strictMembers(item)
		if err != nil {
			return nil, invalidAuthorization("%s: %v", path, err)
		}
		q, err := questionFromMembers(itemMembers, path)
		if err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, nil
}

func questionFromMembers(members map[string]json.RawMessage, path string) (AuthorizationQuestion, error) {
	required := []string{"schema_version", "principal_reference", "action", "resource", "context"}
	if err := closedMembers(members, path, required, required); err != nil {
		return AuthorizationQuestion{}, err
	}
	if err := constString(members["schema_version"], path+"/schema_version", authorizationRequestSchema); err != nil {
		return AuthorizationQuestion{}, err
	}
	var q AuthorizationQuestion
	var err error
	if q.PrincipalReference, err = patternString(members["principal_reference"], path+"/principal_reference", principalReferencePattern); err != nil {
		return AuthorizationQuestion{}, err
	}
	if q.Action, err = patternString(members["action"], path+"/action", authorizeActionPattern); err != nil {
		return AuthorizationQuestion{}, err
	}
	resource, err := strictMembers(members["resource"])
	if err != nil {
		return AuthorizationQuestion{}, invalidAuthorization("%s/resource: %v", path, err)
	}
	if err := closedMembers(resource, path+"/resource", []string{"type", "key"}, []string{"type", "organization_id", "key"}); err != nil {
		return AuthorizationQuestion{}, err
	}
	if q.ResourceType, err = enumString(resource["type"], path+"/resource/type", authorizeResourceTypes); err != nil {
		return AuthorizationQuestion{}, err
	}
	keyPattern := authorizeResourceKeyPattern
	if q.ResourceType == "repository" {
		keyPattern = authorizeRepositoryKeyPattern
	}
	if q.ResourceKey, err = patternString(resource["key"], path+"/resource/key", keyPattern); err != nil {
		return AuthorizationQuestion{}, err
	}
	if raw, ok := resource["organization_id"]; ok {
		if q.OrganizationID, err = plainString(raw, path+"/resource/organization_id"); err != nil {
			return AuthorizationQuestion{}, err
		}
		if q.OrganizationID == "" {
			return AuthorizationQuestion{}, invalidAuthorization("%s/resource/organization_id is empty", path)
		}
	}
	context, err := strictMembers(members["context"])
	if err != nil {
		return AuthorizationQuestion{}, invalidAuthorization("%s/context: %v", path, err)
	}
	contextMembers := []string{"request_id", "client_service"}
	if err := closedMembers(context, path+"/context", contextMembers, contextMembers); err != nil {
		return AuthorizationQuestion{}, err
	}
	if q.RequestID, err = patternString(context["request_id"], path+"/context/request_id", authorizeRequestIDPattern); err != nil {
		return AuthorizationQuestion{}, err
	}
	if q.ClientService, err = enumString(context["client_service"], path+"/context/client_service", authorizeClientServices); err != nil {
		return AuthorizationQuestion{}, err
	}
	return q, nil
}

// strictMembers reads one JSON object: no repeated member, nothing after it.
// Member names are kept exactly as sent, so a member in another case is an
// unknown member, not an alias encoding/json would fold onto a field.
func strictMembers(raw []byte) (map[string]json.RawMessage, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, invalidAuthorization("not a JSON object")
	}
	members := map[string]json.RawMessage{}
	for dec.More() {
		keyTok, err := dec.Token()
		key, isString := keyTok.(string)
		if err != nil || !isString {
			return nil, invalidAuthorization("member name is not a string")
		}
		if _, seen := members[key]; seen {
			return nil, invalidAuthorization("member %q is repeated", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, invalidAuthorization("member %q is not JSON", key)
		}
		members[key] = value
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') {
		return nil, invalidAuthorization("object is not closed")
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, invalidAuthorization("data after the object")
	}
	return members, nil
}

func strictArray(raw json.RawMessage, path string) ([]json.RawMessage, error) {
	if len(raw) == 0 || raw[0] != '[' {
		return nil, invalidAuthorization("%s is not an array", path)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, invalidAuthorization("%s is not an array", path)
	}
	return items, nil
}

func closedMembers(members map[string]json.RawMessage, path string, required, allowed []string) error {
	for name := range members {
		if !slices.Contains(allowed, name) {
			return invalidAuthorization("%s has a member the contract does not define", pathOrRoot(path))
		}
	}
	for _, name := range required {
		if _, ok := members[name]; !ok {
			return invalidAuthorization("%s/%s is required", path, name)
		}
	}
	return nil
}

func pathOrRoot(path string) string {
	if path == "" {
		return "/"
	}
	return path
}

func plainString(raw json.RawMessage, path string) (string, error) {
	if len(raw) == 0 || raw[0] != '"' {
		return "", invalidAuthorization("%s is not a string", path)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", invalidAuthorization("%s is not a string", path)
	}
	return s, nil
}

func constString(raw json.RawMessage, path, want string) error {
	s, err := plainString(raw, path)
	if err != nil {
		return err
	}
	if s != want {
		return invalidAuthorization("%s is not %s", path, want)
	}
	return nil
}

func patternString(raw json.RawMessage, path string, pattern *regexp.Regexp) (string, error) {
	s, err := plainString(raw, path)
	if err != nil {
		return "", err
	}
	if !pattern.MatchString(s) {
		return "", invalidAuthorization("%s does not match its pattern", path)
	}
	return s, nil
}

func enumString(raw json.RawMessage, path string, set []string) (string, error) {
	s, err := plainString(raw, path)
	if err != nil {
		return "", err
	}
	if !slices.Contains(set, s) {
		return "", invalidAuthorization("%s is outside its closed set", path)
	}
	return s, nil
}
