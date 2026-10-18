package audit

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// allowedAttributeKeys is the explicit G-65 allowlist: the only keys an audit
// row's attributes may carry. A key is added here, in review, with the code
// that writes it; nothing is allowed by default. Credential material, request
// bodies, authorization codes, passwords and provider responses have no key.
var allowedAttributeKeys = map[string]struct{}{
	"reason_code":         {},
	"target_kind":         {},
	"target_id":           {},
	"role_key":            {},
	"action_key":          {},
	"resource_kind":       {},
	"resource_id":         {},
	"provider":            {},
	"client_service":      {},
	"changed_fields":      {},
	"previous_status":     {},
	"new_status":          {},
	"membership_revision": {},
	"policy_revision":     {},
	"credential_class":    {},
	"credential_id":       {},
	"display_prefix":      {},
}

// ValidateAttributes refuses an audit attribute map that has a key outside the
// allowlist or a value outside that key's closed rule: a member of the key's
// vocabulary, an id of the key's shape, or a whole number. Free text has no
// slot, so nothing a caller can type is stored. Violations are reported by key
// only; a refused value is never echoed.
func ValidateAttributes(attributes map[string]any) error {
	var problems []string
	for key, value := range attributes {
		if _, ok := allowedAttributeKeys[key]; !ok {
			problems = append(problems, fmt.Sprintf("%q is not an allowed audit attribute", key))
			continue
		}
		if !valueAllowed(key, value) {
			problems = append(problems, fmt.Sprintf("%q has a value outside its closed rule", key))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("%w: %s", ErrAttributeNotAllowed, strings.Join(problems, "; "))
}

func valueAllowed(key string, value any) bool {
	if vocab, ok := attributeVocabulary[key]; ok {
		switch v := value.(type) {
		case string:
			return inVocabulary(vocab, v)
		case []string:
			if len(v) == 0 {
				return false
			}
			for _, item := range v {
				if !inVocabulary(vocab, item) {
					return false
				}
			}
			return true
		}
		return false
	}
	if rule, ok := attributeIDRules[key]; ok {
		v, isString := value.(string)
		return isString && rule.MatchString(v)
	}
	if _, ok := attributeNumberKeys[key]; ok {
		switch value.(type) {
		case int, int32, int64:
			return true
		}
	}
	return false
}

func inVocabulary(vocab []string, v string) bool {
	for _, member := range vocab {
		if member == v {
			return true
		}
	}
	return false
}

// eventTypePattern is the shape of an audit event_type: lowercase dotted
// words ("organization.created").
var eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

func validateEventType(v string) error {
	if !eventTypePattern.MatchString(v) {
		return fmt.Errorf("%w: audit event_type must be lowercase dotted words", ErrAttributeNotAllowed)
	}
	return nil
}

// validateRequestID: a request id is a UUID the service generated, never text.
func validateRequestID(v string) error {
	if !uuidPattern.MatchString(v) {
		return fmt.Errorf("%w: audit request_id must be a UUID", ErrAttributeNotAllowed)
	}
	return nil
}

// attributeVocabulary is the closed value set of each enumerated attribute
// key. Free text has no slot: a value is one of these strings, or (for the
// id keys in attributeIDKeys) a UUID/registry id, or a number.
var attributeVocabulary = map[string][]string{
	"reason_code": {
		"role_granted", "role_revoked", "membership_added", "membership_removed", "membership_status_changed",
		"grant_created", "grant_revoked", "credential_issued", "credential_rotated", "credential_revoked",
		"session_revoked", "organization_created", "organization_updated", "principal_disabled",
		"principal_enabled", "identity_linked", "identity_unlinked", "policy_revision_activated",
		"entitlement_snapshot_applied", "denied_no_membership", "denied_no_grant", "denied_policy",
	},
	"target_kind": {
		"principal", "organization", "membership", "role_assignment", "resource_grant", "credential",
		"session", "external_identity", "service_account", "workload", "policy_revision", "entitlement_snapshot",
	},
	"resource_kind":    {"organization", "repository", "project", "team"},
	"previous_status":  {"active", "disabled", "revoked", "pending", "expired", "suspended"},
	"new_status":       {"active", "disabled", "revoked", "pending", "expired", "suspended"},
	"provider":         {"local", "github", "gitlab", "jira", "linear", "google", "microsoft", "saml", "oidc"},
	"client_service":   {"auth-service", "api", "web", "billing-edge", "worker", "acr-api", "acr-mcp"},
	"credential_class": {"user_access_token", "refresh_credential", "workload_token", "acr_client", "external_push", "service_account_key"},
	"changed_fields": {
		"role_key", "status", "display_name", "disabled_at", "expires_at", "revoked_at", "email_verified_at",
		"scopes", "audiences", "organization_id",
	},
}

// AttributeVocabulary returns a copy of the enumerated attribute vocabularies.
func AttributeVocabulary() map[string][]string {
	out := make(map[string][]string, len(attributeVocabulary))
	for k, v := range attributeVocabulary {
		out[k] = append([]string(nil), v...)
	}
	return out
}

// attributeIDRules are the id-valued keys and the rule each value must satisfy.
var attributeIDRules = map[string]*regexp.Regexp{
	"target_id":      uuidPattern,
	"credential_id":  uuidPattern,
	"resource_id":    regexp.MustCompile(`^(\*|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[a-z0-9][a-z0-9._-]*/[a-z0-9][a-z0-9._-]*)$`),
	"role_key":       regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,3}$`),
	"action_key":     regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){1,3}$`),
	"display_prefix": regexp.MustCompile(`^(fcacr|fcpush|svc|rt)_[A-Za-z0-9]{2,8}$`),
}

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// attributeNumberKeys take a whole number.
var attributeNumberKeys = map[string]struct{}{"membership_revision": {}, "policy_revision": {}}
