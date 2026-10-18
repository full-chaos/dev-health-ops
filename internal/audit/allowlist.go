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

// maxAttributeValueLen bounds a string value. Long values are where pasted
// bodies and tokens live.
const maxAttributeValueLen = 128

// valueToken is the CLOSED value grammar: an identifier-shaped token with no
// whitespace, no "=", no quotes, no "@". Audit text is a code, an id or a name
// from a vocabulary, never free text, so "password=...", an email, a JSON body
// or a sentence cannot be stored whatever key carries it. (A pattern list of
// bad shapes can only ever be as complete as its author's imagination; this
// admits a shape instead of excluding some.)
var valueToken = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]*$`)

// ValidateToken reports whether v may be stored in an audit row's text field.
func ValidateToken(v string) error {
	if v == "" || len(v) > maxAttributeValueLen || !valueToken.MatchString(v) {
		return fmt.Errorf("%w: a text value must be a token of 1..%d characters from [A-Za-z0-9_.:/-] starting alphanumeric", ErrAttributeNotAllowed, maxAttributeValueLen)
	}
	for _, shape := range secretShapes {
		if shape.MatchString(v) {
			return fmt.Errorf("%w: a text value is shaped like a credential", ErrAttributeNotAllowed)
		}
	}
	return nil
}

// secretShapes are value shapes that must never appear in an audit row even
// under an allowed key: a JWT, an Authorization header value, a PEM block, or
// a platform credential with its secret part (a display prefix is short).
var secretShapes = []*regexp.Regexp{
	regexp.MustCompile(`eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]*`),
	regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}`),
	regexp.MustCompile(`-----BEGIN [A-Z ]*(PRIVATE KEY|CERTIFICATE)-----`),
	regexp.MustCompile(`\b(fcacr|fcpush|svc|rt)_[A-Za-z0-9_-]{16,}`),
}

// ValidateAttributes refuses an audit attribute map that has a key outside the
// allowlist, a non-scalar value, an over-long string, or a value shaped like a
// secret. It reports every violation, keyed by attribute name only: the value
// that tripped a rule is never echoed.
func ValidateAttributes(attributes map[string]any) error {
	var problems []string
	for key, value := range attributes {
		if _, ok := allowedAttributeKeys[key]; !ok {
			problems = append(problems, fmt.Sprintf("%q is not an allowed audit attribute", key))
			continue
		}
		switch v := value.(type) {
		case nil, bool, int, int32, int64, float64:
		case string:
			if p := stringProblem(key, v); p != "" {
				problems = append(problems, p)
			}
		case []string:
			for _, item := range v {
				if p := stringProblem(key, item); p != "" {
					problems = append(problems, p)
				}
			}
		default:
			problems = append(problems, fmt.Sprintf("%q has a value of type %T; only scalars and string lists are allowed", key, value))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	sort.Strings(problems)
	return fmt.Errorf("%w: %s", ErrAttributeNotAllowed, strings.Join(problems, "; "))
}

func stringProblem(key, v string) string {
	if err := ValidateToken(v); err != nil {
		return fmt.Sprintf("%q: %s", key, strings.TrimPrefix(err.Error(), ErrAttributeNotAllowed.Error()+": "))
	}
	return ""
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
