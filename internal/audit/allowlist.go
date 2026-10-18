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
const maxAttributeValueLen = 256

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
	if len(v) > maxAttributeValueLen {
		return fmt.Sprintf("%q is longer than %d bytes", key, maxAttributeValueLen)
	}
	for _, shape := range secretShapes {
		if shape.MatchString(v) {
			return fmt.Sprintf("%q holds a value shaped like a credential", key)
		}
	}
	return ""
}
