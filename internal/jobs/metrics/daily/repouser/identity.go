package repouser

import "strings"

// DefaultNormalizeIdentity mirrors normalize_git_identity (providers/identity.py)
// with no IdentityResolver -- see the package doc comment for why this
// package never resolves configured aliases. Email wins over name; both
// fall back to "unknown".
func DefaultNormalizeIdentity(authorEmail, authorName string) string {
	if trimmed := strings.TrimSpace(authorEmail); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(authorName); trimmed != "" {
		return trimmed
	}
	return "unknown"
}
