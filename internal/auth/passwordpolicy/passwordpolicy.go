// Package passwordpolicy is password_policy.py: the one password strength
// check every route that sets a password applies (the admin set-password
// route and self-registration).
package passwordpolicy

import (
	_ "embed"
	"strings"
	"sync"
	"unicode"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

//go:embed data/common_passwords.txt
var commonPasswordsFile string

const (
	minPasswordLength = 12
	maxPasswordLength = 128
)

var (
	commonPasswordsOnce sync.Once
	commonPasswords     map[string]struct{}
)

// loadCommonPasswords is password_policy.py's _load_common_passwords: one
// parse of the checked-in denylist, lowercased, comments (#) and blank
// lines dropped. The list is a byte-for-byte copy of
// src/dev_health_ops/data/common_passwords.txt, because `//go:embed` cannot
// reach outside the package directory; TestEmbeddedListMatchesThePythonOne
// fails when either side drifts.
func loadCommonPasswords() map[string]struct{} {
	commonPasswordsOnce.Do(func() {
		commonPasswords = map[string]struct{}{}
		for _, line := range strings.Split(commonPasswordsFile, "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "#") {
				continue
			}
			commonPasswords[strings.ToLower(trimmed)] = struct{}{}
		}
	})
	return commonPasswords
}

// Validate is password_policy.py's validate_password, same
// violation strings and same order (length checks, then letter, then
// digit, then common-password lookup).
func Validate(password string) []string {
	var violations []string

	length := len([]rune(password))
	if length < minPasswordLength {
		violations = append(violations, "Password must be at least 12 characters long")
	}
	if length > maxPasswordLength {
		violations = append(violations, "Password must be no more than 128 characters long")
	}

	// character.isalpha() has zero measured divergence from
	// unicode.IsLetter (both are Unicode category L*), so hasLetter needs
	// no parity helper. character.isdigit() is BROADER than
	// unicode.IsDigit (decimal-only): pythonparity.IsDigit is the one
	// canonical str.isdigit() -- e.g. "Abcdefghijk²" (U+00B2 SUPERSCRIPT
	// TWO) satisfies Python's digit requirement and unicode.IsDigit does
	// not.
	hasLetter, hasDigit := false, false
	for _, r := range password {
		if unicode.IsLetter(r) {
			hasLetter = true
		}
		if pythonparity.IsDigit(r) {
			hasDigit = true
		}
	}
	if !hasLetter {
		violations = append(violations, "Password must include at least one letter")
	}
	if !hasDigit {
		violations = append(violations, "Password must include at least one number")
	}

	if _, common := loadCommonPasswords()[strings.ToLower(password)]; common {
		violations = append(violations, "Password is too common")
	}

	return violations
}
