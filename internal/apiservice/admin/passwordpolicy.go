package admin

import (
	_ "embed"
	"strings"
	"sync"
	"unicode"
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
// lines dropped. The list is copied byte-for-byte from
// ops/src/dev_health_ops/data/common_passwords.txt (Go embeds its own copy
// rather than reading across the repo boundary at runtime) -- keep the two
// in sync if the Python list changes.
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

// validatePassword is password_policy.py's validate_password, same
// violation strings and same order (length checks, then letter, then
// digit, then common-password lookup).
func validatePassword(password string) []string {
	var violations []string

	length := len([]rune(password))
	if length < minPasswordLength {
		violations = append(violations, "Password must be at least 12 characters long")
	}
	if length > maxPasswordLength {
		violations = append(violations, "Password must be no more than 128 characters long")
	}

	hasLetter, hasDigit := false, false
	for _, r := range password {
		if unicode.IsLetter(r) {
			hasLetter = true
		}
		if unicode.IsDigit(r) {
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
