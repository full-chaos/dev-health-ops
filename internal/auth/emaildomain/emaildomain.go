// Package emaildomain is the one allowed-domains compare of the federated
// sign-in paths.
package emaildomain

import "strings"

// Allowed reports whether domain is one of the configured allowed entries.
// Case differs only in ASCII letters (RFC 4343): both sides compare in ASCII
// lower case, and any other byte compares exactly. Only the configured
// entries are trimmed; the asserted domain is never trimmed, so a padded or
// empty domain is refused.
func Allowed(list []string, domain string) bool {
	if domain == "" {
		return false
	}
	want := asciiLower(domain)
	for _, entry := range list {
		if asciiLower(strings.TrimSpace(entry)) == want {
			return true
		}
	}
	return false
}

func asciiLower(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; 'A' <= c && c <= 'Z' {
			b := []byte(s)
			for j := i; j < len(b); j++ {
				if 'A' <= b[j] && b[j] <= 'Z' {
					b[j] += 'a' - 'A'
				}
			}
			return string(b)
		}
	}
	return s
}
