package oidc

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// profile is what the flow reads from the merged claims.
type profile struct {
	Email         string
	EmailVerified bool
	FullName      string
}

// mergeClaims is go-api's merge: userinfo first, then the ID token's claims
// over it. A userinfo document whose sub differs from the ID token's is
// refused by the caller before this runs.
func mergeClaims(userinfo, id map[string]any) map[string]any {
	merged := make(map[string]any, len(userinfo)+len(id))
	for k, v := range userinfo {
		merged[k] = v
	}
	for k, v := range id {
		merged[k] = v
	}
	return merged
}

// readProfile is go-api's claim mapping (mapAttributes, then the raw "email"
// claim). email_verified counts only as the boolean true or the string
// "true"; absent or anything else is unverified.
func readProfile(merged map[string]any, mapping map[string]string) profile {
	mapped := map[string]string{}
	for target, source := range mapping {
		if value, ok := merged[source].(string); ok {
			if trimmed := pythonparity.Strip(value); trimmed != "" {
				mapped[target] = trimmed
			}
		}
	}
	p := profile{Email: mapped["email"], FullName: mapped["full_name"]}
	if p.Email == "" {
		p.Email, _ = merged["email"].(string)
	}
	if p.FullName == "" {
		p.FullName = mapped["name"]
	}
	switch v := merged["email_verified"].(type) {
	case bool:
		p.EmailVerified = v
	case string:
		p.EmailVerified = v == "true"
	}
	return p
}

// emailDomain is the part after the last "@", or "" when there is none.
func emailDomain(email string) string {
	at := strings.LastIndex(email, "@")
	if at < 0 {
		return ""
	}
	return email[at+1:]
}
