package repouser

import "strings"

// DefaultNormalizeIdentity mirrors normalize_git_identity (providers/identity.py)
// as production ACTUALLY calls it: job_daily.py always constructs a real
// IdentityResolver (`identity = load_identity_resolver()`, unconditional --
// there is no "no resolver" code path in production), so the resolver
// branch is the one that matters, not the resolver-is-None fallback this
// function mirrored in an earlier revision (a codex adversarial review
// caught the mismatch: that fallback never lowercases the email, but the
// resolver branch does).
//
// With a resolver present but its alias_to_canonical map EMPTY (the
// default -- identity_mapping.yaml ships `identities: []`; see the package
// doc comment for the alias-resolution gap this does not close), Python's
// resolver branch reduces to exactly this:
//
//	if email:
//	    normalized = email.strip().lower()
//	    if normalized:
//	        return normalized               # alias_to_canonical.get(key, normalized) with an empty map
//	if display_name:
//	    display_norm = display_name.strip()  # NOT lowercased
//	    if display_norm:
//	        return display_norm
//	return "unknown"
//
// Email wins over name whenever present, is lowercased; name is only
// trimmed. This is the piece that is ALWAYS active regardless of whether an
// org has configured aliases -- an org with mixed-case author emails in its
// git config diverges on every commit without this fix, not just when
// aliases are configured (which is the narrower, still-open gap this
// package's doc comment describes).
func DefaultNormalizeIdentity(authorEmail, authorName string) string {
	if trimmed := strings.TrimSpace(authorEmail); trimmed != "" {
		return strings.ToLower(trimmed)
	}
	if trimmed := strings.TrimSpace(authorName); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

// NoResolverNormalizeIdentity mirrors normalize_git_identity's resolver-IS-
// None branch verbatim: email wins over name, NEITHER is case-folded, only
// stripped. Use this ONLY for a call site whose Python counterpart genuinely
// never passes an identity_resolver -- compute_single_owner_file_ratio's
// real call site in job_daily.py is exactly that (unlike compute_daily_
// metrics, which always receives one). Reaching for DefaultNormalizeIdentity
// there instead would silently apply lowercasing Python's own call site
// never performs.
func NoResolverNormalizeIdentity(authorEmail, authorName string) string {
	if trimmed := strings.TrimSpace(authorEmail); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(authorName); trimmed != "" {
		return trimmed
	}
	return "unknown"
}
