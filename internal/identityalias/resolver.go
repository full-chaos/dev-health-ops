// Package identityalias ports providers/identity.py's IdentityResolver
// (load_identity_resolver/resolve/membership_facets) to Go, shared by every
// native team-catalog collector (GitHub/GitLab/Jira/Linear). Before this
// package existed, each collector's own *MembershipFacets helper hard-coded
// the assumption that alias_to_canonical is empty (true only because this
// deployment's checked-in src/dev_health_ops/config/identity_mapping.yaml
// ships `identities: []`) -- an org that actually populates that file would
// see Go's native collectors silently ignore its aliases while the retired
// Python bridge honoured them. This package closes that gap by reading the
// SAME file (via the SAME IDENTITY_MAPPING_PATH env var / default path) and
// reproducing resolve()'s exact candidate ladder and membership_facets()'s
// exact facet ordering, so a populated alias map behaves identically on
// both paths.
package identityalias

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultIdentityMappingPath mirrors identity.py's DEFAULT_IDENTITY_MAPPING_PATH:
// a repo-root-relative path, matching Python's own relative literal.
const DefaultIdentityMappingPath = "src/dev_health_ops/config/identity_mapping.yaml"

type aliasEntry struct {
	Canonical string   `yaml:"canonical"`
	Aliases   []string `yaml:"aliases"`
}

type aliasFile struct {
	Identities []aliasEntry `yaml:"identities"`
}

// Resolver mirrors providers/identity.py's IdentityResolver dataclass: a
// best-effort, config-driven cross-provider identity resolver.
//
// Strategy (identical to the Python docstring):
//   - Prefer email when present.
//   - Otherwise map known aliases (config-driven).
//   - Otherwise fall back to provider-qualified username (e.g. github:octocat).
type Resolver struct {
	AliasToCanonical map[string]string
}

// normKey mirrors identity.py's _norm_key: lowercase, then collapse any
// run of whitespace to a single space (Python's `" ".join(s.split())`
// after `.strip().lower()` -- str.split() with no argument already trims
// and collapses internal whitespace, so strings.Fields does the same job).
func normKey(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

// normEmail mirrors identity.py's _norm_email: lowercase and trim only --
// deliberately NOT whitespace-collapsing, unlike normKey.
func normEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// ProviderQualifiedIdentity mirrors identity.py's provider_qualified_identity:
// the stable, no-email facet both the work-item assignee ladder and team
// auto-import converge on (CHAOS-2609). Priority: username before
// account_id. Returns "" when neither is present (Python's None).
func ProviderQualifiedIdentity(provider, username, accountID string) string {
	if trimmed := strings.TrimSpace(username); trimmed != "" {
		return provider + ":" + trimmed
	}
	if trimmed := strings.TrimSpace(accountID); trimmed != "" {
		return provider + ":accountid:" + trimmed
	}
	return ""
}

// Load ports load_identity_resolver (providers/identity.py): reads path, or
// the IDENTITY_MAPPING_PATH env var when path is "", or
// DefaultIdentityMappingPath, tolerating a missing or unparsable file
// exactly like Python's `except FileNotFoundError: payload = {}` (an
// unparsable file is treated the same way -- config committed to the repo
// is assumed well-formed, the same trust boundary yaml.safe_load has).
func Load(path string) *Resolver {
	if path == "" {
		if env := os.Getenv("IDENTITY_MAPPING_PATH"); env != "" {
			path = env
		} else {
			path = DefaultIdentityMappingPath
		}
	}
	aliasToCanonical := map[string]string{}
	data, err := os.ReadFile(path) //nolint:gosec // path is operator/repo config, not user input.
	if err == nil {
		var file aliasFile
		if yaml.Unmarshal(data, &file) == nil {
			for _, entry := range file.Identities {
				if entry.Canonical == "" {
					continue
				}
				canonicalNorm := normEmail(entry.Canonical)
				if canonicalNorm == "" {
					canonicalNorm = strings.TrimSpace(entry.Canonical)
				}
				if canonicalNorm == "" {
					continue
				}
				aliasToCanonical[normKey(canonicalNorm)] = canonicalNorm
				for _, alias := range entry.Aliases {
					aliasNorm := normKey(alias)
					if aliasNorm == "" {
						continue
					}
					aliasToCanonical[aliasNorm] = canonicalNorm
				}
			}
		}
	}
	return &Resolver{AliasToCanonical: aliasToCanonical}
}

// LoadDefault loads from IDENTITY_MAPPING_PATH / DefaultIdentityMappingPath,
// re-reading the file on every call by design -- team-catalog collection
// runs once per sync per provider, not a hot path, so re-reading a small
// static config file costs nothing that matters. This mirrors
// cmd/query-api/internal/quadrant/identity.go's loadIdentityAliases, which
// takes the same non-caching posture for the identical reason: caching here
// would silently defeat IDENTITY_MAPPING_PATH-based test isolation (a test
// setting the env var per-case, as Python's own tests do, would otherwise
// see a stale resolver from an earlier case).
func LoadDefault() *Resolver {
	return Load("")
}

// Resolve mirrors IdentityResolver.resolve exactly:
//
//  1. If email is non-empty once normalized (lowercased, trimmed), look it
//     up in the alias map (keyed by the further whitespace-collapsed
//     normKey); return the mapped canonical if found, else the normalized
//     email itself. This branch short-circuits: a present email NEVER
//     falls through to the username/account_id/display_name ladder below,
//     even when it has no alias entry.
//  2. Otherwise, walk candidates in order -- "<provider>:<username>",
//     "<provider>:accountid:<accountID>", the bare username, the bare
//     display name -- and return the first one whose normKey has an alias
//     mapping.
//  3. Otherwise, fall back to ProviderQualifiedIdentity (username before
//     account_id), then the trimmed display name, then "unknown".
//
// An empty string for any of email/username/accountID/displayName means
// Python's None -- both are falsy in the same way here.
func (resolver *Resolver) Resolve(provider, email, username, accountID, displayName string) string {
	if email != "" {
		normalized := normEmail(email)
		if normalized != "" {
			if mapped, ok := resolver.AliasToCanonical[normKey(normalized)]; ok && mapped != "" {
				return mapped
			}
			return normalized
		}
	}

	candidates := make([]string, 0, 4)
	if username != "" {
		candidates = append(candidates, provider+":"+username)
	}
	if accountID != "" {
		candidates = append(candidates, provider+":accountid:"+accountID)
	}
	if username != "" {
		candidates = append(candidates, username)
	}
	if displayName != "" {
		candidates = append(candidates, displayName)
	}
	for _, candidate := range candidates {
		key := normKey(candidate)
		if key == "" {
			continue
		}
		if mapped, ok := resolver.AliasToCanonical[key]; ok && mapped != "" {
			return mapped
		}
	}

	if qualified := ProviderQualifiedIdentity(provider, username, accountID); qualified != "" {
		return qualified
	}
	if displayName != "" {
		if trimmed := strings.TrimSpace(displayName); trimmed != "" {
			return trimmed
		}
	}
	return "unknown"
}

// MembershipFacets mirrors IdentityResolver.membership_facets exactly:
// every identity a member could resolve to under this org's alias map, so
// both the work-item assignee ladder and team-catalog membership rows agree.
//
// Order (de-duplicated, "unknown"/empty dropped):
//  1. The no-email identity (Resolve with no email) -- what an assignee
//     WITHOUT an email resolves to.
//  2. ProviderQualifiedIdentity, as a robustness fallback (may equal #1).
//  3. If email is non-empty: Resolve with that email (alias-mapped if the
//     email itself is aliased), then the normalized (lowercased, trimmed)
//     raw email.
func (resolver *Resolver) MembershipFacets(provider, username, accountID, email string) []string {
	noEmailIdentity := resolver.Resolve(provider, "", username, accountID, "")
	qualified := ProviderQualifiedIdentity(provider, username, accountID)
	candidates := []string{noEmailIdentity, qualified}
	if email != "" {
		candidates = append(candidates, resolver.Resolve(provider, email, "", "", ""))
		candidates = append(candidates, normEmail(email))
	}
	facets := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate != "" && candidate != "unknown" && !containsString(facets, candidate) {
			facets = append(facets, candidate)
		}
	}
	return facets
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
