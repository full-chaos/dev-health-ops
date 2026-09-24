// Identity-alias resolution for search_people_response (api/services/
// people.py:398-449) -- ports:
//   - api/services/people_identity.py's load_identity_aliases/
//     person_id_for_identity/display_name_for_identity/parse_identity/
//     identities_for_person
//   - api/utils/identity_aliases.py's normalize_alias/build_reverse_alias_map
//
// Config-file reality (documented precedent:
// internal/queryapi/quadrant/identity.go's own package doc comment,
// internal/jobs/metrics/daily/repouser/identity.go): identity_mapping.yaml
// ships `identities: []` in every known deployment, so
// loadIdentityAliases returns an empty map in practice and every search
// result's canonical identity is just the raw identity ClickHouse
// returned. The full algorithm is still ported (not hardcoded to the
// empty case) so an org that populates the file gets the same behaviour
// Python would give it -- verified against a real populated file via this
// package's own golden fixture (see golden_test.go).
//
// This is a SEPARATE, unexported copy of the alias-map/display-name/
// identities-for-person functions internal/queryapi/quadrant/
// identity.go already carries for that package's own scope (person-scope
// quadrant resolution). The two packages port the same Python source
// files for two different routes; quadrant's copy is private to that
// package and this package needs its own display-name/
// identities-for-person shape (identitiesForPerson's
// [{provider, handle}] response shape) that quadrant's copy never builds
// (quadrant only ever needs a flat identity list for a SQL IN clause, via
// identityVariants). Sharing a single extracted package for THOSE is a
// reasonable follow-up, not attempted here to avoid touching quadrant's
// already-shipped, already-tested package for an unrelated ticket. The
// one function both packages must produce byte-identical output for --
// person_id_for_identity's md5 digest, an identity-derivation contract
// every reader of a person_id has to agree on -- is NOT duplicated here:
// search.go calls the exported quadrant.PersonIDForIdentity directly, so
// this binary has exactly one md5 call site for it.
package people

import (
	"os"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"
)

// defaultIdentityMappingPath ports load_identity_aliases's fallback path
// (people_identity.py:27-30) -- a repo-root-relative path, matching
// Python's own relative literal.
const defaultIdentityMappingPath = "src/dev_health_ops/config/identity_mapping.yaml"

type identityAliasEntry struct {
	Canonical string   `yaml:"canonical"`
	Aliases   []string `yaml:"aliases"`
}

type identityAliasFile struct {
	Identities []identityAliasEntry `yaml:"identities"`
}

// normalizeEmailIdentity ports people_identity.py's _norm_email (17-18).
func normalizeEmailIdentity(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// normalizeAlias ports utils/identity_aliases.py's normalize_alias (11-22).
func normalizeAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// loadIdentityAliases ports load_identity_aliases (people_identity.py:
// 21-54). Python module-caches this for the interpreter's lifetime
// (_ALIAS_CACHE); this port re-reads per call instead -- the file is
// static config, never hot-reloaded either way, so the only cost is a
// redundant file read on a code path (people search) that is not hot,
// same non-caching posture internal/queryapi/quadrant/identity.go's
// own loadIdentityAliases takes for the identical reason (also keeps a
// test that sets IDENTITY_MAPPING_PATH per case from seeing a stale
// result from an earlier case). A missing or unparsable file returns an
// empty map, same as Python's `except FileNotFoundError: payload = {}` (a
// YAML parse error has no Python equivalent to mirror -- config committed
// to the repo is assumed well-formed, same trust boundary Python's
// yaml.safe_load implicitly has).
func loadIdentityAliases() map[string][]string {
	path := os.Getenv("IDENTITY_MAPPING_PATH")
	if path == "" {
		path = defaultIdentityMappingPath
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is operator/repo config, not user input.
	if err != nil {
		return map[string][]string{}
	}
	var file identityAliasFile
	if err := yaml.Unmarshal(data, &file); err != nil {
		return map[string][]string{}
	}
	aliases := map[string][]string{}
	for _, entry := range file.Identities {
		canonical := entry.Canonical
		if canonical == "" {
			continue
		}
		canonicalNorm := normalizeEmailIdentity(canonical)
		if canonicalNorm == "" {
			canonicalNorm = strings.TrimSpace(canonical)
		}
		if canonicalNorm == "" {
			continue
		}
		if _, ok := aliases[canonicalNorm]; !ok {
			aliases[canonicalNorm] = []string{}
		}
		for _, alias := range entry.Aliases {
			aliasStr := strings.TrimSpace(alias)
			if aliasStr != "" {
				aliases[canonicalNorm] = append(aliases[canonicalNorm], aliasStr)
			}
		}
	}
	return aliases
}

// buildReverseAliasMap ports utils/identity_aliases.py's
// build_reverse_alias_map (identity_aliases.py:25-40).
func buildReverseAliasMap(aliases map[string][]string) map[string]string {
	reverse := map[string]string{}
	for canonical, aliasList := range aliases {
		for _, alias := range aliasList {
			if key := normalizeAlias(alias); key != "" {
				reverse[key] = canonical
			}
		}
	}
	return reverse
}

// pythonTitle ports Python's str.title(): uppercase the first letter of
// each maximal run of letters, lowercase the rest, where ANY non-letter
// (not just whitespace) starts a new "word" -- Go's strings.Title/
// cases.Title only break on whitespace, so displayNameForIdentity needs
// this rather than either. Verbatim copy of
// internal/queryapi/quadrant/identity.go's own pythonTitle (same
// Python builtin, same fix).
func pythonTitle(s string) string {
	var b strings.Builder
	prevLetter := false
	for _, r := range s {
		if unicode.IsLetter(r) {
			if prevLetter {
				b.WriteRune(unicode.ToLower(r))
			} else {
				b.WriteRune(unicode.ToUpper(r))
			}
			prevLetter = true
		} else {
			b.WriteRune(r)
			prevLetter = false
		}
	}
	return b.String()
}

// displayNameForIdentity ports display_name_for_identity (people_identity.py:
// 62-68).
func displayNameForIdentity(identity string) string {
	if idx := strings.Index(identity, "@"); idx != -1 {
		local := identity[:idx]
		local = strings.ReplaceAll(local, ".", " ")
		local = strings.ReplaceAll(local, "_", " ")
		return pythonTitle(local)
	}
	if idx := strings.Index(identity, ":"); idx != -1 {
		rest := identity[idx+1:]
		if rest != "" {
			return rest
		}
		return identity
	}
	return identity
}

// parseIdentity ports parse_identity (people_identity.py:71-77).
func parseIdentity(identity string) (provider, handle string) {
	if strings.Contains(identity, "@") {
		return "email", identity
	}
	if idx := strings.Index(identity, ":"); idx != -1 {
		provider, handle = identity[:idx], identity[idx+1:]
		if provider == "" {
			provider = "identity"
		}
		if handle == "" {
			handle = identity
		}
		return provider, handle
	}
	return "identity", identity
}

// PersonIdentity ports one entry of identities_for_person's return list
// -- also this package's JSON wire type for PersonSearchResult.Identities
// (PersonIdentity, api/models/schemas.py:361-363).
type PersonIdentity struct {
	Provider string `json:"provider"`
	Handle   string `json:"handle"`
}

// identitiesForPerson ports identities_for_person (people_identity.py:
// 80-94). Order is preserved exactly as Python builds it: identity first,
// then aliases in the order load_identity_aliases's YAML list gave them
// (a Go slice, not a map, so order is not at risk the way Go map
// iteration would be) -- de-duplicated on "<provider>:<handle>", same key
// Python's own `seen` set uses.
func identitiesForPerson(identity string, aliases []string) []PersonIdentity {
	seen := map[string]bool{}
	results := make([]PersonIdentity, 0, 1+len(aliases))
	items := make([]string, 0, 1+len(aliases))
	items = append(items, identity)
	items = append(items, aliases...)
	for _, item := range items {
		if item == "" {
			continue
		}
		provider, handle := parseIdentity(item)
		key := provider + ":" + handle
		if seen[key] {
			continue
		}
		seen[key] = true
		results = append(results, PersonIdentity{Provider: provider, Handle: handle})
	}
	return results
}

// identityVariants ports identity_variants (people_identity.py:97-109):
// the set of raw identity strings (the canonical identity, every alias,
// an email's local-part/lowercased form, and a "provider:handle"
// identity's bare handle) that all resolve to the same person -- fed
// into a SQL IN clause by resolveIdentityContext's callers
// (_identity_inputs, services/people.py:301-303). Iteration order over a
// Go map is not the Python set's insertion order, but the result only
// ever feeds a SQL IN list -- membership, not order, is the only thing
// that matters downstream. A separate copy of
// internal/queryapi/quadrant/identity.go's own identityVariants:
// same Python source, different package, same "repeat, don't couple for
// something this narrow" posture the rest of this file's doc comment
// already explains.
func identityVariants(identity string, aliases []string) []string {
	variants := map[string]bool{}
	if identity != "" {
		variants[identity] = true
	}
	for _, alias := range aliases {
		if alias != "" {
			variants[alias] = true
		}
	}
	if idx := strings.Index(identity, ":"); idx != -1 {
		if handle := identity[idx+1:]; handle != "" {
			variants[handle] = true
		}
	}
	if idx := strings.Index(identity, "@"); idx != -1 {
		if local := identity[:idx]; local != "" {
			variants[local] = true
		}
		variants[normalizeEmailIdentity(identity)] = true
	}
	out := make([]string, 0, len(variants))
	for v := range variants {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
