// Identity resolution for GET /api/v1/quadrant's person/developer scope,
// CHAOS-5550 (remaining scope) -- ports:
//   - api/services/people_identity.py's load_identity_aliases/
//     person_id_for_identity/display_name_for_identity/identity_variants
//   - api/utils/identity_aliases.py's normalize_alias/build_reverse_alias_map
//   - api/services/quadrant.py's _resolve_identity_variants (quadrant.py:
//     330-355)
//   - api/queries/people.py's resolve_person_identity/fetch_person_team_id,
//     backed by sql/people/person_lookup.sql and sql/people/person_team.sql
//     (inlined verbatim below -- this package has no SQL-file loader, same
//     posture as quadrant.go's own inlined queries)
//
// Config-file reality (documented precedent: internal/jobs/metrics/daily/
// repouser/identity.go's package doc comment): identity_mapping.yaml ships
// `identities: []` in every known deployment, so load_identity_aliases()
// returns an empty map in practice and this whole alias-reconciliation path
// degrades to "use the identity resolve_person_identity found, as-is". The
// full algorithm is still ported (not hardcoded to the empty case) so an org
// that populates the file gets the same behaviour Python would give it.
package quadrant

import (
	"context"
	"crypto/md5" //nolint:gosec // parity requirement: Python's person_id_for_identity uses hashlib.md5 as a non-security identifier hash, not for authentication.
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"unicode"

	"gopkg.in/yaml.v3"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
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

// normalizeEmailIdentity ports people_identity.py's _norm_email.
func normalizeEmailIdentity(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

// normalizeAlias ports utils/identity_aliases.py's normalize_alias.
func normalizeAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// loadIdentityAliases ports load_identity_aliases (people_identity.py:
// 21-51). Python module-caches this for the interpreter's lifetime
// (_ALIAS_CACHE); this port re-reads per call instead -- the file is static
// config, never hot-reloaded either way, so the only cost is a redundant
// file read on a code path (person-scope quadrant requests) that is not
// hot. A missing or unparsable file returns an empty map, same as Python's
// `except FileNotFoundError: payload = {}` (a YAML parse error has no
// Python equivalent to mirror -- config committed to the repo is assumed
// well-formed, same trust boundary Python's yaml.safe_load implicitly has).
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
// build_reverse_alias_map (identity_aliases.py:26-49).
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

// personIDForIdentity ports person_id_for_identity (people_identity.py:
// 57-59): an md5 hex digest, not a security hash -- it is this system's
// stable per-identity id, verbatim.
func personIDForIdentity(identity string) string {
	sum := md5.Sum([]byte(identity)) //nolint:gosec // parity requirement, see package doc comment.
	return hex.EncodeToString(sum[:])
}

// pythonTitle ports Python's str.title(): uppercase the first letter of
// each maximal run of letters, lowercase the rest, where ANY non-letter
// (not just whitespace) starts a new "word" -- Go's strings.Title/
// cases.Title only break on whitespace, so display_name_for_identity needs
// this rather than either.
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

// identityVariants ports identity_variants (people_identity.py:97-109).
// Iteration order over a Go map is not the Python set's insertion order,
// but the result only ever feeds a SQL `IN` list (fetch_person_team_id) --
// membership, not order, is the only thing that matters downstream.
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

// resolveIdentityVariants ports _resolve_identity_variants (services/
// quadrant.py:330-355): resolve scope_id (an md5 person id) to the set of
// raw identity strings that refer to the same person, via a ClickHouse
// lookup plus the (in-practice-empty, see package doc comment) alias
// config.
func resolveIdentityVariants(ctx context.Context, client QueryClient, personID, orgID string) ([]string, error) {
	aliases := loadIdentityAliases()
	reverse := buildReverseAliasMap(aliases)

	identity, err := resolvePersonIdentity(ctx, client, personID, orgID)
	if err != nil {
		return nil, err
	}
	if identity != "" {
		canonical, ok := reverse[normalizeAlias(identity)]
		if !ok {
			canonical = identity
		}
		aliasList := append([]string(nil), aliases[canonical]...)
		inList := false
		for _, a := range aliasList {
			if a == identity {
				inList = true
				break
			}
		}
		if !inList && identity != canonical {
			aliasList = append(aliasList, identity)
		}
		return identityVariants(canonical, aliasList), nil
	}

	for canonical, aliasList := range aliases {
		if personIDForIdentity(canonical) == personID {
			return identityVariants(canonical, aliasList), nil
		}
		for _, alias := range aliasList {
			if personIDForIdentity(alias) == personID {
				return identityVariants(canonical, aliasList), nil
			}
		}
	}
	return []string{}, nil
}

// resolvePersonIdentity ports resolve_person_identity (queries/people.py:
// 35-45) and its person_lookup.sql, inlined verbatim, with one declared
// fix: the reference's load_sql call bypasses dedup_from for this read
// (a duplicate identity/user_identity string collapses via UNION DISTINCT
// regardless of which physical row it came from, so the VALUE was never
// wrong), but user_metrics_daily is ReplacingMergeTree(computed_at)
// (migration 096) and this package's own class ruling is one dedup shape
// for every ReplacingMergeTree read, not a per-query exception -- so this
// branch reads FINAL too, matching work_item_user_metrics_daily's branch
// two lines below, which already carried it.
func resolvePersonIdentity(ctx context.Context, client QueryClient, personID, orgID string) (string, error) {
	const query = `
        WITH identities AS (
            SELECT identity_id AS identity
            FROM user_metrics_daily FINAL
            WHERE identity_id != ''
              AND org_id = {org_id:String}

            UNION DISTINCT

            SELECT user_identity AS identity
            FROM work_item_user_metrics_daily FINAL
            WHERE user_identity != ''
              AND org_id = {org_id:String}
        )
        SELECT
            identity AS identity_id
        FROM identities
        WHERE lower(hex(MD5(identity))) = {person_id:String}
        LIMIT 1
    `
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "person_id", Value: personID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", fmt.Errorf("quadrant: resolve_person_identity query: %w", err)
	}
	defer rows.Close()

	var identity string
	if rows.Next() {
		if err := rows.Scan(&identity); err != nil {
			return "", fmt.Errorf("quadrant: resolve_person_identity scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("quadrant: resolve_person_identity: %w", err)
	}
	return identity, nil
}

// fetchPersonTeamID ports fetch_person_team_id (queries/people.py:65-79)
// and its person_team.sql, inlined verbatim. "" (Python's None) means no
// active team was found for any of the given identity variants.
func fetchPersonTeamID(ctx context.Context, client QueryClient, identities []string, orgID string) (string, error) {
	filtered := make([]string, 0, len(identities))
	for _, id := range identities {
		if id != "" {
			filtered = append(filtered, id)
		}
	}
	if len(filtered) == 0 {
		return "", nil
	}

	const query = `
        SELECT
            team_id
        FROM (
            SELECT
                arrayJoin(team_ids) AS team_id,
                updated_at
            FROM identities FINAL
            WHERE (
                canonical_id IN {identities:Array(String)}
                OR email IN {identities:Array(String)}
                OR arrayExists(
                    identity -> (
                        (
                            startsWith(identity, 'github:')
                            AND has(
                                JSONExtract(provider_identities, 'github', 'Array(String)'),
                                replaceRegexpOne(identity, '^github:', '')
                            )
                        )
                        OR (
                            startsWith(identity, 'gitlab:')
                            AND has(
                                JSONExtract(provider_identities, 'gitlab', 'Array(String)'),
                                replaceRegexpOne(identity, '^gitlab:', '')
                            )
                        )
                        OR (
                            startsWith(identity, 'linear:')
                            AND has(
                                JSONExtract(provider_identities, 'linear', 'Array(String)'),
                                replaceRegexpOne(identity, '^linear:', '')
                            )
                        )
                        OR (
                            startsWith(identity, 'jira:accountid:')
                            AND has(
                                JSONExtract(provider_identities, 'jira', 'Array(String)'),
                                replaceRegexpOne(identity, '^jira:accountid:', '')
                            )
                        )
                        OR (
                            startsWith(identity, 'accountid:')
                            AND has(
                                JSONExtract(provider_identities, 'jira', 'Array(String)'),
                                replaceRegexpOne(identity, '^accountid:', '')
                            )
                        )
                    ),
                    {identities:Array(String)}
                )
            )
              AND org_id = {org_id:String}
              AND is_active = 1
        )
        WHERE team_id != ''
        ORDER BY updated_at DESC, team_id
        LIMIT 1
    `
	bindings := []dhclickhouse.Binding{
		{Name: "identities", Value: filtered},
		{Name: "org_id", Value: orgID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", fmt.Errorf("quadrant: fetch_person_team_id query: %w", err)
	}
	defer rows.Close()

	var teamID string
	if rows.Next() {
		if err := rows.Scan(&teamID); err != nil {
			return "", fmt.Errorf("quadrant: fetch_person_team_id scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("quadrant: fetch_person_team_id: %w", err)
	}
	return teamID, nil
}
