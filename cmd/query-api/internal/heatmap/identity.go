// Identity resolution for the active_hours (individual) heatmap metric --
// ports:
//   - api/services/people_identity.py's load_identity_aliases/
//     identity_variants
//   - api/utils/identity_aliases.py's normalize_alias/build_reverse_alias_map
//   - api/services/heatmap.py's _resolve_identity_variants
//     (services/heatmap.py:191-217), a near-verbatim copy of
//     api/services/quadrant.py's own _resolve_identity_variants
//   - api/queries/people.py's resolve_person_identity, backed by
//     sql/people/person_lookup.sql, inlined verbatim below with the same
//     declared FINAL fix cmd/query-api/internal/quadrant/identity.go's
//     own resolvePersonIdentity and cmd/query-api/internal/people's own
//     copy already carry for the identical query
//
// Duplicated rather than imported from quadrant/people, matching this
// binary's own "repeat, don't couple" convention those two packages'
// doc comments already establish for this exact identity-resolution
// helper set -- the ONE piece that must stay a single call site across
// every package (person_id_for_identity's md5 digest) is NOT duplicated
// here: this package calls quadrant.PersonIDForIdentity directly.
package heatmap

import (
	"context"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/quadrant"
)

const defaultIdentityMappingPath = "src/dev_health_ops/config/identity_mapping.yaml"

type identityAliasEntry struct {
	Canonical string   `yaml:"canonical"`
	Aliases   []string `yaml:"aliases"`
}

type identityAliasFile struct {
	Identities []identityAliasEntry `yaml:"identities"`
}

func normalizeEmailIdentity(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func normalizeAlias(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

// loadIdentityAliases ports load_identity_aliases (people_identity.py:
// 21-51) -- see quadrant/identity.go's own copy for the config-file
// reality (identity_mapping.yaml ships `identities: []` in every known
// deployment) that makes this a practical no-op today.
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

// identityVariants ports identity_variants (people_identity.py:97-109).
// Iteration order over a Go map is not the Python set's insertion order,
// but the result only ever feeds a SQL IN list -- membership, not order,
// is the only thing that matters downstream.
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
// heatmap.py:191-217).
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
		if quadrant.PersonIDForIdentity(canonical) == personID {
			return identityVariants(canonical, aliasList), nil
		}
		for _, alias := range aliasList {
			if quadrant.PersonIDForIdentity(alias) == personID {
				return identityVariants(canonical, aliasList), nil
			}
		}
	}
	return []string{}, nil
}

// resolvePersonIdentity ports resolve_person_identity (queries/people.py:
// 35-45) and its person_lookup.sql, inlined verbatim, with the same
// declared FINAL fix quadrant/identity.go's own copy carries: the
// reference bypasses dedup_from for this read, but user_metrics_daily and
// work_item_user_metrics_daily are both ReplacingMergeTree(computed_at)
// (migration 096/055), so this branch reads both FINAL.
//
// NO LEADING WITH: this was also a `WITH identities AS (...)
// SELECT ...` CTE. dev-health-go's client-side read-only guard
// (clickhouse/client.go's validateReadOnlyStatement) requires a
// statement's FIRST token to be the literal "SELECT", so a query
// beginning "WITH ..." is rejected before it ever reaches ClickHouse
// (ErrUnsafeStatement, "clickhouse runtime: unsafe statement") -- the
// swallowed cause of the heatmap 503 for the individual/developer scope.
// Same fix shape as quadrant/identity.go's own resolvePersonIdentity: the
// "identities" CTE is referenced exactly once (in the outer FROM), so
// inlining it as an ordinary derived-table subquery is a purely
// mechanical, semantically identical rewrite.
func resolvePersonIdentity(ctx context.Context, client QueryClient, personID, orgID string) (string, error) {
	query := fmt.Sprintf(`
        SELECT
            identity AS identity_id
        FROM (
            SELECT identity_id AS identity
            FROM user_metrics_daily FINAL
            WHERE identity_id != ''
              AND org_id = {org_id:String}

            UNION DISTINCT

            SELECT user_identity AS identity
            FROM work_item_user_metrics_daily FINAL
            WHERE user_identity != ''
              AND org_id = {org_id:String}
        ) AS identities
        WHERE lower(hex(MD5(identity))) = {person_id:String}
        LIMIT 1
        %s
    `, settingsMaxExecutionTime())
	bindings := []dhclickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "person_id", Value: personID},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return "", fmt.Errorf("heatmap: resolve_person_identity query: %w", err)
	}
	defer rows.Close()

	var identity string
	if rows.Next() {
		if err := rows.Scan(&identity); err != nil {
			return "", fmt.Errorf("heatmap: resolve_person_identity scan: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("heatmap: resolve_person_identity: %w", err)
	}
	return identity, nil
}
