package datahealth

import (
	"context"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

const observedIdentitiesSQL = `
SELECT provider, identity, display_name, sum(observed_count) AS observed_count
FROM (
    SELECT 'git' AS provider, lower(author_email) AS identity,
           any(author_name) AS display_name, count() AS observed_count
    FROM git_commits
    WHERE org_id = {org_id:String} AND lower(author_email) != ''
    GROUP BY identity
    UNION ALL
    SELECT provider, arrayJoin(assignees) AS identity,
           identity AS display_name, count() AS observed_count
    FROM work_items FINAL
    WHERE org_id = {org_id:String} AND has(assignees, '') = 0
    GROUP BY provider, identity
)
GROUP BY provider, identity, display_name
ORDER BY observed_count DESC
LIMIT 100`

const mappedIdentitiesSQL = `
SELECT canonical_id, email, display_name, provider_identities, team_ids
FROM identities FINAL
WHERE org_id = {org_id:String} AND is_active = 1`

type observedIdentity struct {
	provider    *string
	identity    *string
	displayName *string
	count       uint64
}

// mappedIdentity is one active identity of the org.
type mappedIdentity struct {
	canonicalID        string
	email              string
	displayName        string
	providerIdentities [][]string
	teamIDs            []string
}

type unmapped struct {
	provider    string
	email       *string
	displayName *string
	count       int
}

// IdentityMapping ports resolve_identity_mapping: identities seen in commits
// and work items that no active identity of the org accounts for, with alias
// suggestions from a matching e-mail local part. team narrows the identities
// that count as mapped to those without team scope or on that team.
func (r *Reader) IdentityMapping(ctx context.Context, orgID, team string) *model.IdentityMappingHealth {
	observed := r.observedIdentities(ctx, orgID)
	mapped := r.mappedIdentities(ctx, orgID, team)
	keys := identityKeys(mapped)

	list := make([]unmapped, 0, len(observed))
	for _, row := range observed {
		identity := unmappedIdentity(row)
		if !isMapped(identity, keys) {
			list = append(list, identity)
		}
	}
	sortStable(list, func(a, b unmapped) bool { return a.count > b.count })

	suggestions := aliasSuggestions(list, mapped)
	if len(suggestions) > maxAliasSuggestions {
		suggestions = suggestions[:maxAliasSuggestions]
	}
	shown := list
	if len(shown) > maxUnmappedIdentities {
		shown = shown[:maxUnmappedIdentities]
	}
	return &model.IdentityMappingHealth{
		UnmappedCount:      len(list),
		UnmappedIdentities: toModelIdentities(shown),
		SuggestedAliases:   suggestions,
	}
}

func (r *Reader) observedIdentities(ctx context.Context, orgID string) []observedIdentity {
	var out []observedIdentity
	r.queryRows(ctx, "identity_observed", observedIdentitiesSQL,
		[]clickhouse.Binding{{Name: "org_id", Value: orgID}},
		func(rows clickhouse.RowScanner) error {
			var row observedIdentity
			if err := rows.Scan(&row.provider, &row.identity, &row.displayName, &row.count); err != nil {
				return err
			}
			out = append(out, row)
			return nil
		})
	return out
}

func (r *Reader) mappedIdentities(ctx context.Context, orgID, team string) []mappedIdentity {
	var out []mappedIdentity
	r.queryRows(ctx, "identity_mapped", mappedIdentitiesSQL,
		[]clickhouse.Binding{{Name: "org_id", Value: orgID}},
		func(rows clickhouse.RowScanner) error {
			var canonical string
			var email, display *string
			var providerJSON string
			var teamIDs []string
			if err := rows.Scan(&canonical, &email, &display, &providerJSON, &teamIDs); err != nil {
				return err
			}
			row := mappedIdentity{canonicalID: canonical, providerIdentities: decodeProviderIdentities(providerJSON), teamIDs: teamIDs}
			if email != nil {
				row.email = *email
			}
			if display != nil {
				row.displayName = *display
			}
			out = append(out, row)
			return nil
		})
	if team == "" {
		return out
	}
	scoped := make([]mappedIdentity, 0, len(out))
	for _, identity := range out {
		if len(identity.teamIDs) == 0 || containsString(identity.teamIDs, team) {
			scoped = append(scoped, identity)
		}
	}
	return scoped
}

func containsString(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// decodeProviderIdentities ports _decode_provider_identities for the JSON text
// the column holds: an object of lists, values rendered with str(). Anything
// else decodes to no identities.
func decodeProviderIdentities(text string) [][]string {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	decoded, ok := decodeOrdered(text)
	if !ok {
		return nil
	}
	object, ok := decoded.(*orderedMap)
	if !ok || len(object.keys) == 0 {
		return nil
	}
	out := make([][]string, 0, len(object.keys))
	for _, value := range object.vals {
		out = append(out, providerValues(value))
	}
	return out
}

func providerValues(value any) []string {
	list, ok := value.([]any)
	if !ok {
		return nil
	}
	values := make([]string, 0, len(list))
	for _, item := range list {
		values = append(values, pyStr(item))
	}
	return values
}

// norm ports _norm: strip, lowercase, collapse whitespace runs.
func norm(value string) string {
	return strings.Join(pyFields(pythonparity.Lower(pyStrip(value))), " ")
}

func identityKeys(mapped []mappedIdentity) map[string]struct{} {
	keys := map[string]struct{}{}
	for _, row := range mapped {
		for _, value := range []string{row.canonicalID, row.email, row.displayName} {
			if value != "" {
				keys[norm(value)] = struct{}{}
			}
		}
		for _, values := range row.providerIdentities {
			for _, value := range values {
				if value != "" {
					keys[norm(value)] = struct{}{}
				}
			}
		}
	}
	return keys
}

func unmappedIdentity(row observedIdentity) unmapped {
	identity := deref(row.identity)
	var email *string
	if strings.Contains(identity, "@") {
		e := identity
		email = &e
	}
	display := deref(row.displayName)
	if display == "" {
		display = identity
	}
	var displayName *string
	if display != "" {
		displayName = &display
	}
	provider := deref(row.provider)
	if provider == "" {
		provider = "unknown"
	}
	return unmapped{provider: provider, email: email, displayName: displayName, count: int(row.count)}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func isMapped(identity unmapped, keys map[string]struct{}) bool {
	for _, value := range []*string{identity.email, identity.displayName} {
		if value != nil && *value != "" {
			if _, ok := keys[norm(*value)]; ok {
				return true
			}
		}
	}
	return false
}

// emailLocal ports _email_local: the normalised text before "@", or the whole
// normalised value when it has none; empty means none.
func emailLocal(value string) string {
	normalised := norm(value)
	if i := strings.Index(normalised, "@"); i >= 0 {
		return normalised[:i]
	}
	return normalised
}

func aliasSuggestions(list []unmapped, mapped []mappedIdentity) []model.AliasSuggestion {
	byLocal := map[string]mappedIdentity{}
	for _, row := range mapped {
		for _, value := range []string{row.email, row.canonicalID} {
			if local := emailLocal(value); local != "" {
				if _, seen := byLocal[local]; !seen {
					byLocal[local] = row
				}
			}
		}
	}
	suggestions := []model.AliasSuggestion{}
	for _, identity := range list {
		local := ""
		if identity.email != nil {
			local = emailLocal(*identity.email)
		}
		if local == "" && identity.displayName != nil {
			local = emailLocal(*identity.displayName)
		}
		target, ok := byLocal[local]
		if local == "" || !ok {
			continue
		}
		item := toModelIdentity(identity)
		suggestions = append(suggestions, model.AliasSuggestion{
			UnmappedIdentity:     &item,
			SuggestedCanonicalID: target.canonicalID,
			Confidence:           aliasConfidence,
		})
	}
	return suggestions
}

func toModelIdentity(identity unmapped) model.UnmappedIdentity {
	count := identity.count
	return model.UnmappedIdentity{
		Provider:      identity.provider,
		Email:         identity.email,
		DisplayName:   identity.displayName,
		ObservedCount: &count,
	}
}

func toModelIdentities(list []unmapped) []model.UnmappedIdentity {
	out := make([]model.UnmappedIdentity, 0, len(list))
	for _, identity := range list {
		out = append(out, toModelIdentity(identity))
	}
	return out
}
