package teamsidentity

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"sort"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

var (
	confirmProviderPattern = regexp.MustCompile(`^(github|gitlab|jira)$`)
	confirmActionPattern   = regexp.MustCompile(`^(link|create|skip)$`)
)

// confirmLink is one ConfirmMemberLink (schemas_flat.py:634).
type confirmLink struct {
	ProviderIdentity string
	Provider         string
	CanonicalID      string
	Action           string
}

// inferredAction is one ConfirmInferredMemberAction (schemas_flat.py:670).
type inferredAction struct {
	AccountID   string
	Action      string
	CanonicalID *string
	DisplayName *string
	Email       *string
}

// nestErrors re-roots the body-level locs pybody's helpers report under
// loc (body, <list>, <index>) so element errors read like pydantic's.
func nestErrors(element pybody.Errors, loc []pyjson.Value) pybody.Errors {
	out := make(pybody.Errors, 0, len(element))
	for _, e := range element {
		e.Loc = append(append([]pyjson.Value(nil), loc...), e.Loc[1:]...)
		out = append(out, e)
	}
	return out
}

// patternString validates a required `str` field carrying a pydantic
// `pattern`, reporting string_pattern_mismatch exactly as pydantic does.
func patternString(problems *pybody.Errors, object *pyjson.Object, name, pattern string, re *regexp.Regexp) (string, bool) {
	before := len(*problems)
	text, ok := problems.RequiredString(object, name, 0, 0)
	if !ok || len(*problems) != before {
		return "", false
	}
	if !re.MatchString(text) {
		ctx := pyjson.NewObject()
		ctx.Set("pattern", pattern)
		*problems = append(*problems, pybody.Error{
			Type: "string_pattern_mismatch", Loc: []pyjson.Value{"body", name},
			Msg: "String should match pattern '" + pattern + "'", Input: text, Ctx: ctx,
		})
		return "", false
	}
	return text, true
}

func parseConfirmMembersBody(object *pyjson.Object) (teamID string, links []confirmLink, problems pybody.Errors) {
	teamID, _ = problems.RequiredString(object, "team_id", 0, 0)
	raw, present := object.Get("links")
	switch {
	case !present:
		problems = append(problems, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", "links"}, Msg: "Field required", Input: object})
	default:
		list, isList := raw.([]pyjson.Value)
		if !isList {
			problems = append(problems, pybody.Error{Type: "list_type", Loc: []pyjson.Value{"body", "links"}, Msg: "Input should be a valid list", Input: raw})
			break
		}
		for index, item := range list {
			loc := []pyjson.Value{"body", "links", int64(index)}
			element, isObject := item.(*pyjson.Object)
			if !isObject {
				problems = append(problems, pybody.Error{Type: "model_attributes_type", Loc: loc, Msg: "Input should be a valid dictionary or object to extract fields from", Input: item})
				continue
			}
			var local pybody.Errors
			link := confirmLink{}
			link.ProviderIdentity, _ = local.RequiredString(element, "provider_identity", 0, 0)
			link.Provider, _ = patternString(&local, element, "provider", "^(github|gitlab|jira)$", confirmProviderPattern)
			link.CanonicalID, _ = local.RequiredString(element, "canonical_id", 0, 0)
			link.Action, _ = patternString(&local, element, "action", "^(link|create|skip)$", confirmActionPattern)
			if len(local) > 0 {
				problems = append(problems, nestErrors(local, loc)...)
				continue
			}
			links = append(links, link)
		}
	}
	return teamID, links, problems
}

func literalAddSkip(problems *pybody.Errors, object *pyjson.Object) (string, bool) {
	loc := []pyjson.Value{"body", "action"}
	raw, ok := object.Get("action")
	if !ok {
		*problems = append(*problems, pybody.Error{Type: "missing", Loc: loc, Msg: "Field required", Input: object})
		return "", false
	}
	if text, isString := raw.(string); isString && (text == "add" || text == "skip") {
		return text, true
	}
	ctx := pyjson.NewObject()
	ctx.Set("expected", "'add' or 'skip'")
	*problems = append(*problems, pybody.Error{Type: "literal_error", Loc: loc, Msg: "Input should be 'add' or 'skip'", Input: raw, Ctx: ctx})
	return "", false
}

func parseConfirmInferredBody(object *pyjson.Object) (teamID string, members []inferredAction, problems pybody.Errors) {
	teamID, _ = problems.RequiredString(object, "team_id", 0, 0)
	raw, present := object.Get("members")
	if !present {
		return teamID, nil, problems
	}
	list, isList := raw.([]pyjson.Value)
	if !isList {
		problems = append(problems, pybody.Error{Type: "list_type", Loc: []pyjson.Value{"body", "members"}, Msg: "Input should be a valid list", Input: raw})
		return teamID, nil, problems
	}
	for index, item := range list {
		loc := []pyjson.Value{"body", "members", int64(index)}
		element, isObject := item.(*pyjson.Object)
		if !isObject {
			problems = append(problems, pybody.Error{Type: "model_attributes_type", Loc: loc, Msg: "Input should be a valid dictionary or object to extract fields from", Input: item})
			continue
		}
		var local pybody.Errors
		member := inferredAction{}
		member.AccountID, _ = local.RequiredString(element, "account_id", 0, 0)
		member.Action, _ = literalAddSkip(&local, element)
		if value, ok := local.OptionalString(element, "canonical_id", 0, 0); ok {
			member.CanonicalID = &value
		}
		if value, ok := local.OptionalString(element, "display_name", 0, 0); ok {
			member.DisplayName = &value
		}
		if value, ok := local.OptionalString(element, "email", 0, 0); ok {
			member.Email = &value
		}
		if len(local) > 0 {
			problems = append(problems, nestErrors(local, loc)...)
			continue
		}
		members = append(members, member)
	}
	return teamID, members, problems
}

// batchOwnership is the two ownership checks both confirm routes run per
// actionable entry in pass 1: the stored owner of the provider identity
// (409 when it is a different canonical) and the intra-batch claim.
type batchOwnership struct {
	store Store
	orgID string
	seen  map[[2]string]string
}

// check returns a non-empty 409 detail on a conflict.
func (b *batchOwnership) check(ctx context.Context, canonicalID, provider, identityValue string) (string, error) {
	owner, err := b.store.FindIdentityByProviderIdentity(ctx, b.orgID, provider, identityValue)
	if err != nil {
		return "", err
	}
	if owner != nil && owner.CanonicalID != canonicalID {
		return providerIdentityConflictDetail(provider, identityValue, owner.CanonicalID), nil
	}
	return b.claim(canonicalID, provider, identityValue), nil
}

// claim records (provider, identity) -> canonical for the batch and returns
// a 409 detail when a different canonical already claimed it.
func (b *batchOwnership) claim(canonicalID, provider, identityValue string) string {
	key := [2]string{provider, identityValue}
	if prior, claimed := b.seen[key]; claimed && prior != canonicalID {
		return fmt.Sprintf("Provider identity '%s:%s' is claimed by two different canonical identities in the same request ('%s' and '%s')",
			provider, identityValue, prior, canonicalID)
	}
	b.seen[key] = canonicalID
	return ""
}

// linkedProviders is `dict(existing.provider_identities)` with `provider`'s
// list replaced by sorted({*existing, identity}).
func linkedProviders(existing *Identity, provider, identityValue string) *pybody.OrderedStringListDict {
	providers := pybody.NewOrderedStringListDict()
	var current []string
	if existing != nil && existing.ProviderIdentities != nil {
		for _, key := range existing.ProviderIdentities.Keys {
			values, _ := existing.ProviderIdentities.Get(key)
			providers.Set(key, append([]string(nil), values...))
		}
		current, _ = existing.ProviderIdentities.Get(provider)
	}
	providers.Set(provider, pySortedSet(current, identityValue))
	return providers
}

// pySortedSet is `sorted({*existing, extra})`: unlike sortedUnique it keeps
// an empty string, as a Python set does.
func pySortedSet(existing []string, extra string) []string {
	set := map[string]struct{}{extra: {}}
	for _, v := range existing {
		set[v] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func unionTeamIDs(existing *Identity, teamID string) []string {
	var current []string
	if existing != nil {
		current = existing.TeamIDs
	}
	return pySortedSet(current, teamID)
}

func confirmResponse(linked, created, skipped int) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("linked", int64(linked))
	out.Set("created", int64(created))
	out.Set("skipped", int64(skipped))
	return out
}

// confirmMembers is POST /teams/{team_id}/confirm-members.
func (h handlers) confirmMembers(w http.ResponseWriter, r *http.Request) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	bodyTeamID, links, parseProblems := parseConfirmMembersBody(object)
	if len(parseProblems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(parseProblems), nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)
	teamID := r.PathValue("team_id")
	if bodyTeamID != teamID {
		policy.WriteDetail(w, http.StatusBadRequest, "team_id mismatch between path and body", nil)
		return
	}
	team, err := h.store.GetTeam(ctx, orgID, teamID)
	if err != nil {
		h.internal(w, r, "get team", err)
		return
	}
	if team == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}

	// Pass 1: validate the whole batch with no write.
	skipped := 0
	ownership := &batchOwnership{store: h.store, orgID: orgID, seen: map[[2]string]string{}}
	var actionable []confirmLink
	for _, link := range links {
		if link.Action == "skip" {
			skipped++
			continue
		}
		if link.Action == "link" {
			existing, err := h.store.GetIdentity(ctx, orgID, link.CanonicalID)
			if err != nil {
				h.internal(w, r, "get identity", err)
				return
			}
			if existing == nil {
				policy.WriteDetail(w, http.StatusNotFound, "Identity '"+link.CanonicalID+"' not found", nil)
				return
			}
		}
		conflict, err := ownership.check(ctx, link.CanonicalID, link.Provider, link.ProviderIdentity)
		if err != nil {
			h.internal(w, r, "check provider identity ownership", err)
			return
		}
		if conflict != "" {
			policy.WriteDetail(w, http.StatusConflict, conflict, nil)
			return
		}
		actionable = append(actionable, link)
	}

	// Pass 2: apply.
	linked, created := 0, 0
	facets := map[string]bool{}
	for _, link := range actionable {
		existing, err := h.store.GetIdentity(ctx, orgID, link.CanonicalID)
		if err != nil {
			h.internal(w, r, "get identity", err)
			return
		}
		teamIDs := unionTeamIDs(existing, teamID)
		stored, err := h.store.CreateOrUpdateIdentity(ctx, orgID, IdentityWrite{
			CanonicalID:        link.CanonicalID,
			ProviderIdentities: linkedProviders(existing, link.Provider, link.ProviderIdentity),
			TeamIDs:            &teamIDs,
		})
		if err != nil {
			h.internal(w, r, "create or update identity", err)
			return
		}
		for facet := range storedFacets(stored) {
			facets[facet] = true
		}
		if link.Action == "link" {
			linked++
		} else {
			created++
		}
	}
	if len(facets) > 0 {
		if _, err := h.store.AddMembers(ctx, orgID, teamID, sortedKeysBool(facets)); err != nil {
			h.internal(w, r, "add members", err)
			return
		}
	}
	policy.WriteJSON(w, http.StatusOK, confirmResponse(linked, created, skipped), nil)
}

// confirmInferredMembers is POST /teams/{team_id}/confirm-inferred-members.
func (h handlers) confirmInferredMembers(w http.ResponseWriter, r *http.Request) {
	body := bodyFromContext(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	bodyTeamID, members, parseProblems := parseConfirmInferredBody(object)
	if len(parseProblems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(parseProblems), nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)
	teamID := r.PathValue("team_id")
	if bodyTeamID != teamID {
		policy.WriteDetail(w, http.StatusBadRequest, "team_id in path/body must match", nil)
		return
	}
	team, err := h.store.GetTeam(ctx, orgID, teamID)
	if err != nil {
		h.internal(w, r, "get team", err)
		return
	}
	if team == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}

	type actionableMember struct {
		member      inferredAction
		canonicalID string
	}
	skipped := 0
	ownership := &batchOwnership{store: h.store, orgID: orgID, seen: map[[2]string]string{}}
	var actionable []actionableMember
	for _, member := range members {
		if member.Action != "add" || member.AccountID == "" {
			skipped++
			continue
		}
		var canonicalID string
		// Python: `if member.canonical_id:` -- an empty string counts as
		// absent and mints jira:{account_id}.
		if member.CanonicalID != nil && *member.CanonicalID != "" {
			canonicalID = *member.CanonicalID
			existing, err := h.store.GetIdentity(ctx, orgID, canonicalID)
			if err != nil {
				h.internal(w, r, "get identity", err)
				return
			}
			if existing == nil {
				policy.WriteDetail(w, http.StatusNotFound, "Identity '"+canonicalID+"' not found", nil)
				return
			}
		} else {
			canonicalID = "jira:" + member.AccountID
		}
		conflict, err := ownership.check(ctx, canonicalID, "jira", member.AccountID)
		if err != nil {
			h.internal(w, r, "check provider identity ownership", err)
			return
		}
		if conflict != "" {
			policy.WriteDetail(w, http.StatusConflict, conflict, nil)
			return
		}
		actionable = append(actionable, actionableMember{member: member, canonicalID: canonicalID})
	}

	linked, created := 0, 0
	facets := map[string]bool{}
	for _, entry := range actionable {
		existing, err := h.store.GetIdentity(ctx, orgID, entry.canonicalID)
		if err != nil {
			h.internal(w, r, "get identity", err)
			return
		}
		teamIDs := unionTeamIDs(existing, teamID)
		displayName, email := entry.member.DisplayName, entry.member.Email
		// Fill-only: a stored non-empty value wins over the inferred one.
		if existing != nil {
			if existing.DisplayName != nil && *existing.DisplayName != "" {
				displayName = existing.DisplayName
			}
			if existing.Email != nil && *existing.Email != "" {
				email = existing.Email
			}
		}
		stored, err := h.store.CreateOrUpdateIdentity(ctx, orgID, IdentityWrite{
			CanonicalID:        entry.canonicalID,
			DisplayName:        displayName,
			Email:              email,
			ProviderIdentities: linkedProviders(existing, "jira", entry.member.AccountID),
			TeamIDs:            &teamIDs,
		})
		if err != nil {
			h.internal(w, r, "create or update identity", err)
			return
		}
		for facet := range storedFacets(stored) {
			facets[facet] = true
		}
		if entry.member.CanonicalID != nil && *entry.member.CanonicalID != "" {
			linked++
		} else {
			created++
		}
	}
	if len(facets) > 0 {
		keys := make([]string, 0, len(facets))
		for facet := range facets {
			keys = append(keys, facet)
		}
		sort.Strings(keys)
		if _, err := h.store.AddMembers(ctx, orgID, teamID, keys); err != nil {
			h.internal(w, r, "add members", err)
			return
		}
	}
	policy.WriteJSON(w, http.StatusOK, confirmResponse(linked, created, skipped), nil)
}
