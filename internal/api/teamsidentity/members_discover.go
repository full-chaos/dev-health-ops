package teamsidentity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// memberCredentialID names the transient credentials the member routes
// build from a resolved row (the id is required by the client constructors,
// and no sync unit exists to name).
const memberCredentialID = "admin-members"

// discoveredMember is DiscoveredMember (schemas_flat.py:609).
type discoveredMember struct {
	ProviderType     string
	ProviderIdentity string
	DisplayName      *string
	Email            *string
	Role             *string
}

// memberMatch is MemberMatchResult (schemas_flat.py:617).
type memberMatch struct {
	Discovered       discoveredMember
	Status           string
	Matched          *Identity
	Confidence       *float64
	SuggestionReason *string
}

// optionalStringField is one `str | None` model field read off a provider
// payload object: absent or null is None, a string is itself, anything else
// is the pydantic validation error Python's DiscoveredMember(...) raises
// (an unhandled 500 on the route).
func optionalStringField(object *pyjson.Object, key string) (*string, error) {
	raw, ok := object.Get(key)
	if !ok || raw == nil {
		return nil, nil
	}
	text, isString := raw.(string)
	if !isString {
		return nil, fmt.Errorf("provider field %q is %T, want a string", key, raw)
	}
	return &text, nil
}

func decodeObject(raw []byte, what string) (*pyjson.Object, error) {
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", what, err)
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		return nil, fmt.Errorf("decode %s: not a JSON object", what)
	}
	return object, nil
}

func decodeObjectList(raw []byte, what string) ([]*pyjson.Object, error) {
	value, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", what, err)
	}
	list, ok := value.([]pyjson.Value)
	if !ok {
		return nil, fmt.Errorf("decode %s: not a JSON list", what)
	}
	out := make([]*pyjson.Object, 0, len(list))
	for _, item := range list {
		object, ok := item.(*pyjson.Object)
		if !ok {
			return nil, fmt.Errorf("decode %s: element is not a JSON object", what)
		}
		out = append(out, object)
	}
	return out, nil
}

func readBody(response *http.Response) ([]byte, error) {
	defer response.Body.Close()
	return io.ReadAll(response.Body)
}

var pyQuoteUnreserved = regexp.MustCompile(`[A-Za-z0-9_.~-]`)

// pyQuoteNoSafe is urllib.parse.quote(value, safe=""): every UTF-8 byte
// outside the unreserved set is percent-encoded.
func pyQuoteNoSafe(value string) string {
	var out strings.Builder
	for _, b := range []byte(value) {
		if pyQuoteUnreserved.Match([]byte{b}) {
			out.WriteByte(b)
		} else {
			fmt.Fprintf(&out, "%%%02X", b)
		}
	}
	return out.String()
}

// githubUserURLPath is where PyGithub completes a partial NamedUser: the
// user's own "url" field. The API host is fixed, so only its path is used.
func githubUserURLPath(entry *pyjson.Object, login string) string {
	if raw, ok := entry.Get("url"); ok {
		if text, isString := raw.(string); isString {
			if parsed, err := url.Parse(text); err == nil && parsed.Path != "" {
				return parsed.Path
			}
		}
	}
	return "/users/" + pyQuoteNoSafe(login)
}

// discoverMembersGitHub mirrors TeamMembershipService.discover_members_github:
// the team (completed), its member list (paged, per_page=100), then -- because PyGithub's
// NamedUser completes lazily and a team-member list entry carries neither
// `name` nor `email` -- one GET per member on the first attribute the list
// entry lacks.
func discoverMembersGitHub(ctx context.Context, token secrets.Value, orgName, teamSlug string) ([]discoveredMember, error) {
	credential := providerfoundation.NewCredential("github", memberCredentialID, nil, map[string]secrets.Value{"token": token})
	client, err := providerfoundation.NewGitHubClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	// PyGithub's get_team_by_slug builds a Team from its URL and completes it
	// at once (GET /orgs/{org}/teams/{slug}); the member list is then
	// requested at the team's own "url" from that payload, not at the path
	// the slug came in on. A missing team fails here, as it does in Python.
	detailPath := "/orgs/" + orgName + "/teams/" + pyQuoteNoSafe(teamSlug)
	response, err := client.Do(ctx, "GET", detailPath, nil)
	if err != nil {
		return nil, err
	}
	rawDetail, err := readBody(response)
	if err != nil {
		return nil, err
	}
	detail, err := decodeObject(rawDetail, "github team")
	if err != nil {
		return nil, err
	}
	teamPath := detailPath
	if raw, ok := detail.Get("url"); ok {
		if text, isString := raw.(string); isString {
			if parsed, parseErr := url.Parse(text); parseErr == nil && parsed.Path != "" {
				teamPath = parsed.Path
			}
		}
	}
	var entries []*pyjson.Object
	next := teamPath + "/members?per_page=100"
	for next != "" {
		response, err := client.Do(ctx, "GET", next, nil)
		if err != nil {
			return nil, err
		}
		link := response.Header.Get("Link")
		raw, err := readBody(response)
		if err != nil {
			return nil, err
		}
		page, err := decodeObjectList(raw, "github team members page")
		if err != nil {
			return nil, err
		}
		entries = append(entries, page...)
		next = githubNextLink(link)
	}
	members := make([]discoveredMember, 0, len(entries))
	for _, entry := range entries {
		loginValue, _ := entry.Get("login")
		login, _ := loginValue.(string)
		source := entry
		completed := false
		attribute := func(key string) (*string, error) {
			if _, present := source.Get(key); !present && !completed {
				response, err := client.Do(ctx, "GET", githubUserURLPath(entry, login), nil)
				if err != nil {
					return nil, err
				}
				raw, err := readBody(response)
				if err != nil {
					return nil, err
				}
				if source, err = decodeObject(raw, "github user"); err != nil {
					return nil, err
				}
				completed = true
			}
			return optionalStringField(source, key)
		}
		name, err := attribute("name")
		if err != nil {
			return nil, err
		}
		email, err := attribute("email")
		if err != nil {
			return nil, err
		}
		members = append(members, discoveredMember{ProviderType: "github", ProviderIdentity: login, DisplayName: name, Email: email})
	}
	return members, nil
}

// discoverMembersGitLab mirrors discover_members_gitlab: the group by path,
// then its members (per_page=100, every page, following the Link header the
// way python-gitlab does); an entry without a username is dropped.
func discoverMembersGitLab(ctx context.Context, token secrets.Value, baseURL, groupPath string) ([]discoveredMember, error) {
	config := map[string]string{}
	if baseURL != "" {
		config["url"] = baseURL
	}
	credential := providerfoundation.NewCredential("gitlab", memberCredentialID, config, map[string]secrets.Value{"token": token})
	client, err := providerfoundation.NewGitLabClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	group, err := gitlabGetGroup(ctx, client, groupPath)
	if err != nil {
		return nil, err
	}
	var entries []*pyjson.Object
	next := "/api/v4/groups/" + strconv.FormatInt(group.ID, 10) + "/members?per_page=100"
	for next != "" {
		response, err := client.Do(ctx, "GET", next, nil)
		if err != nil {
			return nil, err
		}
		link := response.Header.Get("Link")
		raw, err := readBody(response)
		if err != nil {
			return nil, err
		}
		page, err := decodeObjectList(raw, "gitlab group members page")
		if err != nil {
			return nil, err
		}
		entries = append(entries, page...)
		next = githubNextLink(link)
	}
	members := make([]discoveredMember, 0, len(entries))
	for _, entry := range entries {
		identity := ""
		if raw, ok := entry.Get("username"); ok {
			identity = pythonStr(raw)
		}
		name, err := optionalStringField(entry, "name")
		if err != nil {
			return nil, err
		}
		email, err := optionalStringField(entry, "email")
		if err != nil {
			return nil, err
		}
		var role *string
		if raw, ok := entry.Get("access_level"); ok {
			if text := pythonStr(raw); text != "" {
				role = &text
			}
		}
		if identity == "" {
			continue
		}
		members = append(members, discoveredMember{ProviderType: "gitlab", ProviderIdentity: identity, DisplayName: name, Email: email, Role: role})
	}
	return members, nil
}

const linearTeamMembersQuery = `
query TeamMembers($teamId: String!, $first: Int!, $after: String) {
  team(id: $teamId) {
    members(first: $first, after: $after) {
      nodes {
        id
        name
        email
        active
      }
      pageInfo {
        hasNextPage
        endCursor
      }
    }
  }
}
`

func linearGraphQL(ctx context.Context, client *providerfoundation.HTTPClient, query string, variables map[string]any) (*pyjson.Object, error) {
	body, err := json.Marshal(linearGraphQLRequest{Query: query, Variables: variables})
	if err != nil {
		return nil, err
	}
	response, err := client.Do(ctx, "POST", "/graphql", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	raw, err := readBody(response)
	if err != nil {
		return nil, err
	}
	payload, err := decodeObject(raw, "linear response")
	if err != nil {
		return nil, err
	}
	if errs, ok := payload.Get("errors"); ok && truthy(errs) {
		return nil, fmt.Errorf("linear graphql error: %s", pythonStr(errs))
	}
	data, _ := payload.Get("data")
	dataObject, _ := data.(*pyjson.Object)
	if dataObject == nil {
		dataObject = pyjson.NewObject()
	}
	return dataObject, nil
}

func objectAt(object *pyjson.Object, key string) *pyjson.Object {
	raw, _ := object.Get(key)
	nested, _ := raw.(*pyjson.Object)
	if nested == nil {
		return pyjson.NewObject()
	}
	return nested
}

func listAt(object *pyjson.Object, key string) []pyjson.Value {
	raw, _ := object.Get(key)
	list, _ := raw.([]pyjson.Value)
	return list
}

func linearNodeMembers(nodes []pyjson.Value) ([]discoveredMember, error) {
	var members []discoveredMember
	for _, item := range nodes {
		node, ok := item.(*pyjson.Object)
		if !ok {
			return nil, errors.New("linear member node is not an object")
		}
		if active, present := node.Get("active"); present {
			if flag, isBool := active.(bool); isBool && !flag {
				continue
			}
		}
		email, err := optionalStringField(node, "email")
		if err != nil {
			return nil, err
		}
		var identity string
		if email != nil && *email != "" {
			identity = *email
		} else if id, present := node.Get("id"); present && truthy(id) {
			identity = pythonStr(id)
		}
		if identity == "" {
			continue
		}
		name, err := optionalStringField(node, "name")
		if err != nil {
			return nil, err
		}
		members = append(members, discoveredMember{ProviderType: "linear", ProviderIdentity: identity, DisplayName: name, Email: email})
	}
	return members, nil
}

// discoverMembersLinear mirrors discover_members_linear: walk the teams
// query until the team whose key matches (a "linear:" prefix is stripped),
// take its inline member page, and fetch the full member list only when
// that page says more exist.
func discoverMembersLinear(ctx context.Context, apiKey secrets.Value, teamKey string) ([]discoveredMember, error) {
	credential := providerfoundation.NewCredential("linear", memberCredentialID, nil, map[string]secrets.Value{"api_key": apiKey})
	client, err := providerfoundation.NewLinearClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	normalized := strings.TrimPrefix(teamKey, "linear:")
	cursor := ""
	for {
		variables := map[string]any{"first": linearTeamsPerPage}
		if cursor != "" {
			variables["after"] = cursor
		}
		data, err := linearGraphQL(ctx, client, linearTeamsQuery, variables)
		if err != nil {
			return nil, err
		}
		teams := objectAt(data, "teams")
		for _, item := range listAt(teams, "nodes") {
			team, ok := item.(*pyjson.Object)
			if !ok {
				return nil, errors.New("linear team node is not an object")
			}
			if key, _ := team.Get("key"); key != normalized {
				continue
			}
			membersPage := objectAt(team, "members")
			nodes := listAt(membersPage, "nodes")
			if hasNext, _ := objectAt(membersPage, "pageInfo").Get("hasNextPage"); truthy(hasNext) {
				teamID, _ := team.Get("id")
				fetched, err := linearAllTeamMembers(ctx, client, strings.TrimSpace(strOrEmpty(teamID)))
				if err != nil {
					return nil, err
				}
				nodes = fetched
			}
			return linearNodeMembers(nodes)
		}
		pageInfo := objectAt(teams, "pageInfo")
		if hasNext, _ := pageInfo.Get("hasNextPage"); !truthy(hasNext) {
			return []discoveredMember{}, nil
		}
		endCursor, _ := pageInfo.Get("endCursor")
		cursor, _ = endCursor.(string)
	}
}

// linearAllTeamMembers is LinearClient.get_team_members: every page,
// inactive users dropped (a missing `active` counts as active).
func linearAllTeamMembers(ctx context.Context, client *providerfoundation.HTTPClient, teamID string) ([]pyjson.Value, error) {
	var members []pyjson.Value
	cursor := ""
	for {
		variables := map[string]any{"teamId": teamID, "first": linearTeamsPerPage}
		if cursor != "" {
			variables["after"] = cursor
		}
		data, err := linearGraphQL(ctx, client, linearTeamMembersQuery, variables)
		if err != nil {
			return nil, err
		}
		page := objectAt(objectAt(data, "team"), "members")
		for _, item := range listAt(page, "nodes") {
			node, ok := item.(*pyjson.Object)
			if !ok {
				return nil, errors.New("linear member node is not an object")
			}
			active, present := node.Get("active")
			if !present || truthy(active) {
				members = append(members, item)
			}
		}
		pageInfo := objectAt(page, "pageInfo")
		if hasNext, _ := pageInfo.Get("hasNextPage"); !truthy(hasNext) {
			return members, nil
		}
		endCursor, _ := pageInfo.Get("endCursor")
		cursor, _ = endCursor.(string)
	}
}

// discoverMembersJira mirrors discover_members_jira: the project's lead is
// the only member Jira's project endpoint exposes.
func discoverMembersJira(ctx context.Context, credential providerfoundation.Credential, projectKey string) ([]discoveredMember, error) {
	client, err := providerfoundation.NewJiraClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	response, err := client.Do(ctx, "GET", "/rest/api/3/project/"+projectKey, nil)
	if err != nil {
		return nil, err
	}
	raw, err := readBody(response)
	if err != nil {
		return nil, err
	}
	payload, err := decodeObject(raw, "jira project")
	if err != nil {
		return nil, err
	}
	lead := objectAt(payload, "lead")
	identity := ""
	for _, key := range []string{"accountId", "emailAddress", "displayName"} {
		if value, _ := lead.Get(key); truthy(value) {
			identity = pythonStr(value)
			break
		}
	}
	if identity == "" {
		return []discoveredMember{}, nil
	}
	name, err := optionalStringField(lead, "displayName")
	if err != nil {
		return nil, err
	}
	email, err := optionalStringField(lead, "emailAddress")
	if err != nil {
		return nil, err
	}
	role := "lead"
	return []discoveredMember{{ProviderType: "jira", ProviderIdentity: identity, DisplayName: name, Email: email, Role: &role}}, nil
}

// matchMembers is TeamMembershipService._match_members_clickhouse over the
// org's active identities: provider identity exact (1.0), then email exact
// (0.95), then display-name similarity >= 0.8 (difflib ratio, rounded to 2
// places), else unmatched.
func matchMembers(members []discoveredMember, candidates []Identity) ([]memberMatch, error) {
	out := make([]memberMatch, 0, len(members))
	for _, member := range members {
		if match := matchByProviderIdentity(member, candidates); match != nil {
			one := 1.0
			out = append(out, memberMatch{Discovered: member, Status: "matched", Matched: match, Confidence: &one})
			continue
		}
		if member.Email != nil && *member.Email != "" {
			if match := matchByEmail(*member.Email, candidates); match != nil {
				score, reason := 0.95, "email_match"
				out = append(out, memberMatch{Discovered: member, Status: "suggested", Matched: match, Confidence: &score, SuggestionReason: &reason})
				continue
			}
		}
		if member.DisplayName != nil && *member.DisplayName != "" {
			var best *Identity
			bestScore := 0.0
			memberName := pythonparity.Lower(*member.DisplayName)
			for index := range candidates {
				candidate := &candidates[index]
				if candidate.DisplayName == nil || *candidate.DisplayName == "" {
					continue
				}
				score := pythonparity.SequenceRatio(memberName, pythonparity.Lower(*candidate.DisplayName))
				if score > bestScore {
					bestScore, best = score, candidate
				}
			}
			if best != nil && bestScore >= 0.8 {
				rounded, err := pythonparity.Round(bestScore, 2)
				if err != nil {
					return nil, err
				}
				reason := "display_name_similarity"
				out = append(out, memberMatch{Discovered: member, Status: "suggested", Matched: best, Confidence: &rounded, SuggestionReason: &reason})
				continue
			}
		}
		out = append(out, memberMatch{Discovered: member, Status: "unmatched"})
	}
	return out, nil
}

func matchByProviderIdentity(member discoveredMember, candidates []Identity) *Identity {
	for index := range candidates {
		if candidates[index].ProviderIdentities == nil {
			continue
		}
		values, _ := candidates[index].ProviderIdentities.Get(member.ProviderType)
		for _, value := range values {
			if value == member.ProviderIdentity {
				return &candidates[index]
			}
		}
	}
	return nil
}

func matchByEmail(email string, candidates []Identity) *Identity {
	for index := range candidates {
		if candidates[index].Email != nil && *candidates[index].Email == email {
			return &candidates[index]
		}
	}
	return nil
}

func discoveredMemberJSON(member discoveredMember) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("provider_type", member.ProviderType)
	out.Set("provider_identity", member.ProviderIdentity)
	setOptionalString(out, "display_name", member.DisplayName)
	setOptionalString(out, "email", member.Email)
	setOptionalString(out, "role", member.Role)
	return out
}

func memberMatchJSON(match memberMatch) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("discovered", discoveredMemberJSON(match.Discovered))
	out.Set("match_status", match.Status)
	if match.Matched != nil {
		out.Set("matched_identity", identityJSON(*match.Matched))
	} else {
		out.Set("matched_identity", nil)
	}
	if match.Confidence != nil {
		out.Set("confidence", *match.Confidence)
	} else {
		out.Set("confidence", nil)
	}
	setOptionalString(out, "suggestion_reason", match.SuggestionReason)
	return out
}

// membersDiscoverJSON is TeamMembersDiscoverResponse.
func membersDiscoverJSON(teamID, provider string, matches []memberMatch) *pyjson.Object {
	values := make([]pyjson.Value, len(matches))
	for index, match := range matches {
		values[index] = memberMatchJSON(match)
	}
	out := pyjson.NewObject()
	out.Set("team_id", teamID)
	out.Set("provider", provider)
	out.Set("members", values)
	out.Set("total", int64(len(matches)))
	return out
}

// resolveMemberCredential is the credential step both member routes share
// with team discovery: 409 on an ambiguous match, 404 when none resolves.
// It writes the response itself and reports whether the caller may go on.
func (h handlers) resolveMemberCredential(w http.ResponseWriter, r *http.Request, provider, credentialID, credentialName string) (providerfoundation.Credential, bool) {
	ctx := r.Context()
	credential, err := h.credentials.resolve(ctx, orgIDOf(ctx), provider, credentialID, credentialName)
	if err != nil {
		var ambiguous *providerfoundation.CredentialAmbiguousError
		switch {
		case errors.As(err, &ambiguous):
			policy.WriteDetail(w, http.StatusConflict, ambiguous.Error(), nil)
		case errors.Is(err, providerfoundation.ErrCredentialNotFound):
			policy.WriteDetail(w, http.StatusNotFound, fmt.Sprintf("No credentials found for provider '%s'", provider), nil)
		default:
			h.internal(w, r, "resolve member credential", err)
		}
		return providerfoundation.Credential{}, false
	}
	return credential, true
}

// jiraMemberCredential is `jira_credentials_from_mapping({**config,
// **decrypted})` for the member routes: the merged credential, or ok=false
// when the Jira client refuses it (email, api_token or url missing).
func jiraMemberCredential(credential providerfoundation.Credential) (providerfoundation.Credential, bool) {
	merged := mergeJiraConfigIntoSecrets(credential)
	if _, err := providerfoundation.NewJiraClient(merged, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{}); err != nil {
		return providerfoundation.Credential{}, false
	}
	return merged, true
}

func lastOrEmpty(query url.Values, name string) string {
	if value := pybody.LastQuery(query, name); value != nil {
		return *value
	}
	return ""
}

// discoverMembers is GET /teams/{team_id}/discover-members.
func (h handlers) discoverMembers(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	providerValue := pybody.LastQuery(query, "provider")
	if providerValue == nil {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, discoverMissingProviderDetail(), nil)
		return
	}
	provider := *providerValue
	if !discoverProviderPattern.MatchString(provider) {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, discoverInvalidProviderDetail(provider), nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)
	teamID := r.PathValue("team_id")
	team, err := h.store.GetTeam(ctx, orgID, teamID)
	if err != nil {
		h.internal(w, r, "get team", err)
		return
	}
	if team == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Team not found", nil)
		return
	}
	credential, ok := h.resolveMemberCredential(w, r, provider, lastOrEmpty(query, "credential_id"), lastOrEmpty(query, "credential_name"))
	if !ok {
		return
	}

	var members []discoveredMember
	switch provider {
	case "github":
		token, hasToken := credential.Secret("token")
		org := credential.Config["org"]
		if !hasToken || !token.Configured() || org == "" {
			policy.WriteDetail(w, http.StatusBadRequest, "GitHub credentials require token and config.org", nil)
			return
		}
		members, err = discoverMembersGitHub(ctx, token, org, strings.TrimPrefix(teamID, "gh:"))
	case "gitlab":
		token, hasToken := credential.Secret("token")
		groupPath := strings.TrimPrefix(teamID, "gl:")
		if !hasToken || !token.Configured() || groupPath == "" {
			policy.WriteDetail(w, http.StatusBadRequest, "GitLab credentials require token and team provider path", nil)
			return
		}
		members, err = discoverMembersGitLab(ctx, token, credential.Config["url"], groupPath)
	case "linear":
		key, hasKey := credential.Secret("apiKey")
		if !hasKey || !key.Configured() {
			key, hasKey = credential.Secret("api_key")
		}
		if !hasKey || !key.Configured() {
			policy.WriteDetail(w, http.StatusBadRequest, "Linear credentials require apiKey", nil)
			return
		}
		members, err = discoverMembersLinear(ctx, key, teamID)
	default:
		projectKey := teamID
		if index := strings.Index(projectKey, ":"); index >= 0 {
			projectKey = projectKey[index+1:]
		}
		if projectKey == "" && len(team.ProjectKeys) > 0 {
			projectKey = team.ProjectKeys[0]
		}
		jira, valid := jiraMemberCredential(credential)
		if !valid || projectKey == "" {
			policy.WriteDetail(w, http.StatusBadRequest, "Jira credentials require email, api_token, url, and project key", nil)
			return
		}
		members, err = discoverMembersJira(ctx, jira, projectKey)
	}
	if err != nil {
		h.internal(w, r, "discover team members", err)
		return
	}

	candidates, err := h.store.ListIdentities(ctx, orgID, true)
	if err != nil {
		h.internal(w, r, "list identities", err)
		return
	}
	matches, err := matchMembers(members, candidates)
	if err != nil {
		h.internal(w, r, "match members", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, membersDiscoverJSON(teamID, provider, matches), nil)
}
