package teamsidentity

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	jiraInferPageSize   = 100 // JiraClient.per_page default
	jiraInferIssueLimit = 500 // iter_issues(limit=500) in JiraActivityInferenceService
)

// jiraStrptimeLayouts are the two strptime formats
// JiraActivityInferenceService._parse_jira_datetime falls back to when
// fromisoformat refuses a string: "%Y-%m-%dT%H:%M:%S.%f%z" and
// "%Y-%m-%dT%H:%M:%S%z". Python compiles them case-insensitively (so a
// lower-case "t" matches), with strptime's own field patterns: one- or
// two-digit month/day/hour/minute/second, a day that may carry a leading
// space, a fraction of one to six digits, and %z as Z or ±HH[:]MM[[:]SS[.f]].
var jiraStrptimeLayouts = []*regexp.Regexp{
	regexp.MustCompile(`(?i)^(\d\d\d\d)-(1[0-2]|0[1-9]|[1-9])-(3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])T(2[0-3]|[0-1]\d|\d):([0-5]\d|\d):(6[0-1]|[0-5]\d|\d)\.([0-9]{1,6})(Z|[+-]\d\d:?[0-5]\d(?::?[0-5]\d(?:\.\d{1,6})?)?)$`),
	regexp.MustCompile(`(?i)^(\d\d\d\d)-(1[0-2]|0[1-9]|[1-9])-(3[0-1]|[1-2]\d|0[1-9]|[1-9]| [1-9])T(2[0-3]|[0-1]\d|\d):([0-5]\d|\d):(6[0-1]|[0-5]\d|\d)()(Z|[+-]\d\d:?[0-5]\d(?::?[0-5]\d(?:\.\d{1,6})?)?)$`),
}

// jiraStrptime is datetime.strptime over those two layouts: an aware
// datetime, or nil where Python raises (an impossible date, a second of 60
// or 61, an offset of a day or more).
func jiraStrptime(text string) *pytime.DateTime {
	for _, layout := range jiraStrptimeLayouts {
		match := layout.FindStringSubmatch(text)
		if match == nil {
			continue
		}
		number := func(index int) int {
			value, _ := strconv.Atoi(strings.TrimSpace(match[index]))
			return value
		}
		year, month, day, hour, minute, second := number(1), number(2), number(3), number(4), number(5), number(6)
		micro := 0
		if fraction := match[7]; fraction != "" {
			micro, _ = strconv.Atoi((fraction + "000000")[:6])
		}
		if year < 1 || day > time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day() || second > 59 {
			return nil
		}
		offsetSeconds, offsetMicro := 0, 0
		if zone := match[8]; !strings.EqualFold(zone, "Z") {
			// _strptime's %z handling, step for step: a colon at index 3
			// must be matched by one at index 5 (else "Inconsistent use of
			// :"), the colons are dropped, and hours, minutes, seconds and
			// the fraction are sliced from fixed positions -- a slice that
			// is not digits (a stray ":" when index 3 held none) raises.
			z := zone
			if z[3] == ':' {
				z = z[:3] + z[4:]
				if len(z) > 5 {
					if z[5] != ':' {
						return nil
					}
					z = z[:5] + z[6:]
				}
			}
			slice := func(from, to int) string {
				if from >= len(z) {
					return ""
				}
				if to > len(z) {
					to = len(z)
				}
				return z[from:to]
			}
			atoi := func(text string) (int, bool) {
				if text == "" {
					return 0, true
				}
				for _, c := range text {
					if c < '0' || c > '9' {
						return 0, false
					}
				}
				value, _ := strconv.Atoi(text)
				return value, true
			}
			hours, okH := atoi(slice(1, 3))
			minutes, okM := atoi(slice(3, 5))
			seconds, okS := atoi(slice(5, 7))
			remainder := slice(8, len(z))
			fraction, okF := atoi((remainder + "000000")[:6])
			if !okH || !okM || !okS || !okF {
				return nil
			}
			offsetSeconds = hours*3600 + minutes*60 + seconds
			offsetMicro = fraction
			if z[0] == '-' {
				offsetSeconds, offsetMicro = -offsetSeconds, -offsetMicro
			}
			if total := int64(offsetSeconds)*1_000_000 + int64(offsetMicro); total <= -86400_000_000 || total >= 86400_000_000 {
				return nil
			}
		}
		wall := time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, time.UTC)
		instant := wall.Add(-time.Duration(offsetSeconds)*time.Second - time.Duration(offsetMicro)*time.Microsecond)
		return &pytime.DateTime{Time: instant, Aware: true, Offset: offsetSeconds, OffsetMicro: offsetMicro}
	}
	return nil
}

// parseJiraDatetime is JiraActivityInferenceService._parse_jira_datetime:
// every "Z" becomes "+00:00", then datetime.fromisoformat (Python 3.14's
// grammar, pytime.FromISOFormat), then the two strptime layouts; anything
// else, or a non-string, is None.
func parseJiraDatetime(raw pyjson.Value) *pytime.DateTime {
	text, ok := raw.(string)
	if !ok || text == "" {
		return nil
	}
	text = strings.ReplaceAll(text, "Z", "+00:00")
	if parsed, ok := pytime.FromISOFormat(text); ok {
		return &parsed
	}
	return jiraStrptime(text)
}

// inferredMember is InferredMember (schemas_flat.py:650).
type inferredMember struct {
	AccountID   string
	DisplayName *string
	Email       *string
	// displayRaw/emailRaw hold the payload values as Python keeps them
	// (any JSON type) until InferredMember's `str | None` validation.
	displayRaw pyjson.Value
	emailRaw   pyjson.Value
	Count      int
	Roles      map[string]bool
	LastActive *pytime.DateTime
}

func confidenceForCount(count int) string {
	switch {
	case count >= 5:
		return "core"
	case count >= 2:
		return "active"
	}
	return "peripheral"
}

// jiraSearchIssues is JiraClient.iter_issues(jql, fields, expand_changelog=
// False, limit=500): pages of 100 by startAt, or by nextPageToken when the
// page carries one, stopping at an empty page, isLast, or 500 issues.
func jiraSearchIssues(ctx context.Context, client *providerfoundation.HTTPClient, jql string) ([]*pyjson.Object, error) {
	var issues []*pyjson.Object
	startAt := 0
	token := ""
	for {
		values := url.Values{}
		values.Set("jql", jql)
		values.Set("maxResults", strconv.Itoa(jiraInferPageSize))
		if token != "" {
			values.Set("nextPageToken", token)
		} else {
			values.Set("startAt", strconv.Itoa(startAt))
		}
		values.Set("fields", "assignee,reporter,creator,comment")
		response, err := client.Do(ctx, "GET", "/rest/api/3/search/jql?"+values.Encode(), nil)
		if err != nil {
			return nil, err
		}
		raw, err := readBody(response)
		if err != nil {
			return nil, err
		}
		page, err := decodeObject(raw, "jira search page")
		if err != nil {
			return nil, err
		}
		list := listAt(page, "issues")
		if len(list) == 0 {
			return issues, nil
		}
		for _, item := range list {
			issue, ok := item.(*pyjson.Object)
			if !ok {
				return nil, fmt.Errorf("jira issue is not an object")
			}
			issues = append(issues, issue)
			if len(issues) >= jiraInferIssueLimit {
				return issues, nil
			}
		}
		if next, _ := page.Get("nextPageToken"); truthy(next) {
			token = pythonStr(next)
		} else {
			token = ""
			startAt += len(list)
		}
		if last, present := page.Get("isLast"); present && last == true {
			return issues, nil
		}
	}
}

// inferJiraMembers is JiraActivityInferenceService.infer_members.
func inferJiraMembers(ctx context.Context, credential providerfoundation.Credential, projectKey string, windowDays int64) ([]inferredMember, error) {
	client, err := providerfoundation.NewJiraClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, err
	}
	jql := fmt.Sprintf("project = '%s' AND updated >= '-%dd'", projectKey, windowDays)
	issues, err := jiraSearchIssues(ctx, client, jql)
	if err != nil {
		return nil, err
	}
	activity := map[string]*inferredMember{}
	var order []string
	var comparisonErr error
	touch := func(actor pyjson.Value, role string, updated *pytime.DateTime) {
		object, ok := actor.(*pyjson.Object)
		if !ok {
			return
		}
		idValue, _ := object.Get("accountId")
		if !truthy(idValue) {
			return
		}
		// Python keys the map by the raw value: a list or an object is
		// unhashable (TypeError, a 500 on the route).
		switch idValue.(type) {
		case []pyjson.Value, *pyjson.Object:
			comparisonErr = fmt.Errorf("unhashable type: accountId is %T", idValue)
			return
		}
		// Python keys the map by the raw value, so 12 and "12" stay apart.
		key := fmt.Sprintf("%T:%s", idValue, pythonStr(idValue))
		current := activity[key]
		displayName, _ := object.Get("displayName")
		email, _ := object.Get("emailAddress")
		if current == nil {
			current = &inferredMember{AccountID: pythonStr(idValue), displayRaw: displayName, emailRaw: email, Roles: map[string]bool{}}
			activity[key] = current
			order = append(order, key)
		}
		current.Count++
		current.Roles[role] = true
		if !truthy(current.displayRaw) && truthy(displayName) {
			current.displayRaw = displayName
		}
		if !truthy(current.emailRaw) && truthy(email) {
			current.emailRaw = email
		}
		if updated != nil {
			if current.LastActive == nil {
				current.LastActive = updated
			} else if current.LastActive.Aware != updated.Aware {
				comparisonErr = fmt.Errorf("can't compare offset-naive and offset-aware datetimes")
			} else if updated.Time.After(current.LastActive.Time) {
				current.LastActive = updated
			}
		}
	}
	for _, issue := range issues {
		fieldsValue, _ := issue.Get("fields")
		fields := pyjson.NewObject()
		if truthy(fieldsValue) {
			object, isObject := fieldsValue.(*pyjson.Object)
			if !isObject {
				return nil, fmt.Errorf("jira issue fields is %T, want an object", fieldsValue)
			}
			fields = object
		}
		updatedRaw, _ := fields.Get("updated")
		updated := parseJiraDatetime(updatedRaw)
		assignee, _ := fields.Get("assignee")
		reporter, _ := fields.Get("reporter")
		creator, _ := fields.Get("creator")
		touch(assignee, "assignee", updated)
		touch(reporter, "reporter", updated)
		touch(creator, "commenter", updated)
		if comparisonErr != nil {
			return nil, comparisonErr
		}
	}
	for _, key := range order {
		member := activity[key]
		var err error
		if member.DisplayName, err = optionalTextOf(member.displayRaw); err != nil {
			return nil, err
		}
		if member.Email, err = optionalTextOf(member.emailRaw); err != nil {
			return nil, err
		}
	}
	out := make([]inferredMember, 0, len(activity))
	for _, key := range order {
		out = append(out, *activity[key])
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].AccountID < out[j].AccountID
	})
	return out, nil
}

// optionalTextOf is InferredMember's `str | None` validation of an actor
// value: None stays None, a string is itself, anything else is the pydantic
// error Python raises (a 500 on the route).
func optionalTextOf(value pyjson.Value) (*string, error) {
	if value == nil {
		return nil, nil
	}
	text, ok := value.(string)
	if !ok {
		return nil, fmt.Errorf("inferred member text field is %T, want a string", value)
	}
	return &text, nil
}

func inferredMemberJSON(member inferredMember) *pyjson.Object {
	roles := make([]string, 0, len(member.Roles))
	for role := range member.Roles {
		roles = append(roles, role)
	}
	sort.Strings(roles)
	out := pyjson.NewObject()
	out.Set("account_id", member.AccountID)
	setOptionalString(out, "display_name", member.DisplayName)
	setOptionalString(out, "email", member.Email)
	out.Set("activity_count", int64(member.Count))
	out.Set("confidence", confidenceForCount(member.Count))
	out.Set("roles", stringsToValues(roles))
	if member.LastActive != nil {
		out.Set("last_active", pytime.Pydantic(*member.LastActive))
	} else {
		out.Set("last_active", nil)
	}
	return out
}

func inferResponseJSON(teamID, projectKey string, windowDays int64, members []inferredMember) *pyjson.Object {
	values := make([]pyjson.Value, len(members))
	for index, member := range members {
		values[index] = inferredMemberJSON(member)
	}
	out := pyjson.NewObject()
	out.Set("team_id", teamID)
	out.Set("project_key", projectKey)
	out.Set("window_days", windowDays)
	out.Set("inferred_members", values)
	out.Set("total", int64(len(members)))
	return out
}

// inferMembers is GET /teams/{team_id}/infer-members.
func (h handlers) inferMembers(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var problems pybody.Errors
	ge, le := int64(1), int64(365)
	windowDays, _ := problems.QueryInt("window_days", pybody.LastQuery(query, "window_days"), 90, &ge, &le)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	days := windowDays.Int64()
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
	projectKey := ""
	if len(team.ProjectKeys) > 0 {
		projectKey = team.ProjectKeys[0]
	}
	if projectKey == "" && !strings.Contains(teamID, ":") {
		projectKey = teamID
	}
	if projectKey == "" {
		policy.WriteDetail(w, http.StatusBadRequest, "Team does not have a Jira project key configured", nil)
		return
	}
	credential, ok := h.resolveMemberCredential(w, r, "jira", lastOrEmpty(query, "credential_id"), lastOrEmpty(query, "credential_name"))
	if !ok {
		return
	}
	jira, valid := jiraMemberCredential(credential)
	if !valid {
		policy.WriteDetail(w, http.StatusBadRequest, "Jira credentials require email, api_token, and url", nil)
		return
	}
	members, err := inferJiraMembers(ctx, jira, projectKey, days)
	if err != nil {
		h.internal(w, r, "infer team members", err)
		return
	}
	identities, err := h.store.ListIdentities(ctx, orgID, false)
	if err != nil {
		h.internal(w, r, "list identities", err)
		return
	}
	for index := range members {
		matched := findJiraIdentity(identities, members[index].AccountID)
		if matched == nil {
			continue
		}
		if (members[index].DisplayName == nil || *members[index].DisplayName == "") && matched.DisplayName != nil && *matched.DisplayName != "" {
			members[index].DisplayName = matched.DisplayName
		}
		if (members[index].Email == nil || *members[index].Email == "") && matched.Email != nil && *matched.Email != "" {
			members[index].Email = matched.Email
		}
	}
	policy.WriteJSON(w, http.StatusOK, inferResponseJSON(teamID, projectKey, days, members), nil)
}

func findJiraIdentity(identities []Identity, accountID string) *Identity {
	for index := range identities {
		if identities[index].ProviderIdentities == nil {
			continue
		}
		values, _ := identities[index].ProviderIdentities.Get("jira")
		for _, value := range values {
			if value == accountID {
				return &identities[index]
			}
		}
	}
	return nil
}
