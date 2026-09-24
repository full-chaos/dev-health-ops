package teamsidentity

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	jiraInferPageSize   = 100 // JiraClient.per_page default
	jiraInferIssueLimit = 500 // iter_issues(limit=500) in JiraActivityInferenceService
)

// jiraStamp is a parsed Jira timestamp: an aware datetime keeps its own
// offset, a naive one (no offset in the text) has none -- Python refuses to
// order the two, which the route surfaces as a 500.
type jiraStamp struct {
	Time  time.Time
	Aware bool
}

// parseJiraDatetime is JiraActivityInferenceService._parse_jira_datetime:
// every "Z" becomes "+00:00", then ISO 8601 text of the shapes Jira and
// datetime.fromisoformat share is parsed (YYYY-MM-DD or YYYYMMDD, a single
// separator character, HH[:MM[:SS[.f]]] or the compact form, an optional
// +HH[:MM[:SS]] / +HHMM offset); anything else is None. ISO week dates,
// whitespace before the offset and the other exotic fromisoformat grammars
// are not parsed (Jira emits none of them).
func parseJiraDatetime(raw pyjson.Value) *jiraStamp {
	text, ok := raw.(string)
	if !ok || text == "" {
		return nil
	}
	text = strings.ReplaceAll(text, "Z", "+00:00")
	runes := []rune(text)
	var date time.Time
	var rest []rune
	digits := func(s []rune, n int) (int, bool) {
		if len(s) < n {
			return 0, false
		}
		value := 0
		for _, c := range s[:n] {
			if c < '0' || c > '9' {
				return 0, false
			}
			value = value*10 + int(c-'0')
		}
		return value, true
	}
	var year, month, day int
	switch {
	case len(runes) >= 10 && runes[4] == '-' && runes[7] == '-':
		y, ok1 := digits(runes[0:], 4)
		m, ok2 := digits(runes[5:], 2)
		d, ok3 := digits(runes[8:], 2)
		if !ok1 || !ok2 || !ok3 {
			return nil
		}
		year, month, day, rest = y, m, d, runes[10:]
	case len(runes) >= 8:
		y, ok1 := digits(runes[0:], 4)
		m, ok2 := digits(runes[4:], 2)
		d, ok3 := digits(runes[6:], 2)
		if !ok1 || !ok2 || !ok3 {
			return nil
		}
		year, month, day, rest = y, m, d, runes[8:]
	default:
		return nil
	}
	if month < 1 || month > 12 || day < 1 || year < 1 {
		return nil
	}
	date = time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
	if date.Day() != day {
		return nil
	}
	hour, minute, second, micro := 0, 0, 0, 0
	aware := false
	offsetSeconds := 0
	if len(rest) > 0 {
		rest = rest[1:] // the separator: any single character
		// Split the time from its offset at the first + or -.
		timePart, offsetPart := rest, []rune(nil)
		for index, c := range rest {
			if c == '+' || c == '-' {
				timePart, offsetPart = rest[:index], rest[index:]
				break
			}
		}
		if len(timePart) == 0 {
			return nil
		}
		var okTime bool
		hour, minute, second, micro, okTime = parseISOTimeOfDay(timePart)
		if !okTime {
			return nil
		}
		if len(offsetPart) > 0 {
			seconds, okOffset := parseISOOffset(offsetPart)
			if !okOffset {
				return nil
			}
			aware, offsetSeconds = true, seconds
		}
	}
	// 24:00[:00[.0]] is accepted and means midnight of the next day; any
	// other hour-24 time is refused, as are minutes/seconds past 59 and an
	// offset of a day or more.
	if hour == 24 && minute == 0 && second == 0 && micro == 0 {
		hour = 0
		date = date.AddDate(0, 0, 1)
		year, month, day = date.Year(), int(date.Month()), date.Day()
	}
	if hour > 23 || minute > 59 || second > 59 || offsetSeconds <= -86400 || offsetSeconds >= 86400 {
		return nil
	}
	zone := time.UTC
	if aware {
		zone = time.FixedZone("", offsetSeconds)
	}
	return &jiraStamp{Time: time.Date(year, time.Month(month), day, hour, minute, second, micro*1000, zone), Aware: aware}
}

func parseISOTimeOfDay(s []rune) (hour, minute, second, micro int, ok bool) {
	body := string(s)
	fraction := ""
	if index := strings.IndexAny(body, ".,"); index >= 0 {
		fraction, body = body[index+1:], body[:index]
		if fraction == "" {
			return 0, 0, 0, 0, false
		}
		for _, c := range fraction {
			if c < '0' || c > '9' {
				return 0, 0, 0, 0, false
			}
		}
		for len(fraction) < 6 {
			fraction += "0"
		}
		micro, _ = strconv.Atoi(fraction[:6])
	}
	num := func(t string) (int, bool) {
		if len(t) != 2 {
			return 0, false
		}
		v, err := strconv.Atoi(t)
		return v, err == nil && t[0] != '+' && t[0] != '-'
	}
	var parts []string
	if strings.Contains(body, ":") {
		parts = strings.Split(body, ":")
	} else {
		for i := 0; i < len(body); i += 2 {
			end := i + 2
			if end > len(body) {
				end = len(body)
			}
			parts = append(parts, body[i:end])
		}
	}
	if len(parts) < 1 || len(parts) > 3 || (fraction != "" && len(parts) != 3) {
		return 0, 0, 0, 0, false
	}
	values := [3]int{}
	for i, part := range parts {
		v, valid := num(part)
		if !valid {
			return 0, 0, 0, 0, false
		}
		values[i] = v
	}
	return values[0], values[1], values[2], micro, true
}

func parseISOOffset(s []rune) (int, bool) {
	sign := 1
	if s[0] == '-' {
		sign = -1
	}
	body := string(s[1:])
	var parts []string
	if strings.Contains(body, ":") {
		parts = strings.Split(body, ":")
	} else {
		for i := 0; i < len(body); i += 2 {
			end := i + 2
			if end > len(body) {
				end = len(body)
			}
			parts = append(parts, body[i:end])
		}
	}
	if len(parts) < 1 || len(parts) > 3 {
		return 0, false
	}
	total := 0
	for i, part := range parts {
		if len(part) != 2 {
			return 0, false
		}
		v, err := strconv.Atoi(part)
		if err != nil {
			return 0, false
		}
		total += v * []int{3600, 60, 1}[i]
	}
	return sign * total, true
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
	LastActive *jiraStamp
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
	touch := func(actor pyjson.Value, role string, updated *jiraStamp) {
		object, ok := actor.(*pyjson.Object)
		if !ok {
			return
		}
		idValue, _ := object.Get("accountId")
		if !truthy(idValue) {
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

// stampJSON renders a datetime the way pydantic's JSON encoder does: an
// aware UTC value ends in "Z", another offset in +HH:MM, a naive value has
// no suffix; microseconds appear as six digits when non-zero.
func stampJSON(stamp *jiraStamp) pyjson.Value {
	if stamp == nil {
		return nil
	}
	text := stamp.Time.Format("2006-01-02T15:04:05")
	if micro := stamp.Time.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	if stamp.Aware {
		_, offset := stamp.Time.Zone()
		if offset == 0 {
			text += "Z"
		} else {
			sign := "+"
			if offset < 0 {
				sign, offset = "-", -offset
			}
			text += fmt.Sprintf("%s%02d:%02d", sign, offset/3600, offset%3600/60)
			if offset%60 != 0 {
				text += fmt.Sprintf(":%02d", offset%60)
			}
		}
	}
	return text
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
	out.Set("last_active", stampJSON(member.LastActive))
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
