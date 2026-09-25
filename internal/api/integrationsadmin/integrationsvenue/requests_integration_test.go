//go:build integration

package integrationsvenue

import (
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const integrationsPath = "/api/v1/admin/integrations"

// integrationRequests is the whole request sequence. Writes are ordered so a
// later read sees them; a request whose answer carries a time or an id the
// plane generated is named "clock: ..." and compared with those blanked.
func integrationRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	bearer := func(name string) map[string]string {
		return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
	}
	jsonHeaders := func(name string) map[string]string {
		headers := bearer(name)
		headers["Content-Type"] = "application/json"
		return headers
	}
	var out []venueoracle.Request
	get := func(name, path, token string) {
		out = append(out, venueoracle.Request{Name: name, Method: "GET", Path: integrationsPath + path, Headers: bearer(token)})
	}
	send := func(name, method, path, token, body string) {
		out = append(out, venueoracle.Request{Name: name, Method: method, Path: integrationsPath + path, Headers: jsonHeaders(token), Body: venueoracle.B64(body)})
	}
	id := func(u uuid.UUID) string { return u.String() }

	// --- reads of the seeded state ------------------------------------
	get("list org A", "", "adminA")
	get("list org B", "", "adminB")
	get("list org C", "", "adminC")
	for name, u := range map[string]uuid.UUID{
		"github": v.intGitHub, "jira inactive": v.intJira, "linear null config": v.intLinear, "pagerduty empty-list config": v.intPagerDuty,
		"empty gitlab": v.intEmpty, "upper-case provider": v.intUpper,
	} {
		get("get "+name, "/"+id(u), "adminA")
	}
	get("get other org integration", "/"+id(v.intB), "adminA")
	get("get unknown integration", "/"+uuid.NewString(), "adminA")
	for name, spelling := range map[string]string{
		"upper-case uuid": strings.ToUpper(id(v.intGitHub)), "braces": "{" + id(v.intGitHub) + "}", "urn": "urn:uuid:" + id(v.intGitHub),
		"hex32": strings.ReplaceAll(id(v.intGitHub), "-", ""), "not a uuid": "nope", "empty-ish": "%20", "percent space": "%20" + id(v.intGitHub),
	} {
		get("get spelling "+name, "/"+spelling, "adminA")
	}
	get("get trailing slash", "/"+id(v.intGitHub)+"/", "adminA")
	get("sources github", "/"+id(v.intGitHub)+"/sources", "adminA")
	get("sources jira", "/"+id(v.intJira)+"/sources", "adminA")
	get("sources empty", "/"+id(v.intEmpty)+"/sources", "adminA")
	get("sources other org", "/"+id(v.intB)+"/sources", "adminA")
	get("sources unknown", "/"+uuid.NewString()+"/sources", "adminA")
	get("sources not a uuid", "/zzz/sources", "adminA")
	get("datasets github", "/"+id(v.intGitHub)+"/datasets", "adminA")
	get("datasets jira", "/"+id(v.intJira)+"/datasets", "adminA")
	get("datasets empty", "/"+id(v.intEmpty)+"/datasets", "adminA")
	get("datasets other org", "/"+id(v.intB)+"/datasets", "adminA")
	get("datasets not a uuid", "/zzz/datasets", "adminA")

	// --- the guard domain, on every route -----------------------------
	routes := []struct{ method, path, body string }{
		{"GET", "", ""}, {"POST", "", `{"name":"x","provider":"github"}`},
		{"GET", "/" + id(v.intGitHub), ""}, {"PATCH", "/" + id(v.intGitHub), `{"name":"x"}`},
		{"GET", "/" + id(v.intGitHub) + "/sources", ""},
		{"PATCH", "/" + id(v.intGitHub) + "/sources/" + id(v.srcGit), `{"is_enabled":true}`},
		{"GET", "/" + id(v.intGitHub) + "/datasets", ""},
		{"PATCH", "/" + id(v.intGitHub) + "/datasets", `{"datasets":[]}`},
	}
	for _, route := range routes {
		for _, who := range []string{"", "memberA", "adminNoOrg", "superNoOrg"} {
			name := "guard " + route.method + " " + route.path + " as " + who
			request := venueoracle.Request{Name: name, Method: route.method, Path: integrationsPath + route.path}
			if route.body != "" {
				request.Body = venueoracle.B64(route.body)
			}
			if who == "" {
				request.Headers = map[string]string{"Content-Type": "application/json"}
			} else {
				request.Headers = jsonHeaders(who)
			}
			out = append(out, request)
		}
		out = append(out, venueoracle.Request{Name: "guard " + route.method + " " + route.path + " bad scheme", Method: route.method, Path: integrationsPath + route.path,
			Headers: map[string]string{"Authorization": "Basic abc", "Content-Type": "application/json"}, Body: venueoracle.B64(route.body)})
	}

	// --- create: validation (no rows written) -------------------------
	for name, body := range map[string]string{
		"empty body object": `{}`, "missing name": `{"provider":"github"}`, "missing provider": `{"name":"x"}`,
		"empty name": `{"name":"","provider":"github"}`, "empty provider": `{"name":"x","provider":""}`,
		"name 256":  `{"name":"` + strings.Repeat("n", 256) + `","provider":"github"}`,
		"name null": `{"name":null,"provider":"github"}`, "name int": `{"name":5,"provider":"github"}`,
		"provider list": `{"name":"x","provider":["a"]}`, "config list": `{"name":"x","provider":"github","config":[1]}`,
		"config null": `{"name":"x","provider":"github","config":null}`, "config string": `{"name":"x","provider":"github","config":"a"}`,
		"is_active maybe": `{"name":"x","provider":"github","is_active":"maybe"}`, "is_active null": `{"name":"x","provider":"github","is_active":null}`,
		"is_active 2": `{"name":"x","provider":"github","is_active":2}`, "cron int": `{"name":"x","provider":"github","schedule_cron":5}`,
		"timezone list": `{"name":"x","provider":"github","timezone":[]}`, "credential int": `{"name":"x","provider":"github","credential_id":5}`,
		"body list": `[1]`, "body string": `"x"`, "body null": `null`, "body number": `5`, "invalid json": `{"name":`, "empty body": ``,
		"several errors": `{"name":"","provider":1,"config":3,"is_active":"z","timezone":1}`,
	} {
		send("create invalid: "+name, "POST", "", "adminA", body)
	}
	for name, body := range map[string]string{
		"credential not a uuid":     `{"name":"x","provider":"github","credential_id":"zzz"}`,
		"credential empty string":   `{"name":"x","provider":"github","credential_id":""}`,
		"credential unknown":        `{"name":"x","provider":"github","credential_id":"` + uuid.NewString() + `"}`,
		"credential other org":      `{"name":"x","provider":"github","credential_id":"` + id(v.credGitHubB) + `"}`,
		"credential wrong provider": `{"name":"x","provider":"jira","credential_id":"` + id(v.credGitHub) + `"}`,
		"credential provider case":  `{"name":"x","provider":"GitHub","credential_id":"` + id(v.credGitHub) + `"}`,
	} {
		send("create refused: "+name, "POST", "", "adminA", body)
	}

	// --- create: rows written ------------------------------------------
	for name, body := range map[string]string{
		"minimal":                   `{"name":"created-min","provider":"github"}`,
		"full":                      `{"name":"created-full","provider":"github","credential_id":"` + id(v.credGitHub) + `","config":{"owner":"acme","f":1.0,"e":1e2,"big":12345678901234567890,"uni":"café 😀","nested":{"b":1,"a":[1.5,"x",null,true]},"dup":1,"dup":2},"is_active":false,"schedule_cron":"*/5 * * * *","timezone":"Europe/Paris"}`,
		"credential spelling upper": `{"name":"created-upper","provider":"github","credential_id":"` + strings.ToUpper(id(v.credGitHub)) + `"}`,
		"credential braces":         `{"name":"created-braces","provider":"github","credential_id":"{` + id(v.credGitHub) + `}"}`,
		"is_active yes":             `{"name":"created-yes","provider":"jira","is_active":"yes"}`,
		"is_active zero":            `{"name":"created-zero","provider":"jira","is_active":0}`,
		"name 255":                  `{"name":"` + strings.Repeat("é", 255) + `","provider":"linear"}`,
		"extra fields ignored":      `{"name":"created-extra","provider":"linear","surprise":1}`,
		"nulls for optionals":       `{"name":"created-nulls","provider":"gitlab","credential_id":null,"schedule_cron":null,"timezone":null}`,
		"empty cron and timezone":   `{"name":"created-empty","provider":"gitlab","schedule_cron":"","timezone":""}`,
		"provider with spaces":      `{"name":"created-spaced","provider":" Jira "}`,
		"config float exponent":     `{"name":"created-floats","provider":"github","config":{"a":1e400,"b":-0.0,"c":5e-324,"d":1.7976931348623157e308,"e":100000000000000000000.0}}`,
	} {
		send("clock: create "+name, "POST", "", "adminA", body)
	}
	send("clock: create in org B", "POST", "", "adminB", `{"name":"created-b","provider":"jira"}`)
	get("clock: list after creates A", "", "adminA")
	get("clock: list after creates B", "", "adminB")

	// --- update -----------------------------------------------------------
	for name, body := range map[string]string{
		"body list": `[1]`, "body null": `null`, "invalid json": `{"name":`, "empty body": ``, "config list": `{"config":[1]}`,
		"is_active maybe": `{"is_active":"maybe"}`, "name int": `{"name":5}`, "cron list": `{"schedule_cron":[]}`, "several": `{"name":1,"config":2,"is_active":"z"}`,
		"credential int": `{"credential_id":5}`,
	} {
		send("update invalid: "+name, "PATCH", "/"+id(v.intGitHub), "adminA", body)
	}
	send("update invalid body before 404", "PATCH", "/"+uuid.NewString(), "adminA", `{"name":5}`)
	send("update unknown", "PATCH", "/"+uuid.NewString(), "adminA", `{"name":"x"}`)
	send("update other org", "PATCH", "/"+id(v.intB), "adminA", `{"name":"x"}`)
	send("update not a uuid", "PATCH", "/zzz", "adminA", `{"name":"x"}`)
	for name, body := range map[string]string{
		"credential not a uuid": `{"credential_id":"zzz"}`, "credential other org": `{"credential_id":"` + id(v.credGitHubB) + `"}`,
		"credential wrong provider": `{"credential_id":"` + id(v.credJira) + `"}`, "credential unknown": `{"credential_id":"` + uuid.NewString() + `"}`,
		"credential empty": `{"credential_id":""}`,
	} {
		send("update refused: "+name, "PATCH", "/"+id(v.intGitHub), "adminA", body)
	}
	// No-op updates emit no UPDATE: updated_at is unchanged and compared
	// exactly (no "clock:" prefix).
	for name, body := range map[string]string{
		"empty object": `{}`, "all nulls": `{"name":null,"credential_id":null,"config":null,"is_active":null,"schedule_cron":null,"timezone":null}`,
		"same name": `{"name":"seed-a-github"}`, "same credential": `{"credential_id":"` + id(v.credGitHub) + `"}`,
		"same credential upper": `{"credential_id":"` + strings.ToUpper(id(v.credGitHub)) + `"}`, "same is_active": `{"is_active":true}`,
		"same is_active spelled": `{"is_active":"on"}`, "same cron": `{"schedule_cron":"0 * * * *"}`, "same timezone": `{"timezone":"UTC"}`,
		"equal config":                    `{"config":{"owner":"acme","n":1,"f":1.0,"e":100.0,"big":12345678901234567890,"tiny":1e-7,"uni":"café 😀","dup":2,"nested":{"b":1,"a":[1.5,"x",null,true]}}}`,
		"equal config other number types": `{"config":{"owner":"acme","n":1.0,"f":1,"e":100,"big":12345678901234567890,"tiny":1e-7,"uni":"café 😀","dup":2,"nested":{"b":true,"a":[1.5,"x",null,true]}}}`,
	} {
		send("update noop: "+name, "PATCH", "/"+id(v.intGitHub), "adminA", body)
	}
	for name, body := range map[string]string{
		"name": `{"name":"seed-a-github-renamed"}`, "is_active": `{"is_active":false}`, "cron": `{"schedule_cron":"1 2 3 4 5"}`,
		"timezone": `{"timezone":"Asia/Tokyo"}`, "empty cron": `{"schedule_cron":""}`,
		"config": `{"config":{"replaced":true,"list":[1,2.5,"é"]}}`, "empty config": `{"config":{}}`,
		"credential": `{"credential_id":"` + id(v.credGitHub) + `"}`,
		"everything": `{"name":"seed-a-github-final","config":{"z":1},"is_active":true,"schedule_cron":"9 9 9 9 9","timezone":"UTC"}`,
	} {
		send("clock: update "+name, "PATCH", "/"+id(v.intGitHub), "adminA", body)
	}
	send("clock: update jira set credential", "PATCH", "/"+id(v.intJira), "adminA", `{"credential_id":"`+id(v.credJira)+`","is_active":true}`)
	send("clock: update null config row", "PATCH", "/"+id(v.intLinear), "adminA", `{"name":"seed-a-linear-renamed"}`)
	send("clock: update empty list config row", "PATCH", "/"+id(v.intPagerDuty), "adminA", `{"config":{}}`)
	get("clock: get after updates github", "/"+id(v.intGitHub), "adminA")
	get("clock: list after updates A", "", "adminA")

	// --- sources ------------------------------------------------------------
	source := func(integration, src uuid.UUID) string { return "/" + id(integration) + "/sources/" + id(src) }
	for name, body := range map[string]string{
		"missing is_enabled": `{}`, "null": `{"is_enabled":null}`, "string yes": `{"is_enabled":"yes"}`, "maybe": `{"is_enabled":"maybe"}`,
		"int 2": `{"is_enabled":2}`, "list": `{"is_enabled":[]}`, "body list": `[1]`, "body null": `null`, "empty body": ``, "invalid json": `{"is_en`,
	} {
		send("source invalid: "+name, "PATCH", source(v.intGitHub, v.srcGit), "adminA", body)
	}
	send("source invalid body before 404", "PATCH", "/"+uuid.NewString()+"/sources/"+uuid.NewString(), "adminA", `{}`)
	send("source unknown integration", "PATCH", "/"+uuid.NewString()+"/sources/"+id(v.srcGit), "adminA", `{"is_enabled":true}`)
	send("source unknown source", "PATCH", "/"+id(v.intGitHub)+"/sources/"+uuid.NewString(), "adminA", `{"is_enabled":true}`)
	send("source not a uuid", "PATCH", "/"+id(v.intGitHub)+"/sources/zzz", "adminA", `{"is_enabled":true}`)
	send("source of another integration", "PATCH", source(v.intGitHub, v.srcCapped), "adminA", `{"is_enabled":true}`)
	send("source of another org", "PATCH", "/"+id(v.intB)+"/sources/"+id(v.srcB), "adminA", `{"is_enabled":true}`)
	send("source unknown integration not a uuid", "PATCH", "/zzz/sources/"+id(v.srcGit), "adminA", `{"is_enabled":true}`)
	// Org A is at its repo limit (3 of 3): every jira enable is refused.
	send("source jira enable over the limit", "PATCH", source(v.intJira, v.srcCapped), "adminA", `{"is_enabled":true}`)
	send("source JIRA provider case over the limit", "PATCH", source(v.intJira, v.srcUpper), "adminA", `{"is_enabled":true}`)
	send("source padded provider over the limit", "PATCH", source(v.intJira, v.srcSpaced), "adminA", `{"is_enabled":true}`)
	send("source falsy markers over the limit", "PATCH", source(v.intJira, v.srcFalsy), "adminA", `{"is_enabled":"true"}`)
	// Not a jira enable, or not an enable at all: no limit check.
	send("source github enable", "PATCH", source(v.intGitHub, v.srcGit), "adminA", `{"is_enabled":true}`)
	send("source github enable again", "PATCH", source(v.intGitHub, v.srcGit), "adminA", `{"is_enabled":true}`)
	send("source github disable", "PATCH", source(v.intGitHub, v.srcGit), "adminA", `{"is_enabled":false}`)
	send("source jira already enabled, markers cleared", "PATCH", source(v.intJira, v.srcEnabledMarked), "adminA", `{"is_enabled":true}`)
	send("source jira disable, markers untouched afterwards", "PATCH", source(v.intJira, v.srcEnabledMarked), "adminA", `{"is_enabled":false}`)
	send("source jira disable a disabled marked source", "PATCH", source(v.intJira, v.srcCapped), "adminA", `{"is_enabled":false}`)
	send("source jira disable a falsy-marker source", "PATCH", source(v.intJira, v.srcFalsy), "adminA", `{"is_enabled":0}`)
	send("source other enabled stays", "PATCH", source(v.intGitHub, v.srcOther), "adminA", `{"is_enabled":true}`)
	// Org B has no limit; org C has room for exactly one more.
	send("source unlimited org enable clears the marker", "PATCH", "/"+id(v.intB)+"/sources/"+id(v.srcB), "adminB", `{"is_enabled":true}`)
	send("source org C enable within the limit", "PATCH", source(v.intC, v.srcC1), "adminC", `{"is_enabled":true}`)
	send("source org C enable over the limit", "PATCH", source(v.intC, v.srcC2), "adminC", `{"is_enabled":true}`)
	send("source org C disable then enable again", "PATCH", source(v.intC, v.srcC1), "adminC", `{"is_enabled":false}`)
	send("source org C enable the other after room", "PATCH", source(v.intC, v.srcC2), "adminC", `{"is_enabled":true}`)
	get("sources after writes jira", "/"+id(v.intJira)+"/sources", "adminA")
	get("sources after writes github", "/"+id(v.intGitHub)+"/sources", "adminA")
	get("sources after writes B", "/"+id(v.intB)+"/sources", "adminB")
	get("sources after writes C", "/"+id(v.intC)+"/sources", "adminC")

	dsPath := func(integration uuid.UUID) string { return "/" + id(integration) + "/datasets" }
	// --- stored JSON that dict() reads as a list of pairs -------------------
	get("pairs list org D (one row cannot be rendered)", "", "adminD")
	get("pairs config row", "/"+id(v.intPairs), "adminD")
	get("pairs config row with a non-string key", "/"+id(v.intBadPairs), "adminD")
	get("pairs sources", "/"+id(v.intPairs)+"/sources", "adminD")
	get("pairs datasets", "/"+id(v.intPairs)+"/datasets", "adminD")
	send("clock: pairs update renders the pair config", "PATCH", "/"+id(v.intPairs), "adminD", `{"name":"seed-d-pairs-renamed"}`)
	send("pairs update of the unrenderable row rolls back", "PATCH", "/"+id(v.intBadPairs), "adminD", `{"name":"seed-d-bad-pairs-renamed"}`)
	send("clock: pairs update of the unrenderable row with a config", "PATCH", "/"+id(v.intBadPairs), "adminD", `{"config":{"fixed":true}}`)
	send("pairs source enable (metadata has no get)", "PATCH", source(v.intPairs, v.srcPairsMeta), "adminD", `{"is_enabled":true}`)
	send("pairs dataset patch renders the pair options", "PATCH", dsPath(v.intPairs), "adminD", `{"datasets":[{"dataset_key":"commits","is_enabled":false}]}`)
	get("clock: pairs list org D after the rollbacks", "", "adminD")
	// The provider names Python's strip() and lower() treat differently from Go's defaults.
	send("source capital-I-with-dot provider enable (not jira: no limit)", "PATCH", source(v.intJira, v.srcTurkish), "adminA", `{"is_enabled":true}`)
	send("source separator-padded provider enable (jira: over the limit)", "PATCH", source(v.intJira, v.srcSep), "adminA", `{"is_enabled":true}`)

	// --- datasets -----------------------------------------------------------
	for name, body := range map[string]string{
		"missing datasets": `{}`, "null datasets": `{"datasets":null}`, "datasets dict": `{"datasets":{}}`, "datasets string": `{"datasets":"a"}`,
		"item not object": `{"datasets":[1]}`, "item null": `{"datasets":[null]}`, "item missing key": `{"datasets":[{"is_enabled":true}]}`,
		"item missing enabled": `{"datasets":[{"dataset_key":"commits"}]}`, "key int": `{"datasets":[{"dataset_key":5,"is_enabled":true}]}`,
		"key null": `{"datasets":[{"dataset_key":null,"is_enabled":true}]}`, "enabled maybe": `{"datasets":[{"dataset_key":"commits","is_enabled":"maybe"}]}`,
		"enabled null": `{"datasets":[{"dataset_key":"commits","is_enabled":null}]}`, "several items bad": `{"datasets":[{"dataset_key":1},{"is_enabled":"x"},3]}`,
		"body list": `[1]`, "body null": `null`, "empty body": ``, "invalid json": `{"datasets":[`,
	} {
		send("datasets invalid: "+name, "PATCH", dsPath(v.intGitHub), "adminA", body)
	}
	send("datasets invalid body before 404", "PATCH", "/"+uuid.NewString()+"/datasets", "adminA", `{}`)
	send("datasets unknown integration", "PATCH", "/"+uuid.NewString()+"/datasets", "adminA", `{"datasets":[]}`)
	send("datasets other org", "PATCH", dsPath(v.intB), "adminA", `{"datasets":[]}`)
	send("datasets not a uuid", "PATCH", "/zzz/datasets", "adminA", `{"datasets":[]}`)
	send("datasets empty batch", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[]}`)
	send("datasets toggle existing", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":false},{"dataset_key":"prs","is_enabled":true}]}`)
	send("datasets same values", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":false}]}`)
	send("datasets spelled enabled", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":"yes"},{"dataset_key":"prs","is_enabled":0}]}`)
	send("clock: datasets create registered keys github", "PATCH", dsPath(v.intGitHub), "adminA",
		`{"datasets":[{"dataset_key":"repo-metadata","is_enabled":true},{"dataset_key":"cicd","is_enabled":false},{"dataset_key":"work-items","is_enabled":true},{"dataset_key":"security","is_enabled":true}]}`)
	send("datasets unknown key for github", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"incidents","is_enabled":true}]}`)
	send("datasets unknown key with quotes", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"it's \"é\"","is_enabled":true}]}`)
	send("datasets empty key", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"","is_enabled":true}]}`)
	send("datasets case sensitive key", "PATCH", dsPath(v.intGitHub), "adminA", `{"datasets":[{"dataset_key":"Commits","is_enabled":true}]}`)
	send("datasets batch fails after a valid change (rolls back)", "PATCH", dsPath(v.intGitHub), "adminA",
		`{"datasets":[{"dataset_key":"files","is_enabled":true},{"dataset_key":"commits","is_enabled":true},{"dataset_key":"nope","is_enabled":true}]}`)
	get("clock: datasets after the rolled-back batch", "/"+id(v.intGitHub)+"/datasets", "adminA")
	send("clock: datasets key repeated in one batch", "PATCH", dsPath(v.intGitHub), "adminA",
		`{"datasets":[{"dataset_key":"blame","is_enabled":true},{"dataset_key":"blame","is_enabled":false},{"dataset_key":"commits","is_enabled":true},{"dataset_key":"commits","is_enabled":false}]}`)
	send("clock: datasets jira create and toggle", "PATCH", dsPath(v.intJira), "adminA",
		`{"datasets":[{"dataset_key":"work-items","is_enabled":false},{"dataset_key":"incidents","is_enabled":true},{"dataset_key":"work-item-history","is_enabled":true}]}`)
	send("datasets jira key not in the jira registry", "PATCH", dsPath(v.intJira), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":true}]}`)
	send("clock: datasets upper-case provider registry lookup", "PATCH", dsPath(v.intUpper), "adminA", `{"datasets":[{"dataset_key":"work-items","is_enabled":true}]}`)
	send("clock: datasets linear valid keys", "PATCH", dsPath(v.intLinear), "adminA", `{"datasets":[{"dataset_key":"work-item-comments","is_enabled":true},{"dataset_key":"work-items","is_enabled":false}]}`)
	send("datasets linear key it lacks", "PATCH", dsPath(v.intLinear), "adminA", `{"datasets":[{"dataset_key":"incidents","is_enabled":true}]}`)
	send("clock: datasets pagerduty valid keys", "PATCH", dsPath(v.intPagerDuty), "adminA", `{"datasets":[{"dataset_key":"on-calls","is_enabled":true},{"dataset_key":"incidents","is_enabled":true},{"dataset_key":"incident-notes","is_enabled":false}]}`)
	send("datasets pagerduty key it lacks", "PATCH", dsPath(v.intPagerDuty), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":true}]}`)
	send("clock: datasets gitlab", "PATCH", dsPath(v.intGitLab), "adminA", `{"datasets":[{"dataset_key":"feature-flags","is_enabled":true},{"dataset_key":"incidents","is_enabled":true}]}`)
	send("datasets gitlab key only github has", "PATCH", dsPath(v.intGitLab), "adminA", `{"datasets":[{"dataset_key":"nope","is_enabled":true}]}`)
	send("clock: datasets gitlab keys created", "PATCH", dsPath(v.intEmpty), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":true},{"dataset_key":"security","is_enabled":true}]}`)
	send("datasets provider with no registry entry", "PATCH", dsPath(v.intCustom), "adminA", `{"datasets":[{"dataset_key":"commits","is_enabled":true}]}`)
	send("datasets provider with no registry entry, empty batch", "PATCH", dsPath(v.intCustom), "adminA", `{"datasets":[]}`)
	get("clock: datasets after writes github", "/"+id(v.intGitHub)+"/datasets", "adminA")
	get("clock: datasets after writes jira", "/"+id(v.intJira)+"/datasets", "adminA")
	get("clock: datasets after writes upper", "/"+id(v.intUpper)+"/datasets", "adminA")
	get("clock: datasets after writes linear", "/"+id(v.intLinear)+"/datasets", "adminA")
	get("clock: datasets after writes gitlab", "/"+id(v.intGitLab)+"/datasets", "adminA")
	get("clock: datasets after writes gitlab keys created", "/"+id(v.intEmpty)+"/datasets", "adminA")
	get("datasets after writes custom", "/"+id(v.intCustom)+"/datasets", "adminA")
	_ = fmt.Sprint
	return out
}
