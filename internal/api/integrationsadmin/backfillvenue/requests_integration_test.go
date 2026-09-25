//go:build integration

package backfillvenue

import (
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const backfillPath = "/api/v1/admin/integrations/"

func bearer(venue *venueoracle.Venue, name string) map[string]string {
	if name == "" {
		return map[string]string{}
	}
	return map[string]string{"Authorization": "Bearer " + venue.Tokens[name]}
}

func post(venue *venueoracle.Venue, name, id, token string, body *string) venueoracle.Request {
	headers := bearer(venue, token)
	if body != nil {
		headers["Content-Type"] = "application/json"
	}
	return venueoracle.Request{Name: name, Method: "POST", Path: backfillPath + id + "/backfill", Headers: headers, Body: body}
}

func window(since, before string) string {
	return `"since": "` + since + `", "before": "` + before + `"`
}

// sameRequests are the requests both planes must answer alike.
func sameRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	json := venueoracle.B64
	a := "adminA"
	id := func(name string) string { return v.get(name).id.String() }
	flat := id("flat")
	var out []venueoracle.Request
	for _, who := range []string{"", "memberA", "adminNoOrg"} {
		out = append(out, post(venue, "guard as "+who, flat, who, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)))
	}
	subset := v.get("flatsubset")
	out = append(out,
		venueoracle.Request{Name: "guard bad scheme", Method: "POST", Path: backfillPath + flat + "/backfill",
			Headers: map[string]string{"Authorization": "Basic abc", "Content-Type": "application/json"}, Body: json(`{}`)},
		post(venue, "no body", flat, a, nil),
		post(venue, "body null", flat, a, json(`null`)),
		post(venue, "body is a list", flat, a, json(`[]`)),
		post(venue, "empty object", flat, a, json(`{}`)),
		post(venue, "only since", flat, a, json(`{"since": "2026-09-01T00:00:00Z"}`)),
		post(venue, "only before", flat, a, json(`{"before": "2026-09-01T00:00:00Z"}`)),
		post(venue, "since null", flat, a, json(`{"since": null, "before": "2026-09-01T00:00:00Z"}`)),
		post(venue, "since is not a datetime", flat, a, json(`{"since": "yesterday", "before": "2026-09-01T00:00:00Z"}`)),
		post(venue, "clock: since and before are unix numbers", id("number"), a, json(`{"since": 1790000000, "before": 1790600000}`)),
		post(venue, "since is a list", flat, a, json(`{"since": [], "before": "2026-09-01T00:00:00Z"}`)),
		post(venue, "source_ids is a string", flat, a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`, "source_ids": "x"}`)),
		post(venue, "dataset_keys holds a number", flat, a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`, "dataset_keys": ["a", 1]}`)),
		post(venue, "selector is a string", flat, a, json(`{"selector": "x"}`)),
		post(venue, "selector is empty", flat, a, json(`{"selector": {}}`)),
		post(venue, "selector without before", flat, a, json(`{"selector": {"since": "2026-09-01T00:00:00Z"}}`)),
		post(venue, "selector with a bad datetime and a bad list", flat, a, json(`{"selector": {"since": "x", "before": "2026-09-01", "source_ids": 1}}`)),
		post(venue, "selector mixed with flat since", flat, a, json(`{"selector": {`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}, "since": "2026-09-01T00:00:00Z"}`)),
		post(venue, "selector mixed with flat source_ids", flat, a, json(`{"selector": {`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}, "source_ids": []}`)),
		post(venue, "clock: selector null and a flat window", id("selectornull"), a, json(`{"selector": null, `+window("2026-09-01T00:00:00Z", "2026-09-02T00:00:00Z")+`}`)),
		post(venue, "the body is validated before the integration", uuid.NewString(), a, json(`{}`)),
		post(venue, "unknown integration", uuid.NewString(), a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)),
		post(venue, "not a uuid", "zzz", a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)),
		post(venue, "another org's integration", id("otherorg"), a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)),
		// clock: the run and its units carry ids each plane makes.
		post(venue, "clock: flat window over three weeks, every enabled source", flat, a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-20T00:00:00Z")+`}`)),
		post(venue, "clock: selector window", id("selector"), a, json(`{"selector": {`+window("2026-08-01T06:30:00Z", "2026-08-16T18:45:10.5Z")+`}}`)),
		post(venue, "clock: flat window, explicit source and dataset", subset.id.String(), a,
			json(`{`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`, "source_ids": ["`+subset.srcs[0].String()+`", "`+subset.srcs[2].String()+`"], "dataset_keys": ["commits"]}`)),
		post(venue, "clock: selector with empty lists", id("selectorempty"), a,
			json(`{"selector": {`+window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z")+`, "source_ids": [], "dataset_keys": null}}`)),
		post(venue, "clock: naive datetimes", id("naive"), a, json(`{`+window("2026-09-01T00:00:00", "2026-09-05T12:00:00")+`}`)),
		post(venue, "clock: offset datetimes", id("offset"), a, json(`{`+window("2026-09-01T00:00:00+02:00", "2026-09-05T23:30:00-05:00")+`}`)),
		post(venue, "clock: date-only strings", id("dateonly"), a, json(`{`+window("2026-09-01", "2026-09-15")+`}`)),
		post(venue, "clock: chunk edges (exact weeks)", id("chunkedges"), a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-15T00:00:00Z")+`}`)),
		post(venue, "clock: an enabled source no configuration tagged", id("untagged"), a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-04T00:00:00Z")+`}`)),
		post(venue, "clock: path spelling is echoed (upper-case uuid)", strings.ToUpper(id("upper")), a, json(`{`+window("2026-09-01T00:00:00Z", "2026-09-04T00:00:00Z")+`}`)),
	)
	return out
}

// refusedRequests are planned by Python and refused by the hand-off.
func refusedRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	json := venueoracle.B64
	body := json(`{` + window("2026-09-01T00:00:00Z", "2026-09-10T00:00:00Z") + `}`)
	return []venueoracle.Request{
		post(venue, "inactive integration", v.get("inactive").id.String(), "adminA", body),
		post(venue, "integration without a configuration", v.get("noconfig").id.String(), "adminA", body),
		post(venue, "configuration neither planner-managed nor pinned", v.get("unmanaged").id.String(), "adminA", body),
	}
}

// windowRequests are windows Python's planner refuses with 400 once a unit is
// planned, and the scheduler's planner refuses too, in its own words.
func windowRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	json := venueoracle.B64
	return []venueoracle.Request{
		post(venue, "since after before", v.get("reversed").id.String(), "adminA", json(`{`+window("2026-09-10T00:00:00Z", "2026-09-01T00:00:00Z")+`}`)),
		post(venue, "since equal to before", v.get("equal").id.String(), "adminA", json(`{`+window("2026-09-10T00:00:00Z", "2026-09-10T00:00:00Z")+`}`)),
	}
}
