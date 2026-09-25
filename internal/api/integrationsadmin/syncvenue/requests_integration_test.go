//go:build integration

package syncvenue

import (
	"strings"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

const syncPath = "/api/v1/admin/integrations/"

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
	return venueoracle.Request{Name: name, Method: "POST", Path: syncPath + id + "/sync", Headers: headers, Body: body}
}

// sameRequests are the requests both planes must answer alike.
func sameRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	json := venueoracle.B64
	a := "adminA"
	var out []venueoracle.Request
	for _, who := range []string{"", "memberA", "adminNoOrg"} {
		out = append(out, post(venue, "guard as "+who, v.incremental.id.String(), who, json(`{}`)))
	}
	out = append(out,
		venueoracle.Request{Name: "guard bad scheme", Method: "POST", Path: syncPath + v.incremental.id.String() + "/sync",
			Headers: map[string]string{"Authorization": "Basic abc", "Content-Type": "application/json"}, Body: json(`{}`)},
		post(venue, "no body", v.incremental.id.String(), a, nil),
		post(venue, "body null", v.incremental.id.String(), a, json(`null`)),
		post(venue, "body is a list", v.incremental.id.String(), a, json(`[]`)),
		post(venue, "source_ids is a string", v.incremental.id.String(), a, json(`{"source_ids": "x"}`)),
		post(venue, "source_ids holds a number", v.incremental.id.String(), a, json(`{"source_ids": ["a", 1]}`)),
		post(venue, "dataset_keys is an object", v.incremental.id.String(), a, json(`{"dataset_keys": {}}`)),
		post(venue, "full_resync is not a bool", v.incremental.id.String(), a, json(`{"full_resync": "maybe"}`)),
		post(venue, "the body is validated before the integration", uuid.NewString(), a, json(`{"full_resync": []}`)),
		post(venue, "a source id that is not a uuid", v.incremental.id.String(), a, json(`{"source_ids": ["not-a-uuid"]}`)),
		post(venue, "the first source id that is not a uuid is named", v.incremental.id.String(), a,
			json(`{"source_ids": ["`+v.incremental.srcs[0].String()+`", "{`+v.incremental.srcs[1].String()+`}", "nope", "worse"]}`)),
		post(venue, "unknown integration", uuid.NewString(), a, json(`{}`)),
		post(venue, "not a uuid", "zzz", a, json(`{}`)),
		post(venue, "another org's integration", v.otherOrg.id.String(), a, json(`{}`)),
		// clock: the run and its units carry ids each plane makes.
		post(venue, "clock: incremental, every enabled source", v.incremental.id.String(), a, json(`{}`)),
		post(venue, "clock: full resync", v.full.id.String(), a, json(`{"full_resync": true}`)),
		post(venue, "clock: explicit source and datasets", v.subset.id.String(), a,
			json(`{"source_ids": ["`+v.subset.srcs[0].String()+`", "`+v.subset.srcs[2].String()+`"], "dataset_keys": ["commits"]}`)),
		post(venue, "clock: explicit empty source list", v.emptySrc.id.String(), a, json(`{"source_ids": []}`)),
		post(venue, "clock: dataset_keys only, null source_ids, unknown fields", v.datasets.id.String(), a,
			json(`{"source_ids": null, "dataset_keys": ["prs"], "extra": 1}`)),
		post(venue, "clock: explicit empty dataset list", v.emptyData.id.String(), a, json(`{"dataset_keys": []}`)),
		post(venue, "clock: path spelling is echoed (upper-case uuid)", strings.ToUpper(v.nullBody.id.String()), a, json(`{}`)),
	)
	return out
}

// divergingRequests are answered differently on purpose (named in the ticket):
// Python plans whatever the integration is; the hand-off needs a configuration
// the scheduler can run.
func divergingRequests(venue *venueoracle.Venue, v ids) []venueoracle.Request {
	json := venueoracle.B64
	return []venueoracle.Request{
		post(venue, "inactive integration", v.inactive.id.String(), "adminA", json(`{}`)),
		post(venue, "integration without a configuration", v.noConfig.id.String(), "adminA", json(`{}`)),
		post(venue, "configuration neither planner-managed nor pinned", v.unmanaged.id.String(), "adminA", json(`{}`)),
	}
}
