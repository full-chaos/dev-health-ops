//go:build integration

package server

import (
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestCompareMutationResponseRefusesTheSameWrongOffsetOnBothPlanes(t *testing.T) {
	respond := func(body string) venueoracle.Response {
		return venueoracle.Response{Status: http.StatusOK, Body: body}
	}
	wrap := func(stamp string) string {
		return `{"data": {"createSavedReport": {"createdAt": "` + stamp + `"}}}`
	}
	wrapError := func(stamp string) string {
		return `{"data": {"createSavedReport": {"createdAt": "` + stamp + `"}}, "errors": [{"message": "x"}]}`
	}

	for name, stamp := range map[string]string{
		"scrubbed placeholder": "<ts:nofrac:+01:00>",
		"plain instant":        "2026-10-19T12:00:09+01:00",
	} {
		for _, differs := range []string{"", "message differs"} {
			c := oracleCase{name: name, messageDiffers: differs}
			body := respond(wrap(stamp))
			if differs != "" {
				body = respond(wrapError(stamp))
			}
			same, why := compareMutationResponse(c, body, body)
			if same || !strings.Contains(why, "wrong-offset marker") {
				t.Errorf("%s (messageDiffers=%q): identical wrong offsets accepted: same=%v reason=%q", name, differs, same, why)
			}
		}
	}

	c := oracleCase{name: "control"}
	python := respond(wrap("2026-10-19T12:00:09+00:00"))
	goResp := respond(wrap("2026-10-19T12:00:09Z"))
	if same, why := compareMutationResponse(c, python, goResp); !same {
		t.Errorf("each plane's own offset form must still compare equal: reason=%q", why)
	}
}
