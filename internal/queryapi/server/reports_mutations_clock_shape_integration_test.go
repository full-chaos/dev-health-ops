//go:build integration

package server

import (
	"strings"
	"testing"
)

func TestNormalizeMutationBodyLiveClockShape(t *testing.T) {
	const python = `{"data": {"createSavedReport": {"createdAt": "<ts:frac:+00:00>", "updatedAt": "<ts:frac:+00:00>"}}}`
	body := func(created, updated string) string {
		return `{"data": {"createSavedReport": {"createdAt": "` + created + `", "updatedAt": "` + updated + `"}}}`
	}
	normalize := func(raw, plane string) string {
		t.Helper()
		out, err := normalizeMutationBody(raw, plane)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	want := normalize(python, "python")

	for name, goBody := range map[string]string{
		"fraction":      body("2026-10-19T12:00:09.123456Z", "2026-10-19T12:00:09.123456Z"),
		"zero fraction": body("2026-10-19T12:00:09Z", "2026-10-19T12:00:09Z"),
	} {
		if got := normalize(goBody, "go"); got != want {
			t.Errorf("%s: live createdAt/updatedAt shape must not decide the compare:\ngo     %s\npython %s", name, got, want)
		}
	}

	if got := normalize(body("2026-10-19T12:00:09.5+00:00", "2026-10-19T12:00:09Z"), "go"); got == want || !strings.Contains(got, "wrong offset") {
		t.Errorf("the offset form stays checked on a live clock field: %s", got)
	}

	const otherField = `{"data": {"x": {"lastRunAt": "%s"}}}`
	frozen := normalize(strings.Replace(otherField, "%s", "<ts:frac:+00:00>", 1), "python")
	if got := normalize(strings.Replace(otherField, "%s", "2026-10-19T12:00:09Z", 1), "go"); got == frozen {
		t.Errorf("a timestamp outside the live-clock list keeps its shape in the compare: %s", got)
	}

	nested := func(top, inner string) string {
		return `{"data": {"createSavedReport": {"reportPlan": {"createdAt": "` + inner + `", "list": [{"updatedAt": "` + inner + `"}]}, "createdAt": "` + top + `"}}}`
	}
	nestedWant := normalize(nested("<ts:frac:+00:00>", "<ts:frac:+00:00>"), "python")
	if got := normalize(nested("2026-10-19T12:00:09Z", "2026-10-19T12:00:09Z"), "go"); got == nestedWant {
		t.Errorf("a createdAt/updatedAt inside user JSON (reportPlan) is data and keeps its shape: %s", got)
	}
	if got := normalize(nested("2026-10-19T12:00:09Z", "2026-10-19T12:00:09.5Z"), "go"); got != nestedWant {
		t.Errorf("the nested members with the recorded shape, and a live top-level member of either shape, compare equal: %s", got)
	}
}
