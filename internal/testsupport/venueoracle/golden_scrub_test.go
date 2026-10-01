package venueoracle

import (
	"strings"
	"testing"
	"time"
)

func TestScrubRunValuesBlanksGeneratedIDsAndRunTimesOnly(t *testing.T) {
	floor := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	ceiling := time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC)
	seededV4 := "10000000-0000-4000-8000-000000000001"
	scrub := ScrubRunValues(floor, ceiling, seededV4)
	seeded := StableUUID("seed-1")
	random := "3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f"
	timeOrdered := "0192f3a4-5b6c-7d8e-8f90-a1b2c3d4e5f6"
	in := strings.Join([]string{
		"id=" + seeded, "id=" + random, "id=" + timeOrdered, "id=" + seededV4, "id=" + strings.ToUpper(seededV4),
		"run=2026-10-01T12:00:00.123456Z", "run=2026-10-01 12:00:00.5+00", "run=2026-10-01 12:00:00+00:00", "run=2026-10-01T12:00:00",
		"seeded=2026-08-01T00:00:00Z", "request=2031-01-01T00:00:00.5+05:30", "notatime=2026-13-45T99:99:99",
	}, "\n")
	want := strings.Join([]string{
		"id=" + seeded, "id=<id>", "id=<id>", "id=" + seededV4, "id=" + strings.ToUpper(seededV4),
		"run=<now>", "run=<now>", "run=<now>", "run=<now>",
		"seeded=2026-08-01T00:00:00Z", "request=2031-01-01T00:00:00.5+05:30", "notatime=2026-13-45T99:99:99",
	}, "\n")
	got := scrub(in)
	if got != want {
		t.Fatalf("scrub =\n%s\nwant\n%s", got, want)
	}
	if again := scrub(got); again != got {
		t.Fatalf("scrub is not idempotent:\n%s", again)
	}
	if !strings.Contains(seeded, "-5") {
		t.Fatalf("StableUUID %s is not a version 5 id; the scrub's premise (seeded ids are version 5) is broken", seeded)
	}
}

func TestGoldenProjectAppliesTheSpecsProjectionToAGoPlaneResponse(t *testing.T) {
	golden, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it",
		Scrub: ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":7}`, "sig")
	got := golden.Project(t, Response{Status: 200, Headers: map[string]string{"x-request-id": "r-1", "x-keep": "k"},
		Body: `{"t":"` + token + `","id":"3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f","at":"2026-10-01T12:00:00Z"}`})
	if strings.Contains(got.Body, token) || !strings.Contains(got.Body, "sub=string:u1") || !strings.Contains(got.Body, `"id":"<id>"`) || !strings.Contains(got.Body, `"at":"<now>"`) {
		t.Fatalf("body not projected: %s", got.Body)
	}
	if got.Headers["x-request-id"] != volatileHeaderValue || got.Headers["x-keep"] != "k" {
		t.Fatalf("headers not projected: %v", got.Headers)
	}
}

func TestContentLengthIsAPlaceholderOnlyWhenTheProjectionChangedTheBody(t *testing.T) {
	scrub := ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))
	golden, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it", Scrub: scrub}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := golden.projectResponse(Response{Headers: map[string]string{"content-length": "49"}, Body: `{"id":"0192f3a4-5b6c-7d8e-8f90-a1b2c3d4e5f6"}`})
	if err != nil {
		t.Fatal(err)
	}
	if changed.Headers["content-length"] != projectedLengthValue {
		t.Fatalf("content-length of a changed body = %q", changed.Headers["content-length"])
	}
	same, err := golden.projectResponse(Response{Headers: map[string]string{"content-length": "7"}, Body: `{"a":1}`})
	if err != nil {
		t.Fatal(err)
	}
	if same.Headers["content-length"] != "7" {
		t.Fatalf("content-length of an unchanged body = %q", same.Headers["content-length"])
	}
}
