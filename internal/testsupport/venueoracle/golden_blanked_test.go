package venueoracle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLeafDiffsNamesTheLeavesAProjectionReplacedByPathWithoutIndexes(t *testing.T) {
	before := `{"a":{"id":"3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f"},"items":[{"at":"x1"},{"at":"x2"}],"n":1,"keep":"same"}`
	after := `{"a":{"id":"<id>"},"items":[{"at":"<now>"},{"at":"<now>"}],"n":1,"keep":"same"}`
	got := leafDiffs("body", before, after)
	patterns := []string{}
	for _, leaf := range got {
		patterns = append(patterns, leaf.pattern)
	}
	if strings.Join(patterns, ",") != "body $.a.id,body $.items[].at,body $.items[].at" {
		t.Fatalf("patterns = %v", patterns)
	}
	if got[0].raw != "3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f" {
		t.Fatalf("raw = %q", got[0].raw)
	}
	text := leafDiffs("rows mail", "link 3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f\nline two", "link <id>\nline two")
	if len(text) != 1 || text[0].pattern != "rows mail text" {
		t.Fatalf("text diff = %+v", text)
	}
	if len(leafDiffs("body", before, before)) != 0 {
		t.Fatal("equal texts differ")
	}
}

func recordingWith(t *testing.T, scrub func(string) string) *Golden {
	t.Helper()
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it", Scrub: scrub}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	golden.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
	return golden
}

func TestARecordingWritesWhatItBlankedAsPatternsAndOnlyDigestsOfRawValues(t *testing.T) {
	scrub := ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))
	golden := recordingWith(t, scrub)
	rawID := "3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f"
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":7}`, "sig")
	answer, err := golden.projectResponseAt("login", Response{Status: 200, Headers: map[string]string{"x-request-id": "req-1", "x-keep": "k"},
		Body: `{"id":"` + rawID + `","token":"` + token + `","at":"2026-10-01T12:00:00Z","same":"s"}`})
	if err != nil {
		t.Fatal(err)
	}
	golden.recorded.Requests = []goldenRequest{{Name: "login", Status: answer.Status, Headers: answer.Headers, Body: answer.Body}}
	if _, err := golden.writeCandidate(false); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(golden.spec.Path + GoldenCandidateSuffix)
	var file goldenFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	want := map[string]int{"body $.id": 1, "body $.token": 1, "body $.at": 1, "header x-request-id": 1}
	if len(file.Header.Blanked) != len(want) {
		t.Fatalf("blanked = %v, want %v", file.Header.Blanked, want)
	}
	for pattern, count := range want {
		if file.Header.Blanked[pattern] != count {
			t.Fatalf("blanked = %v, want %v", file.Header.Blanked, want)
		}
	}
	sidecar, err := os.ReadFile(golden.spec.Path + GoldenCandidateSuffix + scrubSidecarSuffix)
	if err != nil {
		t.Fatal(err)
	}
	var entries map[string]string
	if err := json.Unmarshal(sidecar, &entries); err != nil {
		t.Fatal(err)
	}
	if _, ok := entries["login|body $.id|1"]; !ok || len(entries) != 2 {
		t.Fatalf("sidecar = %v: want the scrubbed id and time of request login, not the token or the Volatile header", entries)
	}
	if strings.Contains(string(sidecar), rawID) || strings.Contains(string(sidecar), "2026-10-01") {
		t.Fatalf("the sidecar holds a raw value: %s", sidecar)
	}
}

func TestAReplayThatBlanksAPatternTheRecordingDidNotIsRefused(t *testing.T) {
	scrub := ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))
	golden := recordingWith(t, scrub)
	golden.recording = false
	golden.loaded.Header.Blanked = map[string]int{"body $.id": 1}
	random := "3f2a9c10-7b1e-4c55-9d02-1a2b3c4d5e6f"
	if _, err := golden.projectResponseAt("r", Response{Body: `{"id":"` + random + `"}`}); err != nil {
		t.Fatalf("a listed pattern was refused: %v", err)
	}
	if _, err := golden.projectResponseAt("r", Response{Body: `{"other":"` + random + `"}`}); err == nil || !strings.Contains(err.Error(), "body $.other") {
		t.Fatalf("a pattern the recording did not blank was accepted: %v", err)
	}
	golden.loaded.Header.Blanked = nil
	if _, err := golden.projectResponseAt("r", Response{Body: `{"other":"` + random + `"}`}); err != nil {
		t.Fatalf("a golden recorded before the list was checked against it: %v", err)
	}
}

func TestTimestampsInTheKeepListStayCompared(t *testing.T) {
	scrub := ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC), "2026-10-02T00:00:00Z")
	if got := scrub(`a=2026-10-02T00:00:00Z b=2026-10-02T00:00:01Z`); got != "a=2026-10-02T00:00:00Z b=<now>" {
		t.Fatalf("scrub = %q", got)
	}
}
