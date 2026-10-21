package venueoracle

import (
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

func TestAReplayThatBlanksAPatternTheRecordingDidNotIsRefused(t *testing.T) {
	scrub := ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))
	golden := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild, Scrub: scrub}}
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
