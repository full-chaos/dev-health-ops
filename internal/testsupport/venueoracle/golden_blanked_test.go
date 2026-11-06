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

// CHAOS-13107: a leaf only the Go plane blanks is declared on the test's spec, by one exact pattern and the reason its
// value is not stable; the header's "blanked" lies inside the bytes the spec's SHA256 pins and stays as recorded.
func TestAGoOnlyBlankedPatternIsAcceptedWhereTheSpecDeclaresItWithItsReason(t *testing.T) {
	token := mintJWT(`{"alg":"EdDSA","typ":"at+jwt"}`, `{"iss":"https://issuer.example","sub":"usr_1"}`, "signature")
	body := `{"access_token":"` + token + `","session_access_token":"` + token + `"}`
	golden := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild}}
	golden.loaded.Header.Blanked = map[string]int{"body $.access_token": 1}
	if _, err := golden.projectResponseAt("", Response{Body: body}); err == nil || !strings.Contains(err.Error(), `"body $.session_access_token"`) {
		t.Fatalf("a Go-only blanked pattern no spec declares was accepted: %v", err)
	}
	golden.spec.GoBlanked = map[string]string{"body $.session_access_token": "a fresh token per run"}
	if _, err := golden.projectResponseAt("", Response{Body: body}); err != nil {
		t.Fatalf("the declared Go-only pattern was refused: %v", err)
	}
	if err := golden.unusedGoBlanked(); err != nil {
		t.Fatalf("a declared pattern that blanked a leaf is reported unused: %v", err)
	}
	// One exact path: a sibling leaf of another name is still refused.
	other := `{"access_token":"` + token + `","session_access_tokens":"` + token + `"}`
	if _, err := golden.projectResponseAt("", Response{Body: other}); err == nil || !strings.Contains(err.Error(), `"body $.session_access_tokens"`) {
		t.Fatalf("a pattern the spec does not name exactly was accepted: %v", err)
	}
}

// A declared Go-only pattern that blanked nothing by Finish is stale: it covers no leaf.
func TestADeclaredGoOnlyBlankedPatternThatBlankedNothingIsReported(t *testing.T) {
	golden := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild,
		GoBlanked: map[string]string{"body $.session_access_token": "a fresh token per run"}}}
	golden.loaded.Header.Blanked = map[string]int{"body $.access_token": 1}
	if err := golden.unusedGoBlanked(); err == nil || !strings.Contains(err.Error(), "body $.session_access_token") {
		t.Fatalf("a declared pattern that blanked nothing was not reported: %v", err)
	}
}

// The declaration is refused when the golden opens unless it is one exact pattern the recording did not list, with a
// reason, on a golden whose header lists what it blanks.
func TestAGoOnlyBlankedDeclarationIsRefusedWhenItIsNotOneExactUnlistedPatternWithAReason(t *testing.T) {
	file := sampleGolden(sampleRequests())
	file.Header.Blanked = map[string]int{"body $.access_token": 1}
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	unlisted := goldenFile{Header: file.Header, Requests: file.Requests, Rows: file.Rows}
	unlisted.Header.Blanked = nil
	unlistedPath, unlistedDigest := writeGoldenFile(t, t.TempDir(), unlisted)
	for name, c := range map[string]struct {
		path, digest string
		declared     map[string]string
		want         string
	}{
		"no reason":                {path, digest, map[string]string{"body $.session_access_token": " "}, "reason"},
		"a glob":                   {path, digest, map[string]string{"body $.*": "a fresh token per run"}, "glob"},
		"a golden with no blanked": {unlistedPath, unlistedDigest, map[string]string{"body $.session_access_token": "a fresh token per run"}, "lists no blanked"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := openGolden(GoldenSpec{Path: c.path, PythonBuild: goldenBuild, SHA256: c.digest, GoBlanked: c.declared}, "TestSample")
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want a refusal naming %q, got %v", c.want, err)
			}
		})
	}
	if _, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest,
		GoBlanked: map[string]string{"body $.session_access_token": "a fresh token per run"}}, "TestSample"); err != nil {
		t.Fatalf("a well-formed declaration was refused: %v", err)
	}
}

// CHAOS-13107: a pattern the recording's header lists (a token it projected to its claims) may be declared too, when the
// Go plane's projection differs from the recording's by design: the test then compares that leaf by a place holder on
// both planes, checks the value itself, and reports each such comparison with Unvalued. The projection alone never
// counts as a use, so a declaration the test never reports is still refused at Finish.
func TestAHeaderListedGoBlankedPatternCountsOnlyWhereTheTestComparesItUnvalued(t *testing.T) {
	file := sampleGolden(sampleRequests())
	file.Header.Blanked = map[string]int{"body $.access_token": 1}
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	const reason = "EdDSA class: issuer URL iss + prn_ sub differ from the HS256 recording by design (13107)"
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest,
		GoBlanked: map[string]string{"body $.access_token": reason}}, "TestSample")
	if err != nil {
		t.Fatalf("a declared pattern the header lists was refused: %v", err)
	}
	token := mintJWT(`{"alg":"EdDSA","typ":"at+jwt"}`, `{"iss":"https://issuer.example","sub":"prn_AAAAAAAAAAAAAAAAAAAAAA"}`, "signature")
	if _, err := golden.projectResponseAt("", Response{Body: `{"access_token":"` + token + `"}`}); err != nil {
		t.Fatalf("the projection of a listed pattern was refused: %v", err)
	}
	if err := golden.unusedGoBlanked(); err == nil || !strings.Contains(err.Error(), "body $.access_token") {
		t.Fatalf("the projection alone counted as a use of the declaration: %v", err)
	}
	for name, pattern := range map[string]string{"undeclared": "body $.refresh_token", "declared nowhere, empty": ""} {
		if err := golden.Unvalued(pattern); err == nil {
			t.Errorf("%s: Unvalued(%q) was accepted", name, pattern)
		}
	}
	if err := golden.Unvalued("body $.access_token"); err != nil {
		t.Fatalf("Unvalued of the declared listed pattern: %v", err)
	}
	if err := golden.unusedGoBlanked(); err != nil {
		t.Fatalf("a declaration the test compared unvalued is reported unused: %v", err)
	}
	// A declared pattern the header does not list is noted by the projection, never by Unvalued.
	unlisted := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild,
		GoBlanked: map[string]string{"body $.session_access_token": "a fresh token per run"}}}
	unlisted.loaded.Header.Blanked = map[string]int{"body $.access_token": 1}
	if err := unlisted.Unvalued("body $.session_access_token"); err == nil || !strings.Contains(err.Error(), "does not list") {
		t.Fatalf("Unvalued of a pattern the header does not list was accepted: %v", err)
	}
	if err := unlisted.unusedGoBlanked(); err == nil {
		t.Fatal("a refused Unvalued counted as a use")
	}
}
