package venueoracle

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func keyGolden(t *testing.T, scrub func(string) string) *Golden {
	t.Helper()
	g, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it", KeyScrub: scrub}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	g.recorded.Header.ProducerDigest = strings.Repeat("c", 64)
	return g
}

var linkScrub = func(text string) string {
	return strings.NewReplacer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "<link>", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "<link>").Replace(text)
}

func TestARequestWithoutATokenKeepsItsOldKeyByteForByte(t *testing.T) {
	g := keyGolden(t, ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC)))
	request := Request{Name: "plain", Method: "POST", Path: "/x?y=1", Body: B64(`{"a":1,"id":"` + StableUUID("seed") + `"}`), Headers: map[string]string{"Content-Type": "application/json"}}
	if got, want := g.keyOf(request), requestKey(request); !reflect.DeepEqual(got, want) {
		t.Fatalf("a request the projection leaves alone changed its key:\n%+v\n%+v", got, want)
	}
}

func TestRequestsCarryingTokensKeyByTheirProjectionInRecordingAndReplay(t *testing.T) {
	g := keyGolden(t, linkScrub)
	jwtA := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1,"exp":61}`, "sig-a")
	jwtB := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":900,"exp":960}`, "sig-b")
	build := func(jwt, link string) Request {
		return Request{Name: "refresh", Method: "POST", Path: "/accept?token=" + link,
			Body:    B64(`{"refresh_token":"` + jwt + `"}`),
			Headers: map[string]string{"X-Link": link}}
	}
	one, two := g.keyOf(build(jwtA, strings.Repeat("a", 32))), g.keyOf(build(jwtB, strings.Repeat("b", 32)))
	if !reflect.DeepEqual(one, two) {
		t.Fatalf("two mints of the same caller key differently:\n%+v\n%+v", one, two)
	}
	// The replay builds the request from the projected answer.
	replay := build(ProjectTokens(jwtA), "<link>")
	if got := g.keyOf(replay); !reflect.DeepEqual(got, one) {
		t.Fatalf("the replay's request keys differently from the recording's:\n%+v\n%+v", got, one)
	}
	other := build(mintJWT(`{"alg":"HS256"}`, `{"sub":"u2","iat":1,"exp":61}`, "sig"), strings.Repeat("a", 32))
	if reflect.DeepEqual(g.keyOf(other), one) {
		t.Fatal("a different caller keys the same")
	}
	raw, _ := json.Marshal(one)
	if found := TokenShapesIn(string(raw)); len(found) > 0 {
		t.Fatalf("the key holds a token shape: %v", found)
	}
}

func TestTwoRequestsWithOneProjectedKeyAndDifferentAnswersAreRefused(t *testing.T) {
	g := keyGolden(t, linkScrub)
	record := func(link, body string) error {
		request := Request{Name: "accept", Method: "GET", Path: "/accept?token=" + link}
		entry := g.keyOf(request)
		entry.Status, entry.Body = 200, body
		if err := g.sameKeySameAnswerErr(entry); err != nil {
			return err
		}
		g.recorded.Requests = append(g.recorded.Requests, entry)
		return nil
	}
	if err := record(strings.Repeat("a", 32), `{"ok":true}`); err != nil {
		t.Fatal(err)
	}
	if err := record(strings.Repeat("b", 32), `{"ok":true}`); err != nil {
		t.Fatalf("two requests answered alike must fold: %v", err)
	}
	if err := record(strings.Repeat("b", 32), `{"ok":false}`); err == nil || !strings.Contains(err.Error(), `two requests named "accept"`) {
		t.Fatalf("a second request with another token and another answer was accepted: %v", err)
	}
}

func TestAGoldenRecordedFromTokenRequestsHoldsNoTokenShape(t *testing.T) {
	g := keyGolden(t, nil)
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1}`, "sig")
	request := Request{Name: "r", Method: "GET", Path: "/x?t=" + token, Body: B64(`{"t":"` + token + `"}`), Headers: map[string]string{"X-Token": token, "Authorization": "Bearer " + token}}
	entry := g.keyOf(request)
	entry.Status, entry.Body = 200, "{}"
	g.recorded.Requests = []goldenRequest{entry}
	if _, err := g.writeCandidate(false); err != nil {
		t.Fatalf("a recorded token request was refused: %v", err)
	}
	raw, err := os.ReadFile(g.spec.Path + GoldenCandidateSuffix)
	if err != nil {
		t.Fatal(err)
	}
	if found := TokenShapesIn(string(raw)); len(found) > 0 || strings.Contains(string(raw), token) {
		t.Fatalf("the golden holds a token through its request key: %v", found)
	}
}

// TestARecordingRefusesTwoRequestsOfOneProjectedKeyAnsweredDifferently runs the
// recording path (Golden.answer) in a child test process, because the refusal
// is t.Fatal: it shows the check is wired into the recording, not only that the
// helper works.
func TestARecordingRefusesTwoRequestsOfOneProjectedKeyAnsweredDifferently(t *testing.T) {
	if os.Getenv("VENUEORACLE_CHILD_SAMEKEY") == "1" {
		g := keyGolden(t, linkScrub)
		g.verifiedRoot = "x"
		requests := []Request{
			{Name: "accept", Method: "GET", Path: "/accept?token=" + strings.Repeat("a", 32)},
			{Name: "accept", Method: "GET", Path: "/accept?token=" + strings.Repeat("b", 32)},
		}
		answers := []Response{{Status: 200, Body: `{"ok":true}`}, {Status: 200, Body: `{"ok":false}`}}
		g.answer(t, "Python", "", requests, func() error { return nil }, func() []Response { return answers }, nil)
		return
	}
	command := exec.Command(os.Args[0], "-test.run=^TestARecordingRefusesTwoRequestsOfOneProjectedKeyAnsweredDifferently$", "-test.v")
	command.Env = append(os.Environ(), "VENUEORACLE_CHILD_SAMEKEY=1")
	out, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(out), `two requests named "accept"`) {
		t.Fatalf("the recording did not refuse: err=%v\n%s", err, out)
	}
}

func TestAnIdTheTestWritesInARequestStaysInItsKeyWhateverTheAnswerScrub(t *testing.T) {
	g, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it",
		Scrub: ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	request := Request{Name: "ent", Method: "GET", Path: "/entitlements/00000000-0000-4000-8000-000000000000?at=2026-10-01T00:00:00Z"}
	if !reflect.DeepEqual(g.keyOf(request), requestKey(request)) {
		t.Fatal("the answer Scrub changed a request's key: every merged golden whose requests hold a v4-shaped id or a time would stop matching")
	}
}
