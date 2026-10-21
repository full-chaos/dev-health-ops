package venueoracle

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func keyGolden(t *testing.T, scrub func(string) string) *Golden {
	t.Helper()
	return &Golden{spec: GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, KeyScrub: scrub}}
}

// goldenText is a golden file holding requests, as the scan of every golden in
// the repo (tokenShapeErr, GoldenTokenViolations) reads it.
func goldenText(t *testing.T, requests ...goldenRequest) []byte {
	t.Helper()
	raw, err := json.Marshal(goldenFile{Header: goldenHeader{Test: t.Name(), PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("c", 64)}, Requests: requests})
	if err != nil {
		t.Fatal(err)
	}
	return raw
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

func TestAGoldenKeyedByTokenRequestsHoldsNoTokenShape(t *testing.T) {
	g := keyGolden(t, nil)
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1}`, "sig")
	request := Request{Name: "r", Method: "GET", Path: "/x?t=" + token, Body: B64(`{"t":"` + token + `"}`), Headers: map[string]string{"X-Token": token, "Authorization": "Bearer " + token}}
	entry := g.keyOf(request)
	entry.Status, entry.Body = 200, "{}"
	raw := goldenText(t, entry)
	if err := tokenShapeErr(g.spec.Path, raw); err != nil {
		t.Fatalf("a golden keyed by a token request was refused: %v", err)
	}
	if found := TokenShapesIn(string(raw)); len(found) > 0 || strings.Contains(string(raw), token) {
		t.Fatalf("the golden holds a token through its request key: %v", found)
	}
}

func TestAnIdTheTestWritesInARequestStaysInItsKeyWhateverTheAnswerScrub(t *testing.T) {
	g := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild, Scrub: ScrubRunValues(time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC), time.Date(2030, 12, 1, 0, 0, 0, 0, time.UTC))}}
	request := Request{Name: "ent", Method: "GET", Path: "/entitlements/00000000-0000-4000-8000-000000000000?at=2026-10-01T00:00:00Z"}
	if !reflect.DeepEqual(g.keyOf(request), requestKey(request)) {
		t.Fatal("the answer Scrub changed a request's key: every merged golden whose requests hold a v4-shaped id or a time would stop matching")
	}
}

func TestTheAuthorizationHeaderKeepsItsKeyAndIsNotProjectedTwice(t *testing.T) {
	g := keyGolden(t, linkScrub)
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1,"exp":61}`, "sig")
	request := Request{Name: "me", Method: "GET", Path: "/me", Headers: map[string]string{"Authorization": "Bearer " + token}}
	if got, want := g.keyOf(request), requestKey(request); !reflect.DeepEqual(got, want) {
		t.Fatalf("a bearer token in authorization changed the key (headersDigest already reduces it to its claims):\n%+v\n%+v", got, want)
	}
}

func TestAReplayRequestCarryingAProjectedTokenFindsItsRecordedAnswerOnlyThroughTheProjectedKey(t *testing.T) {
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1,"exp":61}`, "sig")
	recording := keyGolden(t, nil)
	real := Request{Name: "refresh", Method: "POST", Path: "/refresh", Body: B64(`{"refresh_token":"` + token + `"}`)}
	entry := recording.keyOf(real)
	entry.Status, entry.Body = 200, `{"ok":true}`
	replay := &Golden{spec: GoldenSpec{Path: "g.json", PythonBuild: goldenBuild}, loaded: goldenFile{Requests: []goldenRequest{entry}}}
	projected := Request{Name: "refresh", Method: "POST", Path: "/refresh", Body: B64(`{"refresh_token":"` + ProjectTokens(token) + `"}`)}
	answers, err := replay.frozenAnswers([]Request{projected})
	if err != nil || len(answers) != 1 || answers[0].Body != `{"ok":true}` {
		t.Fatalf("a request carrying the projected token did not find its answer: %v %+v", err, answers)
	}
	// A replay request that still carries a real token (another mint of the
	// same caller) finds the answer too: only the projected key says so.
	replay.served = 0
	another := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":900,"exp":960}`, "other-signature")
	if _, err := replay.frozenAnswers([]Request{{Name: "refresh", Method: "POST", Path: "/refresh", Body: B64(`{"refresh_token":"` + another + `"}`)}}); err != nil {
		t.Fatalf("a request carrying another mint of the same caller did not find the answer: %v", err)
	}
	replay.served = 0
	if _, err := replay.frozenAnswers([]Request{{Name: "refresh", Method: "POST", Path: "/refresh", Body: B64(`{"refresh_token":"other"}`)}}); err == nil {
		t.Fatal("a request with another body found the recorded answer")
	}
}

func TestAProgramRequestKeepsItsKeyAndIsNotProjected(t *testing.T) {
	g := keyGolden(t, linkScrub)
	token := mintJWT(`{"alg":"HS256"}`, `{"sub":"u1","iat":1,"exp":61}`, "sig")
	stdin := []byte(strings.Repeat("corpus line "+token+"\n", 3))
	request := ProgramRequest("corpus", stdin)
	if !reflect.DeepEqual(g.keyOf(request), requestKey(request)) {
		t.Fatal("a program request was projected: its corpus is not a credential and projecting it costs minutes under the race detector")
	}
}

// TestACredentialShapedCorpusReachesNoGoldenThroughItsProgramKey pins why a program
// request needs no projection: only the sha256 of its stdin enters the key, so a
// corpus (credential-shaped fixtures included) never appears in the golden.
func TestACredentialShapedCorpusReachesNoGoldenThroughItsProgramKey(t *testing.T) {
	sample := tokenSamples()["github-token"]
	g := keyGolden(t, nil)
	request := ProgramRequest("redaction inputs", []byte("input one\n"+sample+"\ninput three\n"))
	entry := g.keyOf(request)
	entry.Status, entry.Body = 0, "ok"
	raw := goldenText(t, entry)
	if err := tokenShapeErr(g.spec.Path, raw); err != nil {
		t.Fatalf("a golden keyed by a program request with a credential-shaped corpus was refused: %v", err)
	}
	if strings.Contains(string(raw), sample) || strings.Contains(string(raw), "input one") || len(TokenShapesIn(string(raw))) > 0 {
		t.Fatalf("the corpus reached the golden through the program request key: %s", raw)
	}
	if entry.BodySHA256 == "" || len(entry.BodySHA256) != 64 {
		t.Fatalf("the key holds %q of the body, want its sha256", entry.BodySHA256)
	}
}
