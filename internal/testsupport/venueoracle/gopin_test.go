package venueoracle

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writePinFile(t *testing.T, text string) (string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pin.json")
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return path, sha256Hex([]byte(text))
}

const pinFileText = `{"a": "200 {}", "b": "409 x"}` + "\n"

func TestAGoPinComparesEveryPinnedAnswerOnce(t *testing.T) {
	path, digest := writePinFile(t, pinFileText)
	pin, err := openGoPin(GoPinSpec{Path: path, SHA256: digest, Ruling: "r"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range [][2]string{{"a", "200 {}"}, {"b", "409 x"}} {
		if err := pin.check(check[0], check[1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := pin.finish(); err != nil {
		t.Fatal(err)
	}
}

func TestAGoPinRefusesAnEditedFileAChangedAnswerAndAnUncheckedOne(t *testing.T) {
	path, digest := writePinFile(t, pinFileText)
	spec := GoPinSpec{Path: path, SHA256: digest, Ruling: "r"}
	if _, err := openGoPin(GoPinSpec{Path: path, SHA256: strings.Repeat("0", 64), Ruling: "r"}, false); err == nil || !strings.Contains(err.Error(), "edited by hand") {
		t.Errorf("a file whose digest is not the pinned one was opened: %v", err)
	}
	if _, err := openGoPin(GoPinSpec{Path: path, SHA256: digest}, false); err == nil {
		t.Error("a pin without a ruling was opened")
	}
	pin, err := openGoPin(spec, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := pin.check("a", `200 {"changed":1}`); err == nil || !strings.Contains(err.Error(), "differs from the pinned one") {
		t.Errorf("a changed answer was accepted: %v", err)
	}
	if err := pin.check("a", "200 {}"); err == nil || !strings.Contains(err.Error(), "checked twice") {
		t.Errorf("a second check of one name was accepted: %v", err)
	}
	if err := pin.check("c", "200 {}"); err == nil || !strings.Contains(err.Error(), "holds no answer") {
		t.Errorf("an answer the file does not hold was accepted: %v", err)
	}
	if err := pin.finish(); err == nil || !strings.Contains(err.Error(), "never checked: b") {
		t.Errorf("an unchecked pinned answer was accepted: %v", err)
	}
}

func TestAGoPinRecordingWritesTheFileAndNeverPasses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pin.json")
	pin, err := openGoPin(GoPinSpec{Path: path, Ruling: "r"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := pin.check("a", `200 {"t":"<time>"}`); err != nil {
		t.Fatal(err)
	}
	if err := pin.finish(); !errors.Is(err, errGoPinRecorded) {
		t.Fatalf("a recording finished with %v, want the recorded refusal", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{\n  \"a\": \"200 {\\\"t\\\":\\\"<time>\\\"}\"\n}\n" {
		t.Fatalf("recorded file = %q", raw)
	}
}

// A retired answer is used once and counted in the proof; a retirement needs
// a reason.
func TestARetiredAnswerIsUsedOnceAndNamedInTheProof(t *testing.T) {
	golden, requests, answers := handOut(t)
	if err := golden.bindAnswers(requests, answers); err != nil {
		t.Fatal(err)
	}
	golden.retireBound(answers[1], "ruling X")
	if err := golden.answersCompared(); err != nil {
		t.Fatalf("a retired answer was counted as uncompared: %v", err)
	}
	if !golden.slots[1].consumed || golden.slots[1].compared {
		t.Fatalf("slot 2 = %+v, want retired (consumed, not compared)", golden.slots[1])
	}
	if err := golden.consume(answers[1:]); err == nil || !strings.Contains(err.Error(), "already used once") {
		t.Fatalf("a retired answer was used again: %v", err)
	}
	if text := golden.retiredText(); text != "; 1 answers and 0 row comparisons retired by ruling X" {
		t.Fatalf("proof text = %q", text)
	}
	if err := retiredReasonErr(" "); err == nil {
		t.Fatal("an empty reason was accepted")
	}
}

func TestAGoPinWithAnUnrecordedDigestNeverOpens(t *testing.T) {
	path, _ := writePinFile(t, pinFileText)
	if _, err := openGoPin(GoPinSpec{Path: path, SHA256: "RECORD-ME", Ruling: "r"}, false); err == nil {
		t.Fatal("a placeholder digest opened a pin file")
	}
	if _, err := openGoPin(GoPinSpec{Path: filepath.Join(t.TempDir(), "missing.json"), SHA256: "RECORD-ME", Ruling: "r"}, false); err == nil {
		t.Fatal("a missing pin file opened")
	}
}

// Retire wins over Retirements, the first matching Retirement wins after it, and a request no rule names is
// compared (no pin).
func TestDiffRetiringPinPicksTheRulingThatNamesTheRequest(t *testing.T) {
	first, second, third := &GoPin{spec: GoPinSpec{Ruling: "first"}}, &GoPin{spec: GoPinSpec{Ruling: "second"}}, &GoPin{spec: GoPinSpec{Ruling: "third"}}
	named := func(names ...string) func(Request) bool {
		return func(r Request) bool {
			for _, name := range names {
				if r.Name == name {
					return true
				}
			}
			return false
		}
	}
	options := DiffOptions{Retire: named("a"), Pin: first, Retirements: []Retirement{{Match: named("a", "b"), Pin: second}, {Match: named("b", "c"), Pin: third}}}
	for name, want := range map[string]*GoPin{"a": first, "b": second, "c": third, "d": nil} {
		if got := options.retiringPin(Request{Name: name}); got != want {
			t.Errorf("%s: pin %v, want %v", name, got, want)
		}
	}
	if got := (DiffOptions{Retirements: []Retirement{{Match: named("x"), Pin: second}}}).retiringPin(Request{Name: "x"}); got != second {
		t.Errorf("a Retirement without Retire: pin %v, want the second", got)
	}
}

func TestFinishPinsWritesEveryRecordingPinAndNeverPasses(t *testing.T) {
	firstPath := filepath.Join(t.TempDir(), "first.json")
	secondPath := filepath.Join(t.TempDir(), "second.json")
	first, err := openGoPin(GoPinSpec{Path: firstPath, Ruling: "r1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := openGoPin(GoPinSpec{Path: secondPath, Ruling: "r2"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.check("a", "200 {}"); err != nil {
		t.Fatal(err)
	}
	if err := second.check("b", "401 x"); err != nil {
		t.Fatal(err)
	}
	err = finishPins([]*GoPin{first, second})
	if !errors.Is(err, errGoPinRecorded) {
		t.Fatalf("finishPins = %v, want a recorded error", err)
	}
	firstRaw, firstErr := os.ReadFile(firstPath)
	if firstErr != nil || !strings.Contains(string(firstRaw), `"a"`) {
		t.Errorf("first pin not written: %q %v", firstRaw, firstErr)
	}
	secondRaw, secondErr := os.ReadFile(secondPath)
	if secondErr != nil || !strings.Contains(string(secondRaw), `"b"`) {
		t.Errorf("second pin not written: %q %v", secondRaw, secondErr)
	}
	for _, path := range []string{firstPath, secondPath} {
		if !strings.Contains(err.Error(), path) {
			t.Errorf("finishPins error %q does not name %s", err, path)
		}
	}
}

func TestFinishPinsPassesOnlyWhenEveryPinPasses(t *testing.T) {
	path, digest := writePinFile(t, pinFileText)
	good, err := openGoPin(GoPinSpec{Path: path, SHA256: digest, Ruling: "r"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, check := range [][2]string{{"a", "200 {}"}, {"b", "409 x"}} {
		if err := good.check(check[0], check[1]); err != nil {
			t.Fatal(err)
		}
	}
	unchecked, err := openGoPin(GoPinSpec{Path: path, SHA256: digest, Ruling: "r"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := finishPins([]*GoPin{good}); err != nil {
		t.Fatalf("finishPins of a checked pin = %v, want nil", err)
	}
	if err := finishPins([]*GoPin{good, unchecked}); err == nil || !strings.Contains(err.Error(), "never checked") {
		t.Fatalf("finishPins with an unchecked pin = %v, want its error", err)
	}
}

// A known divergence names its ticket; the first that matches a request wins, and a request none names is compared.
// Its subtest fails once the answers agree, so the entry is removed.
func TestDiffKnownDivergencePicksTheTicketAndFailsOnceTheAnswersAgree(t *testing.T) {
	named := func(names ...string) func(Request) bool {
		return func(r Request) bool { return slices.Contains(names, r.Name) }
	}
	options := DiffOptions{KnownDivergences: []KnownDivergence{{Match: named("a", "b"), Ticket: "CHAOS-1: first"}, {Match: named("b", "c"), Ticket: "CHAOS-2: second"}}}
	for name, want := range map[string]string{"a": "CHAOS-1: first", "b": "CHAOS-1: first", "c": "CHAOS-2: second", "d": ""} {
		if got, known := options.knownDivergence(Request{Name: name}); got != want || known != (want != "") {
			t.Errorf("%s: ticket %q %v, want %q", name, got, known, want)
		}
	}
	if err := knownDivergenceErr("a", "CHAOS-1: first", false); err != nil {
		t.Errorf("a divergent answer: %v, want a skip", err)
	}
	if err := knownDivergenceErr("a", "CHAOS-1: first", true); err == nil || !strings.Contains(err.Error(), "remove the known divergence (CHAOS-1: first)") {
		t.Errorf("an agreeing answer: %v, want the failure naming the ticket", err)
	}
	for _, ticket := range []string{"", "no id", "chaos-1: lower", "CHAOS-1: two\nlines"} {
		if knownDivergenceTicket.MatchString(ticket) && !strings.ContainsAny(ticket, "\n\r") {
			t.Errorf("ticket %q accepted", ticket)
		}
	}
}

// Diff skips a known divergence whose Go answer differs, names it in the receipt and compares every other request.
func TestDiffSkipsADivergentKnownDivergence(t *testing.T) {
	goPlane := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/gap" {
			w.WriteHeader(http.StatusUnauthorized)
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	defer goPlane.Close()
	requests := []Request{{Name: "gap", Method: "GET", Path: "/gap"}, {Name: "same", Method: "GET", Path: "/same"}}
	answer := func(status int) Response {
		return Response{Status: status, Headers: map[string]string{"content-type": "application/json", "content-length": "2"}, Body: `{}`}
	}
	receipt := Diff(t, goPlane.URL, requests, []Response{answer(200), answer(200)}, DiffOptions{
		KnownDivergences: []KnownDivergence{{Match: func(r Request) bool { return r.Name == "gap" }, Ticket: "CHAOS-1: gap"}},
	})
	if !strings.Contains(receipt, "python=200 go=401 SKIP (CHAOS-1: gap)") || !strings.Contains(receipt, "python=200 go=200 SAME") {
		t.Fatalf("receipt:\n%s", receipt)
	}
}
