package venueoracle

import (
	"errors"
	"os"
	"path/filepath"
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
