package venueoracle

import (
	"path/filepath"
	"strings"
	"testing"
)

const sampleProgram = "import sys\nprint(sys.stdin.read().upper())\n"

func programGolden(t *testing.T, requests []Request, bodies ...string) (string, string) {
	t.Helper()
	file := goldenFile{Header: goldenHeader{Test: t.Name(), PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it"}}
	for index, request := range requests {
		entry := requestKey(request)
		entry.Status, entry.Headers, entry.Body = 0, map[string]string{"stderr": ""}, bodies[index]
		file.Requests = append(file.Requests, entry)
	}
	return writeGoldenFile(t, t.TempDir(), file)
}

func TestFrozenProduceAnswersFromTheFileWithoutRunningTheProducer(t *testing.T) {
	t.Setenv(goldenUpdateEnv, "")
	t.Setenv(goldenCandidateEnv, "")
	requests := []Request{ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"})}
	path, digest := programGolden(t, requests, "ABC\n")
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
	answers := golden.Produce(t, "/no/python/here", requests, func(string, []Request) []Response {
		t.Fatal("a frozen golden ran its producer")
		return nil
	})
	if len(answers) != 1 || answers[0].Body != "ABC\n" || answers[0].Status != 0 {
		t.Fatalf("answers = %+v", answers)
	}
	golden.Consumed(t, answers...)
	golden.SkipDiff(t)
	golden.Finish(t)
}

// The request key holds the program text's digest, its stdin and its
// environment: a frozen answer is never handed to a changed program, input or
// configuration.
func TestAProducerRequestIsKeyedByProgramInputAndEnvironment(t *testing.T) {
	base := ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"})
	for name, other := range map[string]Request{
		"program":     ProgramRequest("corpus", sampleProgram+"# changed\n", []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"}),
		"stdin":       ProgramRequest("corpus", sampleProgram, []byte("abd"), map[string]string{"PYTHONHASHSEED": "0"}),
		"environment": ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "1"}),
		"no env":      ProgramRequest("corpus", sampleProgram, []byte("abc"), nil),
	} {
		if requestIdentity(other) == requestIdentity(base) {
			t.Errorf("a changed %s keeps the request's identity", name)
		}
	}
	if requestIdentity(ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"})) != requestIdentity(base) {
		t.Error("the same program, input and environment is a different request")
	}

	t.Setenv(goldenUpdateEnv, "")
	t.Setenv(goldenCandidateEnv, "")
	path, digest := programGolden(t, []Request{base}, "ABC\n")
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, t.Name(), false)
	if err != nil {
		t.Fatal(err)
	}
	changed := ProgramRequest("corpus", sampleProgram+"# changed\n", []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"})
	if _, err := golden.frozenAnswers([]Request{changed}); err == nil || !strings.Contains(err.Error(), "regenerate") {
		t.Fatalf("a changed program was answered from the file: %v", err)
	}
}

func TestARecordingRunsTheProducerOnlyFromTheVerifiedRoot(t *testing.T) {
	golden, err := openGolden(GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	verified, other := t.TempDir(), t.TempDir()
	if err := golden.producerRootErr(verified); err == nil || !strings.Contains(err.Error(), "PythonRoot") {
		t.Fatalf("an unverified root was accepted: %v", err)
	}
	golden.verifiedRoot = verified
	if err := golden.producerRootErr(verified); err != nil {
		t.Fatalf("the verified root was refused: %v", err)
	}
	if err := golden.producerRootErr(other); err == nil || !strings.Contains(err.Error(), "not from the verified checkout") {
		t.Fatalf("another root was accepted: %v", err)
	}
	// Recording, the producer runs and its answers are what the file will hold.
	golden.state = stateOpen
	requests := []Request{ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)}
	ran := 0
	answers := golden.Produce(t, verified, requests, func(root string, got []Request) []Response {
		ran++
		if root != verified || len(got) != 1 {
			t.Fatalf("producer ran from %s with %d requests", root, len(got))
		}
		return []Response{{Status: 3, Headers: map[string]string{"stderr": "boom"}, Body: "out"}}
	})
	if ran != 1 || answers[0].Status != 3 || answers[0].Body != "out" {
		t.Fatalf("ran %d, answers %+v", ran, answers)
	}
	if len(golden.recorded.Requests) != 1 || golden.recorded.Requests[0].Status != 3 || golden.recorded.Requests[0].Headers["stderr"] != "boom" ||
		golden.recorded.Requests[0].Path != requests[0].Path {
		t.Fatalf("recorded %+v", golden.recorded.Requests)
	}
}

func TestAPackedBodyUnpacksToTheSameBytesAndPacksTheSameWay(t *testing.T) {
	raw := []byte("{\"exit\": 2, \"ns\": null}\n\xff\x00 caf\xc3\xa9 " + strings.Repeat("x", 5000))
	packed := PackBody(raw)
	if !strings.HasPrefix(packed, packedPrefix) || len(packed) >= len(raw) {
		t.Fatalf("packed %d bytes into %d: %.40s", len(raw), len(packed), packed)
	}
	if again := PackBody(raw); again != packed {
		t.Fatal("the same output packed to another text: a recording and its replay would disagree")
	}
	if got := UnpackBody(t, packed); got != string(raw) {
		t.Fatal("the unpacked body is not the packed bytes")
	}
	for name, body := range map[string]string{
		"not packed":  string(raw),
		"not base64":  packedPrefix + "%%%",
		"not gzip":    packedPrefix + "aGVsbG8=",
		"truncated":   packed[:len(packed)-12],
		"empty input": packedPrefix,
	} {
		if _, err := unpackBody(body); err == nil {
			t.Errorf("%s: unpacked without an error", name)
		}
	}
}
