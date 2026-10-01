package venueoracle

import (
	"os"
	"os/exec"
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
	answers := golden.Produce(t, "/no/python/here", requests, func(*Producer, []Request) []Response {
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
		"program":       ProgramRequest("corpus", sampleProgram+"# changed\n", []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"}),
		"stdin":         ProgramRequest("corpus", sampleProgram, []byte("abd"), map[string]string{"PYTHONHASHSEED": "0"}),
		"environment":   ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "1"}),
		"no env":        ProgramRequest("corpus", sampleProgram, []byte("abc"), nil),
		"env name case": ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"pythonhashseed": "0"}),
		"extra env":     ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "0", "A": ""}),
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
	if _, err := golden.frozenAnswers([]Request{changed}, ""); err == nil || !strings.Contains(err.Error(), "regenerate") {
		t.Fatalf("a changed program was answered from the file: %v", err)
	}
}

// A producer request carries no headers: they are keyed case-folded (HTTP
// semantics), which would let an environment that differs only in a name's
// case replay another's answers.
func TestProduceRefusesARequestThatCarriesHeaders(t *testing.T) {
	plain := ProgramRequest("corpus", sampleProgram, []byte("abc"), map[string]string{"PYTHONHASHSEED": "0"})
	withHeaders := plain
	withHeaders.Headers = map[string]string{"PYTHONHASHSEED": "0"}
	if os.Getenv("VENUEORACLE_PRODUCE_HEADERS_CHILD") == "1" {
		// The child: Produce must fail the test before any answer is read.
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		path, digest := programGolden(t, []Request{plain}, "ABC\n")
		golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"})
		golden.Produce(t, "/no/python/here", []Request{withHeaders}, func(*Producer, []Request) []Response {
			t.Fatal("the producer ran")
			return nil
		})
		t.Log("PRODUCE RETURNED")
		return
	}
	if err := producerRequestsErr([]Request{plain}); err != nil {
		t.Fatalf("a program request was refused: %v", err)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestProduceRefusesARequestThatCarriesHeaders$", "-test.v")
	child.Env = append(os.Environ(), "VENUEORACLE_PRODUCE_HEADERS_CHILD=1")
	output, err := child.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "carries headers") || strings.Contains(string(output), "PRODUCE RETURNED") {
		t.Fatalf("Produce answered a request that carries headers (err %v):\n%s", err, output)
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
	answers := golden.Produce(t, verified, requests, func(producer *Producer, got []Request) []Response {
		ran++
		if producer.Root != verified || len(got) != 1 {
			t.Fatalf("producer ran from %s with %d requests", producer.Root, len(got))
		}
		// While the producer runs, a Python child that inherits the process
		// environment cannot start (producer.go).
		if got := os.Getenv(producerPoisonName); got != producerPoison {
			t.Fatalf("while the producer runs %s = %q, want the poison", producerPoisonName, got)
		}
		return []Response{{Status: 3, Headers: map[string]string{"stderr": "boom"}, Body: "out"}}
	})
	if ran != 1 || answers[0].Status != 3 || answers[0].Body != "out" {
		t.Fatalf("ran %d, answers %+v", ran, answers)
	}
	if got, held := os.LookupEnv(producerPoisonName); held {
		t.Fatalf("after the producer ran %s is still %q", producerPoisonName, got)
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

// A variable the recorder passes by name is part of the golden: the recording
// is refused unless the spec declares exactly the passed names, the header
// holds them, and a frozen run refuses a header that differs from the spec.
func TestAPassedVariableIsPartOfTheGolden(t *testing.T) {
	spec := GoldenSpec{Path: filepath.Join(t.TempDir(), "g.json"), PythonBuild: goldenBuild, Recipe: "record it"}
	for name, c := range map[string]struct {
		declared []string
		passed   string
		ok       bool
	}{
		"nothing passed, nothing declared": {nil, "", true},
		"passed but not declared":          {nil, "SECRET_KEY", false},
		"declared but not passed":          {[]string{"SECRET_KEY"}, "", false},
		"the same names, any order":        {[]string{"B", "A"}, "A,B", true},
		"another name":                     {[]string{"A"}, "a", false},
		"one more passed":                  {[]string{"A"}, "A,B", false},
	} {
		spec.PassEnv = c.declared
		if err := passedEnvErr(spec, c.passed); (err == nil) != c.ok {
			t.Errorf("%s: err = %v", name, err)
		}
	}

	// Recording: openGolden reads what the recorder passed.
	spec.PassEnv = nil
	t.Setenv(goldenPassedEnv, "SECRET_KEY")
	if _, err := openGolden(spec, "TestSample", true); err == nil || !strings.Contains(err.Error(), "SECRET_KEY") {
		t.Fatalf("a recording with an undeclared passed variable was opened: %v", err)
	}
	spec.PassEnv = []string{"SECRET_KEY"}
	recording, err := openGolden(spec, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if got := recording.recorded.Header.PassedEnv; len(got) != 1 || got[0] != "SECRET_KEY" {
		t.Fatalf("the header holds %q", got)
	}

	// Frozen: the header's names must be the spec's.
	t.Setenv(goldenPassedEnv, "")
	t.Setenv(goldenUpdateEnv, "")
	t.Setenv(goldenCandidateEnv, "")
	request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it", PassedEnv: []string{"SECRET_KEY"}}}
	entry := requestKey(request)
	entry.Body = "ABC\n"
	file.Requests = append(file.Requests, entry)
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	frozen := GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}
	if _, err := openGolden(frozen, "TestSample", false); err == nil || !strings.Contains(err.Error(), "SECRET_KEY") {
		t.Fatalf("a golden recorded with a passed variable the test does not declare was opened: %v", err)
	}
	frozen.PassEnv = []string{"SECRET_KEY"}
	if _, err := openGolden(frozen, "TestSample", false); err != nil {
		t.Fatalf("the golden with its declared passed variable was refused: %v", err)
	}
}
