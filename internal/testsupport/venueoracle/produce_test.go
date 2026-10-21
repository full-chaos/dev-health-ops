package venueoracle

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

func programGolden(t *testing.T, requests []Request, bodies ...string) (string, string) {
	t.Helper()
	file := goldenFile{Header: goldenHeader{Test: t.Name(), PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64)}}
	for index, request := range requests {
		entry := requestKey(request)
		entry.Path = "program sha256 " + strings.Repeat("b", 64) + " env sha256 " + strings.Repeat("c", 64)
		entry.Status, entry.Headers, entry.Body = 0, map[string]string{"stderr": ""}, bodies[index]
		file.Requests = append(file.Requests, entry)
	}
	return writeGoldenFile(t, t.TempDir(), file)
}

func TestFrozenProduceAnswersFromTheFile(t *testing.T) {
	requests := []Request{ProgramRequest("corpus", []byte("abc"))}
	path, digest := programGolden(t, requests, "ABC\n")
	golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest})
	answers := golden.Produce(t, requests)
	if len(answers) != 1 || answers[0].Body != "ABC\n" || answers[0].Status != 0 {
		t.Fatalf("answers = %+v", answers)
	}
	golden.Consumed(t, answers...)
	golden.SkipDiff(t)
	golden.Finish(t)
}

// A program request is found by its name and its stdin: a frozen answer is
// never handed to another program name or another input. The recorded path
// (the digests of the program text and of its environment) is not compared.
func TestAProgramRequestIsKeyedByNameAndInput(t *testing.T) {
	base := ProgramRequest("corpus", []byte("abc"))
	path, digest := programGolden(t, []Request{base}, "ABC\n")
	for name, other := range map[string]Request{
		"name":  ProgramRequest("other corpus", []byte("abc")),
		"stdin": ProgramRequest("corpus", []byte("abd")),
	} {
		golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, t.Name())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := golden.frozenAnswers([]Request{other}); err == nil || !strings.Contains(err.Error(), "recorded for") {
			t.Errorf("a changed %s was answered from the file: %v", name, err)
		}
	}
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest}, t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if answers, err := golden.frozenAnswers([]Request{ProgramRequest("corpus", []byte("abc"))}); err != nil || answers[0].Body != "ABC\n" {
		t.Fatalf("the same name and input was not answered: %+v, %v", answers, err)
	}
}

// A producer request carries no headers.
func TestProduceRefusesARequestThatCarriesHeaders(t *testing.T) {
	plain := ProgramRequest("corpus", []byte("abc"))
	withHeaders := plain
	withHeaders.Headers = map[string]string{"PYTHONHASHSEED": "0"}
	if os.Getenv("VENUEORACLE_PRODUCE_HEADERS_CHILD") == "1" {
		// The child: Produce must fail the test before any answer is read.
		path, digest := programGolden(t, []Request{plain}, "ABC\n")
		golden := OpenGolden(t, GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest})
		golden.Produce(t, []Request{withHeaders})
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
