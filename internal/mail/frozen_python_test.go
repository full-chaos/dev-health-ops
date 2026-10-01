package mail

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

func TestMain(m *testing.M) { os.Exit(venueoracle.RunTests(m)) }

// mailGoldens is the set of this package's frozen Python answers: each oracle
// program was executed once on Build, and what it sent to the test's own mail
// server or Resend stand-in is frozen under testdata/golden. The producers
// are the e-mail service of that build, the standard library's SMTP client
// and the Resend SDK with its HTTP client, so Identity names those
// distributions. A golden recorded by another producer is refused.
var mailGoldens = programoracle.Set{
	Package:       "./internal/mail/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nrequests 2.34.2\nresend 2.47.0",
	Distributions: []string{"requests", "resend"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"resend.golden.json": "1e28d58236b307e70999803824620631f975a2f73c9f0704bca807f9c1b9b2bb",
		"smtp.golden.json":   "108ad4265ff8023db1cadfb4af9ca3fb458950a2a63ec88f6a079cf04251c3f4",
	},
}

// frozenPython returns the answer of each program, in order, from the golden
// of the running test. No Python runs. A golden that is missing, edited,
// recorded for another program or input, or recorded by another producer
// fails the test; so does a program that exited non-zero when it was recorded.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	return mailGoldens.Outputs(t, root, golden, programs...)
}

// textDigest is how a golden holds a credential the Python plane sent: the
// SHA-256 of its text and the length of the text in bytes. No golden holds a
// token.
type textDigest struct {
	SHA256 string `json:"sha256"`
	Length int    `json:"length"`
}

func digestOf(text string) textDigest {
	sum := sha256.Sum256([]byte(text))
	return textDigest{SHA256: hex.EncodeToString(sum[:]), Length: len(text)}
}
