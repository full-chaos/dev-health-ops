package daily

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// pooledSamplesMaxPerRow is unexported, so only this package can assign it; this census proves no non-test file of the
// package does, past its declaration. Only a test lowers the cap.
var capAssignment = regexp.MustCompile(`\bpooledSamplesMaxPerRow\s*(=[^=]|\+=|-=|\*=|/=|\+\+|--)`)

const capDeclaration = "var pooledSamplesMaxPerRow = pooledSamplesCap"

func capAssignments(file, body string) []string {
	var found []string
	for _, line := range strings.Split(body, "\n") {
		if capAssignment.MatchString(line) && strings.TrimSpace(line) != capDeclaration {
			found = append(found, file+":"+strings.TrimSpace(line))
		}
	}
	return found
}

func TestNoProductionCodeChangesThePooledSamplesCap(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	declared := 0
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		declared += strings.Count(string(body), capDeclaration)
		found = append(found, capAssignments(name, string(body))...)
	}
	if declared != 1 {
		t.Fatalf("the cap declaration %q is found %d times, want once", capDeclaration, declared)
	}
	if len(found) != 0 {
		t.Fatalf("non-test code assigns the pooled samples cap:\n%s", strings.Join(found, "\n"))
	}
	if pooledSamplesMaxPerRow != pooledSamplesCap || pooledSamplesCap != 100_000 {
		t.Fatalf("the cap is %d (constant %d), want 100000", pooledSamplesMaxPerRow, pooledSamplesCap)
	}
}

// A planted assignment fails the census; a comparison and the declaration do not.
func TestCapCensusFlagsAPlantedAssignment(t *testing.T) {
	for line, want := range map[string]bool{
		"\tpooledSamplesMaxPerRow = 5":           true,
		"\tpooledSamplesMaxPerRow += 1":          true,
		"\tpooledSamplesMaxPerRow++":             true,
		"\tif samples <= pooledSamplesMaxPerRow": false,
		"\tif pooledSamplesMaxPerRow == 0 {":     false,
		capDeclaration:                           false,
	} {
		if got := len(capAssignments("planted.go", line)) == 1; got != want {
			t.Errorf("%q: flagged %v, want %v", line, got, want)
		}
	}
}
