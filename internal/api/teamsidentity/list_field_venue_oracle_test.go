//go:build integration

package teamsidentity

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestListFieldVenueOracle runs the Go pythonListField and the real Python
// _list_field over the same JSON values and requires identical lists. The
// values cover every branch (null, string, list, non-list scalars and
// dicts) and every element kind whose str() is not the obvious one
// (bools, ints past int64, floats incl. exponent forms, nested list/dict
// with quoting).
func TestListFieldVenueOracle(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	python := pyoracle.Resolve(t, filepath.Dir(filepath.Dir(filepath.Dir(packageDir))))

	inputs := []string{
		`null`, `"single"`, `""`, `[]`, `5`, `1.5`, `true`, `{"a":1}`,
		`[7,"ENG"]`, `["a",null,"b"]`, `[true,false]`, `[1.5,2.0,-0.0,1e16,1e-5,1e22,123456789.125]`,
		`[123456789012345678901234567890,-5,0]`, `[[1,"a'b"],{"k":null,"x":"y"}]`,
		`[{"nested":{"deep":[1,2.5,null,true,"q\"r"]}}]`, `["ünï","日本","a\nb"]`, `[[],{}]`,
	}
	payload := "[" + strings.Join(inputs, ",") + "]"
	output, err := exec.Command(python, filepath.Join(packageDir, "testdata", "venue_oracle_list_field.py"), payload).Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			t.Fatalf("python oracle: %v: %s", err, exitErr.Stderr)
		}
		t.Fatal(err)
	}
	var want [][]string
	if err := json.Unmarshal(output, &want); err != nil {
		t.Fatalf("decode %s: %v", output, err)
	}
	if len(want) != len(inputs) {
		t.Fatalf("python answered %d for %d inputs", len(want), len(inputs))
	}
	for i, input := range inputs {
		decoded, err := pyjson.DecodeString(input)
		if err != nil {
			t.Fatalf("decode %s: %v", input, err)
		}
		got := pythonListField(decoded)
		if strings.Join(got, "\x00") != strings.Join(want[i], "\x00") || len(got) != len(want[i]) {
			t.Errorf("%s: go=%q python=%q", input, got, want[i])
		}
	}
	venueoracle.WriteProof(t)
}
