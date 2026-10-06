package server

import (
	"path/filepath"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// modelGoldens is the frozen Python answer for the query-body contract. Its
// producer remains the original build while the settled CHAOS-8509 response
// model revision below is scoped only to the three nullable coverage fields.
// A golden recorded by another producer is refused.
var modelGoldens = programoracle.Set{
	Package:       "./internal/queryapi/server/",
	Build:         "a4847c5e93607451a0c987b314d37e02fc43ce85",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nfastapi 0.136.3\nhttpx 0.28.1\npydantic 2.13.5\npydantic-core 2.46.5\nstarlette 1.7.0",
	Distributions: []string{"fastapi", "httpx", "pydantic", "pydantic-core", "starlette"},
	// The goldenrecord verb writes each digest when it promotes a recording; a
	// new golden starts as "PIN:" + its file name without ".json".
	Pins: map[string]string{
		"query-bodies.golden.json": "2b0614047c45642536c6ab0d64984b7f8a7dd5c4919a0294e551b545b1800879",
	},
}

// homeResponseModelGoldens is the versioned FastAPI producer for the three
// nullable Home freshness coverage fields. It intentionally leaves all other
// response-model and body producers frozen at their existing builds.
var homeResponseModelGoldens = programoracle.Set{
	Package:       "./internal/queryapi/server/",
	Build:         "a59ff1763e0371687653bec167c011e921073c72",
	Identity:      "python 3.14.7\nunicodedata 16.0.0\nfastapi 0.136.3\nhttpx 0.28.1\npydantic 2.13.5\npydantic-core 2.46.5\nstarlette 1.7.0",
	Distributions: []string{"fastapi", "httpx", "pydantic", "pydantic-core", "starlette"},
	Pins: map[string]string{
		"query-response-models-render.golden.json": "8f71132cca8d0a600b6c3cbf815931371ae55bee7bc4d92793ce6bca4cd0ff47",
		"query-response-models-table.golden.json":  "23a02832fd6e83357147cd45670e0f3fe164844147cda3284fa94103276a7c4e",
	},
}

// frozenPython returns the stdout of each program, in order, from the golden
// of the running test. No Python runs.
func frozenPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	return frozenPythonFrom(t, modelGoldens, golden, programs...)
}

// frozenHomeResponseModelPython returns the versioned producer's answers for
// the Home response-model contract only.
func frozenHomeResponseModelPython(t *testing.T, golden string, programs ...programoracle.Program) []string {
	return frozenPythonFrom(t, homeResponseModelGoldens, golden, programs...)
}

func frozenPythonFrom(t *testing.T, goldens programoracle.Set, golden string, programs ...programoracle.Program) []string {
	t.Helper()
	_, file, _, ok := moduleroot.Caller(0)
	if !ok {
		t.Fatal("cannot locate the test source")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	return goldens.Outputs(t, root, golden, programs...)
}
