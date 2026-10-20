package routing

import (
	"errors"
	"fmt"
	"io"
)

// Stub: the subcommands and the helpers the tests reach exist with their signatures and do nothing yet.

const (
	HostRule          = "Host(`traefik`)"
	BigboyLocalSource = "the bigboy-local Python paths (BIGBOY_LOCAL_PYTHON_PATHS in this script)"
)

var errNotPorted = errors.New("not ported yet")

type Segments [][]string

type pyMap struct {
	keys []any
	vals []any
}

type pyRaise struct{ msg string }

type entry struct{ path, pathType any }

type namedEntries struct {
	name    string
	entries []entry
}

type claimedPath struct {
	plane    string
	path     any
	segments Segments
}

func loadYAML([]byte, bool) (any, error) { return nil, errNotPorted }

func pyStr(any) string { return "" }

func pyRepr(any) string { return "" }

func item(any, any) any { return nil }

func pathsFromDoc(any) ([]string, []string) { return nil, nil }

func pythonAllowListFromDoc(any) []entry { return nil }

func CombinedRegex([]string) string { return "" }

func EmitLabels(string, string) string { return "" }

func EmitDynamicConfig(string, string, []entry) string { return "" }

func traefikPathRule([]entry) string { return "" }

func pathsOnTwoPlanes(any) []string { return nil }

func queryAPIPathsThatDifferByHost(any) []string { return nil }

func pathsInTwoObjects(any) []string { return nil }

func pythonSources(any) []namedEntries { return nil }

func goPlanePaths(any) []claimedPath { return nil }

func segmentsRegex(Segments) string { return "" }

func pythonEntryOverlaps(any, any, Segments) bool { return false }

func queryapiEntrySegments(any, any) Segments { return nil }

func pyEscape(string) string { return "" }

func notPorted(stderr io.Writer) int {
	fmt.Fprintln(stderr, errNotPorted)
	return 2
}

func RunPlaneSplitRouter(_ []string, _ Deps, _, stderr io.Writer) int { return notPorted(stderr) }

func RunQueryAPIFlags(_ []string, _ Deps, _, stderr io.Writer) int { return notPorted(stderr) }

func RunRouteCoverage(_ []string, _ Deps, _, stderr io.Writer) int { return notPorted(stderr) }

func RunWebEnvParity(_ []string, _ Deps, _, stderr io.Writer) int { return notPorted(stderr) }
