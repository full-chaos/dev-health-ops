// Command home_coverage_policy_generate records the bounded CHAOS-8509 Home
// coverage policy from the three fixture-only captures. It never edits the
// frozen Python producer, legacy goldens, or the CHAOS-8169 ledger.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const generatedPolicyPath = "internal/queryapi/server/home_coverage_oracle_policy_generated_integration_test.go"

const (
	sourceHead          = "9c438f9ca4e59eaecfb790448731a7e72177aaf3"
	sourceRun           = "37413067348"
	sourceJob           = "112105594511"
	sourceArtifact      = "11390701993"
	sourceReceiptSHA256 = "08a379f6ccf122291b1ff0997aaba0589cc750958663a6f6d36f0ba057c84dd1"
)

type capture struct {
	Oracle           string `json:"oracle"`
	Case             string `json:"case"`
	Python           string `json:"python"`
	Go               string `json:"go"`
	PythonSHA256     string `json:"python_sha256"`
	GoSHA256         string `json:"go_sha256"`
	DifferenceLeaves int    `json:"difference_leaves"`
}

type allowedLeaf struct {
	Path   string
	Python string
	Go     string
}

type input struct {
	File                   string
	FixtureSHA256          string
	Oracle                 string
	Case                   string
	DifferenceLeaves       int
	LegacyDifferenceLeaves int
	LegacyRoots            []string
	Allowed                []allowedLeaf
}

var inputs = []input{
	{
		File:                   "dict-order-home-no-data.json",
		FixtureSHA256:          "a2c77bf9662fd68f572da39c07e3b4e8a47fffe24605d729de73d17ee1551be7",
		Oracle:                 "dict-order",
		Case:                   "home no data",
		DifferenceLeaves:       197,
		LegacyDifferenceLeaves: 192,
		LegacyRoots:            []string{"/summary", "/constraint", "/health_state", "/signals", "/limiting_factor"},
		Allowed: []allowedLeaf{
			{Path: "/freshness/coverage/repos_covered_pct", Python: "0.0", Go: "null"},
			{Path: "/freshness/coverage/prs_linked_to_issues_pct", Python: "0.0", Go: "null"},
			{Path: "/freshness/coverage/issues_with_cycle_states_pct", Python: "0.0", Go: "null"},
			{Path: "/data_confidence/coverage_pct", Python: "0.0", Go: "null"},
			{Path: "/data_confidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
		},
	},
	{
		File:                   "graphql-edge-post-home.json",
		FixtureSHA256:          "7b901c0c452e3b667bc826e9b41b1a8c5fe65e78d91a4ebc1728f0b28de86bb9",
		Oracle:                 "graphql-edge",
		Case:                   "POST home",
		DifferenceLeaves:       204,
		LegacyDifferenceLeaves: 199,
		LegacyRoots:            []string{"/summary", "/constraint", "/healthState", "/signals", "/limitingFactor"},
		Allowed: []allowedLeaf{
			{Path: "/freshness/coverage/reposCoveredPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/prsLinkedToIssuesPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/issuesWithCycleStatesPct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/coveragePct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
		},
	},
	{
		File:                   "graphql-edge-get-home.json",
		FixtureSHA256:          "d892082ebe74f71bc05397e180ff2b21fb6615d394008c78b7aa655e8e882607",
		Oracle:                 "graphql-edge",
		Case:                   "GET home",
		DifferenceLeaves:       204,
		LegacyDifferenceLeaves: 199,
		LegacyRoots:            []string{"/summary", "/constraint", "/healthState", "/signals", "/limitingFactor"},
		Allowed: []allowedLeaf{
			{Path: "/freshness/coverage/reposCoveredPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/prsLinkedToIssuesPct", Python: "0", Go: "null"},
			{Path: "/freshness/coverage/issuesWithCycleStatesPct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/coveragePct", Python: "0", Go: "null"},
			{Path: "/dataConfidence/caveats/0", Python: `"Coverage appears partial; treat cockpit signals as directional."`, Go: `"Coverage could not be computed from available lineage fields."`},
		},
	},
}

func main() {
	root := flag.String("root", ".", "repository root")
	write := flag.Bool("write", false, "write the generated policy source")
	check := flag.Bool("check", false, "fail unless the generated policy source is current")
	flag.Parse()
	if *write == *check {
		fatal("give exactly one of -write or -check")
	}
	generated, err := render(*root)
	if err != nil {
		fatal("render policy: %v", err)
	}
	path := filepath.Join(*root, generatedPolicyPath)
	if *check {
		current, err := os.ReadFile(path)
		if err != nil {
			fatal("read %s: %v", generatedPolicyPath, err)
		}
		if !bytes.Equal(current, generated) {
			fatal("%s is stale; run go run ./internal/queryapi/server/testdata/chaos-8509/home_coverage_policy_generate.go -root . -write", generatedPolicyPath)
		}
		return
	}
	if err := os.WriteFile(path, generated, 0o644); err != nil {
		fatal("write %s: %v", generatedPolicyPath, err)
	}
}

func render(root string) ([]byte, error) {
	var rendered strings.Builder
	rendered.WriteString("// Code generated by go run ./internal/queryapi/server/testdata/chaos-8509/home_coverage_policy_generate.go -root . -write; DO NOT EDIT.\n")
	rendered.WriteString("//go:build integration\n\npackage server\n\n")
	rendered.WriteString("// The fixture-only capture source is hosted venue run 37413067348, job 112105594511, artifact 11390701993 at 9c438f9ca4e59eaecfb790448731a7e72177aaf3. The capture path records equal status and non-content-length headers before it writes a body.\n")
	rendered.WriteString("// It is synthetic harness data only: no local-org data, credentials, or personal data is represented here.\n")
	rendered.WriteString("var chaos8509HomeCapturePolicyOrder = []chaos8169HomeNoDataLedgerKey{\n")
	for _, in := range inputs {
		fmt.Fprintf(&rendered, "\t{Oracle: %q, Case: %q},\n", in.Oracle, in.Case)
	}
	rendered.WriteString("}\n\nvar chaos8509HomeCapturePolicies = map[chaos8169HomeNoDataLedgerKey]chaos8509HomeCapturePolicy{\n")
	for _, in := range inputs {
		capture, err := readCapture(root, in)
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&rendered, "\t{Oracle: %q, Case: %q}: {\n", in.Oracle, in.Case)
		fmt.Fprintf(&rendered, "\t\tFile: %q,\n", in.File)
		fmt.Fprintf(&rendered, "\t\tFixtureSHA256: %q,\n", in.FixtureSHA256)
		fmt.Fprintf(&rendered, "\t\tPythonSHA256: %q,\n", capture.PythonSHA256)
		fmt.Fprintf(&rendered, "\t\tGoSHA256: %q,\n", capture.GoSHA256)
		fmt.Fprintf(&rendered, "\t\tSourceHead: %q,\n", sourceHead)
		fmt.Fprintf(&rendered, "\t\tSourceRun: %q,\n", sourceRun)
		fmt.Fprintf(&rendered, "\t\tSourceJob: %q,\n", sourceJob)
		fmt.Fprintf(&rendered, "\t\tSourceArtifact: %q,\n", sourceArtifact)
		fmt.Fprintf(&rendered, "\t\tSourceReceiptSHA256: %q,\n", sourceReceiptSHA256)
		rendered.WriteString("\t\tExpectedStatus: 200,\n\t\tHeadersEqual: true,\n")
		fmt.Fprintf(&rendered, "\t\tDifferenceLeaves: %d,\n", in.DifferenceLeaves)
		fmt.Fprintf(&rendered, "\t\tLegacyDifferenceLeaves: %d,\n", in.LegacyDifferenceLeaves)
		rendered.WriteString("\t\tEntries: []chaos8509HomeCaptureEntry{\n")
		for _, entry := range in.Allowed {
			fmt.Fprintf(&rendered, "\t\t\t{Path: %q, Python: %q, Go: %q},\n", entry.Path, entry.Python, entry.Go)
		}
		rendered.WriteString("\t\t},\n\t},\n")
	}
	rendered.WriteString("}\n")
	formatted, err := format.Source([]byte(rendered.String()))
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

func readCapture(root string, in input) (capture, error) {
	path := filepath.Join(root, "internal/queryapi/server/testdata/chaos-8509", in.File)
	raw, err := os.ReadFile(path)
	if err != nil {
		return capture{}, err
	}
	if got := digest(raw); got != in.FixtureSHA256 {
		return capture{}, fmt.Errorf("%s SHA-256 = %s, want %s", in.File, got, in.FixtureSHA256)
	}
	var got capture
	if err := json.Unmarshal(raw, &got); err != nil {
		return capture{}, fmt.Errorf("decode %s: %w", in.File, err)
	}
	if got.Oracle != in.Oracle || got.Case != in.Case {
		return capture{}, fmt.Errorf("%s identity = %q/%q, want %q/%q", in.File, got.Oracle, got.Case, in.Oracle, in.Case)
	}
	if !json.Valid([]byte(got.Python)) || !json.Valid([]byte(got.Go)) {
		return capture{}, fmt.Errorf("%s has invalid captured JSON", in.File)
	}
	if got.PythonSHA256 != digest([]byte(got.Python)) || got.GoSHA256 != digest([]byte(got.Go)) {
		return capture{}, fmt.Errorf("%s inner body digest does not match its recorded capture", in.File)
	}
	python, err := decode(got.Python)
	if err != nil {
		return capture{}, fmt.Errorf("decode %s Python body: %w", in.File, err)
	}
	goBody, err := decode(got.Go)
	if err != nil {
		return capture{}, fmt.Errorf("decode %s Go body: %w", in.File, err)
	}
	differences := differenceLeaves(python, true, goBody, true, "")
	if got.DifferenceLeaves != in.DifferenceLeaves || len(differences) != in.DifferenceLeaves {
		return capture{}, fmt.Errorf("%s difference leaves = recorded %d actual %d, want %d", in.File, got.DifferenceLeaves, len(differences), in.DifferenceLeaves)
	}
	if len(in.Allowed) != in.DifferenceLeaves-in.LegacyDifferenceLeaves {
		return capture{}, fmt.Errorf("%s allowed leaves = %d, want difference %d - legacy %d", in.File, len(in.Allowed), in.DifferenceLeaves, in.LegacyDifferenceLeaves)
	}
	allowed := make(map[string]allowedLeaf, len(in.Allowed))
	for _, entry := range in.Allowed {
		if _, duplicate := allowed[entry.Path]; duplicate {
			return capture{}, fmt.Errorf("%s has duplicate allowed path %s", in.File, entry.Path)
		}
		allowed[entry.Path] = entry
	}
	seen := make(map[string]int, len(in.Allowed))
	legacy := make(map[string]int, len(in.LegacyRoots))
	for _, difference := range differences {
		if entry, ok := allowed[difference.Path]; ok {
			if difference.Left != entry.Python || difference.Right != entry.Go {
				return capture{}, fmt.Errorf("%s allowed path %s = Python %s Go %s, want Python %s Go %s", in.File, difference.Path, difference.Left, difference.Right, entry.Python, entry.Go)
			}
			seen[difference.Path]++
			continue
		}
		root := topLevelRoot(difference.Path)
		if !contains(in.LegacyRoots, root) {
			return capture{}, fmt.Errorf("%s has unapproved difference at %s", in.File, difference.Path)
		}
		legacy[root]++
	}
	for _, entry := range in.Allowed {
		if seen[entry.Path] != 1 {
			return capture{}, fmt.Errorf("%s allowed path %s count = %d, want 1", in.File, entry.Path, seen[entry.Path])
		}
	}
	legacyCount := 0
	for _, count := range legacy {
		legacyCount += count
	}
	if legacyCount != in.LegacyDifferenceLeaves {
		return capture{}, fmt.Errorf("%s legacy difference leaves = %d, want %d", in.File, legacyCount, in.LegacyDifferenceLeaves)
	}
	return got, nil
}

type difference struct {
	Path  string
	Left  string
	Right string
}

func decode(body string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, errors.New("multiple JSON values")
	}
	return value, nil
}

func differenceLeaves(left any, leftOK bool, right any, rightOK bool, path string) []difference {
	switch {
	case !leftOK && !rightOK:
		return nil
	case !leftOK:
		return leaves(right, path, false)
	case !rightOK:
		return leaves(left, path, true)
	}
	leftObject, leftIsObject := left.(map[string]any)
	rightObject, rightIsObject := right.(map[string]any)
	if leftIsObject || rightIsObject {
		if !leftIsObject || !rightIsObject {
			return []difference{{Path: path, Left: jsonText(left), Right: jsonText(right)}}
		}
		keys := make(map[string]bool, len(leftObject)+len(rightObject))
		for key := range leftObject {
			keys[key] = true
		}
		for key := range rightObject {
			keys[key] = true
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		var out []difference
		for _, key := range ordered {
			leftValue, leftExists := leftObject[key]
			rightValue, rightExists := rightObject[key]
			out = append(out, differenceLeaves(leftValue, leftExists, rightValue, rightExists, path+"/"+key)...)
		}
		return out
	}
	leftList, leftIsList := left.([]any)
	rightList, rightIsList := right.([]any)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return []difference{{Path: path, Left: jsonText(left), Right: jsonText(right)}}
		}
		length := len(leftList)
		if len(rightList) > length {
			length = len(rightList)
		}
		var out []difference
		for index := 0; index < length; index++ {
			leftExists, rightExists := index < len(leftList), index < len(rightList)
			var leftValue, rightValue any
			if leftExists {
				leftValue = leftList[index]
			}
			if rightExists {
				rightValue = rightList[index]
			}
			out = append(out, differenceLeaves(leftValue, leftExists, rightValue, rightExists, path+"/"+strconv.Itoa(index))...)
		}
		return out
	}
	if jsonText(left) == jsonText(right) {
		return nil
	}
	return []difference{{Path: path, Left: jsonText(left), Right: jsonText(right)}}
}

func leaves(value any, path string, left bool) []difference {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return []difference{{Path: path, Left: missing(left), Right: missing(!left)}}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var out []difference
		for _, key := range keys {
			out = append(out, leaves(typed[key], path+"/"+key, left)...)
		}
		return out
	case []any:
		if len(typed) == 0 {
			return []difference{{Path: path, Left: missing(left), Right: missing(!left)}}
		}
		var out []difference
		for index, item := range typed {
			out = append(out, leaves(item, path+"/"+strconv.Itoa(index), left)...)
		}
		return out
	default:
		return []difference{{Path: path, Left: choose(left, jsonText(value), "<missing>"), Right: choose(left, "<missing>", jsonText(value))}}
	}
}

func missing(left bool) string { return choose(left, jsonText(nil), "<missing>") }
func choose(condition bool, yes, no string) string {
	if condition {
		return yes
	}
	return no
}
func jsonText(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}
func topLevelRoot(path string) string {
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return "/"
	}
	return "/" + parts[0]
}
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
func digest(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
