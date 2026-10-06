// Command home_no_data_ledger_generate records the approved CHAOS-8169 Home
// ledger from exact captured bodies and writes its Go test source. It never
// edits a Python producer, a frozen golden, or a response body.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

const (
	dictGoldenPath  = "internal/queryapi/server/testdata/venue/dict-order.json"
	dictGoPath      = "internal/queryapi/server/testdata/chaos-8169/home-no-data-go-b6.json"
	edgeCapturePath = "internal/queryapi/server/testdata/chaos-8169/home-no-data-edge-073cc.json"
	ledgerPath      = "internal/queryapi/server/home_no_data_oracle_ledger_generated_integration_test.go"

	edgeWorkflowRun = "37343463320"
	edgeJob         = "111876064807"
	edgeHead        = "073cc339d4cc8a63d0a458c13a84e08a033f9e0c"
	edgePythonBuild = "a4847c5e93607451a0c987b314d37e02fc43ce85"
)

type capture struct {
	Source    captureSource     `json:"source"`
	Responses []captureResponse `json:"responses"`
}

type captureSource struct {
	WorkflowRun string `json:"workflow_run"`
	Job         string `json:"job"`
	Head        string `json:"head"`
	PythonBuild string `json:"python_build"`
}

type captureResponse struct {
	Case   string `json:"case"`
	Python string `json:"python"`
	Go     string `json:"go"`
}

type dictGolden struct {
	Requests []struct {
		Name string `json:"name"`
		Body string `json:"body"`
	} `json:"requests"`
}

type dictAnswer struct {
	Status int    `json:"status"`
	Body   string `json:"body"`
}

type ledgerInput struct {
	Oracle     string
	Case       string
	PythonBody string
	GoBody     string
	Allowed    []string
	Strict     []string
}

type ledgerEntry struct {
	Path   string
	Python string
	Go     string
	Leaves int
}

type strictRoot struct {
	Path   string
	Kind   string
	Length int
}

type ledger struct {
	Entries []ledgerEntry
	Strict  []strictRoot
}

func main() {
	root := flag.String("root", ".", "repository root")
	jobLog := flag.String("job-log", "", "exact hosted job log used to capture the edge Home bodies")
	writeCapture := flag.Bool("write-capture", false, "extract the two Home pairs from -job-log")
	write := flag.Bool("write", false, "write the generated ledger source")
	check := flag.Bool("check", false, "fail unless the generated ledger source is current")
	flag.Parse()

	if *writeCapture {
		if *jobLog == "" {
			fatal("-write-capture requires -job-log")
		}
		if err := writeEdgeCapture(*root, *jobLog); err != nil {
			fatal("write capture: %v", err)
		}
	}
	if !*write && !*check {
		fatal("give -write or -check")
	}
	if *write && *check {
		fatal("give only one of -write and -check")
	}
	generated, err := render(*root)
	if err != nil {
		fatal("render ledger: %v", err)
	}
	path := filepath.Join(*root, ledgerPath)
	if *check {
		current, err := os.ReadFile(path)
		if err != nil {
			fatal("read %s: %v", ledgerPath, err)
		}
		if !bytes.Equal(current, generated) {
			fatal("%s is stale; run go run ./internal/queryapi/server/testdata/chaos-8169/home_no_data_ledger_generate.go -root . -write", ledgerPath)
		}
		return
	}
	if err := os.WriteFile(path, generated, 0o644); err != nil {
		fatal("write %s: %v", ledgerPath, err)
	}
}

func writeEdgeCapture(root, jobLog string) error {
	text, err := os.ReadFile(jobLog)
	if err != nil {
		return err
	}
	responses := make([]captureResponse, 0, 2)
	for _, name := range []string{"POST home", "GET home"} {
		python, goBody, err := extractPair(string(text), name)
		if err != nil {
			return err
		}
		if !json.Valid([]byte(python)) || !json.Valid([]byte(goBody)) {
			return fmt.Errorf("%s has an invalid captured JSON body", name)
		}
		responses = append(responses, captureResponse{Case: name, Python: python, Go: goBody})
	}
	capture := capture{Source: captureSource{WorkflowRun: edgeWorkflowRun, Job: edgeJob, Head: edgeHead, PythonBuild: edgePythonBuild}, Responses: responses}
	encoded, err := json.MarshalIndent(capture, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, edgeCapturePath), append(encoded, '\n'), 0o644)
}

func extractPair(log, name string) (string, string, error) {
	marker := "graphql_edge_frozen_venue_oracle_integration_test.go:89: " + name + ":"
	start := -1
	lines := strings.Split(log, "\n")
	for index, line := range lines {
		if strings.Contains(jobPayload(line), marker) {
			start = index
			break
		}
	}
	if start < 0 {
		return "", "", fmt.Errorf("%s marker not found", name)
	}
	var python, goBody strings.Builder
	state := ""
	for _, line := range lines[start+1:] {
		text := jobPayload(line)
		if strings.Contains(text, "graphql_edge_frozen_venue_oracle_integration_test.go:89:") {
			break
		}
		hasHeaders := strings.Contains(text, " map[")
		switch {
		case strings.HasPrefix(text, "         python 200 "):
			state = "python"
			appendBody(&python, strings.TrimPrefix(text, "         python 200 "))
		case strings.HasPrefix(text, "         go     200 "):
			state = "go"
			appendBody(&goBody, strings.TrimPrefix(text, "         go     200 "))
		case state == "python":
			appendBody(&python, text)
		case state == "go":
			appendBody(&goBody, text)
		}
		if hasHeaders {
			state = ""
		}
		if python.Len() > 0 && goBody.Len() > 0 && state == "" {
			break
		}
	}
	if python.Len() == 0 || goBody.Len() == 0 {
		return "", "", fmt.Errorf("%s bodies not found", name)
	}
	return trimMapSuffix(python.String()), trimMapSuffix(goBody.String()), nil
}

func jobPayload(line string) string {
	if before, after, ok := strings.Cut(line, "Z "); ok {
		_ = before
		return after
	}
	return line
}

func appendBody(builder *strings.Builder, text string) {
	if before, _, ok := strings.Cut(text, " map["); ok {
		builder.WriteString(before)
		return
	}
	builder.WriteString(text)
}

func trimMapSuffix(body string) string {
	if before, _, ok := strings.Cut(body, " map["); ok {
		return before
	}
	return body
}

func render(root string) ([]byte, error) {
	dictPython, dictGo, err := dictBodies(root)
	if err != nil {
		return nil, err
	}
	edge, captureDigest, err := edgeBodies(root)
	if err != nil {
		return nil, err
	}
	dict, err := buildLedger(ledgerInput{
		Oracle: "dict-order", Case: "home no data", PythonBody: dictPython, GoBody: dictGo,
		Allowed: []string{"summary", "constraint", "health_state", "signals", "limiting_factor"}, Strict: []string{"deltas", "tiles"},
	})
	if err != nil {
		return nil, err
	}
	post, err := buildLedger(ledgerInput{
		Oracle: "graphql-edge", Case: "POST home", PythonBody: edge["POST home"].Python, GoBody: edge["POST home"].Go,
		Allowed: []string{"summary", "constraint", "healthState", "signals", "limitingFactor"}, Strict: []string{"deltas", "tiles"},
	})
	if err != nil {
		return nil, err
	}
	get, err := buildLedger(ledgerInput{
		Oracle: "graphql-edge", Case: "GET home", PythonBody: edge["GET home"].Python, GoBody: edge["GET home"].Go,
		Allowed: []string{"summary", "constraint", "healthState", "signals", "limitingFactor"}, Strict: []string{"deltas", "tiles"},
	})
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(post, get) {
		return nil, errors.New("POST and GET Home ledgers differ; D4840 requires both exact captured cases")
	}

	var out strings.Builder
	fmt.Fprintln(&out, "// Code generated by go run ./internal/queryapi/server/testdata/chaos-8169/home_no_data_ledger_generate.go -root . -write; DO NOT EDIT.")
	fmt.Fprintln(&out, "//go:build integration")
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "package server")
	fmt.Fprintln(&out)
	fmt.Fprintf(&out, "const chaos8169GraphQLEdgeCaptureSHA256 = %q\n", captureDigest)
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "var chaos8169GraphQLEdgeHomeLedger = "+formatLedger(post))
	fmt.Fprintln(&out)
	fmt.Fprintln(&out, "var chaos8169HomeNoDataLedgers = map[chaos8169HomeNoDataLedgerKey]chaos8169HomeNoDataLedger{")
	fmt.Fprintln(&out, "\tchaos8169DictOrderHomeLedgerKey: "+formatLedger(dict)+",")
	fmt.Fprintln(&out, "\tchaos8169GraphQLEdgeHomeLedgerKeys[\"POST home\"]: chaos8169GraphQLEdgeHomeLedger,")
	fmt.Fprintln(&out, "\tchaos8169GraphQLEdgeHomeLedgerKeys[\"GET home\"]: chaos8169GraphQLEdgeHomeLedger,")
	fmt.Fprintln(&out, "}")
	formatted, err := format.Source([]byte(out.String()))
	if err != nil {
		return nil, fmt.Errorf("format generated Go: %w", err)
	}
	return formatted, nil
}

func dictBodies(root string) (string, string, error) {
	raw, err := os.ReadFile(filepath.Join(root, dictGoldenPath))
	if err != nil {
		return "", "", err
	}
	var golden dictGolden
	if err := json.Unmarshal(raw, &golden); err != nil {
		return "", "", err
	}
	if len(golden.Requests) != 1 || golden.Requests[0].Name != "dict order services" {
		return "", "", errors.New("dict-order golden no longer has its one service request")
	}
	result, ok := strings.CutPrefix(golden.Requests[0].Body, "RESULT ")
	if !ok {
		return "", "", errors.New("dict-order golden has no RESULT payload")
	}
	var answers []dictAnswer
	if err := json.Unmarshal([]byte(result), &answers); err != nil {
		return "", "", err
	}
	if len(answers) != 8 || answers[1].Status != 200 {
		return "", "", fmt.Errorf("dict-order Home answer = result[1] status %d of %d, want status 200 of 8", answers[1].Status, len(answers))
	}
	goRaw, err := os.ReadFile(filepath.Join(root, dictGoPath))
	if err != nil {
		return "", "", err
	}
	return answers[1].Body, string(goRaw), nil
}

func edgeBodies(root string) (map[string]captureResponse, string, error) {
	raw, err := os.ReadFile(filepath.Join(root, edgeCapturePath))
	if err != nil {
		return nil, "", err
	}
	var captured capture
	if err := json.Unmarshal(raw, &captured); err != nil {
		return nil, "", err
	}
	if captured.Source != (captureSource{WorkflowRun: edgeWorkflowRun, Job: edgeJob, Head: edgeHead, PythonBuild: edgePythonBuild}) {
		return nil, "", fmt.Errorf("edge capture source = %+v, want workflow %s job %s head %s python %s", captured.Source, edgeWorkflowRun, edgeJob, edgeHead, edgePythonBuild)
	}
	pairs := make(map[string]captureResponse, len(captured.Responses))
	for _, response := range captured.Responses {
		if response.Case != "POST home" && response.Case != "GET home" {
			return nil, "", fmt.Errorf("edge capture case %q is not an approved Home case", response.Case)
		}
		if _, exists := pairs[response.Case]; exists {
			return nil, "", fmt.Errorf("edge capture repeats %s", response.Case)
		}
		pythonHome, err := graphQLHome(response.Python)
		if err != nil {
			return nil, "", fmt.Errorf("%s Python body: %w", response.Case, err)
		}
		goHome, err := graphQLHome(response.Go)
		if err != nil {
			return nil, "", fmt.Errorf("%s Go body: %w", response.Case, err)
		}
		response.Python, response.Go = pythonHome, goHome
		pairs[response.Case] = response
	}
	if len(pairs) != 2 || pairs["POST home"].Case == "" || pairs["GET home"].Case == "" {
		return nil, "", errors.New("edge capture must have exactly POST home and GET home")
	}
	return pairs, fmt.Sprintf("%x", sha256.Sum256(raw)), nil
}

func graphQLHome(body string) (string, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &envelope); err != nil {
		return "", err
	}
	data, ok := envelope["data"]
	if !ok {
		return "", errors.New("data is absent")
	}
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return "", err
	}
	home, ok := values["home"]
	if !ok {
		return "", errors.New("data.home is absent")
	}
	return string(home), nil
}

func buildLedger(input ledgerInput) (ledger, error) {
	python, err := rawObject(input.PythonBody)
	if err != nil {
		return ledger{}, fmt.Errorf("%s/%s Python: %w", input.Oracle, input.Case, err)
	}
	goBody, err := rawObject(input.GoBody)
	if err != nil {
		return ledger{}, fmt.Errorf("%s/%s Go: %w", input.Oracle, input.Case, err)
	}
	allowed := make(map[string]bool, len(input.Allowed))
	entries := make([]ledgerEntry, 0, len(input.Allowed))
	for _, root := range input.Allowed {
		allowed[root] = true
		left, leftOK := python[root]
		right, rightOK := goBody[root]
		if !leftOK || !rightOK {
			return ledger{}, fmt.Errorf("%s/%s approved root %s is absent", input.Oracle, input.Case, root)
		}
		leaves, err := differenceLeaves(left, right, "/"+root)
		if err != nil {
			return ledger{}, err
		}
		if len(leaves) == 0 {
			return ledger{}, fmt.Errorf("%s/%s approved root %s does not differ", input.Oracle, input.Case, root)
		}
		pythonText, err := canonicalJSON(left)
		if err != nil {
			return ledger{}, fmt.Errorf("%s/%s Python %s: %w", input.Oracle, input.Case, root, err)
		}
		goText, err := canonicalJSON(right)
		if err != nil {
			return ledger{}, fmt.Errorf("%s/%s Go %s: %w", input.Oracle, input.Case, root, err)
		}
		entries = append(entries, ledgerEntry{Path: "/" + root, Python: pythonText, Go: goText, Leaves: len(leaves)})
	}
	for root, left := range python {
		right, rightOK := goBody[root]
		if !rightOK {
			return ledger{}, fmt.Errorf("%s/%s root %s disappears", input.Oracle, input.Case, root)
		}
		leaves, err := differenceLeaves(left, right, "/"+root)
		if err != nil {
			return ledger{}, err
		}
		if len(leaves) > 0 && !allowed[root] {
			return ledger{}, fmt.Errorf("%s/%s has unapproved differences under /%s", input.Oracle, input.Case, root)
		}
	}
	for root := range goBody {
		if _, ok := python[root]; !ok {
			return ledger{}, fmt.Errorf("%s/%s root %s appears only in Go", input.Oracle, input.Case, root)
		}
	}
	strict := make([]strictRoot, 0, len(input.Strict))
	for _, root := range input.Strict {
		left, leftOK := python[root]
		right, rightOK := goBody[root]
		if !leftOK || !rightOK || !bytes.Equal(left, right) {
			return ledger{}, fmt.Errorf("%s/%s strict root %s differs", input.Oracle, input.Case, root)
		}
		kind, length, err := shape(left)
		if err != nil {
			return ledger{}, fmt.Errorf("%s/%s strict root %s: %w", input.Oracle, input.Case, root, err)
		}
		strict = append(strict, strictRoot{Path: "/" + root, Kind: kind, Length: length})
	}
	return ledger{Entries: entries, Strict: strict}, nil
}

// canonicalJSON follows the integration comparator's pyjson serialization.
// The capture itself retains exact hosted bytes; ledger values use the same
// decoded representation that the guard compares at runtime.
func canonicalJSON(raw json.RawMessage) (string, error) {
	value, err := pyjson.Decode(raw)
	if err != nil {
		return "", err
	}
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func rawObject(body string) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("is not a JSON object")
	}
	return object, nil
}

func shape(raw json.RawMessage) (string, int, error) {
	var array []json.RawMessage
	if err := json.Unmarshal(raw, &array); err == nil && array != nil {
		return "array", len(array), nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err == nil && object != nil {
		return "object", len(object), nil
	}
	return "", 0, errors.New("is not a JSON array or object")
}

func differenceLeaves(left, right json.RawMessage, path string) ([]string, error) {
	var leftValue any
	if err := json.Unmarshal(left, &leftValue); err != nil {
		return nil, err
	}
	var rightValue any
	if err := json.Unmarshal(right, &rightValue); err != nil {
		return nil, err
	}
	return valueDifferenceLeaves(leftValue, true, rightValue, true, path), nil
}

func valueDifferenceLeaves(left any, leftOK bool, right any, rightOK bool, path string) []string {
	switch {
	case !leftOK && !rightOK:
		return nil
	case !leftOK:
		return valueLeafPaths(right, path)
	case !rightOK:
		return valueLeafPaths(left, path)
	}
	leftObject, leftIsObject := left.(map[string]any)
	rightObject, rightIsObject := right.(map[string]any)
	if leftIsObject || rightIsObject {
		if !leftIsObject || !rightIsObject {
			return []string{path}
		}
		keys := make([]string, 0, len(leftObject)+len(rightObject))
		seen := map[string]bool{}
		for _, object := range []map[string]any{leftObject, rightObject} {
			for key := range object {
				if !seen[key] {
					seen[key] = true
					keys = append(keys, key)
				}
			}
		}
		sort.Strings(keys)
		var out []string
		for _, key := range keys {
			leftValue, leftExists := leftObject[key]
			rightValue, rightExists := rightObject[key]
			out = append(out, valueDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, path+"/"+key)...)
		}
		return out
	}
	leftArray, leftIsArray := left.([]any)
	rightArray, rightIsArray := right.([]any)
	if leftIsArray || rightIsArray {
		if !leftIsArray || !rightIsArray {
			return []string{path}
		}
		limit := len(leftArray)
		if len(rightArray) > limit {
			limit = len(rightArray)
		}
		var out []string
		for index := 0; index < limit; index++ {
			var leftValue, rightValue any
			leftExists, rightExists := index < len(leftArray), index < len(rightArray)
			if leftExists {
				leftValue = leftArray[index]
			}
			if rightExists {
				rightValue = rightArray[index]
			}
			out = append(out, valueDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	}
	if !reflect.DeepEqual(left, right) {
		return []string{path}
	}
	return nil
}

func valueLeafPaths(value any, path string) []string {
	switch typed := value.(type) {
	case map[string]any:
		if len(typed) == 0 {
			return []string{path}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		var out []string
		for _, key := range keys {
			out = append(out, valueLeafPaths(typed[key], path+"/"+key)...)
		}
		return out
	case []any:
		if len(typed) == 0 {
			return []string{path}
		}
		var out []string
		for index, item := range typed {
			out = append(out, valueLeafPaths(item, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	default:
		return []string{path}
	}
}

func formatLedger(value ledger) string {
	var out strings.Builder
	out.WriteString("chaos8169HomeNoDataLedger{\n\tEntries: []chaos8169HomeNoDataLedgerEntry{\n")
	for _, entry := range value.Entries {
		fmt.Fprintf(&out, "\t\t{Path: %q, Python: %q, Go: %q, Leaves: %d},\n", entry.Path, entry.Python, entry.Go, entry.Leaves)
	}
	out.WriteString("\t},\n\tStrictRoots: []chaos8169HomeNoDataStrictRoot{\n")
	for _, root := range value.Strict {
		fmt.Fprintf(&out, "\t\t{Path: %q, Kind: %q, Length: %d},\n", root.Path, root.Kind, root.Length)
	}
	out.WriteString("\t},\n}")
	return out.String()
}

func fatal(format string, values ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", values...)
	os.Exit(1)
}
