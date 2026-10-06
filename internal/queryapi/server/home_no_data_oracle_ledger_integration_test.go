//go:build integration

package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// chaos8169HomeNoDataLedgerTicket names the D4834/D4840 exception. It reads
// exact captured bodies and permits only their generated, keyed leaf ledger.
// It does not normalize, mask, skip, or alter either response.
const chaos8169HomeNoDataLedgerTicket = "CHAOS-8169"

type chaos8169HomeNoDataLedgerKey struct {
	Oracle string
	Case   string
}

type chaos8169HomeNoDataLedgerEntry struct {
	Path   string
	Python string
	Go     string
	Leaves int
}

type chaos8169HomeNoDataStrictRoot struct {
	Path   string
	Kind   string
	Length int
}

type chaos8169HomeNoDataLedger struct {
	Entries     []chaos8169HomeNoDataLedgerEntry
	StrictRoots []chaos8169HomeNoDataStrictRoot
}

var (
	chaos8169DictOrderHomeLedgerKey    = chaos8169HomeNoDataLedgerKey{Oracle: "dict-order", Case: "home no data"}
	chaos8169GraphQLEdgeHomeLedgerKeys = map[string]chaos8169HomeNoDataLedgerKey{
		"POST home": {Oracle: "graphql-edge", Case: "POST home"},
		"GET home":  {Oracle: "graphql-edge", Case: "GET home"},
	}
)

func chaos8169HomeLedgerFor(t *testing.T, key chaos8169HomeNoDataLedgerKey) chaos8169HomeNoDataLedger {
	t.Helper()
	ledger, ok := chaos8169HomeNoDataLedgers[key]
	if !ok {
		t.Fatalf("%s has no ledger for %s/%s", chaos8169HomeNoDataLedgerTicket, key.Oracle, key.Case)
	}
	return ledger
}

func chaos8169GraphQLEdgeHomeLedgerKey(name string) (chaos8169HomeNoDataLedgerKey, bool) {
	key, ok := chaos8169GraphQLEdgeHomeLedgerKeys[name]
	return key, ok
}

func chaos8169HomeObject(t *testing.T, body string) *pyjson.Object {
	t.Helper()
	value, err := pyjson.Decode([]byte(body))
	if err != nil {
		t.Fatalf("%s ledger: decode response body: %v", chaos8169HomeNoDataLedgerTicket, err)
	}
	object, ok := value.(*pyjson.Object)
	if !ok {
		t.Fatalf("%s ledger: response body is %T, want JSON object", chaos8169HomeNoDataLedgerTicket, value)
	}
	return object
}

func chaos8169HomeValue(t *testing.T, object *pyjson.Object, path string) pyjson.Value {
	t.Helper()
	key := strings.TrimPrefix(path, "/")
	if key == "" || strings.Contains(key, "/") {
		t.Fatalf("%s ledger has unsupported path %q", chaos8169HomeNoDataLedgerTicket, path)
	}
	value, ok := object.Get(key)
	if !ok {
		t.Fatalf("%s ledger path %s is absent", chaos8169HomeNoDataLedgerTicket, path)
	}
	return value
}

func chaos8169GraphQLHomeBody(t *testing.T, body string) string {
	t.Helper()
	envelope := chaos8169HomeObject(t, body)
	data, ok := envelope.Get("data")
	if !ok {
		t.Fatalf("%s GraphQL response data is absent", chaos8169HomeNoDataLedgerTicket)
	}
	dataObject, ok := data.(*pyjson.Object)
	if !ok {
		t.Fatalf("%s GraphQL response data is %T, want JSON object", chaos8169HomeNoDataLedgerTicket, data)
	}
	home, ok := dataObject.Get("home")
	if !ok {
		t.Fatalf("%s GraphQL response data.home is absent", chaos8169HomeNoDataLedgerTicket)
	}
	return chaos8169JSONText(t, home)
}

func chaos8169JSONText(t *testing.T, value pyjson.Value) string {
	t.Helper()
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		t.Fatalf("%s ledger: marshal JSON value: %v", chaos8169HomeNoDataLedgerTicket, err)
	}
	return string(encoded)
}

func chaos8169HomeLeafPaths(value pyjson.Value, path string) []string {
	switch typed := value.(type) {
	case *pyjson.Object:
		keys := typed.Keys()
		if len(keys) == 0 {
			return []string{path}
		}
		var out []string
		for _, key := range keys {
			item, _ := typed.Get(key)
			out = append(out, chaos8169HomeLeafPaths(item, path+"/"+key)...)
		}
		return out
	case []pyjson.Value:
		if len(typed) == 0 {
			return []string{path}
		}
		var out []string
		for index, item := range typed {
			out = append(out, chaos8169HomeLeafPaths(item, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	default:
		return []string{path}
	}
}

func chaos8169HomeObjectKeys(left, right *pyjson.Object) []string {
	seen := make(map[string]bool, left.Len()+right.Len())
	keys := make([]string, 0, left.Len()+right.Len())
	for _, object := range []*pyjson.Object{left, right} {
		for _, key := range object.Keys() {
			if seen[key] {
				continue
			}
			seen[key] = true
			keys = append(keys, key)
		}
	}
	return keys
}

// chaos8169HomeDifferenceLeaves reports every differing scalar or unmatched
// JSON leaf. It does not mutate either body or make their values equal.
func chaos8169HomeDifferenceLeaves(left pyjson.Value, leftOK bool, right pyjson.Value, rightOK bool, path string) []string {
	switch {
	case !leftOK && !rightOK:
		return nil
	case !leftOK:
		return chaos8169HomeLeafPaths(right, path)
	case !rightOK:
		return chaos8169HomeLeafPaths(left, path)
	}

	leftObject, leftIsObject := left.(*pyjson.Object)
	rightObject, rightIsObject := right.(*pyjson.Object)
	if leftIsObject || rightIsObject {
		if !leftIsObject || !rightIsObject {
			return []string{path}
		}
		var out []string
		for _, key := range chaos8169HomeObjectKeys(leftObject, rightObject) {
			leftValue, leftExists := leftObject.Get(key)
			rightValue, rightExists := rightObject.Get(key)
			out = append(out, chaos8169HomeDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, path+"/"+key)...)
		}
		return out
	}

	leftList, leftIsList := left.([]pyjson.Value)
	rightList, rightIsList := right.([]pyjson.Value)
	if leftIsList || rightIsList {
		if !leftIsList || !rightIsList {
			return []string{path}
		}
		var out []string
		maxLen := len(leftList)
		if len(rightList) > maxLen {
			maxLen = len(rightList)
		}
		for index := 0; index < maxLen; index++ {
			var leftValue, rightValue pyjson.Value
			leftExists, rightExists := index < len(leftList), index < len(rightList)
			if leftExists {
				leftValue = leftList[index]
			}
			if rightExists {
				rightValue = rightList[index]
			}
			out = append(out, chaos8169HomeDifferenceLeaves(leftValue, leftExists, rightValue, rightExists, fmt.Sprintf("%s/%d", path, index))...)
		}
		return out
	}

	if chaos8169JSONTextNoFail(left) != chaos8169JSONTextNoFail(right) {
		return []string{path}
	}
	return nil
}

func chaos8169JSONTextNoFail(value pyjson.Value) string {
	encoded, err := pyjson.Marshal(value)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

func chaos8169HomeStrictRoot(t *testing.T, object *pyjson.Object, root chaos8169HomeNoDataStrictRoot) {
	t.Helper()
	value := chaos8169HomeValue(t, object, root.Path)
	switch root.Kind {
	case "array":
		items, ok := value.([]pyjson.Value)
		if !ok {
			t.Errorf("%s %s is %T, want JSON array", chaos8169HomeNoDataLedgerTicket, root.Path, value)
			return
		}
		if len(items) != root.Length {
			t.Errorf("%s %s length = %d, want %d", chaos8169HomeNoDataLedgerTicket, root.Path, len(items), root.Length)
		}
	case "object":
		item, ok := value.(*pyjson.Object)
		if !ok {
			t.Errorf("%s %s is %T, want JSON object", chaos8169HomeNoDataLedgerTicket, root.Path, value)
			return
		}
		if item.Len() != root.Length {
			t.Errorf("%s %s length = %d, want %d", chaos8169HomeNoDataLedgerTicket, root.Path, item.Len(), root.Length)
		}
	default:
		t.Fatalf("%s ledger strict root %s has unsupported kind %q", chaos8169HomeNoDataLedgerTicket, root.Path, root.Kind)
	}
}

// assertCHAOS8169HomeNoDataLedger verifies a captured Python/Go Home pair. It
// permits only the generated D4834 or D4840 roots; every other JSON leaf and
// strict root remains measured exactly.
func assertCHAOS8169HomeNoDataLedger(t *testing.T, key chaos8169HomeNoDataLedgerKey, pythonBody, goBody string) {
	t.Helper()
	ledger := chaos8169HomeLedgerFor(t, key)
	python := chaos8169HomeObject(t, pythonBody)
	goResponse := chaos8169HomeObject(t, goBody)

	for _, entry := range ledger.Entries {
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, python, entry.Path)); got != entry.Python {
			t.Errorf("%s ledger Python %s = %s, want %s", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Python)
		}
		if got := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, entry.Path)); got != entry.Go {
			t.Errorf("%s ledger Go %s = %s, want %s", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Go)
		}
	}
	for _, root := range ledger.StrictRoots {
		if got, want := chaos8169JSONText(t, chaos8169HomeValue(t, goResponse, root.Path)), chaos8169JSONText(t, chaos8169HomeValue(t, python, root.Path)); got != want {
			t.Errorf("%s %s changed outside the ledger: Go %s, Python %s", chaos8169HomeNoDataLedgerTicket, root.Path, got, want)
		}
		chaos8169HomeStrictRoot(t, python, root)
		chaos8169HomeStrictRoot(t, goResponse, root)
	}

	entries := make(map[string]chaos8169HomeNoDataLedgerEntry, len(ledger.Entries))
	wantLeaves := 0
	for _, entry := range ledger.Entries {
		entries[entry.Path] = entry
		wantLeaves += entry.Leaves
	}
	counts := make(map[string]int, len(entries))
	differences := chaos8169HomeDifferenceLeaves(python, true, goResponse, true, "")
	for _, path := range differences {
		trimmed := strings.TrimPrefix(path, "/")
		root := "/"
		if trimmed != "" {
			root = "/" + strings.Split(trimmed, "/")[0]
		}
		if _, ok := entries[root]; !ok {
			t.Errorf("%s unledgered difference at %s", chaos8169HomeNoDataLedgerTicket, path)
			continue
		}
		counts[root]++
	}
	if len(differences) != wantLeaves {
		t.Errorf("%s difference leaf count = %d, want %d", chaos8169HomeNoDataLedgerTicket, len(differences), wantLeaves)
	}
	for _, entry := range ledger.Entries {
		if got := counts[entry.Path]; got != entry.Leaves {
			t.Errorf("%s difference leaves at %s = %d, want %d", chaos8169HomeNoDataLedgerTicket, entry.Path, got, entry.Leaves)
		}
	}
}
