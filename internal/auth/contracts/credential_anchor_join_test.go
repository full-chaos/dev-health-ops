package contracts

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// joinWalkedClasses are the classes whose anchors other checks walk without reading their lines: the llm_api_keys
// class (llm_api_keys_anchor_test.go) and the effective_principal_envelope class (the queryapi/server row walk). Their
// line text is this package's pin check alone, so every such anchor must have a pin.
var joinWalkedClasses = []string{"llm_api_keys", "effective_principal_envelope"}

// unpinnedAnchors lists the ops anchors of classes that no pin covers (path:line-end), sorted; counted is how many
// anchors it walked.
func unpinnedAnchors(document []byte, pins []anchorPin, classes []string) (missing []string, counted int, err error) {
	refs, err := collectCredentialAnchorRefs(document)
	if err != nil {
		return nil, 0, err
	}
	pinned := map[string]bool{}
	for _, pin := range pins {
		pinned[pinKey(pin.Path, pin.Line, pin.End)] = true
	}
	want := map[string]bool{}
	for _, class := range classes {
		want[class] = true
	}
	for _, ref := range refs {
		if !want[ref.classID] || ref.repo != "ops" {
			continue
		}
		counted++
		if !pinned[pinKey(ref.path, ref.line, ref.end)] {
			missing = append(missing, fmt.Sprintf("class %s anchor %s %s", ref.classID, strings.TrimPrefix(ref.where, "/"), pinKey(ref.path, ref.line, ref.end)))
		}
	}
	sort.Strings(missing)
	return missing, counted, nil
}

func TestEnvelopeAndLLMKeyAnchorsAllHaveAPin(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	pins := loadAnchorPins(t)
	missing, counted, err := unpinnedAnchors(raw, pins, joinWalkedClasses)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range missing {
		t.Errorf("%s has no pin: the walks that skip its line would see no change to it", m)
	}
	if counted < 3 {
		t.Fatalf("only %d anchors of %v were walked: the join measured nothing", counted, joinWalkedClasses)
	}
	t.Logf("join: %d anchors of %v, all pinned", counted, joinWalkedClasses)
}

func TestTheAnchorPinJoinNamesAnAnchorWhosePinIsGone(t *testing.T) {
	document := []byte(`{"classes":[{"class_id":"llm_api_keys","issuer":{"anchors":[{"repo":"ops","path":"a.go","line":3},{"repo":"web","path":"b.ts","line":1}]}},` +
		`{"class_id":"other","issuer":{"anchors":[{"repo":"ops","path":"c.go","line":9}]}}]}`)
	pins := []anchorPin{{Path: "a.go", Line: 3, Text: "x", Symbol: "X"}}
	if missing, counted, err := unpinnedAnchors(document, pins, joinWalkedClasses); err != nil || len(missing) != 0 || counted != 1 {
		t.Fatalf("a pinned ops anchor (and a web anchor, an unwalked class) must pass with 1 counted: %v %d %v", missing, counted, err)
	}
	missing, _, err := unpinnedAnchors(document, nil, joinWalkedClasses)
	if err != nil || len(missing) != 1 || !strings.Contains(missing[0], "class llm_api_keys anchor issuer/anchors[0] a.go:3-0") {
		t.Fatalf("an anchor whose pin is gone must be named: %v %v", missing, err)
	}
	if _, _, err := unpinnedAnchors([]byte("{"), nil, joinWalkedClasses); err == nil {
		t.Fatal("a registry that does not parse must be an error")
	}
}
