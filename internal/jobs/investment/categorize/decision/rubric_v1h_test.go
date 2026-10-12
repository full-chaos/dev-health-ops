package decision

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/oraclecompare"
)

// v1hSHA256 is the digest of the embedded rubric file. It is a literal on
// purpose: a change of the embedded file and of RubricSHA256 together must
// still fail here.
const v1hSHA256 = "956bdcffa17bf2f660f6557143d95fd8f87c5191c88488dc354988c93458f67b"

// v1gSHA256 is the digest of the v1g file that v1h is derived from; v1h
// records it in derived_from.
const v1gSHA256 = "89f96c46b89a2f4291acc5011684019f600942f96b125cf66465f6576cfbd6fb"

const (
	v1fEnablementClause = "and documentation of any kind (customer, marketing or developer documentation). Not: repair"
	v1fFooter           = "Documentation work of any kind (customer, marketing or developer documentation) is work for Enablement and platform, whatever it documents; judge it there."
	v1dEnablementEnd    = "pipeline or environment). Not: repair"
	v1gFixTitleRule     = "A 'fix' type at the start of a title ('fix:' or 'fix(scope):') is context, not proof of a defect; judge the work that the text describes."
	v1gRefactorRepair   = "and repair of a build, CI or configuration setup that changes no product behaviour"
	v1hUpgradePortFix   = "also a port onto one, and each correction that makes ported or migrated code match the behaviour of the code it replaces, delivered or not."
	v1hBugfixNot        = "Bug fixes: correction of a defect in delivered behaviour that is not a gap between ported or migrated code and the code it replaces. Not: a correction that makes ported or migrated code match the behaviour of the code it replaces, delivered or not (Upgrades and migrations); repair of a build, CI or configuration setup with no defect in product behaviour (Refactoring);"
	v1fBugfixBuild      = "also of a broken build, CI or configuration setup"
	v1gNotYetDelivered  = "a correction to a port or migration that is not yet delivered"
)

func TestTheEmbeddedRubricIsTheV1hFile(t *testing.T) {
	sum := sha256.Sum256(rubricJSON)
	if got := hex.EncodeToString(sum[:]); got != v1hSHA256 {
		t.Fatalf("embedded rubric has sha256 %s, the v1h file has %s", got, v1hSHA256)
	}
	if RubricSHA256 != v1hSHA256 || RubricVersion != "decision-support-v1h" {
		t.Fatalf("pinned %s %s", RubricVersion, RubricSHA256)
	}
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	if r.RubricVersion != "decision-support-v1h" {
		t.Fatalf("rubric_version = %q", r.RubricVersion)
	}
	var head struct {
		DerivedFrom struct {
			RubricVersion string `json:"rubric_version"`
			SHA256        string `json:"sha256"`
		} `json:"derived_from"`
	}
	if err := json.Unmarshal(rubricJSON, &head); err != nil {
		t.Fatal(err)
	}
	if head.DerivedFrom.RubricVersion != "decision-support-v1g" || head.DerivedFrom.SHA256 != v1gSHA256 {
		t.Fatalf("derived_from = %+v, want v1g %s", head.DerivedFrom, v1gSHA256)
	}
	// One changed character is refused by the loader.
	changed := bytes.Replace(rubricJSON, []byte("judge it there."), []byte("judge it there!"), 1)
	if bytes.Equal(changed, rubricJSON) {
		t.Fatal("the test did not change the rubric")
	}
	if _, err := loadPinned(changed, RubricSHA256); err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want a digest mismatch", err)
	}
}

func TestTheRequestBodyOfTheRealEncoderCarriesV1h(t *testing.T) {
	transport := &bodyTransport{err: context.Canceled}
	c := newTestCompleter(t, transport)
	_, _ = c.Classify(context.Background(), syntheticBundle(t))
	if transport.calls != 1 {
		t.Fatalf("%d requests were sent, want 1", transport.calls)
	}
	body := transport.sent[0]
	if !json.Valid(body) {
		t.Fatal("the request body is not JSON")
	}
	for _, want := range []string{v1fEnablementClause, v1fFooter, v1gFixTitleRule, v1gRefactorRepair, v1hUpgradePortFix, v1hBugfixNot} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("request body lacks the text %q", want)
		}
	}
	if bytes.Contains(body, []byte(v1dEnablementEnd)) {
		t.Error("request body holds the v1d enablement line")
	}
	if bytes.Contains(body, []byte(v1fBugfixBuild)) {
		t.Error("request body holds the v1f bugfix line that counts a broken build, CI or configuration setup as a bug fix")
	}
	if bytes.Contains(body, []byte(v1gNotYetDelivered)) {
		t.Error("request body holds the v1g rule that limits port corrections to code not yet delivered")
	}
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	built, err := BuildRequest(r, DefaultModel, syntheticBundle(t))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(built.Body, body) {
		t.Error("the body sent is not the body of BuildRequest")
	}
	if !strings.Contains(c.Identity().Stamp(), "prompt=decision-support-v1h@956bdcffa17b;") {
		t.Errorf("stamp = %s", c.Identity().Stamp())
	}
}

// The committed cases are v1d responses. Under v1h the same response bodies
// must decode to the same outcomes, apart from the rubric id that the
// uncertainty sentence names; the request digest must differ. This is what
// shows the decode path does not depend on the rubric text, with no
// hand-made v1h response.
func TestTheV1dResponsesDecodeTheSameUnderV1h(t *testing.T) {
	cases, err := readReplayCases(filepath.Join("testdata", "replay_cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("zero cases: nothing was compared")
	}
	for _, c := range cases {
		bundle, err := c.Fixture.bundle()
		if err != nil {
			t.Fatal(err)
		}
		body, err := base64.StdEncoding.DecodeString(c.ResponseB64)
		if err != nil {
			t.Fatal(err)
		}
		transport := &bodyTransport{body: body}
		got, err := newTestCompleter(t, transport).Classify(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(transport.sent[0])
		if hex.EncodeToString(sum[:]) == c.RequestSHA256 {
			t.Errorf("case %q: the v1h request has the digest of the v1d request", c.CaseID)
		}
		encoded, err := json.Marshal(oraclecompare.TypedEncode(t, reflect.ValueOf(got)))
		if err != nil {
			t.Fatal(err)
		}
		var row map[string]any
		if err := json.Unmarshal(encoded, &row); err != nil {
			t.Fatal(err)
		}
		wantRaw, err := json.Marshal(c.Expected)
		if err != nil {
			t.Fatal(err)
		}
		wantRaw = bytes.ReplaceAll(wantRaw, []byte(rubricV1dVersion), []byte(RubricVersion))
		var want map[string]any
		if err := json.Unmarshal(wantRaw, &want); err != nil {
			t.Fatal(err)
		}
		for _, m := range oraclecompare.DiffRows(c.CaseID, want, row, nil, nil) {
			t.Error(m)
		}
	}
}
