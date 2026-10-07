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

// evaluatedV1fSHA256 is the digest of the rubric file that was evaluated (the
// gates of the v1f test). It is a literal on purpose: a change of the embedded
// file and of RubricSHA256 together must still fail here.
const evaluatedV1fSHA256 = "eac20c674565a7b017450e3b6a9dba7926a9f085f60ac28e35b2b7c2b990762d"

const (
	v1fEnablementClause = "and documentation of any kind (customer, marketing or developer documentation). Not: repair"
	v1fFooter           = "Documentation work of any kind (customer, marketing or developer documentation) is work for Enablement and platform, whatever it documents; judge it there."
	v1dEnablementEnd    = "pipeline or environment). Not: repair"
)

func TestTheEmbeddedRubricIsTheEvaluatedV1fFile(t *testing.T) {
	sum := sha256.Sum256(rubricJSON)
	if got := hex.EncodeToString(sum[:]); got != evaluatedV1fSHA256 {
		t.Fatalf("embedded rubric has sha256 %s, the evaluated v1f file has %s", got, evaluatedV1fSHA256)
	}
	if RubricSHA256 != evaluatedV1fSHA256 || RubricVersion != "decision-support-v1f" {
		t.Fatalf("pinned %s %s", RubricVersion, RubricSHA256)
	}
	r, err := LoadRubric()
	if err != nil {
		t.Fatal(err)
	}
	if r.RubricVersion != "decision-support-v1f" {
		t.Fatalf("rubric_version = %q", r.RubricVersion)
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

func TestTheRequestBodyOfTheRealEncoderCarriesV1f(t *testing.T) {
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
	for _, want := range []string{v1fEnablementClause, v1fFooter} {
		if !bytes.Contains(body, []byte(want)) {
			t.Errorf("request body lacks the v1f text %q", want)
		}
	}
	if bytes.Contains(body, []byte(v1dEnablementEnd)) {
		t.Error("request body holds the v1d enablement line")
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
	if !strings.Contains(c.Identity().Stamp(), "prompt=decision-support-v1f@eac20c674565;") {
		t.Errorf("stamp = %s", c.Identity().Stamp())
	}
}

// The committed cases are v1d responses. Under v1f the same response bodies
// must decode to the same outcomes, apart from the rubric id that the
// uncertainty sentence names; the request digest must differ. This is what
// shows the decode path does not depend on the rubric text, with no
// hand-made v1f response.
func TestTheV1dResponsesDecodeTheSameUnderV1f(t *testing.T) {
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
			t.Errorf("case %q: the v1f request has the digest of the v1d request", c.CaseID)
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
