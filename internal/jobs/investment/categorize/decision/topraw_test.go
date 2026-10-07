package decision

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// zeroSupportBody is the committed zero_support response with the level maps of
// some support keys replaced.
func zeroSupportBody(t *testing.T, maps map[string]map[string]float64) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "responses", "planted-zero-support.json"))
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	answers := doc["answers"].(map[string]any)
	for key, levels := range maps {
		answer, ok := answers[SupportQuestionID(key)].(map[string]any)
		if !ok {
			t.Fatalf("the response has no answer for %s", key)
		}
		answer["probabilities"] = levels
	}
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func classifyBody(t *testing.T, body []byte) Classification {
	t.Helper()
	got, err := newV1dCompleter(t, &bodyTransport{body: body}, "").Classify(context.Background(), syntheticBundle(t))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func levelMap(p0, p1 float64) map[string]float64 {
	return map[string]float64{"0": p0, "1": p1, "2": 0, "3": 0}
}

// The top raw key is the key with the lowest raw P(level 0). Two keys with the
// same value: the first in alphabetical order, whatever the other is.
func TestTopRawKeyIsTheLowestRawLevelZeroAndTheFirstKeyWinsATie(t *testing.T) {
	cases := []struct {
		name string
		maps map[string]map[string]float64
		want string
	}{
		{"one key is lowest", map[string]map[string]float64{"quality.bugfix": levelMap(0.55, 0.45)}, "quality.bugfix"},
		{"a later key that is lower wins", map[string]map[string]float64{
			"maintenance.debt": levelMap(0.6, 0.4), "quality.bugfix": levelMap(0.59, 0.41)}, "quality.bugfix"},
		{"a tie goes to the first key", map[string]map[string]float64{
			"quality.bugfix": levelMap(0.6, 0.4), "maintenance.debt": levelMap(0.6, 0.4), "risk.security": levelMap(0.6, 0.4)}, "maintenance.debt"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyBody(t, zeroSupportBody(t, tc.maps))
			if got.State != StateZeroSupport {
				t.Fatalf("state = %q, want zero_support: the case must stay under the presence floor", got.State)
			}
			if got.TopRawKey != tc.want {
				t.Errorf("TopRawKey = %q, want %q", got.TopRawKey, tc.want)
			}
		})
	}
}

// The raw map decides, not the renormalised one. Key A returns 0.50 of a sum of
// 0.99 (renormalised 0.505...), key B returns 0.505 of a sum of 1.00: raw says
// A, the renormalised values say B.
func TestTopRawKeyReadsTheMapAsReturnedNotTheRenormalisedOne(t *testing.T) {
	got := classifyBody(t, zeroSupportBody(t, map[string]map[string]float64{
		"risk.security":  {"0": 0.50, "1": 0.49, "2": 0, "3": 0},
		"quality.bugfix": {"0": 0.505, "1": 0.495, "2": 0, "3": 0},
	}))
	if got.State != StateZeroSupport {
		t.Fatalf("state = %q", got.State)
	}
	a, b := got.LevelProbabilities["risk.security"][0], got.LevelProbabilities["quality.bugfix"][0]
	if !(b < a) {
		t.Fatalf("the case is not built right: renormalised P(level 0) is %v for risk.security and %v for quality.bugfix", a, b)
	}
	if got.TopRawKey != "risk.security" {
		t.Errorf("TopRawKey = %q, want risk.security (the lowest raw P(level 0))", got.TopRawKey)
	}
}

// A degraded answer (a map that is not a distribution) has no valid raw map:
// it is never the top raw key, even with the lowest value.
func TestADegradedAnswerIsNeverTheTopRawKey(t *testing.T) {
	got := classifyBody(t, zeroSupportBody(t, map[string]map[string]float64{
		"feature_delivery.customer": {"0": 0.1, "1": 0.1, "2": 0, "3": 0},
		"quality.testing":           levelMap(0.7, 0.3),
	}))
	if got.TopRawKey != "quality.testing" {
		t.Errorf("TopRawKey = %q, want quality.testing; warnings %v", got.TopRawKey, got.Warnings)
	}
	if _, ok := got.LevelProbabilities["feature_delivery.customer"]; ok {
		t.Error("the degraded answer has a level distribution: the case is not built right")
	}
}

// TopRawKey is one of the 15 canonical keys or empty: nothing of a response can
// be in it.
func TestTopRawKeyIsACanonicalKeyOrEmptyForEveryCommittedCase(t *testing.T) {
	cases, err := readReplayCases(filepath.Join("testdata", "replay_cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	canonical := map[string]bool{"": true}
	for _, k := range SortedKeys() {
		canonical[k] = true
	}
	withKey := 0
	for _, c := range cases {
		bundle, err := c.Fixture.bundle()
		if err != nil {
			t.Fatal(err)
		}
		body, _ := base64.StdEncoding.DecodeString(c.ResponseB64)
		got, err := newV1dCompleter(t, &bodyTransport{body: body}, c.ModelRequested).Classify(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if !canonical[got.TopRawKey] {
			t.Errorf("case %q: TopRawKey %q is not a canonical key", c.CaseID, got.TopRawKey)
		}
		if got.TopRawKey != "" {
			withKey++
		}
	}
	if withKey == 0 {
		t.Fatal("no committed case has a top raw key: nothing was measured")
	}
}

// LevelMix of the levels of an ok classification is its validated mix: the
// mix a served evidence_none row gets is the mix the ok path would have given.
func TestLevelMixOfAnOkClassificationIsItsValidatedMix(t *testing.T) {
	cases, err := readReplayCases(filepath.Join("testdata", "replay_cases.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	compared := 0
	for _, c := range cases {
		bundle, err := c.Fixture.bundle()
		if err != nil {
			t.Fatal(err)
		}
		body, _ := base64.StdEncoding.DecodeString(c.ResponseB64)
		completer := newV1dCompleter(t, &bodyTransport{body: body}, c.ModelRequested)
		got, err := completer.Classify(context.Background(), bundle)
		if err != nil {
			t.Fatal(err)
		}
		if got.State != StateOK {
			continue
		}
		mix, ok := completer.LevelMix(got.Levels)
		if !ok || !reflect.DeepEqual(mix, got.Subcategories) {
			t.Errorf("case %q: LevelMix = %v (ok %v), the validated mix is %v", c.CaseID, mix, ok, got.Subcategories)
		}
		compared++
	}
	if compared == 0 {
		t.Fatal("no ok case was compared")
	}
}

func TestLevelMixRefusesLevelsItCannotMap(t *testing.T) {
	completer := newV1dCompleter(t, &bodyTransport{}, "")
	full := func(level int) map[string]int {
		levels := map[string]int{}
		for _, k := range SortedKeys() {
			levels[k] = level
		}
		return levels
	}
	if _, ok := completer.LevelMix(full(0)); ok {
		t.Error("levels with no supported key gave a mix")
	}
	missing := full(1)
	delete(missing, "quality.bugfix")
	if _, ok := completer.LevelMix(missing); ok {
		t.Error("levels that do not cover the 15 keys gave a mix")
	}
	for _, bad := range []int{-1, 4} {
		levels := full(1)
		levels["quality.bugfix"] = bad
		if _, ok := completer.LevelMix(levels); ok {
			t.Errorf("level %d gave a mix", bad)
		}
	}
	one := full(0)
	one["quality.bugfix"] = 3
	mix, ok := completer.LevelMix(one)
	if !ok || mix["quality.bugfix"] != 1 || len(mix) != 15 {
		t.Errorf("one primary key: mix = %v, ok %v", mix, ok)
	}
	var none *Completer
	if _, ok := none.LevelMix(full(1)); ok {
		t.Error("a nil completer gave a mix")
	}
}
