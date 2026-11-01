package authapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type authorizeGoldenManifest struct {
	Vectors []struct {
		File  string `json:"file"`
		ID    string `json:"id"`
		Layer string `json:"layer"`
	} `json:"vectors"`
}

type authorizeGoldenVector struct {
	ID         string `json:"id"`
	Layer      string `json:"layer"`
	Why        string `json:"why"`
	RequestRaw string `json:"request_raw"`
	Expect     struct {
		Outcome  string `json:"outcome"`
		Refusal  string `json:"refusal"`
		Question *struct {
			Action         string `json:"action"`
			ResourceType   string `json:"resource_type"`
			ResourceKey    string `json:"resource_key"`
			OrganizationID string `json:"organization_id"`
		} `json:"question"`
	} `json:"expect"`
}

func authorizeGoldenStrict(t *testing.T, path string, into any) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
}

func TestGoldenDecisionPEPRowsReachTheirVerdict(t *testing.T) {
	dir := filepath.Join(repoRoot(t), "contracts/auth/v1/golden/decision")
	var m authorizeGoldenManifest
	authorizeGoldenStrict(t, filepath.Join(dir, "manifest.json"), &m)
	want := 0
	for _, r := range m.Vectors {
		switch r.Layer {
		case "pep":
			want++
		case "pdp":
		default:
			t.Errorf("row %s: layer %q is outside the closed set pdp|pep", r.ID, r.Layer)
		}
	}
	if want == 0 {
		t.Fatal("the manifest holds no pep row; the runner would pass vacuously")
	}
	ran := 0
	for _, r := range m.Vectors {
		if r.Layer != "pep" {
			continue
		}
		var v authorizeGoldenVector
		authorizeGoldenStrict(t, filepath.Join(dir, "vectors", r.File), &v)
		if v.Layer != "pep" || v.ID != r.ID {
			t.Errorf("row %s: file says layer %q id %q", r.ID, v.Layer, v.ID)
			continue
		}
		ran++
		q, err := DecodeAuthorizationRequest([]byte(v.RequestRaw))
		switch v.Expect.Outcome {
		case "accepted":
			if err != nil {
				t.Errorf("%s: refused: %v", v.ID, err)
				continue
			}
			if v.Expect.Question == nil || q.Action != v.Expect.Question.Action || q.ResourceType != v.Expect.Question.ResourceType ||
				q.ResourceKey != v.Expect.Question.ResourceKey || q.OrganizationID != v.Expect.Question.OrganizationID {
				t.Errorf("%s: decoded %+v, want %+v", v.ID, q, v.Expect.Question)
			}
		case "refused":
			if !errors.Is(err, ErrAuthorizationRequestInvalid) {
				t.Errorf("%s: err %v, want a refusal", v.ID, err)
				continue
			}
			if err.Error() != ErrAuthorizationRequestInvalid.Error()+": "+v.Expect.Refusal {
				t.Errorf("%s: refusal %q, want %q", v.ID, err.Error(), v.Expect.Refusal)
			}
			if v.Expect.Question != nil {
				t.Errorf("%s: a refused row carries a question", v.ID)
			}
		default:
			t.Errorf("%s: outcome %q is outside accepted|refused", v.ID, v.Expect.Outcome)
		}
	}
	if ran != want {
		t.Fatalf("ran %d pep rows, manifest holds %d", ran, want)
	}
}
