package authapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

type authorizeGoldenManifest struct {
	Description string          `json:"description"`
	Surface     string          `json:"surface"`
	Catalogs    json.RawMessage `json:"catalogs"`
	Vectors     []struct {
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

type authorizeGoldenPDPVector struct {
	ID     string `json:"id"`
	Layer  string `json:"layer"`
	Expect struct {
		Reason         string          `json:"reason"`
		WireReasonCode json.RawMessage `json:"wire_reason_code"`
		// Obligations is checked by the decision core runner and the router replay; this test only reads past it.
		Obligations json.RawMessage `json:"obligations"`
	} `json:"expect"`
}

// Every pdp row names its wire reason: the string wireReasonCode gives its
// internal reason, or null when the reason is a state failure (a 503, no decision).
func TestGoldenDecisionPDPRowsAssertTheirWireReason(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, "contracts/auth/v1/golden/decision")
	var m authorizeGoldenManifest
	authorizeGoldenStrict(t, filepath.Join(dir, "manifest.json"), &m)
	var schema struct {
		Properties struct {
			ReasonCode struct {
				Enum []string `json:"enum"`
			} `json:"reason_code"`
		} `json:"properties"`
	}
	readJSON(t, filepath.Join(contracts.ContractsDir(root), "jsonschema", "authorization_decision.v1.schema.json"), &schema)
	covered := map[authorization.Reason]bool{}
	rows := 0
	for _, r := range m.Vectors {
		if r.Layer != "pdp" {
			continue
		}
		rows++
		var v authorizeGoldenPDPVector
		raw, err := os.ReadFile(filepath.Join(dir, "vectors", r.File))
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("%s: %v", r.File, err)
		}
		reason := authorization.Reason(v.Expect.Reason)
		covered[reason] = true
		if v.Expect.WireReasonCode == nil {
			t.Errorf("%s: expect.wire_reason_code is absent", v.ID)
			continue
		}
		want, onWire := wireReasonCode(reason)
		if !onWire {
			if string(v.Expect.WireReasonCode) != "null" {
				t.Errorf("%s: reason %s is a state failure, wire_reason_code %s, want null", v.ID, reason, v.Expect.WireReasonCode)
			}
			continue
		}
		var got string
		if err := json.Unmarshal(v.Expect.WireReasonCode, &got); err != nil {
			t.Errorf("%s: wire_reason_code %s is not a string; want %q", v.ID, v.Expect.WireReasonCode, want)
			continue
		}
		if got != want {
			t.Errorf("%s: wire_reason_code %q, want %q", v.ID, got, want)
		}
		if !slices.Contains(schema.Properties.ReasonCode.Enum, got) {
			t.Errorf("%s: wire_reason_code %q is not in the decision schema enum", v.ID, got)
		}
	}
	if rows == 0 {
		t.Fatal("the manifest holds no pdp row")
	}
	for _, reason := range authorization.AllReasons() {
		if !covered[reason] {
			t.Errorf("reason %s has no pdp golden row asserting its wire reason", reason)
		}
	}
}
