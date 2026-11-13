package queuecreds_test

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// The job contract refuses a web approver reference value: the scanner built from the credential-classes contract
// (which carries the reference's header as a second dedicated_header marker of acr_web_assertion) matches it. With the
// marker removed this fails.
func TestAJobValueHoldingAWebApproverReferenceIsRefused(t *testing.T) {
	root := repoRoot(t)
	data, err := os.ReadFile(filepath.Join(root, "contracts", "auth", "v1", "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var contract struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(data, &contract); err != nil {
		t.Fatal(err)
	}
	headers := 0
	for _, class := range contract.Classes {
		if class.ClassID != "acr_web_assertion" {
			continue
		}
		for _, marker := range class.Markers {
			if marker.Kind == "dedicated_header" && (marker.Name == "X-ACR-Web-Assertion" || marker.Name == "X-ACR-Approver-Reference") {
				headers++
			}
		}
	}
	if headers != 2 {
		t.Fatalf("acr_web_assertion carries %d of its 2 dedicated headers", headers)
	}
	enc := base64.RawURLEncoding
	value := enc.EncodeToString([]byte(`{"alg":"EdDSA","kid":"web-test-1","typ":"web-approver+jwt"}`)) + "." +
		enc.EncodeToString([]byte(`{"aud":"auth-service","sid":"s","sub":"u"}`)) + "." + enc.EncodeToString(make([]byte, 64))
	if rule := valueScanner(t, root).Match(value); rule == "" {
		t.Fatal("a job value holding a web approver reference was not refused")
	}
}
