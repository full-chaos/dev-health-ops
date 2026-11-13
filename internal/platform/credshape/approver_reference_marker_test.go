package credshape

import (
	"encoding/base64"
	"encoding/json"
	"testing"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
)

// webApproverShaped builds, at run time, a value of the web approver reference's shape: a compact EdDSA JWT with typ
// web-approver+jwt. No JWT-shaped literal is in the tree.
func webApproverShaped() string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"EdDSA","kid":"web-test-1","typ":"web-approver+jwt"}`))
	claims := enc.EncodeToString([]byte(`{"aud":"auth-service","iss":"https://web.example","sid":"s","sub":"u"}`))
	return header + "." + claims + "." + enc.EncodeToString(make([]byte, 64))
}

// The web approver reference is the second presentation of acr_web_assertion (same signer, key and JWKS): its
// header is a dedicated_header marker of that class, and the scanner finds and so redacts its value, alone and in
// a header line. With the marker removed from the contract this fails.
func TestTheScannerMatchesAWebApproverReference(t *testing.T) {
	var contract struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Markers []struct {
				Kind string `json:"kind"`
				Name string `json:"name"`
			} `json:"markers"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(authv1.CredentialClasses, &contract); err != nil {
		t.Fatal(err)
	}
	marked := false
	for _, class := range contract.Classes {
		for _, marker := range class.Markers {
			if class.ClassID == "acr_web_assertion" && marker.Kind == "dedicated_header" && marker.Name == "X-ACR-Approver-Reference" {
				marked = true
			}
		}
	}
	if !marked {
		t.Fatal("acr_web_assertion has no dedicated_header X-ACR-Approver-Reference marker")
	}
	scanner, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	value := webApproverShaped()
	if rule := scanner.Match(value); rule == "" {
		t.Fatal("a web approver reference value was not matched")
	}
	if rule := scanner.Match("X-ACR-Approver-Reference: " + value); rule == "" {
		t.Fatal("a web approver reference in its header line was not matched")
	}
	if AnyCredentialShaped("X-ACR-Approver-Reference") {
		t.Fatal("the header name alone must not read as a credential; only its value is one")
	}
}
