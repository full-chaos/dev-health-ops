package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func registryClass(t *testing.T, id string) map[string]any {
	t.Helper()
	var found map[string]any
	n := 0
	for _, class := range registryDocument(t) {
		if class["class_id"] == id {
			found = class
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the registry lists %d %s classes, want 1", n, id)
	}
	return found
}

// The class is declared and nothing consumes it: no consumer, no validator,
// no endpoint row. The exchange, the verifier and the rows come in later PRs.
func TestServiceAccountTokenClassIsDeclaredWithNoConsumer(t *testing.T) {
	class := registryClass(t, ServiceAccountTokenClass)
	if class["principal_type"] != "service_account" {
		t.Errorf("principal_type = %v", class["principal_type"])
	}
	if class["status"] != "unconsumed" {
		t.Errorf("status = %v, want unconsumed while no validator exists", class["status"])
	}
	for _, member := range []string{"consumers", "validators"} {
		if list, _ := class[member].([]any); len(list) != 0 {
			t.Errorf("%s lists %d entries, want none", member, len(list))
		}
	}
	markers, _ := json.Marshal(class["markers"])
	for _, want := range []string{`"jwt_header_typ"`, `"sa+jwt"`, `"EdDSA"`, `"Bearer"`} {
		if !strings.Contains(string(markers), want) {
			t.Errorf("markers lack %s: %s", want, markers)
		}
	}
	scope, _ := json.Marshal(class["audience_and_scope"])
	if !strings.Contains(string(scope), ServiceAccountTokenAudience) {
		t.Errorf("audience_and_scope does not name %s: %s", ServiceAccountTokenAudience, scope)
	}
	transport, _ := json.Marshal(class["transport"])
	if !strings.Contains(string(transport), "service-account-token.v1") {
		t.Errorf("transport does not name the wire contract: %s", transport)
	}
}

// The key class keeps its own row: it is the exchange subject, never a Bearer
// and never a token. Its status does not move in this PR.
func TestServiceAccountKeyClassStaysUnconsumedAndSeparateFromTheTokenClass(t *testing.T) {
	key := registryClass(t, "service_account_key")
	if key["status"] != "unconsumed" {
		t.Errorf("service_account_key status = %v, want unconsumed", key["status"])
	}
	if list, _ := key["consumers"].([]any); len(list) != 0 {
		t.Errorf("service_account_key lists %d consumers, want none", len(list))
	}
	if list, _ := key["validators"].([]any); len(list) != 0 {
		t.Errorf("service_account_key lists %d validators, want none", len(list))
	}
}

// Only dev-health-api carries the class; no other audience may.
func TestOnlyTheDevHealthAPIAudienceCarriesTheServiceAccountTokenClass(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary struct {
		Audiences []struct {
			Audience string   `json:"audience"`
			Classes  []string `json:"classes"`
		} `json:"audiences"`
	}
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	var carriers []string
	for _, row := range vocabulary.Audiences {
		for _, class := range row.Classes {
			if class == ServiceAccountTokenClass {
				carriers = append(carriers, row.Audience)
			}
		}
	}
	if strings.Join(carriers, ",") != ServiceAccountTokenAudience {
		t.Fatalf("audiences carrying %s: %v, want only %s", ServiceAccountTokenClass, carriers, ServiceAccountTokenAudience)
	}
}

// No endpoint row accepts the class yet: a token of this class is refused on
// every route until the rows PR names it.
func TestNoEndpointProfileRowAcceptsTheServiceAccountTokenClassYet(t *testing.T) {
	matches, err := filepath.Glob(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.*.json"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no endpoint profile files: %v", err)
	}
	for _, path := range matches {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), ServiceAccountTokenClass) {
			t.Errorf("%s names %s", filepath.Base(path), ServiceAccountTokenClass)
		}
	}
}
