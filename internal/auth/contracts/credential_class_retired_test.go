package contracts

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// endpointProfileInventories are the three ops inventories; a missing one is a gate failure, not a pass.
var endpointProfileInventories = []string{"endpoint-profiles.go-api.json", "endpoint-profiles.query-api.json", "endpoint-profiles.auth-service.json"}

type retiredGateRow struct {
	ID                    string   `json:"id"`
	AcceptedClasses       []string `json:"accepted_credential_classes"`
	AccessTokenValidation *struct {
		LegacyHS256 *bool `json:"legacy_hs256"`
	} `json:"access_token_validation"`
}

func retiredClassIDs(classes []byte) (map[string]bool, error) {
	var document struct {
		Classes []struct {
			ClassID string `json:"class_id"`
			Status  string `json:"status"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(classes, &document); err != nil {
		return nil, err
	}
	if len(document.Classes) == 0 {
		return nil, fmt.Errorf("credential classes: no class read")
	}
	retired := map[string]bool{}
	for _, class := range document.Classes {
		if class.Status == "retired" {
			retired[class.ClassID] = true
		}
	}
	return retired, nil
}

// retiredClassViolations names every row that accepts a retired class or opts into the HS256 access token.
func retiredClassViolations(retired map[string]bool, name string, inventory []byte) ([]string, error) {
	var document struct {
		Rows []retiredGateRow `json:"rows"`
	}
	if err := json.Unmarshal(inventory, &document); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if len(document.Rows) == 0 {
		return nil, fmt.Errorf("%s: no row read", name)
	}
	var out []string
	for _, row := range document.Rows {
		for _, class := range row.AcceptedClasses {
			if retired[class] {
				out = append(out, fmt.Sprintf("%s: %s accepts retired class %s", name, row.ID, class))
			}
		}
		if row.AccessTokenValidation != nil && row.AccessTokenValidation.LegacyHS256 != nil && *row.AccessTokenValidation.LegacyHS256 {
			out = append(out, fmt.Sprintf("%s: %s sets access_token_validation.legacy_hs256 true", name, row.ID))
		}
	}
	return out, nil
}

func readContractFile(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), name))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestTheHS256AccessTokenClassIsRetired(t *testing.T) {
	retired, err := retiredClassIDs(readContractFile(t, "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !retired[legacyHS256Class] {
		t.Fatalf("%s is not status retired in credential-classes.json", legacyHS256Class)
	}
}

func TestNoEndpointProfileRowAcceptsARetiredClass(t *testing.T) {
	retired, err := retiredClassIDs(readContractFile(t, "credential-classes.json"))
	if err != nil {
		t.Fatal(err)
	}
	found, err := filepath.Glob(filepath.Join(ContractsDir(testRoot(t)), "endpoint-profiles.*.json"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, path := range found {
		names[filepath.Base(path)] = true
	}
	for _, name := range endpointProfileInventories {
		if !names[name] {
			t.Fatalf("inventory %s is missing: the gate would read nothing there", name)
		}
	}
	var violations []string
	for name := range names {
		got, err := retiredClassViolations(retired, name, readContractFile(t, name))
		if err != nil {
			t.Fatal(err)
		}
		violations = append(violations, got...)
	}
	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("%d rows accept a retired class or the HS256 opt-in:\n%s", len(violations), strings.Join(violations, "\n"))
	}
}

func TestTheRetiredClassGateRefusesEachPlantedRow(t *testing.T) {
	retired := map[string]bool{legacyHS256Class: true}
	cells := []struct {
		name string
		row  string
		want string
	}{
		{"retired class", `{"id":"GET /a","accepted_credential_classes":["ops_access_token_hs256","user_access_token"],"access_token_validation":{"legacy_hs256":false}}`, "accepts retired class ops_access_token_hs256"},
		{"retired class alone", `{"id":"GET /b","accepted_credential_classes":["ops_access_token_hs256"]}`, "accepts retired class ops_access_token_hs256"},
		{"hs256 opt-in", `{"id":"GET /c","accepted_credential_classes":["user_access_token"],"access_token_validation":{"legacy_hs256":true}}`, "legacy_hs256 true"},
		{"clean row", `{"id":"GET /d","accepted_credential_classes":["user_access_token"],"access_token_validation":{"legacy_hs256":false}}`, ""},
	}
	for _, cell := range cells {
		t.Run(cell.name, func(t *testing.T) {
			got, err := retiredClassViolations(retired, "probe.json", []byte(`{"rows":[`+cell.row+`]}`))
			if err != nil {
				t.Fatal(err)
			}
			if cell.want == "" {
				if len(got) != 0 {
					t.Fatalf("clean row refused: %v", got)
				}
				return
			}
			if len(got) != 1 || !strings.Contains(got[0], cell.want) {
				t.Fatalf("got %v, want one violation containing %q", got, cell.want)
			}
		})
	}
	if _, err := retiredClassViolations(retired, "empty.json", []byte(`{"rows":[]}`)); err == nil {
		t.Fatal("an inventory with no row passed the gate")
	}
}
