package contracts

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// pythonCitation is a citation of the deleted Python api: a source path or a .py file name.
var pythonCitation = regexp.MustCompile(`src/dev_health_ops/|\b[\w/]+\.py\b`)

// validatorAnchorFailures names every validator of the registry that does not name its code: a null anchor, an ops or
// acr anchor that is not a Go file, a web anchor that is not a TypeScript file, a missing earlier_auth_gate or
// foreign_class_validator, or a description that cites the deleted Python api.
func validatorAnchorFailures(document []byte) []string {
	var parsed struct {
		Classes []struct {
			ClassID    string           `json:"class_id"`
			Validators []map[string]any `json:"validators"`
		} `json:"classes"`
	}
	if err := json.Unmarshal(document, &parsed); err != nil {
		return []string{fmt.Sprintf("credential-classes.json does not parse: %v", err)}
	}
	if len(parsed.Classes) == 0 {
		return []string{"credential-classes.json lists no class"}
	}
	var failures []string
	for _, class := range parsed.Classes {
		for i, validator := range class.Validators {
			label := fmt.Sprintf("class %s validators[%d]", class.ClassID, i)
			if description, _ := validator["description"].(string); pythonCitation.MatchString(description) {
				failures = append(failures, fmt.Sprintf("%s: the description cites the deleted Python api: %q", label, pythonCitation.FindString(description)))
			}
			for _, field := range []string{"earlier_auth_gate", "foreign_class_validator"} {
				if _, ok := validator[field].(bool); !ok {
					failures = append(failures, fmt.Sprintf("%s: %s is not stated", label, field))
				}
			}
			anchor, ok := validator["anchor"].(map[string]any)
			if !ok {
				failures = append(failures, label+": no code anchor (null): name the code that decides the class")
				continue
			}
			repo, _ := anchor["repo"].(string)
			path, _ := anchor["path"].(string)
			switch repo {
			case "ops", "acr":
				if !strings.HasSuffix(path, ".go") {
					failures = append(failures, fmt.Sprintf("%s: the %s anchor %s is not a Go file", label, repo, path))
				}
			case "web":
				if !strings.HasSuffix(path, ".ts") && !strings.HasSuffix(path, ".tsx") {
					failures = append(failures, fmt.Sprintf("%s: the web anchor %s is not a TypeScript file", label, path))
				}
			default:
				failures = append(failures, fmt.Sprintf("%s: repo %q is not ops, acr or web", label, repo))
			}
		}
	}
	return failures
}

func TestEveryCredentialValidatorNamesItsCode(t *testing.T) {
	for _, failure := range validatorAnchorFailures(readContractFile(t, "credential-classes.json")) {
		t.Error(failure)
	}
}

func TestTheValidatorAnchorCheckRefusesEachGap(t *testing.T) {
	doc := func(validator string) []byte {
		return []byte(`{"classes":[{"class_id":"demo","validators":[` + validator + `]}]}`)
	}
	good := `{"anchor":{"repo":"ops","path":"internal/x/a.go","line":1},"description":"Decide refuses it","earlier_auth_gate":false,"foreign_class_validator":false}`
	if f := validatorAnchorFailures(doc(good)); len(f) != 0 {
		t.Fatalf("a Go-anchored validator must pass: %v", f)
	}
	for name, validator := range map[string]string{
		"a null anchor":           `{"anchor":null,"description":"Decide refuses it","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"an ops anchor not in Go": `{"anchor":{"repo":"ops","path":"docs/a.md","line":1},"description":"x","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"an acr anchor not in Go": `{"anchor":{"repo":"acr","path":"a.py","line":1},"description":"x","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"a web anchor not in TS":  `{"anchor":{"repo":"web","path":"src/a.go","line":1},"description":"x","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"an unknown repo":         `{"anchor":{"repo":"go","path":"a.go","line":1},"description":"x","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"a missing field":         `{"anchor":{"repo":"ops","path":"a.go","line":1},"description":"x","earlier_auth_gate":false}`,
		"a Python file":           `{"anchor":{"repo":"ops","path":"a.go","line":1},"description":"external_ingest/auth.py's dependency","earlier_auth_gate":false,"foreign_class_validator":false}`,
		"a Python path":           `{"anchor":{"repo":"ops","path":"a.go","line":1},"description":"src/dev_health_ops/api/x","earlier_auth_gate":false,"foreign_class_validator":false}`,
	} {
		if f := validatorAnchorFailures(doc(validator)); len(f) == 0 {
			t.Errorf("%s must fail", name)
		}
	}
}
