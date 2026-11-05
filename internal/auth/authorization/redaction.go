package authorization

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// embeddedRedactionRules reads the rules of vocabulary/redaction-classes.v1.json. A defect in the embedded file
// panics, as a defect in the embedded catalog does: a decision point that cannot read its redaction policy must
// not start and decide without it.
func embeddedRedactionRules(cat catalogView) map[redactionKey][]string {
	var reg struct {
		SchemaVersion string `json:"schema_version"`
		Description   string `json:"description"`
		Classes       []struct {
			ID          string `json:"id"`
			Description string `json:"description"`
			Reserved    bool   `json:"reserved"`
		} `json:"classes"`
		Rules []struct {
			Role      string   `json:"role"`
			Action    string   `json:"action"`
			Redaction []string `json:"redaction"`
		} `json:"rules"`
	}
	dec := json.NewDecoder(bytes.NewReader(vocabulary.RedactionClassesV1))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&reg); err != nil || reg.SchemaVersion != "redaction-classes.v1" || len(reg.Classes) == 0 {
		panic(fmt.Sprintf("authorization: the redaction class registry is not readable: %v", err))
	}
	registered := map[string]bool{}
	for _, c := range reg.Classes {
		registered[c.ID] = true
	}
	rules := map[redactionKey][]string{}
	for _, r := range reg.Rules {
		if _, err := cat.Role(r.Role); err != nil {
			panic("authorization: a redaction rule names an unknown role " + r.Role)
		}
		if _, err := cat.Action(r.Action); err != nil || !cat.Allows(r.Role, r.Action) {
			panic("authorization: a redaction rule names an action the role does not hold: " + r.Role + " " + r.Action)
		}
		if len(r.Redaction) == 0 {
			panic("authorization: a redaction rule names no class: " + r.Role + " " + r.Action)
		}
		for _, class := range r.Redaction {
			if !registered[class] {
				panic("authorization: a redaction rule names an unregistered class " + class)
			}
		}
		key := redactionKey{r.Role, r.Action}
		if _, dup := rules[key]; dup {
			panic("authorization: a redaction rule is repeated: " + r.Role + " " + r.Action)
		}
		rules[key] = append([]string(nil), r.Redaction...)
	}
	return rules
}
