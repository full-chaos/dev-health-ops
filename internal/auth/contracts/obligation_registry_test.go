package contracts

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

const (
	redactionRegistryPath   = "contracts/auth/v1/vocabulary/redaction-classes.v1.json"
	decisionSchemaPath      = "contracts/auth/v1/jsonschema/authorization_decision.v1.schema.json"
	obligationsDocPath      = "docs/reference/auth/obligations.md"
	wantRepositoryAllowlist = 256
)

type redactionClass struct {
	ID          string `json:"id"`
	Description string `json:"description"`
	Reserved    bool   `json:"reserved"`
}

type redactionRule struct {
	Role      string   `json:"role"`
	Action    string   `json:"action"`
	Redaction []string `json:"redaction"`
}

type redactionRegistry struct {
	SchemaVersion string           `json:"schema_version"`
	Description   string           `json:"description"`
	Classes       []redactionClass `json:"classes"`
	Rules         []redactionRule  `json:"rules"`
}

func readRoot(t *testing.T, rel string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), rel))
	if err != nil {
		t.Fatalf("reading %s: %v", rel, err)
	}
	return raw
}

func decodeStrictJSON(t *testing.T, raw []byte, into any) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		t.Fatalf("strict decode: %v", err)
	}
}

func loadRedactionRegistry(t *testing.T) redactionRegistry {
	t.Helper()
	var reg redactionRegistry
	decodeStrictJSON(t, readRoot(t, redactionRegistryPath), &reg)
	return reg
}

func decisionSchemaRedactionEnum(t *testing.T) []string {
	t.Helper()
	var schema struct {
		Properties struct {
			Obligations struct {
				Properties struct {
					Redaction struct {
						Items struct {
							Enum []string `json:"enum"`
						} `json:"items"`
					} `json:"redaction"`
					RepositoryAllowlist struct {
						MaxItems *int `json:"maxItems"`
					} `json:"repository_allowlist"`
				} `json:"properties"`
			} `json:"obligations"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(readRoot(t, decisionSchemaPath), &schema); err != nil {
		t.Fatalf("decision schema: %v", err)
	}
	return schema.Properties.Obligations.Properties.Redaction.Items.Enum
}

func catalogRoleActions(t *testing.T) (roles map[string][]string, actions map[string]bool) {
	t.Helper()
	var r struct {
		Roles []struct {
			Role    string   `json:"role"`
			Actions []string `json:"actions"`
		} `json:"roles"`
	}
	if err := json.Unmarshal(readRoot(t, "contracts/auth/v1/vocabulary/roles.v1.json"), &r); err != nil {
		t.Fatalf("roles: %v", err)
	}
	var a struct {
		Actions []struct {
			Action string `json:"action"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(readRoot(t, "contracts/auth/v1/vocabulary/actions.v1.json"), &a); err != nil {
		t.Fatalf("actions: %v", err)
	}
	roles, actions = map[string][]string{}, map[string]bool{}
	for _, role := range r.Roles {
		roles[role.Role] = role.Actions
	}
	for _, act := range a.Actions {
		actions[act.Action] = true
	}
	return roles, actions
}

// checkRedactionRegistry is the census of the redaction class registry: the
// closed class set equals the decision schema enum, every rule names a catalog
// role that holds the catalog action and registered classes, rules are sorted
// and unique, and a class is either used by a rule or reserved.
func checkRedactionRegistry(reg redactionRegistry, schemaEnum []string, roles map[string][]string, actions map[string]bool) []string {
	var bad []string
	if reg.SchemaVersion != "redaction-classes.v1" {
		bad = append(bad, "schema_version "+reg.SchemaVersion)
	}
	ids := make([]string, 0, len(reg.Classes))
	registered := map[string]redactionClass{}
	for _, c := range reg.Classes {
		if _, dup := registered[c.ID]; dup {
			bad = append(bad, "duplicate class "+c.ID)
		}
		registered[c.ID] = c
		ids = append(ids, c.ID)
		if strings.TrimSpace(c.Description) == "" {
			bad = append(bad, "class "+c.ID+" has no description")
		}
	}
	if !slices.IsSorted(ids) {
		bad = append(bad, "classes are not sorted by id")
	}
	want := slices.Clone(schemaEnum)
	slices.Sort(want)
	got := slices.Clone(ids)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		bad = append(bad, fmt.Sprintf("classes %v differ from the decision schema enum %v", got, want))
	}
	used := map[string]bool{}
	seen := map[string]bool{}
	prev := ""
	for _, r := range reg.Rules {
		key := r.Action + "\x00" + r.Role
		if key < prev {
			bad = append(bad, fmt.Sprintf("rule (%s, %s) is out of order", r.Action, r.Role))
		}
		prev = key
		if seen[key] {
			bad = append(bad, fmt.Sprintf("duplicate rule (%s, %s)", r.Action, r.Role))
		}
		seen[key] = true
		held, roleKnown := roles[r.Role]
		switch {
		case !roleKnown:
			bad = append(bad, "rule names unknown role "+r.Role)
		case !actions[r.Action]:
			bad = append(bad, "rule names unknown action "+r.Action)
		case !slices.Contains(held, r.Action):
			bad = append(bad, fmt.Sprintf("role %s does not hold action %s: the rule is dead", r.Role, r.Action))
		}
		if len(r.Redaction) == 0 {
			bad = append(bad, fmt.Sprintf("rule (%s, %s) names no class", r.Action, r.Role))
		}
		if !slices.IsSorted(r.Redaction) || len(slices.Compact(slices.Clone(r.Redaction))) != len(r.Redaction) {
			bad = append(bad, fmt.Sprintf("rule (%s, %s) classes are not sorted and unique", r.Action, r.Role))
		}
		for _, id := range r.Redaction {
			if _, ok := registered[id]; !ok {
				bad = append(bad, "rule names unregistered class "+id)
			}
			used[id] = true
		}
	}
	for _, c := range reg.Classes {
		switch {
		case used[c.ID] && c.Reserved:
			bad = append(bad, "class "+c.ID+" is reserved but a rule uses it")
		case !used[c.ID] && !c.Reserved:
			bad = append(bad, "class "+c.ID+" is used by no rule and is not reserved")
		}
	}
	return bad
}

func TestRedactionRegistryCensus(t *testing.T) {
	reg := loadRedactionRegistry(t)
	roles, actions := catalogRoleActions(t)
	for _, why := range checkRedactionRegistry(reg, decisionSchemaRedactionEnum(t), roles, actions) {
		t.Errorf("redaction registry: %s", why)
	}
}

func TestRedactionRegistryCensusHasTeeth(t *testing.T) {
	enum := []string{"contact_details", "free_text", "personal_identifiers"}
	roles := map[string][]string{"viewer": {"analytics.read"}, "member": {"analytics.read", "analytics.export"}}
	actions := map[string]bool{"analytics.read": true, "analytics.export": true}
	good := func() redactionRegistry {
		return redactionRegistry{SchemaVersion: "redaction-classes.v1", Classes: []redactionClass{
			{ID: "contact_details", Description: "d", Reserved: true},
			{ID: "free_text", Description: "d"},
			{ID: "personal_identifiers", Description: "d", Reserved: true},
		}, Rules: []redactionRule{{Role: "viewer", Action: "analytics.read", Redaction: []string{"free_text"}}}}
	}
	if bad := checkRedactionRegistry(good(), enum, roles, actions); len(bad) != 0 {
		t.Fatalf("the planted good registry fails its census: %v", bad)
	}
	cases := map[string]func(*redactionRegistry){
		"a class outside the schema enum": func(r *redactionRegistry) {
			r.Classes = append(r.Classes, redactionClass{ID: "zzz_extra", Description: "d", Reserved: true})
		},
		"a schema class missing from the registry": func(r *redactionRegistry) {
			r.Classes = r.Classes[:2]
		},
		"a class with no description":      func(r *redactionRegistry) { r.Classes[0].Description = " " },
		"an unreserved class no rule uses": func(r *redactionRegistry) { r.Classes[0].Reserved = false },
		"a reserved class a rule uses":     func(r *redactionRegistry) { r.Classes[1].Reserved = true },
		"a rule with an unregistered class": func(r *redactionRegistry) {
			r.Rules[0].Redaction = []string{"free_text", "zzz_unknown"}
		},
		"a rule with an unknown role":   func(r *redactionRegistry) { r.Rules[0].Role = "guest" },
		"a rule with an unknown action": func(r *redactionRegistry) { r.Rules[0].Action = "nothing.read" },
		"a rule of a role that does not hold the action": func(r *redactionRegistry) {
			r.Rules[0].Role, r.Rules[0].Action = "viewer", "analytics.export"
		},
		"a duplicate rule": func(r *redactionRegistry) { r.Rules = append(r.Rules, r.Rules[0]) },
		"rules out of order": func(r *redactionRegistry) {
			r.Rules = []redactionRule{{Role: "viewer", Action: "analytics.read", Redaction: []string{"free_text"}},
				{Role: "member", Action: "analytics.export", Redaction: []string{"free_text"}}}
		},
		"a rule with no class":         func(r *redactionRegistry) { r.Rules[0].Redaction = nil },
		"a rule with a repeated class": func(r *redactionRegistry) { r.Rules[0].Redaction = []string{"free_text", "free_text"} },
		"a wrong schema_version":       func(r *redactionRegistry) { r.SchemaVersion = "redaction-classes.v2" },
		"a repeated class id":          func(r *redactionRegistry) { r.Classes = append(r.Classes, r.Classes[0]) },
	}
	for name, mutate := range cases {
		reg := good()
		mutate(&reg)
		if len(checkRedactionRegistry(reg, enum, roles, actions)) == 0 {
			t.Errorf("the census accepts %s", name)
		}
	}
}

func TestRepositoryAllowlistIsBoundedAtTheCap(t *testing.T) {
	var schema struct {
		Properties struct {
			Obligations struct {
				Properties struct {
					RepositoryAllowlist struct {
						MaxItems *int `json:"maxItems"`
					} `json:"repository_allowlist"`
				} `json:"properties"`
			} `json:"obligations"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(readRoot(t, decisionSchemaPath), &schema); err != nil {
		t.Fatal(err)
	}
	got := schema.Properties.Obligations.Properties.RepositoryAllowlist.MaxItems
	if got == nil || *got != wantRepositoryAllowlist {
		t.Fatalf("repository_allowlist maxItems = %v, want %d", got, wantRepositoryAllowlist)
	}
}

func TestObligationsDocNamesTheRegistryAndTheCap(t *testing.T) {
	doc := string(readRoot(t, obligationsDocPath))
	reg := loadRedactionRegistry(t)
	for _, want := range []string{"redaction-classes.v1.json", fmt.Sprint(wantRepositoryAllowlist)} {
		if !strings.Contains(doc, want) {
			t.Errorf("%s does not mention %q", obligationsDocPath, want)
		}
	}
	for _, c := range reg.Classes {
		if !strings.Contains(doc, "`"+c.ID+"`") {
			t.Errorf("%s does not list the class %q", obligationsDocPath, c.ID)
		}
	}
	if strings.Contains(doc, "No service issues a decision") {
		t.Errorf("%s still says no service issues a decision", obligationsDocPath)
	}
}

func TestParseDecisionKeepsTheRepositoryScope(t *testing.T) {
	raw := readRoot(t, "contracts/auth/v1/examples/authorization_decision/decision-valid-allow-repository-scope-organization.json")
	d, err := ParseAuthorizationDecision(testRoot(t), raw, authorizationNow)
	if err != nil {
		t.Fatalf("a decision with repository_scope organization is refused: %v", err)
	}
	if d.Obligations == nil || d.Obligations.RepositoryScope == nil || *d.Obligations.RepositoryScope != "organization" ||
		d.Obligations.RepositoryAllowlist != nil {
		t.Fatalf("obligations %+v", d.Obligations)
	}
}

func TestRepositoryScopeIsClosedToOrganization(t *testing.T) {
	var schema struct {
		Properties struct {
			Obligations struct {
				Properties struct {
					RepositoryScope struct {
						Enum []string `json:"enum"`
					} `json:"repository_scope"`
				} `json:"properties"`
			} `json:"obligations"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(readRoot(t, decisionSchemaPath), &schema); err != nil {
		t.Fatal(err)
	}
	if got := schema.Properties.Obligations.Properties.RepositoryScope.Enum; len(got) != 1 || got[0] != "organization" {
		t.Fatalf("repository_scope enum %v, want [organization]", got)
	}
}
