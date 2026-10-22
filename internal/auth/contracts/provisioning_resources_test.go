package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/provisioning"
	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// The organization, membership and invite provisioning resources and request
// bodies. Each surface is a pure schema document: no rule is client-only.
var provisioningResourceSurfaces = []string{
	"organization", "organization-create-request",
	"membership", "membership-update-request",
	"invite", "invite-create-request", "invite-accept-request",
}

var provisioningResourceKeywordTokens = map[string]string{
	"additionalProperties": "unexpected additional properties",
	"enum":                 "enum:",
	"maximum":              "maximum:",
	"minLength":            "minLength:",
	"minProperties":        "minProperties:",
	"minimum":              "minimum:",
	"pattern":              "pattern:",
	"required":             "required:",
	"type":                 "type:",
}

type provisioningResourceManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
	} `json:"reject"`
}

func TestProvisioningResourceCorporaAreFullyClaimedAndAgreeWithTheSchemas(t *testing.T) {
	root := testRoot(t)
	for _, name := range provisioningResourceSurfaces {
		name := name
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(root, "contracts/auth/v1/examples", name)
			raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			var m provisioningResourceManifest
			if err := json.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			if len(m.Accept) == 0 || len(m.Reject) == 0 {
				t.Fatalf("empty group: accept=%d reject=%d", len(m.Accept), len(m.Reject))
			}
			decode := func(file string) any {
				body, err := os.ReadFile(filepath.Join(dir, file))
				if err != nil {
					t.Fatal(err)
				}
				var doc any
				if err := json.Unmarshal(body, &doc); err != nil {
					t.Fatalf("%s: %v", file, err)
				}
				return doc
			}
			claimed := map[string]bool{"manifest.json": true}
			for _, e := range m.Accept {
				claimed[e.File] = true
				if err := Validate(root, name+".v1", decode(e.File)); err != nil {
					t.Errorf("accept %s refused: %v", e.File, err)
				}
			}
			for _, e := range m.Reject {
				claimed[e.File] = true
				token, ok := provisioningResourceKeywordTokens[e.ExpectKeyword]
				if !ok {
					t.Fatalf("no Go message token for keyword %q", e.ExpectKeyword)
				}
				err := Validate(root, name+".v1", decode(e.File))
				if err == nil || !strings.Contains(err.Error(), token) {
					t.Errorf("reject %s: err %v, want the %s keyword", e.File, err, e.ExpectKeyword)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if !claimed[entry.Name()] {
					t.Errorf("unclaimed fixture %s", entry.Name())
				}
			}
		})
	}
}

func schemaNode(t *testing.T, surface string, path ...string) any {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface+".v1"))
	if err != nil {
		t.Fatal(err)
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	for _, key := range path {
		object, ok := node.(map[string]any)
		if !ok {
			t.Fatalf("%s: %v is not an object", surface, path)
		}
		node = object[key]
	}
	return node
}

func stringList(t *testing.T, node any) []string {
	t.Helper()
	items, ok := node.([]any)
	if !ok {
		t.Fatalf("not a list: %v", node)
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.(string))
	}
	sort.Strings(out)
	return out
}

func equalLists(a, b []string) bool { return strings.Join(a, ",") == strings.Join(b, ",") }

// Every role field is the organization roles of the closed catalog (roles.v1).
func TestProvisioningResourceRolesAreTheCatalogOrganizationRoles(t *testing.T) {
	var want []string
	for _, role := range policycatalog.Default().Roles() {
		if role.Scope == policycatalog.ScopeOrganization {
			want = append(want, role.ID)
		}
	}
	sort.Strings(want)
	for _, surface := range []string{"membership", "membership-update-request", "invite", "invite-create-request"} {
		node := schemaNode(t, surface, "properties", "role")
		object := node.(map[string]any)
		var list any = object["enum"]
		if list == nil {
			list = object["anyOf"].([]any)[0].(map[string]any)["enum"]
		}
		if got := stringList(t, list); !equalLists(got, want) {
			t.Errorf("%s role = %v, catalog organization roles = %v", surface, got, want)
		}
	}
}

// membership.v1 status is the list of the memberships CHECK of the latest
// migration that defines it.
func TestMembershipStatusIsTheMigrationCheckList(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(testRoot(t), "internal/storage/postgres/authschema/migrations/*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("migrations: %v", err)
	}
	sort.Strings(files)
	re := regexp.MustCompile(`(?s)ADD CONSTRAINT memberships_status_check\s+CHECK \(status IN \(([^)]*)\)\)`)
	var list string
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if match := re.FindStringSubmatch(string(raw)); match != nil {
			list = match[1]
		}
	}
	if list == "" {
		t.Fatal("no migration defines memberships_status_check")
	}
	var want []string
	for _, item := range strings.Split(list, ",") {
		want = append(want, strings.Trim(strings.TrimSpace(item), "'"))
	}
	sort.Strings(want)
	if got := stringList(t, schemaNode(t, "membership", "properties", "status", "enum")); !equalLists(got, want) {
		t.Errorf("membership.v1 status = %v, memberships CHECK = %v", got, want)
	}
}

// Id prefixes of the resources are the ones of the provisioning vocabulary;
// inv_ is the invite's own (its aggregate joins the vocabulary with its first
// producer).
func TestProvisioningResourceIDPrefixes(t *testing.T) {
	prefix := func(aggregate string) string {
		a, ok := provisioning.AggregateOf(aggregate)
		if !ok {
			t.Fatalf("no %s aggregate in the provisioning vocabulary", aggregate)
		}
		return a.IDPrefix
	}
	pattern := func(surface, field string) string {
		return schemaNode(t, surface, "properties", field, "pattern").(string)
	}
	for _, c := range []struct{ surface, field, prefix string }{
		{"organization", "organization_id", prefix("organization")},
		{"membership", "membership_id", prefix("membership")},
		{"membership", "principal_id", prefix("principal")},
		{"invite", "invite_id", "inv_"},
		{"invite", "organization_id", prefix("organization")},
		{"invite", "invited_by", prefix("principal")},
	} {
		if got := pattern(c.surface, c.field); !strings.HasPrefix(got, "^"+c.prefix) {
			t.Errorf("%s.%s pattern %q does not start with ^%s", c.surface, c.field, got, c.prefix)
		}
	}
}

// The event spelling of a membership status and of a role id are rules, not
// accidents: invited <-> pending, and role_id = rol_ + the role key.
func TestMembershipEventSpellingRules(t *testing.T) {
	events := stringList(t, schemaNode(t, "provisioning-event", "properties", "payload", "properties", "change", "properties", "status", "enum"))
	if !contains(events, "pending") {
		t.Fatalf("provisioning-event status %v lacks pending", events)
	}
	if !contains(stringList(t, schemaNode(t, "membership", "properties", "status", "enum")), "invited") {
		t.Fatal("membership.v1 status lacks invited")
	}
	readme, err := os.ReadFile(filepath.Join(ContractsDir(testRoot(t)), "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range []string{"`invited` (event `pending`)", "`rol_` + the role key"} {
		if !strings.Contains(string(readme), rule) {
			t.Errorf("README does not state %q", rule)
		}
	}
	rolePattern := regexp.MustCompile(`^rol_[A-Za-z0-9_-]{1,64}$`)
	for _, role := range policycatalog.Default().Roles() {
		if !rolePattern.MatchString("rol_" + role.ID) {
			t.Errorf("rol_%s is not a valid provisioning role_id", role.ID)
		}
	}
}

func contains(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}
