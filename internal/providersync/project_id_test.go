package providersync

import (
	"encoding/json"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const (
	pidOrg     = "org-1"
	pidOtherOr = "70d529e0-aaaa-4bbb-8ccc-000000000001"
)

// TestProjectIDConstructorsAreByteEqualToTheHandBuiltForms pins that every
// constructor stores the exact string the hand-built form stored before the
// type existed (CHAOS-8886: behaviour must not change for a stored id).
func TestProjectIDConstructorsAreByteEqualToTheHandBuiltForms(t *testing.T) {
	for _, org := range []string{pidOrg, pidOtherOr} {
		for _, native := range []string{"10001", "42", "a1b2c3d4-0000-4000-8000-00000000000a", "7"} {
			cases := []struct {
				name string
				got  func() (ProjectID, bool)
				want string // the expression the producers used before the type
			}{
				{"jira native id", func() (ProjectID, bool) { return JiraProjectID(native) }, native},
				{"linear native id", func() (ProjectID, bool) { return LinearProjectID(native) }, native},
				{"gitlab catalog id", func() (ProjectID, bool) { return GitLabCatalogProjectID(org, native) }, org + ":gitlab:" + native},
				{"gitlab path (named exception)", func() (ProjectID, bool) { return GitLabPathOwnershipProjectID("grp/sub/" + native) }, "grp/sub/" + native},
				{"github repo (named exception)", func() (ProjectID, bool) { return GitHubRepoProjectID("Acme/" + native) }, "Acme/" + native},
				{"linear team key (named exception)", func() (ProjectID, bool) { return LinearTeamKeyProjectID(org, "ENG"+native) }, org + ":linear:" + "ENG" + native},
			}
			for _, tc := range cases {
				id, ok := tc.got()
				if !ok || id.String() != tc.want {
					t.Errorf("%s: org %q native %q: got %q ok=%v, want %q", tc.name, org, native, id.String(), ok, tc.want)
				}
			}
		}
	}
}

// TestProjectIDRefusesABlankInput pins that no constructor makes a usable id
// out of nothing, and that a zero id never reaches a batch.
func TestProjectIDRefusesABlankInput(t *testing.T) {
	for name, build := range map[string]func() (ProjectID, bool){
		"jira":                func() (ProjectID, bool) { return JiraProjectID(" ") },
		"linear":              func() (ProjectID, bool) { return LinearProjectID("") },
		"gitlab catalog":      func() (ProjectID, bool) { return GitLabCatalogProjectID(pidOrg, "\t") },
		"gitlab catalog org":  func() (ProjectID, bool) { return GitLabCatalogProjectID("", "42") },
		"gitlab path":         func() (ProjectID, bool) { return GitLabPathOwnershipProjectID("") },
		"github repo":         func() (ProjectID, bool) { return GitHubRepoProjectID("  ") },
		"linear team key":     func() (ProjectID, bool) { return LinearTeamKeyProjectID(pidOrg, "") },
		"linear team key org": func() (ProjectID, bool) { return LinearTeamKeyProjectID("", "ENG") },
	} {
		if id, ok := build(); ok || !id.IsZero() {
			t.Errorf("%s: a blank input gave %q ok=%v", name, id.String(), ok)
		}
	}
	if _, err := (ProjectID{}).Value(); !errors.Is(err, ErrInvalidProjectID) {
		t.Errorf("a zero ProjectID must not be written, Value error = %v", err)
	}
	id, _ := JiraProjectID("10001")
	if got, err := id.Value(); err != nil || got != "10001" {
		t.Errorf("Value = %v, %v, want 10001", got, err)
	}
}

// TestProjectIDKeepsItsWireAndStoredShape pins JSON (effects are stored as
// JSON between the collect and the write) and Scan (a stored id of any form is
// read back as it is).
func TestProjectIDKeepsItsWireAndStoredShape(t *testing.T) {
	type wire struct {
		ProjectID ProjectID `json:"project_id"`
	}
	id, _ := GitLabCatalogProjectID(pidOrg, "42")
	raw, err := json.Marshal(wire{ProjectID: id})
	if err != nil || string(raw) != `{"project_id":"org-1:gitlab:42"}` {
		t.Fatalf("marshal = %s, %v", raw, err)
	}
	var back wire
	if err := json.Unmarshal(raw, &back); err != nil || back.ProjectID != id {
		t.Fatalf("unmarshal = %+v, %v", back, err)
	}
	for _, src := range []any{"legacy:form", []byte("legacy:form")} {
		var scanned ProjectID
		if err := scanned.Scan(src); err != nil || scanned.String() != "legacy:form" {
			t.Errorf("Scan(%T) = %q, %v", src, scanned.String(), err)
		}
	}
	var scanned ProjectID
	if err := scanned.Scan(7); !errors.Is(err, ErrInvalidProjectID) {
		t.Errorf("Scan(int) error = %v, want ErrInvalidProjectID", err)
	}
}

// TestProjectIDProducersMatchTheHandBuiltFormsAcrossProviders runs the REAL
// normalizers of {jira, gitlab, github, linear} and compares the stored id
// with the expression that built it before the type existed.
func TestProjectIDProducersMatchTheHandBuiltFormsAcrossProviders(t *testing.T) {
	now := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)
	mustID := func(id ProjectID, ok bool) ProjectID {
		t.Helper()
		if !ok {
			t.Fatal("constructor refused its input")
		}
		return id
	}

	jiraID := mustID(JiraProjectID("10001"))
	if got := normalizeJiraProjectRow(pidOrg, jiraID, "OPS", "Ops", now).ID.String(); got != "10001" {
		t.Errorf("jira projects.id = %q, want the native id", got)
	}
	if got := normalizeJiraOwnershipRow(pidOrg, "jira:t1", jiraID, "OPS", now).ProjectID.String(); got != "10001" {
		t.Errorf("jira ownership project_id = %q, want the native id", got)
	}

	payload := gitlabTeamCatalogProjectPayload{ID: json.Number("42"), PathWithNamespace: "grp/api"}
	project, ok := normalizeGitLabProjectCatalogRow(pidOrg, payload, now)
	if !ok || project.ID.String() != pidOrg+":gitlab:42" {
		t.Errorf("gitlab projects.id = %q ok=%v, want %q", project.ID.String(), ok, pidOrg+":gitlab:42")
	}
	ownership, ok := normalizeGitLabOwnershipRow(pidOrg, "gl:grp", "grp/api", gitlabTeamCatalogBaseSpecificity, now)
	if !ok || ownership.ProjectID.String() != "grp/api" {
		t.Errorf("gitlab ownership project_id = %q ok=%v, want the path (named exception until CHAOS-8883)", ownership.ProjectID.String(), ok)
	}

	repoFact := OwnershipSnapshotRow{TeamID: "github:t", ProjectID: mustID(GitHubRepoProjectID("Acme/API")), Source: "x", ValidFrom: now}
	if repoFact.ProjectID.String() != "Acme/API" {
		t.Errorf("github ownership project_id = %q, want the repository full name", repoFact.ProjectID.String())
	}

	linearProject, err := normalizeLinearReferenceProject(
		Claim{Unit: Unit{OrgID: pidOrg, Provider: "linear"}}, linearReferenceProjectPayload{ID: "proj-uuid-1"}, now)
	if err != nil || linearProject.ID.String() != "proj-uuid-1" {
		t.Errorf("linear projects.id = %q, %v, want the Linear project id", linearProject.ID.String(), err)
	}
	teamKey := mustID(LinearTeamKeyProjectID(pidOrg, "ENG"))
	if teamKey.String() != pidOrg+":linear:ENG" {
		t.Errorf("linear team-key project_id = %q", teamKey.String())
	}
}

func goBuildMisuse(t *testing.T, dir string) (string, error) {
	t.Helper()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "build", "-o", "/dev/null", "./internal/providersync/testdata/projectid_misuse/"+dir)
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// TestProjectIDCannotBeBuiltFromAString compiles real misuse and requires the
// compiler to refuse it. The control case must compile, so a harness that
// fails for another reason (no go tool, a broken tree) is not read as a pass.
func TestProjectIDCannotBeBuiltFromAString(t *testing.T) {
	if testing.Short() {
		t.Skip("runs the go tool")
	}
	if out, err := goBuildMisuse(t, "control"); err != nil {
		t.Fatalf("the control case must compile (the harness is broken otherwise): %v\n%s", err, out)
	}
	for dir, want := range map[string]string{
		"convert":      "cannot convert",
		"literal":      "unexported field",
		"assign":       "cannot use",
		"ownershiprow": "cannot use",
	} {
		out, err := goBuildMisuse(t, dir)
		if err == nil {
			t.Errorf("%s compiled: a string became a ProjectID", dir)
			continue
		}
		if !strings.Contains(out, want) {
			t.Errorf("%s failed for another reason than the type (want %q):\n%s", dir, want, out)
		}
	}
}
