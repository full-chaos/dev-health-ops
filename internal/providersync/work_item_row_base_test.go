package providersync

import (
	"encoding/json"
	"errors"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var (
	fullWorkItemSourceNow    = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	fullWorkItemSourceRepoID = uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666")
)

// fullWorkItemSource returns a non-zero value for every field.
type fullWorkItemSource struct {
	parent, epic *workItemRef
}

func (fullWorkItemSource) sourceKind() string                   { return "test_full" }
func (fullWorkItemSource) validateRow(Claim, workItemRow) error { return nil }
func (fullWorkItemSource) WorkItemID() string                   { return "jira:OPS-1" }
func (fullWorkItemSource) Provider() string                     { return "jira" }
func (fullWorkItemSource) Title() string                        { return "title" }
func (fullWorkItemSource) Type() string                         { return "story" }
func (fullWorkItemSource) Status() string                       { return "todo" }
func (fullWorkItemSource) StatusRaw() *string                   { return stringPointer("To Do") }
func (fullWorkItemSource) Description() *string                 { return stringPointer("body") }
func (fullWorkItemSource) RepoID() *uuid.UUID                   { return &fullWorkItemSourceRepoID }
func (fullWorkItemSource) NativeTeamKey() *string               { return stringPointer("ENG") }
func (fullWorkItemSource) ProjectKey() *string                  { return stringPointer("OPS") }
func (fullWorkItemSource) ProjectID() *string                   { return stringPointer("10000") }
func (fullWorkItemSource) ProjectName() *string                 { return stringPointer("Ops") }
func (fullWorkItemSource) Assignees() []string                  { return []string{"a@example.com"} }
func (fullWorkItemSource) Reporter() *string                    { return stringPointer("b@example.com") }
func (fullWorkItemSource) CreatedAt() time.Time                 { return fullWorkItemSourceNow }
func (fullWorkItemSource) UpdatedAt() time.Time                 { return fullWorkItemSourceNow }
func (fullWorkItemSource) StartedAt() *time.Time                { return &fullWorkItemSourceNow }
func (fullWorkItemSource) CompletedAt() *time.Time              { return &fullWorkItemSourceNow }
func (fullWorkItemSource) ClosedAt() *time.Time                 { return &fullWorkItemSourceNow }
func (fullWorkItemSource) Labels() []string                     { return []string{"bug"} }
func (fullWorkItemSource) StoryPoints() *float64                { points := 3.0; return &points }
func (fullWorkItemSource) SprintID() *string                    { return stringPointer("7") }
func (fullWorkItemSource) SprintName() *string                  { return stringPointer("S7") }
func (fullWorkItemSource) URL() *string                         { return stringPointer("https://example.test/OPS-1") }
func (fullWorkItemSource) PriorityRaw() *string                 { return stringPointer("High") }
func (fullWorkItemSource) ServiceClass() *string                { return stringPointer("standard") }
func (fullWorkItemSource) DueAt() *time.Time                    { return &fullWorkItemSourceNow }
func (s fullWorkItemSource) ParentID() workItemRef {
	if s.parent == nil {
		return workItemRefKey("P-1")
	}
	return *s.parent
}
func (s fullWorkItemSource) EpicID() workItemRef {
	if s.epic == nil {
		return workItemRefKey("E-1")
	}
	return *s.epic
}

// TestWorkItemHierarchyGrammar pins the one parent/epic grammar of the base:
// "<provider prefix>:<trimmed key>", and a blank or absent key is no reference.
func TestWorkItemHierarchyGrammar(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	exception := declaredWorkItemException(linearIssueSourceKind, workItemFieldEpicID)
	for name, tc := range map[string]struct {
		ref  workItemRef
		want string
	}{
		"key":            {workItemRefKey("OPS-9"), "jira:OPS-9"},
		"key with space": {workItemRefKey(" OPS-9\t"), "jira:OPS-9"},
		"blank key":      {workItemRefKey("  "), ""},
		"empty key":      {workItemRefKey(""), ""},
		"not present":    {workItemRef{Present: false, Key: ""}, ""},
		"nil pointer":    {workItemRefFromPointer(nil), ""},
		"pointer":        {workItemRefFromPointer(stringPointer("OPS-3")), "jira:OPS-3"},
		"exception":      {exception, ""},
	} {
		t.Run(name, func(t *testing.T) {
			ref := tc.ref
			row, err := buildWorkItemRow(claim, fullWorkItemSource{parent: &ref, epic: &ref}, fullWorkItemSourceNow)
			if err != nil {
				t.Fatal(err)
			}
			for field, got := range map[string]*string{"parent_id": row.ParentID, "epic_id": row.EpicID} {
				if valueOrNil(got) != map[bool]string{true: "<nil>", false: tc.want}[tc.want == ""] {
					t.Errorf("%s = %s, want %q", field, valueOrNil(got), tc.want)
				}
			}
		})
	}
}

func TestBuildWorkItemRowRejectsAnotherProviderOrOrg(t *testing.T) {
	if _, err := buildWorkItemRow(nativeTestClaim("linear", "work-items"), fullWorkItemSource{}, fullWorkItemSourceNow); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("provider mismatch: err=%v", err)
	}
	claim := nativeTestClaim("jira", "work-items")
	claim.OrgID = ""
	if _, err := buildWorkItemRow(claim, fullWorkItemSource{}, fullWorkItemSourceNow); !errors.Is(err, providerfoundation.ErrInvalidScope) {
		t.Fatalf("empty org: err=%v", err)
	}
	if _, err := buildWorkItemRow(nativeTestClaim("jira", "work-items"), fullWorkItemSource{}, time.Time{}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("zero normalizedAt: err=%v", err)
	}
}

func TestDeclaredWorkItemExceptionPanicsWhenUndeclared(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("an undeclared exception must panic, never store nothing silently")
		}
	}()
	declaredWorkItemException(jiraIssueSourceKind, workItemFieldParentID)
}

// workItemHierarchyProbe builds one row of an adapter kind through its real
// normalizer, from a payload that carries every hierarchy reference the
// provider has.
type workItemHierarchyProbe func(t *testing.T) workItemRow

func workItemHierarchyProbes() map[string]workItemHierarchyProbe {
	repoID := uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666")
	now := fullWorkItemSourceNow
	return map[string]workItemHierarchyProbe{
		jiraIssueSourceKind: func(t *testing.T) workItemRow {
			input := jiraWorkItemOraclePrepCases()[0].Input
			raw := input["raw_issue"].(map[string]any)
			fields := map[string]any{}
			for key, value := range raw["fields"].(map[string]any) {
				fields[key] = value
			}
			fields["parent"] = map[string]any{"key": "OPS-1"}
			fields["customfield_10014"] = "OPS-7"
			claim := nativeTestClaim("jira", "work-items")
			claim.OrgID = input["org_id"].(string)
			claim.DatasetOptions = map[string]any{"epic_link_field": "customfield_10014"}
			row, _, err := normalizeJiraWorkItem(claim,
				jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}, AtlassianShape: true},
				loadRealStatusMapping(t), jiraOracleIdentity, now)
			if err != nil {
				t.Fatal(err)
			}
			return row
		},
		linearIssueSourceKind: func(t *testing.T) workItemRow {
			var payload linearWorkItemPayload
			if err := json.Unmarshal([]byte(`{"identifier":"ENG-42","title":"Child","createdAt":"2026-10-01T00:00:00Z",
				"updatedAt":"2026-10-02T00:00:00Z","parent":{"identifier":"ENG-1"},"labels":{"nodes":[]},"history":{"nodes":[]}}`), &payload); err != nil {
				t.Fatal(err)
			}
			item, _, err := normalizeLinearWorkItem(nativeTestClaim("linear", "work-items"), payload, now)
			if err != nil {
				t.Fatal(err)
			}
			return workItemRow{Provider: item.Provider, ParentID: item.ParentID, EpicID: item.EpicID}
		},
		githubIssueSourceKind: func(t *testing.T) workItemRow {
			row, err := normalizeGitHubIssueWorkItem(nativeTestClaim("github", "work-items"), sameGitHubRepoNames("acme/api"), repoID,
				json.RawMessage(`{"number":7,"title":"Child","state":"open","created_at":"2026-10-01T00:00:00Z",
				"parent_issue_url":"https://api.github.com/repos/acme/api/issues/1","labels":[]}`), nil, now)
			if err != nil {
				t.Fatal(err)
			}
			return row
		},
		githubPullRequestSourceKind: func(t *testing.T) workItemRow {
			rows, err := normalizeGitHubPullRequestBundle(nativeTestClaim("github", "work-items"), sameGitHubRepoNames("acme/api"), repoID,
				json.RawMessage(`{"number":8,"title":"Change","state":"open","created_at":"2026-10-01T00:00:00Z","labels":[]}`),
				nil, nil, nil, nil, now)
			if err != nil || len(rows.WorkItems) != 1 {
				t.Fatalf("rows=%d err=%v", len(rows.WorkItems), err)
			}
			return rows.WorkItems[0]
		},
		githubProjectV2ItemSourceKind: func(t *testing.T) workItemRow {
			var payload gitHubProjectV2ItemPayload
			if err := json.Unmarshal([]byte(`{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
				"content":{"__typename":"Issue","id":"I_1","number":9,"title":"Board child","state":"OPEN",
				"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-02T00:00:00Z",
				"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
				"fieldValues":{"nodes":[]},"changes":{"nodes":[]}}`), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, kept, err := normalizeGitHubProjectV2Item(nativeTestClaim("github", "work-items"), payload, "ghprojv2:acme/1", nil, now)
			if err != nil || !kept {
				t.Fatalf("kept=%v err=%v", kept, err)
			}
			return row
		},
		gitlabIssueSourceKind: func(t *testing.T) workItemRow {
			var payload gitlabIssueWorkItemPayload
			if err := json.Unmarshal([]byte(`{"iid":42,"title":"Issue in an epic","state":"opened",
				"created_at":"2026-10-01T00:00:00Z","labels":[],"epic":{"id":900,"iid":4,"group_id":12}}`), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, err := normalizeGitLabIssueWorkItem(nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1", repoID, payload, nil,
				loadRealStatusMapping(t), nil, now)
			if err != nil {
				t.Fatal(err)
			}
			return row
		},
		gitlabMergeRequestSourceKind: func(t *testing.T) workItemRow {
			var payload gitlabMergeRequestWorkItemPayload
			if err := json.Unmarshal([]byte(`{"iid":5,"title":"MR","state":"opened","created_at":"2026-10-01T00:00:00Z","labels":[]}`), &payload); err != nil {
				t.Fatal(err)
			}
			row, _, _, err := normalizeGitLabMergeRequestWorkItem(nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1", repoID, payload, nil, nil, now)
			if err != nil {
				t.Fatal(err)
			}
			return row
		},
	}
}

var workItemHierarchyTicket = regexp.MustCompile(`^CHAOS-[0-9]+$`)

// TestWorkItemHierarchyMatrix is the census of {adapter kind} x {parent_id,
// epic_id}: each cell is either filled by the real normalizer from a payload
// that carries the reference, or a declared exception with its ticket under
// which the row stays empty. A stale exception (the adapter now reads the
// field) and a lost reading both fail here.
func TestWorkItemHierarchyMatrix(t *testing.T) {
	probes := workItemHierarchyProbes()
	kinds := make([]string, 0, len(probes))
	providers := map[string]bool{}
	for kind := range probes {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for kind := range workItemHierarchyExceptions {
		if probes[kind] == nil {
			t.Errorf("exception kind %s has no probe", kind)
		}
	}
	for _, kind := range kinds {
		row := probes[kind](t)
		providers[row.Provider] = true
		for field, value := range map[string]*string{workItemFieldParentID: row.ParentID, workItemFieldEpicID: row.EpicID} {
			exception, declared := workItemHierarchyExceptions[kind][field]
			switch {
			case declared && !workItemHierarchyTicket.MatchString(exception.Ticket):
				t.Errorf("%s/%s: exception ticket %q is not a CHAOS id", kind, field, exception.Ticket)
			case declared && strings.TrimSpace(exception.Reason) == "":
				t.Errorf("%s/%s: exception has no reason", kind, field)
			case declared && value != nil:
				t.Errorf("%s/%s: declared exception %s but the row holds %q: remove the exception", kind, field, exception.Ticket, *value)
			case !declared && value == nil:
				t.Errorf("%s/%s: no value and no declared exception", kind, field)
			case !declared && !strings.HasPrefix(*value, workItemRefPrefix[row.Provider]):
				t.Errorf("%s/%s = %q: not the provider grammar", kind, field, *value)
			}
		}
	}
	for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
		if _, ok := SelectWorkItemsRouteHandler(provider, "work-items", WorkItemsRouteDeps{StatusMapping: &StatusMapping{}}); !ok {
			t.Errorf("%s is no longer a work-items route: update the matrix", provider)
		}
		if !providers[provider] {
			t.Errorf("work-items route %s has no adapter kind in the matrix", provider)
		}
	}
}
