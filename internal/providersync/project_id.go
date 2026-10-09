package providersync

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ProjectID is the id a project is stored under: `projects.id` and
// `team_project_ownership.project_id`. It is a value of one of the forms
// below and nothing else. The field is unexported, so a bare string cannot
// become a ProjectID outside this file: every row type that persists a
// project id holds this type, and a sink refuses a zero value (Value).
//
// One constructor per form. The forms are:
//
//   - JiraProjectID: the project's native id, bare (CHAOS-8851). The Jira
//     catalog, the Jira work-items routes and the Atlassian Teams writer all
//     key one project the same way.
//   - LinearProjectID: the Linear project's own id, bare.
//   - GitLabCatalogProjectID: `{org}:gitlab:{native id}`, the id of the
//     GitLab catalog `projects` row.
//
// Three forms are NAMED EXCEPTIONS. Each one exists because a stored
// `team_project_ownership` row has an id that is not the project's identity
// form, and each is listed in projectIDNamedExceptions with its reason. A new
// exception is a deliberate edit of that table and of its census.
//
//   - GitLabPathOwnershipProjectID: GitLab ownership rows use the project
//     path as project_id (CHAOS-8883 moves them to the catalog form).
//   - GitHubRepoProjectID: GitHub has no project object. A team owns a
//     repository, and the repository full name stands in the project column.
//   - LinearTeamKeyProjectID: the `{org}:linear:{team key}` row that
//     team_repo_ownership_derivation reads (CHAOS-4458).
type ProjectID struct{ value string }

// ErrInvalidProjectID is the error of a project id that is empty or not
// storable.
var ErrInvalidProjectID = errors.New("providersync: invalid project id")

// projectIDNamedException records why a form is not the project's identity.
type projectIDNamedException struct {
	Constructor string
	Reason      string
}

// projectIDNamedExceptions is the closed list of non-identity forms. The
// census (project_id_census_test.go) fails when a constructor of an exception
// is called from a file that is not named here, or when this list and the
// constructors in this file differ.
var projectIDNamedExceptions = []projectIDNamedException{
	{"GitLabPathOwnershipProjectID", "GitLab ownership project_id is the project path until CHAOS-8883 moves it to the catalog form"},
	{"GitHubRepoProjectID", "GitHub has no project; ownership names the repository full name (repo-as-project, ruling D5742)"},
	{"LinearTeamKeyProjectID", "the {org}:linear:{team key} ownership row is read by team_repo_ownership_derivation (CHAOS-4458)"},
}

// nonBlank is the one check of every constructor. No constructor changes the
// text it is given: a caller that trims does so before, as it always did, so
// a stored id keeps its exact bytes.
func nonBlank(value string) bool { return strings.TrimSpace(value) != "" }

// JiraProjectID is the id of a Jira project: its native id, bare.
func JiraProjectID(nativeID string) (ProjectID, bool) {
	if !nonBlank(nativeID) {
		return ProjectID{}, false
	}
	return ProjectID{nativeID}, true
}

// LinearProjectID is the id of a Linear project: its own id, bare.
func LinearProjectID(nativeID string) (ProjectID, bool) {
	if !nonBlank(nativeID) {
		return ProjectID{}, false
	}
	return ProjectID{nativeID}, true
}

// GitLabCatalogProjectID is the id of the GitLab catalog `projects` row:
// `{org}:gitlab:{native id}`.
func GitLabCatalogProjectID(orgID, nativeID string) (ProjectID, bool) {
	if !nonBlank(nativeID) || orgID == "" {
		return ProjectID{}, false
	}
	return ProjectID{orgID + ":gitlab:" + nativeID}, true
}

// GitLabPathOwnershipProjectID is a named exception: the project path.
func GitLabPathOwnershipProjectID(projectPath string) (ProjectID, bool) {
	if !nonBlank(projectPath) {
		return ProjectID{}, false
	}
	return ProjectID{projectPath}, true
}

// GitHubRepoProjectID is a named exception: the repository full name.
func GitHubRepoProjectID(repoFullName string) (ProjectID, bool) {
	if !nonBlank(repoFullName) {
		return ProjectID{}, false
	}
	return ProjectID{repoFullName}, true
}

// LinearTeamKeyProjectID is a named exception: `{org}:linear:{team key}`.
func LinearTeamKeyProjectID(orgID, teamKey string) (ProjectID, bool) {
	if orgID == "" {
		return ProjectID{}, false
	}
	if !nonBlank(teamKey) {
		return ProjectID{}, false
	}
	return ProjectID{orgID + ":linear:" + teamKey}, true
}

// String is the stored form.
func (id ProjectID) String() string { return id.value }

// IsLinearTeamKeyForm reports whether the id is the {org}:linear:{team key}
// form of LinearTeamKeyProjectID for orgID. A Linear project's own id is
// never of that form.
func (id ProjectID) IsLinearTeamKeyForm(orgID string) bool {
	prefix := orgID + ":linear:"
	return orgID != "" && strings.HasPrefix(id.value, prefix) && nonBlank(id.value[len(prefix):])
}

// IsZero reports whether the id is unset.
func (id ProjectID) IsZero() bool { return id.value == "" }

// Value is the driver form: a sink that appends a ProjectID writes its string,
// and a zero id is an error before the batch is sent.
func (id ProjectID) Value() (driver.Value, error) {
	if id.value == "" {
		return nil, ErrInvalidProjectID
	}
	return id.value, nil
}

// Scan reads a stored project id back. A stored id may be of any form (rows
// of an older writer stay in the table), so this takes the string as it is.
func (id *ProjectID) Scan(src any) error {
	switch value := src.(type) {
	case string:
		id.value = value
	case []byte:
		id.value = string(value)
	default:
		return fmt.Errorf("%w: scan %T", ErrInvalidProjectID, src)
	}
	return nil
}

// MarshalJSON writes the stored string, so a row keeps its wire shape.
func (id ProjectID) MarshalJSON() ([]byte, error) { return json.Marshal(id.value) }

// UnmarshalJSON reads the stored string: a row decoded from an effect batch
// holds the id its producer constructed.
func (id *ProjectID) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	id.value = value
	return nil
}

// OracleString is how the oracle comparison encodes the id: as a string.
func (id ProjectID) OracleString() string { return id.value }
