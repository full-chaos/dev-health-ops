// Package policycatalog is the closed action and role catalog
// (actions.v1, roles.v1). The embedded JSON files under
// contracts/auth/v1/vocabulary are the single source; they are parsed and
// validated at init. An action or role not in them is refused, no role holds
// every action, and platform and organization authority never mix.
package policycatalog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

const (
	ScopePlatform     = "platform"
	ScopeOrganization = "organization"
)

type Action struct {
	ID           string
	Scope        string
	ResourceKind string
}

type Role struct {
	ID      string
	Scope   string
	Actions []string
}

var (
	ErrUnknownAction = errors.New("policycatalog: unknown action")
	ErrUnknownRole   = errors.New("policycatalog: unknown role")
	ErrInvalid       = errors.New("policycatalog: invalid catalog")
)

var (
	actionPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	rolePattern   = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

type actionsFile struct {
	SchemaVersion  string `json:"schema_version"`
	CatalogVersion int    `json:"catalog_version"`
	Description    string `json:"description"`
	Actions        []struct {
		Action       string `json:"action"`
		Scope        string `json:"scope"`
		ResourceKind string `json:"resource_kind"`
	} `json:"actions"`
}

type rolesFile struct {
	SchemaVersion  string `json:"schema_version"`
	CatalogVersion int    `json:"catalog_version"`
	Description    string `json:"description"`
	Roles          []struct {
		Role    string   `json:"role"`
		Scope   string   `json:"scope"`
		Actions []string `json:"actions"`
	} `json:"roles"`
}

type Catalog struct {
	version int
	actions []Action
	roles   []Role
	byAct   map[string]Action
	byRole  map[string]Role
	grants  map[string]map[string]bool
}

// reservedRoles are names that stand for unbounded authority; no catalog may
// define them (roles.v1 schema carries the same list).
var reservedRoles = map[string]bool{"superuser": true, "root": true, "all": true, "any": true, "everyone": true}

func refuseDuplicateMembers(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	delim, ok := tok.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for dec.More() {
			kt, err := dec.Token()
			if err != nil {
				return err
			}
			key, _ := kt.(string)
			if seen[key] {
				return fmt.Errorf("duplicate object member %q", key)
			}
			seen[key] = true
			if err := refuseDuplicateMembers(dec); err != nil {
				return err
			}
		}
	case '[':
		for dec.More() {
			if err := refuseDuplicateMembers(dec); err != nil {
				return err
			}
		}
	}
	_, err = dec.Token()
	return err
}

func decodeStrict(raw []byte, v any) error {
	if err := refuseDuplicateMembers(json.NewDecoder(bytes.NewReader(raw))); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing data after the document")
	}
	return nil
}

func invalid(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalid, fmt.Sprintf(format, args...))
}

// Load parses and validates one catalog; any defect returns ErrInvalid.
func Load(actionsJSON, rolesJSON []byte) (*Catalog, error) {
	var af actionsFile
	if err := decodeStrict(actionsJSON, &af); err != nil {
		return nil, invalid("actions: %v", err)
	}
	var rf rolesFile
	if err := decodeStrict(rolesJSON, &rf); err != nil {
		return nil, invalid("roles: %v", err)
	}
	if af.SchemaVersion != "actions.v1" || rf.SchemaVersion != "roles.v1" {
		return nil, invalid("schema_version must be actions.v1 and roles.v1")
	}
	if af.CatalogVersion < 1 || af.CatalogVersion != rf.CatalogVersion {
		return nil, invalid("catalog_version must be equal and at least 1")
	}
	c := &Catalog{version: af.CatalogVersion, byAct: map[string]Action{}, byRole: map[string]Role{}, grants: map[string]map[string]bool{}}
	for _, a := range af.Actions {
		if !actionPattern.MatchString(a.Action) {
			return nil, invalid("action %q is not a dotted namespaced name", a.Action)
		}
		if !validScope(a.Scope) || !validScope(a.ResourceKind) {
			return nil, invalid("action %q has an unknown scope or resource_kind", a.Action)
		}
		if _, dup := c.byAct[a.Action]; dup {
			return nil, invalid("duplicate action %q", a.Action)
		}
		act := Action{ID: a.Action, Scope: a.Scope, ResourceKind: a.ResourceKind}
		c.byAct[a.Action] = act
		c.actions = append(c.actions, act)
	}
	if len(c.actions) == 0 {
		return nil, invalid("no actions")
	}
	for _, r := range rf.Roles {
		if !rolePattern.MatchString(r.Role) || reservedRoles[r.Role] {
			return nil, invalid("role %q is not a valid name", r.Role)
		}
		if !validScope(r.Scope) {
			return nil, invalid("role %q has an unknown scope", r.Role)
		}
		if _, dup := c.byRole[r.Role]; dup {
			return nil, invalid("duplicate role %q", r.Role)
		}
		held := map[string]bool{}
		for _, id := range r.Actions {
			a, ok := c.byAct[id]
			if !ok {
				return nil, invalid("role %q lists unknown action %q", r.Role, id)
			}
			if a.Scope != r.Scope {
				return nil, invalid("role %q (%s) lists %s action %q", r.Role, r.Scope, a.Scope, id)
			}
			if held[id] {
				return nil, invalid("role %q lists %q twice", r.Role, id)
			}
			held[id] = true
		}
		if len(held) >= len(c.actions) {
			return nil, invalid("role %q holds every action", r.Role)
		}
		role := Role{ID: r.Role, Scope: r.Scope, Actions: append([]string(nil), r.Actions...)}
		sort.Strings(role.Actions)
		c.byRole[r.Role] = role
		c.roles = append(c.roles, role)
		c.grants[r.Role] = held
	}
	if len(c.roles) == 0 {
		return nil, invalid("no roles")
	}
	return c, nil
}

func validScope(s string) bool { return s == ScopePlatform || s == ScopeOrganization }

var defaultCatalog = mustLoad()

func mustLoad() *Catalog {
	c, err := Load(vocabulary.ActionsV1, vocabulary.RolesV1)
	if err != nil {
		panic(err)
	}
	return c
}

// Default is the catalog built from the embedded vocabulary files.
func Default() *Catalog { return defaultCatalog }

func (c *Catalog) Version() int { return c.version }

func (c *Catalog) Action(id string) (Action, error) {
	a, ok := c.byAct[id]
	if !ok {
		return Action{}, ErrUnknownAction
	}
	return a, nil
}

func (c *Catalog) Role(id string) (Role, error) {
	r, ok := c.byRole[id]
	if !ok {
		return Role{}, ErrUnknownRole
	}
	r.Actions = append([]string(nil), r.Actions...)
	return r, nil
}

// Allows is true only for a known role that lists a known action.
func (c *Catalog) Allows(role, action string) bool { return c.grants[role][action] }

func (c *Catalog) Actions() []Action { return append([]Action(nil), c.actions...) }

func (c *Catalog) Roles() []Role {
	out := make([]Role, len(c.roles))
	for i, r := range c.roles {
		r.Actions = append([]string(nil), r.Actions...)
		out[i] = r
	}
	return out
}
