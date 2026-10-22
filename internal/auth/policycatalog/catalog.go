// Package policycatalog is the closed action and role catalog
// (actions.v1, roles.v1). Not implemented yet.
package policycatalog

import "errors"

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

type Catalog struct{}

func Load(actionsJSON, rolesJSON []byte) (*Catalog, error) { return nil, ErrInvalid }
func Default() *Catalog                                    { return &Catalog{} }
func (c *Catalog) Version() int                            { return 0 }
func (c *Catalog) Action(id string) (Action, error)        { return Action{}, nil }
func (c *Catalog) Role(id string) (Role, error)            { return Role{}, nil }
func (c *Catalog) Allows(role, action string) bool         { return true }
func (c *Catalog) Actions() []Action                       { return nil }
func (c *Catalog) Roles() []Role                           { return nil }

func decodeStrict(raw []byte, v any) error { return nil }
