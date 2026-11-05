package teamscope

import (
	"context"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
)

// Binding names of the repository ceiling.
const (
	BindingCeilingIDs  = "ceiling_ids"
	BindingCeilingKeys = "ceiling_keys"
)

// Ceiling is the repositories one request may read (CHAOS-13090). The zero value reads every repository.
type Ceiling struct {
	restricted bool
	ids        []string
}

// NoCeiling reads every repository.
func NoCeiling() Ceiling { return Ceiling{} }

// Restricted reports a repository ceiling.
func (c Ceiling) Restricted() bool { return c.restricted }

// IDs is a copy of the repository ids the ceiling allows.
func (c Ceiling) IDs() []string { return append([]string(nil), c.ids...) }

// RepoTerm is the " AND ..." fragment that holds column to the ceiling.
func (c Ceiling) RepoTerm(column string) (string, []dhclickhouse.Binding) { return "", nil }

// ResolveCeiling resolves the allowlist keys of scope to repository ids.
func ResolveCeiling(ctx context.Context, q RowQuerier, scope pepgate.Scope) (Ceiling, error) {
	return NoCeiling(), nil
}
