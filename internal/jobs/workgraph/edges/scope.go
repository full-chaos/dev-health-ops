package edges

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment"
)

// nilOrganizationID is a syntactically valid UUID that names no tenant.
const nilOrganizationID = "00000000-0000-0000-0000-000000000000"

// requireEdgeScope refuses to touch work_item_dependencies or work_graph_edges
// without a real tenant.
//
// It defers to investment.RequireOrganizationScope for the shared contract --
// including its Python-faithful strip, which removes four separator code points
// Go's unicode.IsSpace does not -- and then adds ONE path-specific refusal that
// the shared guard cannot make on its own.
//
// # WHY THE NIL UUID NEEDS ITS OWN REFUSAL
//
// The shared guard asks whether a scope was SUPPLIED. The all-zero UUID passes
// that question: it is thirty-six non-blank characters and survives any strip.
// It is nonetheless not a tenant, and it is the value Go produces from a
// zero-valued uuid.UUID -- so it is exactly what an unset field, a failed parse
// whose error was dropped, or a struct built without its organization id all
// serialise to. Passing it is silent in a way an empty string is not.
//
// The two database paths fail differently under it, which is why refusing is
// worth a dedicated check rather than trusting the read to come back empty:
//
//   - The READ matches no rows and looks like a quiet org.
//   - The WRITE succeeds and stamps real derived edges with an org id that
//     names nobody. Those rows are then invisible to every scoped query and
//     untargetable by every scoped delete, so they accumulate with no owner --
//     the same end state as Python's empty-org write (gate 4), reached through
//     a value that passes every non-empty check.
//
// Refusing at the seam turns a silent, permanent data defect into a loud,
// recoverable one.
func requireEdgeScope(organizationID string) error {
	if err := investment.RequireOrganizationScope(organizationID); err != nil {
		return err
	}
	if parsed, err := uuid.Parse(organizationID); err == nil && parsed == uuid.Nil {
		return fmt.Errorf(
			"%w: organization id is the nil UUID, which names no tenant; edges "+
				"written under it are untargetable by every scoped query and delete",
			investment.ErrOrganizationScopeRequired,
		)
	}
	return nil
}
