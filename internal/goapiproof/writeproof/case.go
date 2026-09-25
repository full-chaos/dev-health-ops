// Package writeproof is the single-plane WRITE proof form (CHAOS-6810): how a
// GraphQL MUTATION earns the `write_executed` receipt that alone authorizes
// enabling it.
//
// A mutation cannot be proven the way a query is (run on both planes and compare
// the answers): it would write twice. So a write proof is ONE execution of the
// deployed build inside the per-environment Fixture Org, plus the digest of
// everything that execution persisted (rows and River outbox payloads), compared
// with a baseline digest committed next to the case. That baseline is produced
// by a CI oracle that runs the real Python resolver and the real Go path once
// each on two database copies and compares the normalized effects; the live
// verb never runs the Python resolver. This package owns the parts both sides
// share, so they cannot disagree about what "the same effects" means: the Case
// shape, the Seeder that builds the dataset, the Normalizer that masks what
// legitimately differs between runs (generated ids, timestamps near now, the run
// tag), and the digest.
//
// Guarantees this package enforces rather than documents:
//
//   - The mutation is posted AT MOST ONCE per Execute call and is never retried,
//     whatever the transport says: a write that may have run is not run again.
//   - Nothing is proven from nothing: a case with no comparison tables, no
//     baseline, or an execution that persisted no rows at all is refused, because
//     a digest over an empty set matches any other empty set.
//   - Everything is scoped to one org and one RunTag; the seeder and the teardown
//     take both. Teardown runs only after a match. On any other outcome the
//     dataset is KEPT and named (Forensics) so the next run's cleanup or a human
//     can find it.
package writeproof

import (
	"context"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
)

// RunTag names one execution's rows: a Seeder writes it into everything it
// creates (a name, a label, a metadata field) and every Table query and
// Teardown filters by it, so one run can never read, or delete, another's data.
type RunTag string

// Seeder builds and removes one case's dataset inside a single org.
//
// Seed must create only rows that carry the RunTag or a fixed, case-declared id
// (Case.KeepIDs), and only inside org. Teardown must remove exactly what Seed
// created plus what the mutation persisted, addressed by org and RunTag.
type Seeder interface {
	Seed(ctx context.Context, db goapiproof.Querier, org string, run RunTag) error
	Teardown(ctx context.Context, db goapiproof.Querier, org string, run RunTag) error
}

// Table is one comparison read: a label and a SQL statement taking $1 = org id
// and $2 = run tag. The statement MUST order its rows by something that does not
// depend on a generated id (a name, a fixed id, a kind), so the digest is a
// function of the effects and not of insertion luck.
type Table struct {
	Label string
	SQL   string
}

// Case is one mutation's proof: what to send, what to read afterwards, and the
// committed digest the result must equal.
type Case struct {
	// Name identifies the case; it is part of the digest, so two cases can never
	// share one.
	Name string
	// Operation is the registered GraphQL operation the mutation belongs to (the
	// receipt's selected_operation).
	Operation string
	// VariablesJSON is sent VERBATIM as the request's `variables` value: raw
	// JSON text, never decoded and re-encoded (key order, repeated keys, lone
	// surrogates and big integers are exactly what a re-encoding would change).
	VariablesJSON string
	Seeder        Seeder
	Tables        []Table
	// KeepIDs are seeded ids that stay visible in the normalized effects (a
	// fixed fixture id is data, not noise). Every other UUID-shaped value is
	// masked and numbered by first appearance.
	KeepIDs []string
	// BaselineDigest is the committed digest of the CI oracle's run of this case.
	BaselineDigest string
}

// Validate refuses a case that could not prove anything.
func (c Case) Validate() error {
	switch {
	case strings.TrimSpace(c.Name) == "":
		return fmt.Errorf("writeproof: a case needs a name")
	case strings.TrimSpace(c.Operation) == "":
		return fmt.Errorf("writeproof: case %q names no operation", c.Name)
	case strings.TrimSpace(c.VariablesJSON) == "":
		return fmt.Errorf("writeproof: case %q sends no variables value (send {} explicitly)", c.Name)
	case c.Seeder == nil:
		return fmt.Errorf("writeproof: case %q has no seeder", c.Name)
	case len(c.Tables) == 0:
		return fmt.Errorf("writeproof: case %q reads no tables, so its digest would name nothing", c.Name)
	case goapiproof.NamesNothing(c.BaselineDigest):
		return fmt.Errorf("writeproof: case %q has no committed baseline digest: nothing to compare the execution with", c.Name)
	}
	seen := map[string]bool{}
	for _, table := range c.Tables {
		if strings.TrimSpace(table.Label) == "" || strings.TrimSpace(table.SQL) == "" {
			return fmt.Errorf("writeproof: case %q has a table with no label or no SQL", c.Name)
		}
		if seen[table.Label] {
			return fmt.Errorf("writeproof: case %q reads table label %q twice", c.Name, table.Label)
		}
		seen[table.Label] = true
	}
	return nil
}
