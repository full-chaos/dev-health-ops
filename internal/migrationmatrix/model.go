// Package migrationmatrix renders and validates the "can it say done?"
// columns of docs/go-migration-matrix.md.
//
// The page it feeds has one column today: which language executes a family.
// That column read 100% NATIVE for the whole six-week stretch in which the
// program was declared "done" twenty times and reversed nineteen -- because
// executing in Go is not the same claim as producing correct output, running
// at main, or having been observed to run at main. This package adds the
// other three as first-class, evidence-bearing columns, and refuses to render
// a status whose evidence cell is empty.
//
// Two hard rules shape every type here:
//
//  1. A status is never a promise. `deployed.revision` is whatever the running
//     image's org.opencontainers.image.revision label SAID at `read_at` -- and
//     on a local compose build that string is literally "unknown", which this
//     package renders verbatim rather than papering over.
//  2. Evidence is structural, not prose. A ticket, a 40-hex sha, a proof-run
//     id or an evidence path -- checkable shapes, so Validate can fail a row
//     that claims more than it can show.
package migrationmatrix

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// ParityStatus is the correctness claim for a family: does its Go executor
// produce the same output the retired Python producer did (or the frozen
// oracle it was replaced by)?
type ParityStatus string

const (
	// ParityVerified -- an A/B or regression run compared real output and
	// matched. Requires Ticket AND Sha AND Evidence.
	ParityVerified ParityStatus = "verified"
	// ParityDiverged -- a comparison ran and did NOT match. Requires
	// Ticket AND Evidence; the ticket is the open regression.
	ParityDiverged ParityStatus = "diverged"
	// ParityUnverified -- no comparison has been run against this family's
	// current executor. The honest default, and the one that must never be
	// silently upgraded by a status report.
	ParityUnverified ParityStatus = "unverified"
	// ParityNotApplicable -- there is no prior producer to compare against
	// (a family that was born native). Requires Evidence saying why.
	ParityNotApplicable ParityStatus = "n/a"
)

// UnknownRevision is the literal an image label carries when the build never
// received a COMMIT build-arg. Rendering it verbatim is the point: "unknown"
// is a true statement about the fleet, and a 40-hex sha we cannot read is a
// lie. Local compose builds produce exactly this today.
const UnknownRevision = "unknown"

// NoProof is the literal a proof cell carries when nothing has been recorded.
// Derived, never asserted: it is what a zero-row query returns, not a claim.
const NoProof = "none"

// Deployed is what the fleet was running at the moment it was read. Both
// fields are required together -- a revision with no read timestamp is not a
// fact about the fleet, it is a memory of one.
type Deployed struct {
	// Revision is the org.opencontainers.image.revision label value: a
	// 40-hex sha, or UnknownRevision.
	Revision string `json:"revision"`
	// ReadAt is when the label was read.
	ReadAt time.Time `json:"read_at"`
	// Source names what was read (e.g. "docker inspect dev-health-go-worker-1").
	Source string `json:"source"`
}

// Parity is the correctness claim plus the evidence that backs it.
type Parity struct {
	Status ParityStatus `json:"status"`
	// Ticket is the CHAOS id of the parity/regression work.
	Ticket string `json:"ticket,omitempty"`
	// Sha is the 40-hex ops commit the last A/B or regression run was made at.
	Sha string `json:"sha,omitempty"`
	// Evidence is a path or run id a reader can open.
	Evidence string `json:"evidence,omitempty"`
}

// Proof is the deployed-executed proof: a recorded run against the deployed
// build. A direct call, a 200, or a registry digest is not one.
type Proof struct {
	// Ref is a run id or evidence path, or NoProof.
	Ref string `json:"ref"`
	// Note optionally says what the run was.
	Note string `json:"note,omitempty"`
}

// FamilyStatus is one row of the per-family v2 table. The executing-language
// column is NOT stored here -- it is read from
// contracts/native-families/v1/native-families.json at render time, so this
// file can never disagree with what the worker actually wires.
//
// The row's SCOPE is not a field either: it is half of the map key. Family
// names are NOT unique across scopes -- `work_item_attribution` exists in
// both `daily` (the full daily attribution compute, CHAOS-5078) and
// `remaining` (a narrow staleness-window backstop, CHAOS-3092 PR-B). Those
// are two different executors with two different correctness claims, and a
// ledger keyed by bare name would silently collapse them into one row and
// report whichever was written last.
type FamilyStatus struct {
	Parity   Parity   `json:"parity"`
	Deployed Deployed `json:"deployed"`
	Proof    Proof    `json:"proof"`
	// OpenRegressions are CHAOS ids of defects observed in this family's
	// output after it went native. A non-empty list is the single most
	// important cell on the page: it is what "NATIVE" never showed.
	OpenRegressions []string `json:"open_regressions"`
}

// StatusLedger is contracts/migration-status/v1/status.json: the curated half
// of the matrix. Everything a human decides lives here; nothing a human
// decides lives in the rendered markdown.
type StatusLedger struct {
	SchemaVersion int    `json:"schema_version"`
	Note          string `json:"note"`
	// Families is keyed "<scope>/<family>" -- see FamilyStatus on why the
	// scope is load-bearing rather than decorative.
	Families map[string]FamilyStatus `json:"families"`
}

// OperationRow is one of the Go-API operations, rendered entirely from live
// sources. No cell here is hand-editable.
type OperationRow struct {
	Operation string `json:"operation"`
	// Mode is go_api_routing_state.mode: python|shadow|canary|primary|disabled.
	Mode string `json:"mode"`
	// SchemaDigest is the row's schema_digest. A row at a digest other than
	// the current pin is DEAD -- it can never be matched by the router.
	SchemaDigest string `json:"schema_digest"`
	// DocumentDigest is the registered document this row routes.
	//
	// Carried because two rows can share (schema_digest, operation) and
	// differ only here -- the DOCUMENT_DRIFT shape the Python status
	// surface names. Without it the two are indistinguishable in the
	// render and R8 reports a duplicate (opus r5, P2).
	DocumentDigest string `json:"document_digest"`
	// CandidateBuild is current_candidate_build.
	CandidateBuild string `json:"candidate_build"`
	// Live is false when SchemaDigest != the current SDL pin.
	Live bool `json:"live"`
	// Proven is DERIVED, never a column: the id of a proof run that
	// satisfies goapiproof.EnablementProofClause for this exact
	// (schema_digest, document_digest, operation, candidate_build) and for
	// THIS row's mode, or NoProof.
	//
	// Deliberately not restated here. This comment used to say
	// "deployed_executed/match", which stopped being the rule when
	// CHAOS-5484 admitted a fully-cited mismatch and split admission by
	// target mode -- and a comment describing a rule it does not own is
	// how the copy in live.go drifted unnoticed (codex r2, P1).
	Proven string `json:"proven"`
	// ParityTicket is the open parity ticket for this operation, if any.
	ParityTicket string `json:"parity_ticket,omitempty"`
}

// Render is contracts/migration-status/v1/last-render.json: the committed
// snapshot of the last live render. CI re-renders the doc from THIS file, not
// from a database -- so the check needs no Postgres, no docker, and no
// network, and the committed page always carries the timestamp and source
// shas of the reading it came from.
type Render struct {
	SchemaVersion int `json:"schema_version"`
	// RenderedAt is when -render ran.
	RenderedAt time.Time `json:"rendered_at"`
	// OpsSha is the MERGE-BASE with main at render time -- the last commit
	// on main that the render observed -- NOT the commit the render ran on.
	//
	// This distinction is load-bearing, and getting it wrong took main red.
	// The freshness gate checks ops_sha for ancestry of HEAD; when a branch
	// records its own tip here, that tip is squash-merged into a NEW commit
	// and the original is never reachable from main again. The check then
	// fails on main for every subsequent PR, permanently, by construction --
	// not because anything rotted. The merge-base survives the squash (it is
	// already on main), and it still bounds staleness, because a render made
	// from a long-stale branch has a correspondingly old merge-base.
	OpsSha string `json:"ops_sha"`
	// RenderCommit is the commit actually checked out when -render ran.
	// Recorded for the audit trail and NEVER ancestry-checked, precisely
	// because a branch commit legitimately stops existing after a squash.
	RenderCommit string `json:"render_commit,omitempty"`
	// SchemaDigest is the current contracts/graphql/v1/schema-digest.json pin.
	SchemaDigest string `json:"schema_digest"`
	// FleetReadAt is when the image labels were read (zero if not read).
	FleetReadAt time.Time `json:"fleet_read_at"`
	// FleetSource names how the fleet was read.
	FleetSource string `json:"fleet_source"`
	// Operations is every go_api_routing_state row, live and dead.
	Operations []OperationRow `json:"operations"`
	// ProofRunTotal is the total row count of go_api_proof_run at read time.
	// Rendered as a bare number because zero is the fact that matters.
	ProofRunTotal int `json:"proof_run_total"`
}

// LoadStatusLedger reads and shape-checks status.json.
func LoadStatusLedger(path string) (*StatusLedger, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return nil, fmt.Errorf("read status ledger: %w", err)
	}
	var ledger StatusLedger
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&ledger); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if ledger.SchemaVersion != 1 {
		return nil, fmt.Errorf("%s: schema_version %d, want 1", path, ledger.SchemaVersion)
	}
	return &ledger, nil
}

// LoadRender reads and shape-checks last-render.json.
func LoadRender(path string) (*Render, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return nil, fmt.Errorf("read render snapshot: %w", err)
	}
	var render Render
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&render); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if render.SchemaVersion != 1 {
		return nil, fmt.Errorf("%s: schema_version %d, want 1", path, render.SchemaVersion)
	}
	return &render, nil
}

// NativeFamilies is the executing-language authority,
// contracts/native-families/v1/native-families.json. Only the four family
// sections are read; the Go artifact test owns the file's correctness.
type NativeFamilies struct {
	Daily     map[string]string `json:"daily"`
	Finalize  map[string]string `json:"finalize"`
	Remaining map[string]string `json:"remaining"`
	Workgraph map[string]string `json:"workgraph"`
}

// Scopes returns the sections in the fixed render order.
func (n *NativeFamilies) Scopes() []struct {
	Name     string
	Families map[string]string
} {
	return []struct {
		Name     string
		Families map[string]string
	}{
		{"daily", n.Daily},
		{"finalize", n.Finalize},
		{"remaining", n.Remaining},
		{"workgraph", n.Workgraph},
	}
}

// Keys returns every family name across all four scopes, sorted, paired with
// its scope and executor verdict.
func (n *NativeFamilies) Keys() []FamilyKey {
	var out []FamilyKey
	for _, scope := range n.Scopes() {
		for name, executor := range scope.Families {
			out = append(out, FamilyKey{Name: name, Scope: scope.Name, Executor: executor})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return scopeOrder(out[i].Scope) < scopeOrder(out[j].Scope)
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// FamilyKey pairs a family with its scope and its executing-language verdict.
type FamilyKey struct {
	Name     string
	Scope    string
	Executor string
}

// ID is the ledger key: "<scope>/<family>". Unique where a bare family name
// is not.
func (k FamilyKey) ID() string { return k.Scope + "/" + k.Name }

func scopeOrder(scope string) int {
	switch scope {
	case "daily":
		return 0
	case "finalize":
		return 1
	case "remaining":
		return 2
	case "workgraph":
		return 3
	default:
		return 4
	}
}

// LoadNativeFamilies reads native-families.json's four family sections.
func LoadNativeFamilies(path string) (*NativeFamilies, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return nil, fmt.Errorf("read native families: %w", err)
	}
	// Not DisallowUnknownFields: that artifact carries schema_version and
	// generated_from, and it is owned by another test. We read four keys.
	var families NativeFamilies
	if err := json.Unmarshal(raw, &families); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(families.Daily) == 0 {
		return nil, fmt.Errorf("%s: no daily families", path)
	}
	return &families, nil
}

// Catalog is the registered-operation catalog,
// src/dev_health_ops/api/graphql/go_api_operations.json: the
// (operation, document digest) pairs the edge dispatches by. It is the SAME
// file `dev-hops go-api routing status` reports against.
//
// Why the matrix reads it (opus r6, P2-2). A routing row at the LIVE schema
// digest whose document the catalog does not name cannot be dispatched --
// the edge resolves a request to an operation THROUGH the catalog -- and
// `routing status` names that row DOCUMENT_DRIFT. This page had no way to:
// its only liveness test was `schema_digest == pin`, so the drifted row
// rendered "live, primary, proven" and -check passed, on the page whose
// stated reason for existing is that a silent death must not look like
// health. A reader of the page cannot tell drift from service without the
// pairs the edge actually dispatches by, so the page reads them.
type Catalog struct {
	documents map[string]map[string]bool // operation -> document digests
}

// Names reports whether the catalog registers exactly this pair.
func (c Catalog) Names(operation, documentDigest string) bool {
	return c.documents[operation][documentDigest]
}

// Documents lists the digests the catalog registers for operation, sorted.
func (c Catalog) Documents(operation string) []string {
	out := make([]string, 0, len(c.documents[operation]))
	for digest := range c.documents[operation] {
		out = append(out, digest)
	}
	sort.Strings(out)
	return out
}

// NewCatalog builds a Catalog from (operation, document digest) pairs.
// Refuses an empty operation or digest and a digest registered twice --
// the same refusals the Python loader (go_api_operation_catalog._load)
// makes, so the two readers of the file cannot accept different files.
func NewCatalog(pairs [][2]string) (Catalog, error) {
	catalog := Catalog{documents: map[string]map[string]bool{}}
	owner := map[string]string{}
	for _, pair := range pairs {
		operation, digest := pair[0], pair[1]
		if strings.TrimSpace(operation) == "" || strings.TrimSpace(digest) == "" {
			return Catalog{}, fmt.Errorf("catalog entry %q/%q: operation and digest must both be non-empty", operation, digest)
		}
		if previous, seen := owner[digest]; seen {
			return Catalog{}, fmt.Errorf("catalog registers digest %s twice (operations %q and %q)", digest, previous, operation)
		}
		owner[digest] = operation
		if catalog.documents[operation] == nil {
			catalog.documents[operation] = map[string]bool{}
		}
		catalog.documents[operation][digest] = true
	}
	if len(owner) == 0 {
		return Catalog{}, fmt.Errorf("catalog is empty: a page cannot judge DOCUMENT_DRIFT against a catalog that registers nothing")
	}
	return catalog, nil
}

// LoadCatalog reads go_api_operations.json.
func LoadCatalog(path string) (Catalog, error) {
	raw, err := os.ReadFile(path) //nolint:gosec // caller-supplied repo path
	if err != nil {
		return Catalog{}, fmt.Errorf("read operation catalog: %w", err)
	}
	var entries []struct {
		Operation string `json:"operation"`
		Digest    string `json:"digest"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&entries); err != nil {
		return Catalog{}, fmt.Errorf("parse %s: %w", path, err)
	}
	pairs := make([][2]string, 0, len(entries))
	for _, entry := range entries {
		pairs = append(pairs, [2]string{entry.Operation, entry.Digest})
	}
	catalog, err := NewCatalog(pairs)
	if err != nil {
		return Catalog{}, fmt.Errorf("%s: %w", path, err)
	}
	return catalog, nil
}
