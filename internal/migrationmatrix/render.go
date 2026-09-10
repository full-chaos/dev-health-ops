package migrationmatrix

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Marker pairs bounding the two generated blocks in
// docs/go-migration-matrix.md. Same convention as the four blocks the Python
// generator already owns, so one drift-check habit covers all six.
const (
	FamilyBlockBegin = "<!-- BEGIN GENERATED MIGRATION STATUS V2 -->"
	FamilyBlockEnd   = "<!-- END GENERATED MIGRATION STATUS V2 -->"
	OpsBlockBegin    = "<!-- BEGIN GENERATED GO API OPERATIONS -->"
	OpsBlockEnd      = "<!-- END GENERATED GO API OPERATIONS -->"
)

// RenderFamilyBlock renders the per-family v2 table. The executing-language
// column comes from `families` (the Go-emitted authority); every other column
// comes from `ledger`. That split is deliberate: the one column that always
// read green is the one no human can type.
func RenderFamilyBlock(ledger *StatusLedger, families *NativeFamilies) string {
	var b strings.Builder

	// Every family runs in the same fleet, so the "how was this read"
	// string is the same for all 35 rows. Repeating it per row buried the
	// three cells a reader is here for under 200 characters of provenance;
	// it is stated once, above the table, and still validated per row.
	if sources := distinctDeployedSources(ledger); len(sources) > 0 {
		b.WriteString("_Deployed revisions read from: " + strings.Join(sources, "; ") + "._\n\n")
	}

	b.WriteString("| Family | Scope | Executes | Output parity | Deployed revision (read at) | Deployed-executed proof | Open regressions |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- |\n")
	for _, key := range families.Keys() {
		entry, ok := ledger.Families[key.ID()]
		if !ok {
			// Validate fails on this; render it visibly rather than
			// dropping the row, so a reader of a broken page still sees
			// that the family exists and has no status.
			b.WriteString(fmt.Sprintf("| `%s` | %s | %s | **NO STATUS ROW** | -- | -- | -- |\n",
				key.Name, key.Scope, strings.ToUpper(key.Executor)))
			continue
		}
		b.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s | %s | %s | %s |\n",
			key.Name,
			key.Scope,
			strings.ToUpper(key.Executor),
			renderParity(entry.Parity),
			renderDeployed(entry.Deployed),
			renderProof(entry.Proof),
			renderRegressions(entry.OpenRegressions),
		))
	}
	return b.String()
}

func renderParity(p Parity) string {
	switch p.Status {
	case ParityVerified:
		return fmt.Sprintf("VERIFIED (%s @ `%s`, %s)", p.Ticket, shortSha(p.Sha), p.Evidence)
	case ParityDiverged:
		return fmt.Sprintf("**DIVERGED** (%s, %s)", p.Ticket, p.Evidence)
	case ParityNotApplicable:
		return fmt.Sprintf("n/a (%s)", p.Evidence)
	default:
		return "UNVERIFIED"
	}
}

func renderDeployed(d Deployed) string {
	revision := "`" + shortSha(d.Revision) + "`"
	if d.Revision == UnknownRevision {
		// Rendered in bold precisely because it is the honest answer and
		// the uncomfortable one: the running fleet cannot say what commit
		// it is. A compose build that never received a COMMIT build-arg
		// labels itself "unknown", and no column may improve on that.
		revision = "**unknown**"
	}
	return fmt.Sprintf("%s (read %s)", revision, d.ReadAt.UTC().Format(time.RFC3339))
}

// distinctDeployedSources returns every distinct deployed.source in the
// ledger, sorted. More than one is not an error -- it means different
// families were last read by different means, which a reader should see.
func distinctDeployedSources(ledger *StatusLedger) []string {
	seen := map[string]bool{}
	for _, entry := range ledger.Families {
		if source := strings.TrimSpace(entry.Deployed.Source); source != "" {
			seen[source] = true
		}
	}
	out := make([]string, 0, len(seen))
	for source := range seen {
		out = append(out, source)
	}
	sort.Strings(out)
	return out
}

func renderProof(p Proof) string {
	if strings.TrimSpace(p.Ref) == NoProof {
		return "**none**"
	}
	if p.Note != "" {
		return fmt.Sprintf("`%s` (%s)", p.Ref, p.Note)
	}
	return "`" + p.Ref + "`"
}

func renderRegressions(tickets []string) string {
	if len(tickets) == 0 {
		return "--"
	}
	sorted := append([]string(nil), tickets...)
	sort.Strings(sorted)
	return "**" + strings.Join(sorted, ", ") + "**"
}

// RenderOpsBlock renders the per-operation table plus the provenance line.
// Every cell is machine-read; nothing here is hand-editable, which is the
// whole reason the block exists.
//
// catalog is the registered-operation catalog the edge dispatches by. It
// decides DOCUMENT_DRIFT: a live row whose (operation, document) pair the
// catalog does not name renders as such in the liveness cell and is
// counted on its own line, never as a serving row (opus r6, P2-2).
func RenderOpsBlock(render *Render, catalog Catalog) string {
	var b strings.Builder

	b.WriteString(fmt.Sprintf("_Rendered %s against main merge-base `%s`; SDL digest pin `%s`; fleet read %s via %s._\n\n",
		render.RenderedAt.UTC().Format(time.RFC3339),
		render.OpsSha,
		render.SchemaDigest,
		fleetReadStamp(render.FleetReadAt),
		render.FleetSource,
	))

	// The three numbers a reader should see before any row. They are put on
	// the page rather than left to be tallied by eye, because the tally is
	// exactly what nobody did: every "Go API is live" claim was read off a
	// mode column while these three said otherwise.
	b.WriteString(fmt.Sprintf("_Rows in `go_api_proof_run` at read time: **%d**. Operations reachable to real clients with no deployed-executed proof: **%d**. Rows whose mode says Go but whose schema digest no longer matches the pin, so every request silently falls back to Python: **%d**._\n\n",
		render.ProofRunTotal,
		UnprovenReachable(render.Operations, catalog),
		DeadReachable(render.Operations),
	))
	// Always printed, zero included: a count that appears only when it is
	// non-zero reads the same as a count nobody computed.
	b.WriteString(fmt.Sprintf("_Live rows serving a document the operation catalog does not name, so the edge cannot dispatch them (DOCUMENT_DRIFT, as `dev-hops go-api routing status` reports it): **%d**. Live rows with no recorded document digest, read before the reader carried it, so DOCUMENT_DRIFT cannot be judged for them: **%d**._\n\n",
		DriftedLive(render.Operations, catalog),
		UnjudgedLive(render.Operations),
	))

	b.WriteString("| Operation | Mode | Schema digest | Candidate build | Live at current pin | Proven (derived) | Parity ticket |\n")
	b.WriteString("| --- | --- | --- | --- | --- | --- | --- |\n")

	rows := append([]OperationRow(nil), render.Operations...)
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Live != rows[j].Live {
			return rows[i].Live // live rows first
		}
		if rows[i].SchemaDigest != rows[j].SchemaDigest {
			return rows[i].SchemaDigest < rows[j].SchemaDigest
		}
		if rows[i].Operation != rows[j].Operation {
			return rows[i].Operation < rows[j].Operation
		}
		// Two rows sharing (schema digest, operation) differ only by
		// document -- the DOCUMENT_DRIFT shape. Without this the order of
		// the pair was sort.Slice's, which is not stable, so the committed
		// page and -check's re-render could disagree on identical data.
		return rows[i].DocumentDigest < rows[j].DocumentDigest
	})

	for _, row := range rows {
		live := "**DEAD** (digest moved)"
		switch {
		case DocumentDrift(row, catalog):
			live = "**DOCUMENT_DRIFT** (serves document `" + shortHex(row.DocumentDigest) + "`, which the catalog does not name)"
		case row.Live:
			live = "yes"
		}
		proven := "**none**"
		if row.Proven != NoProof {
			proven = "`" + row.Proven + "`"
		}
		mode := row.Mode
		if row.Live && reachableModes[row.Mode] && row.Proven == NoProof && !DocumentDrift(row, catalog) {
			// The row a client can actually hit with nothing proving it.
			mode = row.Mode + " / **UNPROVEN**"
		}
		ticket := row.ParityTicket
		if ticket == "" {
			ticket = "--"
		}
		b.WriteString(fmt.Sprintf("| `%s` | %s | `%s` | `%s` | %s | %s | %s |\n",
			row.Operation, mode, shortDigest(row.SchemaDigest), shortSha(row.CandidateBuild), live, proven, ticket))
	}
	return b.String()
}

func fleetReadStamp(t time.Time) string {
	if t.IsZero() {
		return "(not read)"
	}
	return t.UTC().Format(time.RFC3339)
}

// shortHex shortens a bare hex document digest for a table cell. Anything
// that is not plain hex is printed whole, so a malformed value is never
// disguised as a real one by truncation.
func shortHex(s string) string {
	if len(s) > 12 && strings.Trim(s, "0123456789abcdef") == "" {
		return s[:12] + "…"
	}
	return s
}

func shortSha(s string) string {
	if len(s) > 12 && shaRe.MatchString(s) {
		return s[:12]
	}
	return s
}

func shortDigest(s string) string {
	if len(s) > 21 && digestRe.MatchString(s) {
		return s[:21] + "…"
	}
	return s
}

// ReplaceBlock swaps the content between begin/end markers, leaving the
// markers themselves in place. It is an error for a marker to be missing or
// out of order -- silently appending a block would let the page carry two
// copies of the same table.
func ReplaceBlock(doc, begin, end, body string) (string, error) {
	beginIdx := strings.Index(doc, begin)
	if beginIdx < 0 {
		return "", fmt.Errorf("marker %q not found in the document", begin)
	}
	endIdx := strings.Index(doc, end)
	if endIdx < 0 {
		return "", fmt.Errorf("marker %q not found in the document", end)
	}
	if endIdx < beginIdx {
		return "", fmt.Errorf("marker %q appears before %q", end, begin)
	}
	if strings.Contains(doc[beginIdx+len(begin):endIdx], begin) {
		return "", fmt.Errorf("marker %q appears twice", begin)
	}
	body = strings.TrimRight(body, "\n")
	return doc[:beginIdx+len(begin)] + "\n" + body + "\n" + doc[endIdx:], nil
}

// ExtractBlock returns the content between begin/end markers, exclusive of
// the markers and of the newline immediately after begin.
func ExtractBlock(doc, begin, end string) (string, error) {
	beginIdx := strings.Index(doc, begin)
	if beginIdx < 0 {
		return "", fmt.Errorf("marker %q not found in the document", begin)
	}
	endIdx := strings.Index(doc, end)
	if endIdx < 0 {
		return "", fmt.Errorf("marker %q not found in the document", end)
	}
	if endIdx < beginIdx {
		return "", fmt.Errorf("marker %q appears before %q", end, begin)
	}
	inner := doc[beginIdx+len(begin) : endIdx]
	return strings.Trim(inner, "\n"), nil
}
