// Package hotspots is the Go port of
// dev_health_ops.api.graphql.resolvers.complexity.resolve_hotspots
// (ops/src/dev_health_ops/api/graphql/resolvers/complexity.py), CHAOS-4352
// plan §6 Wave 3's second operation, ported after complexityTimeseries
// (CHAOS-4369, PR #1992).
//
// Ported deliberately verbatim, with ONE deliberate Go-only divergence
// (CHAOS-4684): the six value columns are selected from a SINGLE
// argMax over a tuple of all six, ordered by (day, computed_at), not
// Python's six independent argMax(<col>, computed_at) calls.
// computed_at is stamped once per compute RUN, not per row, so a single
// run can write the identical computed_at across dozens of different
// days; the bare-computed_at form's GROUP BY (repo_id, file_path) then
// ties every one of those days together and argMax picks an arbitrary
// row among them -- measured on real data: 686/11,953 groups ambiguous,
// 5 in the returned top-50, risk_score spread up to 31.096 on the ORDER
// BY key (CHAOS-4684). ClickHouse compares the tuple lexicographically,
// so this reads latest DAY first, then latest computed_at within that
// day -- the "latest-compute read" this doc comment already claimed but
// the bare form did not deliver. The tuple-of-all-six aggregation (not
// six independent argMax calls, which was this fix's own first attempt)
// is required for the row selection to be atomic: ClickHouse's argMax
// skips a NULL-valued Nullable argument when picking that column's own
// winning row, so six independent argMax calls can still return
// blame_concentration from an EARLIER day than every other column when
// the true latest day's blame_concentration is NULL (codex review round
// 1, EXECUTED against real ClickHouse). See fetchHotspotRows's own doc
// comment. Go-only per the standing ruling (no further Python
// graphql/metrics work); Python keeps the defect as expected divergence.
// This does not reduce the scan -- it costs more (~2x duration, ~3.5x
// memory on the measured dataset) because tuple comparison is pricier
// per row; correctness is the entire justification, not performance.
//
// Same optional repo_ids filter resolved through the org-scoped repos
// catalog (bounded to MAX_ROWS=1000 -- NOT the row limit -- before it
// becomes a bind parameter, exactly mirroring Python's
// `list(repo_ids)[:MAX_ROWS]`), same `ORDER BY risk_score DESC NULLS
// LAST, repo_id, file_path` at the database level (CHAOS-4472: the bare
// `risk_score DESC NULLS LAST` had no tie-break -- ClickHouse does not
// guarantee a stable row order/set among rows with equal risk_score,
// which a LIMIT sitting at a tie boundary can turn into a genuinely
// non-deterministic row SET, not just order, breaking the CHAOS-4381
// stage-2 comparator; `repo_id, file_path` is exactly this resolver's
// own GROUP BY key below, already a total order over the deduplicated
// row set, same fix shape as CHAOS-4421's reviewedges. Python fixed
// first, in its own PR (CHAOS-4472), per the epic's standing rule for a
// non-deterministic inherited ORDER BY -- this port picks up the
// already-fixed Python query rather than diverging from it), same
// 1..MAX_HOTSPOTS_ROWS(500) limit clamp with a default of 50, same
// best-effort repo-label join (falls back to the repo_id string when
// the repos catalog row is missing), and the same deterministic
// evidenceUrl deeplink built from file_path (no external service is
// queried).
//
// Deliberate divergence from complexitytimeseries's date handling,
// confirmed by reading resolve_hotspots line by line, not assumed:
// `since_day`/`until_day` are `input.since_utc.date()`/`input.until_utc.date()`
// -- NO `.astimezone(timezone.utc)` normalization first, unlike
// resolve_complexity_timeseries's `since_utc.astimezone(timezone.utc).date()`.
// This port reproduces that asymmetry exactly: the date is taken from
// whatever offset the input DateTime carries, not normalized to UTC
// first. See dateFromInput below.
//
// Side effects: none to replicate. Verified by reading resolve_hotspots/
// _fetch_hotspot_rows top to bottom: two read-only ClickHouse queries at
// most (hotspot fetch + label lookup) and a dataclass construction -- no
// telemetry/audit hook call inside it or anything it calls (same finding
// class as complexitytimeseries's, reviewedges's, and featureflags's doc
// comments).
//
// Missing-table behavior: unlike featureFlags, resolve_hotspots has NO
// try/except around its ClickHouse calls and HotspotsResult has no
// degradedReason field -- a missing file_hotspot_daily table is a real
// error on the Python side, not a degraded empty result. This port does
// not invent a degraded path Python doesn't have; a ClickHouse error
// propagates as a Go error (and, via schema.resolvers.go, a GraphQL
// error), matching Python's actual behavior exactly.
package hotspots

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// MaxHotspotsRows mirrors Python's MAX_HOTSPOTS_ROWS -- the hard cap on
// the row LIMIT.
const MaxHotspotsRows = 500

// DefaultLimit mirrors Python's DEFAULT_HOTSPOTS_LIMIT, used when the
// caller does not supply one.
const DefaultLimit = 50

// MaxRepoIDsBound mirrors Python's MAX_ROWS (complexity.py's shared
// module-level constant) as used specifically inside _fetch_hotspot_rows
// to bound the repoIds array -- a DIFFERENT constant than the row LIMIT
// (MaxHotspotsRows/DefaultLimit above), confirmed by reading the source:
// `bounded = list(repo_ids)[:MAX_ROWS]` uses the same MAX_ROWS=1000 the
// complexityTimeseries resolvers use for their own repoIds truncation,
// not MAX_HOTSPOTS_ROWS.
const MaxRepoIDsBound = 1000

// QueryClient is the read-only ClickHouse query boundary this package
// needs -- same shape as complexitytimeseries.QueryClient,
// reviewedges.QueryClient, and featureflags.QueryClient, declared
// locally per those packages' own doc comments.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// clampLimit bounds limit to [1, MaxHotspotsRows], mirroring Python's
// `max(1, min(raw_limit, MAX_HOTSPOTS_ROWS))`.
func clampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > MaxHotspotsRows {
		return MaxHotspotsRows
	}
	return limit
}

// effectiveLimit mirrors resolve_hotspots's
// `raw_limit = input.limit if input.limit is not None else DEFAULT_HOTSPOTS_LIMIT`.
func effectiveLimit(rawLimit *int) int {
	limit := DefaultLimit
	if rawLimit != nil {
		limit = *rawLimit
	}
	return clampLimit(limit)
}

// dateFromInput extracts the calendar date from t WITHOUT normalizing to
// UTC first -- see this package's doc comment for why this deliberately
// differs from complexitytimeseries's dateOnly helper. t retains
// whatever time.Location it was parsed with (gqlgen's graphql.Time
// UnmarshalTime preserves the parsed offset), and Go's t.Date() reads
// year/month/day in that same location -- the direct analogue of
// Python's naive `datetime.date()` call on an offset-aware datetime.
func dateFromInput(t time.Time) string {
	year, month, day := t.Date()
	return fmt.Sprintf("%04d-%02d-%02d", year, int(month), day)
}

// boundRepoIDs mirrors Python's `list(repo_ids)[:MAX_ROWS]` inside
// _fetch_hotspot_rows -- bounded to MaxRepoIDsBound, NOT the row limit.
func boundRepoIDs(repoIDs []string) []string {
	if len(repoIDs) <= MaxRepoIDsBound {
		return repoIDs
	}
	return repoIDs[:MaxRepoIDsBound]
}

// hotspotRow is one row of _fetch_hotspot_rows's result set, in SELECT
// column order.
type hotspotRow struct {
	repoID             string
	filePath           string
	churnLoc30d        uint64
	churnCommits30d    uint32
	cyclomaticTotal    uint32
	cyclomaticAvg      float64
	blameConcentration *float64
	riskScore          float64
}

// fetchHotspotRows ports _fetch_hotspot_rows, EXCEPT it selects all six
// value columns from a SINGLE argMax over a tuple of all six, ordered by
// (day, computed_at), rather than Python's six independent
// argMax(<col>, computed_at) calls (CHAOS-4684) -- see the package doc
// comment for why the ordering key changed.
//
// The tuple-of-all-six form is required, not a style choice: codex review
// (round 1) found and this package's own EXECUTED probe confirmed that six
// INDEPENDENT argMax(<col>, (day, computed_at)) calls still mix days when
// the winning day's blame_concentration is NULL. ClickHouse's argMax skips
// a NULL-valued Nullable argument when picking the row to return FOR THAT
// COLUMN ALONE -- so an independent argMax(blame_concentration, (day,
// computed_at)) can fall back to a strictly EARLIER day's non-null value
// even though every other column correctly resolves to the true latest
// day, silently reintroducing a mixed-day row through the one nullable
// column. Aggregating one tuple containing all six columns and extracting
// each field with tupleElement makes the row selection atomic: whichever
// row wins (day, computed_at) is the row every field comes from, NULLs
// included, because there is only one argMax call, not six.
func fetchHotspotRows(ctx context.Context, client QueryClient, orgID, sinceDay, untilDay string, repoIDs []string, limit int) ([]hotspotRow, error) {
	query := `
        SELECT
            repo_id,
            file_path,
            tupleElement(agg, 1) AS churn_loc_30d,
            tupleElement(agg, 2) AS churn_commits_30d,
            tupleElement(agg, 3) AS cyclomatic_total,
            tupleElement(agg, 4) AS cyclomatic_avg,
            tupleElement(agg, 5) AS blame_concentration,
            tupleElement(agg, 6) AS risk_score
        FROM (
            SELECT
                toString(repo_id) AS repo_id,
                file_path,
                argMax(
                    tuple(churn_loc_30d, churn_commits_30d, cyclomatic_total, cyclomatic_avg, blame_concentration, risk_score),
                    (day, computed_at)
                ) AS agg
            FROM file_hotspot_daily
            WHERE org_id = {org_id:String}
              AND day >= {since_day:Date}
              AND day <= {until_day:Date}`

	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "since_day", Value: sinceDay},
		{Name: "until_day", Value: untilDay},
	}

	if len(repoIDs) > 0 {
		bounded := boundRepoIDs(repoIDs)
		query += `
              AND repo_id IN (
                  SELECT id FROM repos
                  WHERE org_id = {org_id:String}
                    AND (repo IN {repo_ids:Array(String)} OR toString(id) IN {repo_ids:Array(String)})
              )`
		bindings = append(bindings, clickhouse.Binding{Name: "repo_ids", Value: bounded})
	}

	query += "\n            GROUP BY repo_id, file_path\n        )"
	query += fmt.Sprintf("\nORDER BY risk_score DESC NULLS LAST, repo_id, file_path\nLIMIT %d", limit)

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("hotspots: query: %w", err)
	}
	defer rows.Close()

	var out []hotspotRow
	for rows.Next() {
		var r hotspotRow
		if scanErr := rows.Scan(&r.repoID, &r.filePath, &r.churnLoc30d, &r.churnCommits30d, &r.cyclomaticTotal, &r.cyclomaticAvg, &r.blameConcentration, &r.riskScore); scanErr != nil {
			return nil, fmt.Errorf("hotspots: scan: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hotspots: rows: %w", err)
	}
	return out, nil
}

// loadRepoLabels ports _load_repo_labels verbatim -- byte-identical
// query/behavior to complexitytimeseries.loadRepoLabels, kept as a
// package-local copy per that package's own documented convention (each
// operation package is self-contained; see reviewedges.go's doc
// comment) rather than a shared helper.
func loadRepoLabels(ctx context.Context, client QueryClient, orgID string, repoIDs []string) (map[string]string, error) {
	if len(repoIDs) == 0 {
		return map[string]string{}, nil
	}
	query := `
        SELECT toString(id) AS repo_id, repo AS full_name
        FROM repos
        WHERE org_id = {org_id:String}
          AND toString(id) IN {repo_ids:Array(String)}`
	bindings := []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: repoIDs},
	}
	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("hotspots: repo labels query: %w", err)
	}
	defer rows.Close()

	labels := map[string]string{}
	for rows.Next() {
		var repoID, fullName string
		if scanErr := rows.Scan(&repoID, &fullName); scanErr != nil {
			return nil, fmt.Errorf("hotspots: repo labels scan: %w", scanErr)
		}
		if fullName == "" {
			fullName = repoID
		}
		labels[repoID] = fullName
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("hotspots: repo labels rows: %w", err)
	}
	return labels, nil
}

// distinctRepoIDs returns the distinct repo_id values seen in rows,
// mirroring Python's `list({str(r["repo_id"]) for r in rows})`.
func distinctRepoIDs(repoIDs []string) []string {
	seen := make(map[string]struct{}, len(repoIDs))
	out := make([]string, 0, len(repoIDs))
	for _, id := range repoIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// Resolve ports resolve_hotspots. orgID must already be the AUTHORIZED
// org (the caller's verified envelope claim) -- same "authorized org
// always wins" behavior complexitytimeseries.Resolve and
// reviewedges.Resolve document, reproduced here by construction (only
// one org parameter, never a caller-supplied org id). limit is clamped
// internally, same as Python -- callers must not pre-clamp.
func Resolve(ctx context.Context, client QueryClient, orgID string, sinceUtc, untilUtc time.Time, repoIDs []string, limit *int) (*model.HotspotsResult, error) {
	if client == nil {
		return nil, errors.New("hotspots: clickhouse client is required")
	}

	effLimit := effectiveLimit(limit)
	sinceDay := dateFromInput(sinceUtc)
	untilDay := dateFromInput(untilUtc)

	rawRows, err := fetchHotspotRows(ctx, client, orgID, sinceDay, untilDay, repoIDs, effLimit)
	if err != nil {
		return nil, err
	}

	seenRepoIDs := make([]string, 0, len(rawRows))
	for _, r := range rawRows {
		seenRepoIDs = append(seenRepoIDs, r.repoID)
	}
	labels, err := loadRepoLabels(ctx, client, orgID, distinctRepoIDs(seenRepoIDs))
	if err != nil {
		return nil, err
	}

	// Non-nil even with zero rows: the schema declares
	// rows: [HotspotRow!]! (non-null list), same "initialize explicitly"
	// convention featureflags.Resolve/reviewedges.Resolve document.
	rowsOut := []model.HotspotRow{}
	for _, r := range rawRows {
		repoName, ok := labels[r.repoID]
		if !ok {
			repoName = r.repoID
		}
		row := model.HotspotRow{
			FilePath:           r.filePath,
			RepoID:             r.repoID,
			RepoName:           repoName,
			ChurnLoc30d:        int(r.churnLoc30d),
			ChurnCommits30d:    int(r.churnCommits30d),
			CyclomaticTotal:    int(r.cyclomaticTotal),
			CyclomaticAvg:      r.cyclomaticAvg,
			BlameConcentration: r.blameConcentration,
			RiskScore:          r.riskScore,
		}
		// Deterministic deeplink -- never calls an external service.
		// Mirrors Python's `f"/code?file={quote(file_path)}" if
		// file_path else None` exactly -- see evidenceURL's doc comment
		// for why neither of Go's stdlib escapers (url.PathEscape,
		// url.QueryEscape) matches Python's quote() and a hand-rolled
		// escaper is used instead.
		if r.filePath != "" {
			u := evidenceURL(r.filePath)
			row.EvidenceURL = &u
		}
		rowsOut = append(rowsOut, row)
	}

	return &model.HotspotsResult{Rows: rowsOut}, nil
}

// evidenceURL ports Python's `f"/code?file={quote(file_path)}"` exactly.
// Verified empirically against the real interpreter (urllib.parse.quote
// with its default `safe="/"`): letters, digits, and `_.-~` are never
// escaped, "/" is left unescaped (the default safe set), and a space
// becomes "%20". Neither Go stdlib escaper matches this: url.PathEscape
// escapes "/" (it targets a single path SEGMENT, confirmed via
// `url.PathEscape("src/main.go") == "src%2Fmain.go"`), and
// url.QueryEscape both escapes "/" and encodes space as "+" instead of
// "%20". quotePython below is a direct byte-for-byte reimplementation of
// quote()'s default safe set rather than reaching for either.
func evidenceURL(filePath string) string {
	return "/code?file=" + quotePython(filePath)
}

// quotePythonSafe reports whether b is left unescaped by Python's
// urllib.parse.quote(s) with its default `safe="/"`: unreserved
// (letters, digits, `_.-~`) plus "/".
func quotePythonSafe(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z', b >= 'a' && b <= 'z', b >= '0' && b <= '9':
		return true
	case b == '_' || b == '.' || b == '-' || b == '~' || b == '/':
		return true
	default:
		return false
	}
}

// quotePython percent-encodes s exactly as Python's
// urllib.parse.quote(s) does with its default `safe="/"` -- operates on
// UTF-8 bytes (Python's quote() UTF-8-encodes the string before percent-
// encoding non-ASCII bytes, which is also what ranging over a Go string
// yields), uppercase hex digits, one "%XX" triplet per unsafe byte.
func quotePython(s string) string {
	const hex = "0123456789ABCDEF"
	var b []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quotePythonSafe(c) {
			b = append(b, c)
			continue
		}
		b = append(b, '%', hex[c>>4], hex[c&0x0f])
	}
	return string(b)
}
