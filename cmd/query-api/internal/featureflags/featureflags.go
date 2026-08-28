// Package featureflags is the Go port of
// dev_health_ops.api.graphql.resolvers.feature_flags.resolve_feature_flags
// (ops/src/dev_health_ops/api/graphql/resolvers/feature_flags.py:71),
// CHAOS-4352 plan §6 Wave 1's first canary operation.
//
// Ported deliberately verbatim -- same WHERE-clause construction, same
// argMax-over-(last_synced, environment) "latest row per (provider,
// project, flag)" selection (migration 075 made environment part of
// feature_flag's physical ReplacingMergeTree identity; the GraphQL
// contract stays one logical item per (provider, project, flag)), same
// ORDER BY, same LIMIT clamp, and the same missing-table (ClickHouse code
// 60 / UNKNOWN_TABLE) degraded-result path instead of raising.
//
// Side effects: this resolver has none to replicate. Verified by reading
// resolve_feature_flags top to bottom (plan §5 stage 2's requirement,
// not assumed): it runs two read-only ClickHouse queries and constructs a
// dataclass -- no telemetry/audit hook call (e.g. the
// record_stale_investment_membership_scope pattern the plan calls out for
// home/investment analytics) exists inside it or anything it calls.
package featureflags

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// FlagID ports work_graph/ids.py's generate_feature_flag_id verbatim:
// SHA256 hex digest of "flag:{org_id}/{provider}/{project_key}/{flag_key}".
// Must stay byte-for-byte identical to the Python side -- this id is a
// durable node identity used elsewhere in the Work Graph, not merely a
// display value this resolver invents.
func FlagID(orgID, provider, projectKey, flagKey string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("flag:%s/%s/%s/%s", orgID, provider, projectKey, flagKey)))
	return hex.EncodeToString(sum[:])
}

// NotMaterializedReason mirrors Python's FEATURE_FLAG_NOT_MATERIALIZED --
// the degraded-result reason surfaced when the feature_flag ClickHouse
// table does not exist yet for this environment (a fresh/unmigrated
// analytics DB), instead of the request failing.
const NotMaterializedReason = "FEATURE_FLAG_NOT_MATERIALIZED"

// maxLimit mirrors Python's FEATURE_FLAG_LIMIT_MAX.
const maxLimit = 1000

// unknownTableExceptionCode is ClickHouse's UNKNOWN_TABLE server exception
// code, mirroring feature_flags.py's `getattr(exc, "code", None) == 60`
// check (see also dev-health-go/clickhouse/budget_error.go for the same
// exception-code-not-message-text convention applied to a different code).
const unknownTableExceptionCode = 60

// unknownTableIdentifierRE mirrors work_graph.py's
// _UNKNOWN_TABLE_IDENTIFIER_RE exactly: ClickHouse's error text also
// echoes the entire failing SQL (which names every table in the query,
// including ones that DO exist), so a naive substring search for
// "feature_flag" would false-positive when a different table is the one
// actually missing. Only the dedicated "Unknown table ... identifier
// '<name>'" clause names the table ClickHouse could not find.
var unknownTableIdentifierRE = regexp.MustCompile(`(?i)Unknown table(?: expression identifier)?\s+'([^']+)'`)

// QueryClient is the read-only ClickHouse query boundary this package
// needs. Declared locally (the same shape as dev-health-go/readers'
// QueryClient) rather than imported from readers: this operation's query
// shape -- no "ids" list, a caller-supplied LIMIT -- does not fit
// readers.QueryOrgScopedNamed's id-keyed convention, and adding a
// feature_flag reader to the shared dev-health-go module is a separate,
// version-bump-owning change (the module's next tag, v0.4.0, is owned by
// the worker-migration orchestrator per the epic handoff, not this lane).
// *clickhouse.Client satisfies this interface directly -- "use its
// ClickHouse client for the read, do not hand-roll a second one" is about
// this Client/Query primitive, which this package uses as-is.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// clampLimit bounds a caller-supplied limit to a safe 1..maxLimit range,
// mirroring feature_flags.py's _clamp_limit: negative values error in
// ClickHouse and arbitrarily large values force expensive org-wide sorts.
func clampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > maxLimit {
		return maxLimit
	}
	return limit
}

// isoformatUTC reproduces Python's datetime.isoformat() as
// feature_flags.py's `_isoformat` actually observes it in production: the
// ClickHouse Python driver (clickhouse_connect) returns a NAIVE datetime
// for created_at/archived_at (no tzinfo attached, even though the column
// is DateTime64(3, 'UTC')) -- confirmed by the existing live-ClickHouse
// precedent test, tests/graphql/test_feature_flags_live.py, whose own
// assertion strips tzinfo before comparing
// (`(older + timedelta(minutes=1)).replace(tzinfo=None).isoformat()`).
// So the wire format carries NO "+00:00"/"Z" suffix at all -- only the
// date/time, with a fractional-second suffix omitted entirely when
// microsecond == 0 (Python's isoformat() never prints ".000000"), else
// exactly 6 digits. Go's time.Format has no equivalent conditional
// fractional-digit rule, so this cannot be a single fixed layout string.
// The value itself IS UTC (the column's declared timezone) -- `.UTC()`
// only normalizes the instant before formatting; it does not add the
// suffix Python's naive datetime never had. Getting this wrong is exactly
// the "client-visible scalar serialization" parity risk plan §4 names,
// and feature_flag's created_at/archived_at are plain GraphQL String
// fields (not the DateTime scalar), so nothing else in the stack
// normalizes it for us.
func isoformatUTC(t time.Time) string {
	t = t.UTC()
	out := t.Format("2006-01-02T15:04:05")
	if microsecond := t.Nanosecond() / 1000; microsecond != 0 {
		out += fmt.Sprintf(".%06d", microsecond)
	}
	return out
}

// isMissingFeatureFlagTable mirrors feature_flags.py's
// _is_missing_clickhouse_table_error(exc, "feature_flag"): true only when
// err is (or wraps) a ClickHouse UNKNOWN_TABLE (code 60) exception AND its
// dedicated "Unknown table ... identifier" clause names "feature_flag"
// specifically -- not merely mentions it somewhere in the echoed SQL.
func isMissingFeatureFlagTable(err error) bool {
	var exception *clickhousedriver.Exception
	if !errors.As(err, &exception) {
		return false
	}
	if exception.Code != unknownTableExceptionCode {
		return false
	}
	for _, match := range unknownTableIdentifierRE.FindAllStringSubmatch(exception.Message, -1) {
		name := match[1]
		if idx := lastIndexByte(name, '.'); idx >= 0 {
			name = name[idx+1:]
		}
		if name == "feature_flag" {
			return true
		}
	}
	return false
}

func lastIndexByte(s string, b byte) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func degradedResult(reason string) *model.FeatureFlagRegistryResult {
	r := reason
	return &model.FeatureFlagRegistryResult{
		Flags:          nil,
		TotalCount:     0,
		DegradedReason: &r,
	}
}

// flagWhere is the shared WHERE-clause + bindings builder for both the
// row query and the count query -- mirrors feature_flags.py building
// where_clauses/params once and reusing them for both statements.
func flagWhere(orgID string, provider, project *string, includeArchived bool) (string, []clickhouse.Binding) {
	clause := "org_id = {org_id:String}"
	bindings := []clickhouse.Binding{{Name: "org_id", Value: orgID}}
	if provider != nil {
		clause += " AND provider = {provider:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "provider", Value: *provider})
	}
	if project != nil {
		clause += " AND project_key = {project:String}"
		bindings = append(bindings, clickhouse.Binding{Name: "project", Value: *project})
	}
	if !includeArchived {
		clause += " AND archived_at IS NULL"
	}
	return clause, bindings
}

// Resolve ports resolve_feature_flags. limit is clamped internally, same
// as the Python side -- callers must not pre-clamp and must not trust the
// GraphQL schema's default alone (a client can send any value).
func Resolve(ctx context.Context, client QueryClient, orgID string, provider, project *string, includeArchived bool, limit int) (*model.FeatureFlagRegistryResult, error) {
	if client == nil {
		return nil, errors.New("featureflags: clickhouse client is required")
	}

	whereClause, bindings := flagWhere(orgID, provider, project, includeArchived)

	rowBindings := append(append([]clickhouse.Binding{}, bindings...), clickhouse.Binding{Name: "limit", Value: clampLimit(limit)})

	rowQuery := `SELECT
    provider,
    flag_key,
    project_key,
    tupleElement(latest, 1) AS flag_type,
    tupleElement(latest, 2) AS created_at,
    tupleElement(latest, 3) AS archived_at
FROM (
    SELECT
        provider,
        flag_key,
        project_key,
        argMax(tuple(flag_type, created_at, archived_at), tuple(last_synced, environment)) AS latest
    FROM feature_flag FINAL
    WHERE ` + whereClause + `
    GROUP BY provider, project_key, flag_key
)
ORDER BY provider, project_key, flag_key
LIMIT {limit:UInt64}`

	countQuery := `SELECT count() AS total
FROM (
    SELECT provider, project_key, flag_key
    FROM feature_flag FINAL
    WHERE ` + whereClause + `
    GROUP BY provider, project_key, flag_key
)`

	rows, err := client.Query(ctx, rowQuery, rowBindings)
	if err != nil {
		if isMissingFeatureFlagTable(err) {
			return degradedResult(NotMaterializedReason), nil
		}
		return nil, fmt.Errorf("featureflags: query: %w", err)
	}
	defer rows.Close()

	var flags []model.FeatureFlagItem
	for rows.Next() {
		var provider, flagKey, projectKey, flagType string
		var createdAt time.Time
		var archivedAt *time.Time
		if scanErr := rows.Scan(&provider, &flagKey, &projectKey, &flagType, &createdAt, &archivedAt); scanErr != nil {
			return nil, fmt.Errorf("featureflags: scan: %w", scanErr)
		}
		item := model.FeatureFlagItem{
			FlagID:     FlagID(orgID, provider, projectKey, flagKey),
			FlagKey:    flagKey,
			Provider:   provider,
			ProjectKey: projectKey,
			FlagType:   flagType,
			CreatedAt:  isoformatUTC(createdAt),
		}
		if archivedAt != nil {
			formatted := isoformatUTC(*archivedAt)
			item.ArchivedAt = &formatted
		}
		flags = append(flags, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("featureflags: rows: %w", err)
	}

	countRows, err := client.Query(ctx, countQuery, bindings)
	if err != nil {
		if isMissingFeatureFlagTable(err) {
			return degradedResult(NotMaterializedReason), nil
		}
		return nil, fmt.Errorf("featureflags: count query: %w", err)
	}
	defer countRows.Close()

	// ClickHouse's count() returns UInt64; the native driver REJECTS
	// scanning it into *int64 outright ("converting UInt64 to *int64 is
	// unsupported"), the same class of column-type-vs-Go-type mismatch
	// dev-health-go/readers' PullRequestStateRow.Number doc comment
	// describes for UInt32. Scan into uint64, convert to int (the
	// model's TotalCount field type) only after the value is safely in
	// Go.
	var total uint64
	if countRows.Next() {
		if scanErr := countRows.Scan(&total); scanErr != nil {
			return nil, fmt.Errorf("featureflags: count scan: %w", scanErr)
		}
	}
	if err := countRows.Err(); err != nil {
		return nil, fmt.Errorf("featureflags: count rows: %w", err)
	}

	return &model.FeatureFlagRegistryResult{
		Flags:      flags,
		TotalCount: int(total),
	}, nil
}
