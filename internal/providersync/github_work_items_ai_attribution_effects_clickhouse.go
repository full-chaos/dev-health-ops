package providersync

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// GitHubAIAttributionClickHouseAdapter mirrors write_ai_attribution
// (metrics/sinks/clickhouse/ai_attribution.py:103) and its _to_row projection.
//
// ai_attribution is the only work-item destination that is PARTITIONED
// (PARTITION BY toYYYYMM(observed_at)) and the only one whose partition
// expression is absent from its sorting key
// (org_id, provider, subject_type, repo_id, subject_id, source).
//
// That is the PR #1535 shape, and the obvious response -- fence the readback on
// toYYYYMM(observed_at) too -- was measured to be WRONG here. Measurement, on
// the container this suite uses, of two rows sharing a sorting key either side
// of a month boundary:
//
//	FINAL, default do_not_merge_across_partitions_select_final=0 -> 1 row
//	FINAL, do_not_merge_across_partitions_select_final=1         -> 2 rows
//	no FINAL                                                     -> 2 rows
//
// Under the default, FINAL merges ACROSS partitions: the sorting key has one
// current row, the greatest computed_at, and the older observed_at row is
// superseded rather than co-resident. Adding a partition predicate then filters
// that single winner out and the readback answers Absent for a row that was
// written and correctly superseded -- an endless replay, which is worse than
// the duplicate it was meant to prevent.
//
// Fencing the sorting key alone and letting found > 1 answer Conflict fixes the
// wrong answer but leaves the VERDICT settings-dependent: under
// do_not_merge_across_partitions_select_final=1 a correctly superseded row comes
// back as two rows and becomes a permanent Conflict, on a server knob this code
// does not control.
//
// So this readback does not use FINAL at all. It resolves the winning row
// explicitly with `ORDER BY computed_at DESC ... LIMIT 1 BY <sorting key>`,
// which reproduces the ReplacingMergeTree rule (greatest version wins) as a
// plain query. Measured on the same two-rows-across-a-month-boundary fixture:
//
//	LIMIT 1 BY, default knob -> 1 row (the later computed_at)
//	LIMIT 1 BY, knob = 1     -> 1 row (the later computed_at)
//	FINAL,      knob = 1     -> 2 rows
//
// LIMIT 1 BY rather than per-column argMax(col, computed_at) on purpose: it
// takes one whole ROW, so a tie on computed_at cannot blend columns from two
// different rows the way independent per-column argMax calls could. The
// record_id tiebreaker makes the choice deterministic when versions do tie.
//
// The other six direct destinations keep FINAL: they are unpartitioned, and the
// knob only governs cross-partition merging, so their verdicts are already
// settings-independent.
type GitHubAIAttributionClickHouseAdapter struct{ Conn driver.Conn }

const gitHubAIAttributionInsert = `INSERT INTO ai_attribution (record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at)`

const gitHubAIAttributionSelectBase = `SELECT record_id, org_id, provider, subject_type, subject_id, repo_id, kind, source, confidence, actor, evidence, observed_at, ingested_at, superseded_by, computed_at FROM ai_attribution WHERE org_id = ? AND provider = ? AND subject_type = ? AND subject_id = ? AND source = ?`

// gitHubAIAttributionSelectResolve collapses the matched rows to the single
// current one, replacing FINAL. It must be appended AFTER the repo_id clause.
const gitHubAIAttributionSelectResolve = ` ORDER BY computed_at DESC, record_id DESC LIMIT 1 BY org_id, provider, subject_type, repo_id, subject_id, source`

type aiAttributionStoredRow struct {
	RecordID     uuid.UUID
	OrgID        uuid.UUID
	Provider     string
	SubjectType  string
	SubjectID    string
	RepoID       *uuid.UUID
	Kind         string
	Source       string
	Confidence   float32
	Actor        *string
	Evidence     string
	ObservedAt   time.Time
	IngestedAt   time.Time
	SupersededBy *uuid.UUID
	ComputedAt   time.Time
}

// projectAIAttribution applies the coercions _to_row applies.
//
// One deliberate, declared divergence: Python stamps `computed_at` from
// wall-clock inside _to_row, which no retry can reproduce. computed_at is the
// ReplacingMergeTree version column, so a wall-clock stamp would make every
// replay of a recovery snapshot write a strictly newer version and the readback
// could never answer Exact for a replayed effect. Go stamps the unit's
// normalizedAt, which already reaches this adapter as IngestedAt. The column
// keeps its meaning (when this unit computed the row) and gains determinism.
//
// PRECONDITION -- SINGLE WRITER PER TABLE PER ORG, as for the other direct
// destinations: sound only while Python's writers are stopped before Go's
// start. See githubWorkItemRow.LastSynced for the full statement; the
// activation layer owns enforcement.
func projectAIAttribution(row githubAIAttributionRow) (aiAttributionStoredRow, error) {
	evidence, err := pythonEvidenceJSON(row.Evidence)
	if err != nil {
		return aiAttributionStoredRow{}, err
	}
	return aiAttributionStoredRow{
		RecordID: row.RecordID, OrgID: row.OrgID, Provider: row.Provider,
		SubjectType: row.SubjectType, SubjectID: row.SubjectID,
		RepoID: row.RepoID, Kind: row.Kind, Source: row.Source,
		Confidence: float32(row.Confidence), Actor: row.Actor,
		Evidence: evidence, ObservedAt: clickHouseMillis(row.ObservedAt),
		IngestedAt: clickHouseMillis(row.IngestedAt), SupersededBy: row.SupersededBy,
		ComputedAt: clickHouseMillis(row.IngestedAt),
	}, nil
}

func (adapter GitHubAIAttributionClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	rows, err := decodeEffectRows[githubAIAttributionRow](effect)
	if err != nil {
		return err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "ai_attribution", adapter.Conn, len(rows),
	); err != nil {
		return err
	}
	stored, err := projectAIAttributionRows(rows, identity)
	if err != nil {
		return err
	}
	if len(stored) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, gitHubAIAttributionInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range stored {
		if err := batch.Append(
			row.RecordID, row.OrgID, row.Provider, row.SubjectType, row.SubjectID,
			row.RepoID, row.Kind, row.Source, row.Confidence, row.Actor,
			row.Evidence, row.ObservedAt, row.IngestedAt, row.SupersededBy,
			row.ComputedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter GitHubAIAttributionClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[githubAIAttributionRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "ai_attribution", adapter.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	stored, err := projectAIAttributionRows(rows, identity)
	if err != nil {
		return EffectConflict, err
	}
	if len(stored) == 0 {
		return emptyEffectInspection(), nil
	}
	return foldWorkItemInspections(stored, func(expected aiAttributionStoredRow) (EffectInspection, error) {
		query := gitHubAIAttributionSelectBase
		arguments := []any{
			expected.OrgID, expected.Provider, expected.SubjectType,
			expected.SubjectID, expected.Source,
		}
		// repo_id is a Nullable key column (allow_nullable_key=1). `= NULL` is
		// never true, so a NULL repo_id needs the IS NULL form or the readback
		// would report a written row as absent and rewrite it forever.
		if expected.RepoID == nil {
			query += ` AND repo_id IS NULL`
		} else {
			query += ` AND repo_id = ?`
			arguments = append(arguments, *expected.RepoID)
		}
		query += gitHubAIAttributionSelectResolve
		result, err := adapter.Conn.Query(ctx, query, arguments...)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual aiAttributionStoredRow
		found := 0
		for result.Next() {
			if err := result.Scan(
				&actual.RecordID, &actual.OrgID, &actual.Provider,
				&actual.SubjectType, &actual.SubjectID, &actual.RepoID,
				&actual.Kind, &actual.Source, &actual.Confidence, &actual.Actor,
				&actual.Evidence, &actual.ObservedAt, &actual.IngestedAt,
				&actual.SupersededBy, &actual.ComputedAt,
			); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.ComputedAt, expected.ComputedAt); final {
			return verdict, nil
		}
		if actual.RecordID != expected.RecordID || actual.OrgID != expected.OrgID ||
			actual.Provider != expected.Provider ||
			actual.SubjectType != expected.SubjectType ||
			actual.SubjectID != expected.SubjectID ||
			!uuidPointersEqual(actual.RepoID, expected.RepoID) ||
			actual.Kind != expected.Kind || actual.Source != expected.Source ||
			actual.Confidence != expected.Confidence ||
			!stringPointersEqual(actual.Actor, expected.Actor) ||
			actual.Evidence != expected.Evidence ||
			!actual.ObservedAt.Equal(expected.ObservedAt) ||
			!actual.IngestedAt.Equal(expected.IngestedAt) ||
			!uuidPointersEqual(actual.SupersededBy, expected.SupersededBy) {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func projectAIAttributionRows(
	rows []githubAIAttributionRow,
	identity GitHubWorkItemEffectIdentity,
) ([]aiAttributionStoredRow, error) {
	// ai_attribution.org_id is a UUID column while the claim carries the tenant
	// as a string; compare through the parsed form so a differently-formatted
	// but equal UUID is not read as a cross-tenant row.
	tenant, err := uuid.Parse(identity.OrgID)
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	stored := make([]aiAttributionStoredRow, 0, len(rows))
	for _, row := range rows {
		if row.OrgID != tenant {
			return nil, ErrInvalidConfiguration
		}
		projected, err := projectAIAttribution(row)
		if err != nil {
			return nil, err
		}
		stored = append(stored, projected)
	}
	// Same-key collision inside one batch, same reasoning and same measured
	// keep-last rule as the other six destinations (see dedupeBySortingKey).
	// Reachable here whenever one pull request yields two signals from the same
	// source, since `source` is part of the key but the signal index is not.
	return dedupeBySortingKey(stored, aiAttributionSortingKey), nil
}

func aiAttributionSortingKey(row aiAttributionStoredRow) string {
	repoID := "null"
	if row.RepoID != nil {
		repoID = row.RepoID.String()
	}
	return strings.Join([]string{
		row.OrgID.String(), row.Provider, row.SubjectType, repoID,
		row.SubjectID, row.Source,
	}, workItemKeySeparator)
}

// -----------------------------------------------------------------------------
// evidence encoding
// -----------------------------------------------------------------------------

// pythonEvidenceKeyOrder pins the insertion order of every evidence shape the
// Python detectors build, because `json.dumps` serialises a dict in insertion
// order while Go's encoding/json sorts map keys. Two of these five shapes
// disagree with alphabetical order (bot_author and pr_body), so marshalling the
// map directly would store bytes Python never writes.
//
// Keyed by the sorted key set so lookup does not depend on Go's map ordering.
var pythonEvidenceKeyOrder = map[string][]string{
	"label":                                 {"label"},
	"app_slug|known_ai_bot|login|user_type": {"login", "user_type", "app_slug", "known_ai_bot"},
	"trailer_key|trailer_value":             {"trailer_key", "trailer_value"},
	"branch|matched_pattern":                {"branch", "matched_pattern"},
	"matched_pattern|matched_text":          {"matched_text", "matched_pattern"},
}

// pythonEvidenceJSON renders evidence exactly as
// `json.dumps(evidence, default=str)` does: insertion order, ", " and ": "
// separators, and ensure_ascii escaping.
//
// It fails closed on an unknown key set rather than falling back to sorted
// order. A silent fallback would store a subtly different string for a shape
// nobody had reviewed, and no test asserting decoded evidence could see it.
func pythonEvidenceJSON(evidence map[string]any) (string, error) {
	if evidence == nil {
		return "", ErrInvalidConfiguration
	}
	keys := make([]string, 0, len(evidence))
	for key := range evidence {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	order, known := pythonEvidenceKeyOrder[strings.Join(keys, "|")]
	if !known || len(order) != len(evidence) {
		return "", ErrInvalidConfiguration
	}
	var builder strings.Builder
	builder.WriteByte('{')
	for index, key := range order {
		value, present := evidence[key]
		if !present {
			return "", ErrInvalidConfiguration
		}
		if index > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(pythonJSONString(key))
		builder.WriteString(": ")
		encoded, err := pythonJSONValue(value)
		if err != nil {
			return "", err
		}
		builder.WriteString(encoded)
	}
	builder.WriteByte('}')
	return builder.String(), nil
}

// pythonJSONValue encodes the value types the evidence detectors actually
// produce. Anything else is refused: `default=str` would stringify it on the
// Python side in a form this code has not been shown to reproduce, and guessing
// is how a parity break gets stored.
func pythonJSONValue(value any) (string, error) {
	switch typed := value.(type) {
	case nil:
		return "null", nil
	case string:
		return pythonJSONString(typed), nil
	case *string:
		if typed == nil {
			return "null", nil
		}
		return pythonJSONString(*typed), nil
	case bool:
		if typed {
			return "true", nil
		}
		return "false", nil
	default:
		return "", ErrInvalidConfiguration
	}
}

// pythonJSONString mirrors CPython's json encoder for str: it escapes the two
// mandatory characters and the short-form control codes, emits \uXXXX for the
// remaining control codes, and — because ensure_ascii defaults to True —
// escapes every non-ASCII rune, using a surrogate pair above the BMP. It does
// NOT escape <, > or &, which Go's encoding/json would.
func pythonJSONString(value string) string {
	var builder strings.Builder
	builder.WriteByte('"')
	for _, runeValue := range value {
		switch runeValue {
		case '"':
			builder.WriteString(`\"`)
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		default:
			switch {
			case runeValue < 0x20:
				builder.WriteString(pythonJSONEscape(uint16(runeValue)))
			case runeValue < 0x7f:
				builder.WriteRune(runeValue)
			case runeValue <= 0xffff:
				builder.WriteString(pythonJSONEscape(uint16(runeValue)))
			default:
				high, low := utf16SurrogatePair(runeValue)
				builder.WriteString(pythonJSONEscape(high))
				builder.WriteString(pythonJSONEscape(low))
			}
		}
	}
	builder.WriteByte('"')
	return builder.String()
}

func pythonJSONEscape(value uint16) string {
	hex := strconv.FormatUint(uint64(value), 16)
	return `\u` + strings.Repeat("0", 4-len(hex)) + hex
}

func utf16SurrogatePair(value rune) (uint16, uint16) {
	value -= 0x10000
	return uint16(0xd800 + (value >> 10)), uint16(0xdc00 + (value & 0x3ff))
}

var _ GitHubWorkItemEffectAdapter = GitHubAIAttributionClickHouseAdapter{}
