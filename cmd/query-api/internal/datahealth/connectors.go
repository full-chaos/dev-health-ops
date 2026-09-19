package datahealth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// The connector read joins each active sync configuration with the newest run
// of any job attached to it. Rows come back ordered by provider, then name.
const connectorsSQL = `
SELECT c.provider, c.name, c.sync_targets, c.last_sync_at, c.last_sync_success,
       c.last_sync_error, c.last_sync_stats, c.updated_at,
       r.status, r.started_at, r.completed_at, r.result, r.error
FROM sync_configurations c
LEFT JOIN LATERAL (
    SELECT jr.status, jr.started_at, jr.completed_at, jr.result, jr.error
    FROM job_runs jr
    JOIN scheduled_jobs sj ON sj.id = jr.job_id AND sj.org_id = c.org_id
    WHERE sj.sync_config_id = c.id
    ORDER BY jr.created_at DESC
    LIMIT 1
) r ON TRUE
WHERE c.org_id = $1 AND c.is_active IS TRUE
ORDER BY c.provider, c.name`

// statsRowKeys are the keys a stats mapping may carry its row count under,
// in the order they are tried.
var statsRowKeys = []string{"rows_ingested", "rows", "items_synced", "items", "count"}

type connectorRow struct {
	provider        string
	name            string
	syncTargets     []byte
	lastSyncAt      *time.Time
	lastSyncSuccess *bool
	lastSyncError   *string
	lastSyncStats   []byte
	updatedAt       *time.Time

	hasRun      bool
	runStatus   *int32
	runStarted  *time.Time
	runComplete *time.Time
	runResult   []byte
	runError    *string
}

// Connectors ports resolve_connectors. Without a Postgres reader the section
// is empty; a failed read fails the field, and the log names the cause.
func (r *Reader) Connectors(ctx context.Context, orgID string) ([]model.ConnectorStatus, error) {
	if r.Postgres == nil {
		slog.ErrorContext(ctx, "query-api: data health connectors unavailable, no postgres reader",
			"operation", "dataHealth", "section", "connectors")
		return []model.ConnectorStatus{}, nil
	}
	rows, err := r.Postgres.Query(ctx, connectorsSQL, orgID)
	if err != nil {
		slog.ErrorContext(ctx, "query-api: data health connectors read failed",
			"operation", "dataHealth", "section", "connectors", "cause", pgErrorClass(err), "error", err)
		return nil, fmt.Errorf("data health connectors: %w", err)
	}
	defer rows.Close()

	statuses := []model.ConnectorStatus{}
	for rows.Next() {
		var c connectorRow
		var runStatus *int32
		var runStarted, runComplete *time.Time
		var runResult []byte
		var runError *string
		if err := rows.Scan(&c.provider, &c.name, &c.syncTargets, &c.lastSyncAt, &c.lastSyncSuccess,
			&c.lastSyncError, &c.lastSyncStats, &c.updatedAt,
			&runStatus, &runStarted, &runComplete, &runResult, &runError); err != nil {
			slog.ErrorContext(ctx, "query-api: data health connectors scan failed",
				"operation", "dataHealth", "section", "connectors", "cause", pgErrorClass(err), "error", err)
			return nil, fmt.Errorf("data health connectors scan: %w", err)
		}
		c.runStatus, c.runStarted, c.runComplete, c.runResult, c.runError = runStatus, runStarted, runComplete, runResult, runError
		// The lateral join yields all-NULL columns when a configuration has no
		// run; a real run always carries a status.
		c.hasRun = runStatus != nil
		statuses = append(statuses, r.connectorStatus(c))
	}
	if err := rows.Err(); err != nil {
		slog.ErrorContext(ctx, "query-api: data health connectors read failed",
			"operation", "dataHealth", "section", "connectors", "cause", pgErrorClass(err), "error", err)
		return nil, fmt.Errorf("data health connectors rows: %w", err)
	}
	return statuses, nil
}

func (r *Reader) connectorStatus(c connectorRow) model.ConnectorStatus {
	stats := jsonMapping(c.lastSyncStats)
	if len(stats) == 0 && c.hasRun {
		stats = jsonMapping(c.runResult)
	}
	lastSync := c.lastSyncAt
	if lastSync == nil && c.hasRun {
		lastSync = c.runComplete
	}
	return model.ConnectorStatus{
		Provider:     c.provider,
		Scope:        connectorScope(c),
		LastSyncAt:   utc(lastSync),
		RowsIngested: rowsIngested(stats),
		LastFailure:  r.connectorFailure(c),
	}
}

func connectorScope(c connectorRow) string {
	var targets []any
	if len(c.syncTargets) > 0 {
		if decoded, ok := decodeOrdered(string(c.syncTargets)); ok {
			targets, _ = decoded.([]any)
		}
	}
	if len(targets) == 0 {
		return c.name
	}
	if len(targets) > 3 {
		targets = targets[:3]
	}
	parts := make([]string, 0, len(targets))
	for _, target := range targets {
		parts = append(parts, pyStr(target))
	}
	return strings.Join(parts, ", ")
}

func (r *Reader) connectorFailure(c connectorRow) *model.ConnectorFailure {
	message := ""
	if c.lastSyncError != nil && *c.lastSyncError != "" {
		message = *c.lastSyncError
	} else if c.hasRun && c.runError != nil {
		message = *c.runError
	}
	failedRun := c.hasRun && c.runStatus != nil && (*c.runStatus == jobRunFailed || *c.runStatus == jobRunCancelled)
	lastSyncFailed := c.lastSyncSuccess != nil && !*c.lastSyncSuccess
	if message == "" && !lastSyncFailed && !failedRun {
		return nil
	}

	var occurred time.Time
	switch {
	case c.hasRun && c.runComplete != nil:
		occurred = *c.runComplete
	case c.hasRun && c.runStarted != nil:
		occurred = *c.runStarted
	case c.lastSyncAt != nil:
		occurred = *c.lastSyncAt
	case c.updatedAt != nil:
		occurred = *c.updatedAt
	default:
		occurred = r.now()
	}
	if message == "" {
		message = "Last sync failed"
	}

	var stage *string
	if c.hasRun {
		if value, ok := orderedLookup(c.runResult, "stage"); ok && truthy(value) {
			s := pyStr(value)
			stage = &s
		}
	}
	return &model.ConnectorFailure{OccurredAt: occurred.UTC(), Message: message, Stage: stage}
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

// jsonMapping decodes a JSON object; anything else is no mapping. Numbers stay
// json.Number so integers keep their text form.
func jsonMapping(raw []byte) map[string]any {
	if len(raw) == 0 {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil
	}
	// json.loads rejects anything after the first value.
	if _, err := decoder.Token(); err != io.EOF {
		return nil
	}
	mapping, _ := value.(map[string]any)
	return mapping
}

// rowsIngested ports _rows_ingested: the first of the known keys present in the
// stats decides the count, even when its value is not a number.
func rowsIngested(stats map[string]any) int {
	if len(stats) == 0 {
		return 0
	}
	for _, key := range statsRowKeys {
		if value, ok := stats[key]; ok {
			return pyInt(value)
		}
	}
	return 0
}

// orderedLookup reads one key of a stored JSON object, keeping the value's own
// key order for its repr.
func orderedLookup(raw []byte, key string) (any, bool) {
	decoded, ok := decodeOrdered(string(raw))
	if !ok {
		return nil, false
	}
	object, ok := decoded.(*orderedMap)
	if !ok {
		return nil, false
	}
	for i, k := range object.keys {
		if k == key {
			return object.vals[i], true
		}
	}
	return nil, false
}

// truthy is Python truthiness for a decoded JSON value.
func truthy(value any) bool {
	switch v := value.(type) {
	case nil:
		return false
	case bool:
		return v
	case string:
		return v != ""
	case json.Number:
		f, err := v.Float64()
		return err != nil || f != 0
	case []any:
		return len(v) > 0
	case map[string]any:
		return len(v) > 0
	case *orderedMap:
		return len(v.keys) > 0
	}
	return true
}

// pyInt ports _int: int(value or 0), with a failed conversion answering 0.
func pyInt(value any) int {
	if !truthy(value) {
		return 0
	}
	switch v := value.(type) {
	case bool:
		return 1
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return int(i)
		}
		if f, err := v.Float64(); err == nil {
			return int(f)
		}
		return 0
	case string:
		return pyIntString(v)
	}
	return 0
}

// pyIntString parses the way int(str) does: surrounding whitespace, an
// optional sign, and decimal digits of any script with single underscores
// between them.
func pyIntString(s string) int {
	s = pyStrip(s)
	if s == "" {
		return 0
	}
	sign := 1
	if s[0] == '+' || s[0] == '-' {
		if s[0] == '-' {
			sign = -1
		}
		s = s[1:]
	}
	runes := []rune(s)
	if len(runes) == 0 || runes[0] == '_' || runes[len(runes)-1] == '_' {
		return 0
	}
	n := 0
	previousUnderscore := false
	for _, r := range runes {
		if r == '_' {
			if previousUnderscore {
				return 0
			}
			previousUnderscore = true
			continue
		}
		previousUnderscore = false
		d, ok := digitValue(r)
		if !ok {
			return 0
		}
		if n > (math.MaxInt64-9)/10 {
			return 0
		}
		n = n*10 + d
	}
	return sign * n
}
