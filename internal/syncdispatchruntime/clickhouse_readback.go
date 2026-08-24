package syncdispatchruntime

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ReadbackVerifier checks ClickHouse for rows a reference-discovery populate
// step claims to have written. Injected as an interface (mirrors
// DiscoveryExecutor's own reasoning) so ReferenceReadbackVerifier's
// poll-with-deadline loop is red-first testable without a real ClickHouse
// connection.
type ReadbackVerifier interface {
	MissingTeamKeys(ctx context.Context, orgID, provider string, expectedKeys []string) ([]string, error)
	MissingSprintIDs(ctx context.Context, orgID, provider string, expectedIDs []string) ([]string, error)
}

// ClickHouseReadbackVerifier ports reference_discovery.py's
// _missing_team_keys/_missing_sprint_ids verbatim: same argMax-per-natural-key
// dedup shape, same GROUP BY, same IN-list membership test. Both queries read
// tables the team-autoimport populators (still Python-side, CHAOS-4198) write
// to -- this verifier is a pure reader, it never writes.
type ClickHouseReadbackVerifier struct {
	conn driver.Conn
}

func NewClickHouseReadbackVerifier(conn driver.Conn) (*ClickHouseReadbackVerifier, error) {
	if conn == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	return &ClickHouseReadbackVerifier{conn: conn}, nil
}

func (verifier *ClickHouseReadbackVerifier) MissingTeamKeys(ctx context.Context, orgID, provider string, expectedKeys []string) ([]string, error) {
	if verifier == nil || verifier.conn == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	if len(expectedKeys) == 0 {
		return nil, nil
	}
	rows, err := verifier.conn.Query(ctx, `
SELECT native_team_key FROM (
    SELECT org_id, provider, native_team_key, argMax(id, updated_at) AS id
    FROM teams
    WHERE org_id = {org_id:String} AND provider = {provider:String}
      AND native_team_key IN {keys:Array(String)}
    GROUP BY org_id, provider, native_team_key
)`, namedClickHouseArguments(map[string]any{
		"org_id": orgID, "provider": provider, "keys": sortedUniqueStrings(expectedKeys),
	})...)
	if err != nil {
		return nil, fmt.Errorf("%w: query missing team keys: %v", ErrReferenceDiscoveryUnavailable, err)
	}
	defer func() { _ = rows.Close() }()
	visible := make(map[string]bool, len(expectedKeys))
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("%w: scan missing team keys: %v", ErrReferenceDiscoveryUnavailable, err)
		}
		visible[key] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate missing team keys: %v", ErrReferenceDiscoveryUnavailable, err)
	}
	return missingFrom(expectedKeys, visible), nil
}

func (verifier *ClickHouseReadbackVerifier) MissingSprintIDs(ctx context.Context, orgID, provider string, expectedIDs []string) ([]string, error) {
	if verifier == nil || verifier.conn == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	if len(expectedIDs) == 0 {
		return nil, nil
	}
	rows, err := verifier.conn.Query(ctx, `
SELECT sprint_id FROM (
    SELECT org_id, provider, sprint_id,
           argMax(name, last_synced) AS name,
           argMax(native_team_key, last_synced) AS native_team_key
    FROM sprints
    WHERE org_id = {org_id:String} AND provider = {provider:String}
      AND sprint_id IN {ids:Array(String)}
    GROUP BY org_id, provider, sprint_id
)`, namedClickHouseArguments(map[string]any{
		"org_id": orgID, "provider": provider, "ids": sortedUniqueStrings(expectedIDs),
	})...)
	if err != nil {
		return nil, fmt.Errorf("%w: query missing sprint ids: %v", ErrReferenceDiscoveryUnavailable, err)
	}
	defer func() { _ = rows.Close() }()
	visible := make(map[string]bool, len(expectedIDs))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%w: scan missing sprint ids: %v", ErrReferenceDiscoveryUnavailable, err)
		}
		visible[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate missing sprint ids: %v", ErrReferenceDiscoveryUnavailable, err)
	}
	return missingFrom(expectedIDs, visible), nil
}

// namedClickHouseArguments binds ClickHouse named parameters, matching the
// {name:Type} placeholders used above -- a package-local copy of
// internal/jobs/metrics/remaining's unexported namedArguments helper (that
// package does not export it, and eight lines is not worth a cross-package
// export for).
func namedClickHouseArguments(arguments map[string]any) []any {
	named := make([]any, 0, len(arguments))
	for name, value := range arguments {
		named = append(named, clickhouse.Named(name, value))
	}
	return named
}

// sortedUniqueStrings matches Python's `sorted(expected_keys)` over a set --
// ClickHouse query results and the SQL parameter itself don't depend on
// order, but deterministic argument order keeps this diffable against the
// Python query in review.
func sortedUniqueStrings(values []string) []string {
	unique := make(map[string]bool, len(values))
	for _, value := range values {
		unique[value] = true
	}
	sorted := make([]string, 0, len(unique))
	for value := range unique {
		sorted = append(sorted, value)
	}
	sort.Strings(sorted)
	return sorted
}

func missingFrom(expected []string, visible map[string]bool) []string {
	var missing []string
	for _, key := range sortedUniqueStrings(expected) {
		if !visible[key] {
			missing = append(missing, key)
		}
	}
	return missing
}

// stringsFromSummaryField ports reference_discovery.py's `_strings` helper:
// a summary field may be a single string, a list of strings, or absent --
// coerce whatever shape a real populator returned into a deduplicated,
// non-empty string set, matching Python's tolerance of non-string list
// elements (`str(item)`).
func stringsFromSummaryField(value any) []string {
	if value == nil {
		return nil
	}
	if text, ok := value.(string); ok {
		if text == "" {
			return nil
		}
		return []string{text}
	}
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	unique := make(map[string]bool, len(items))
	for _, item := range items {
		text := fmt.Sprint(item)
		if text != "" {
			unique[text] = true
		}
	}
	result := make([]string, 0, len(unique))
	for text := range unique {
		result = append(result, text)
	}
	sort.Strings(result)
	return result
}

// readbackTimeoutSeconds ports `_readback_timeout_seconds` verbatim:
// SYNC_REFERENCE_DISCOVERY_READBACK_SECONDS, default 5s, clamped to a 0.1s
// floor, falling back to the default on an unparseable value.
func readbackTimeoutSeconds() time.Duration {
	raw := strings.TrimSpace(os.Getenv("SYNC_REFERENCE_DISCOVERY_READBACK_SECONDS"))
	seconds := 5.0
	if raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil {
			seconds = parsed
		}
	}
	if seconds < 0.1 {
		seconds = 0.1
	}
	return time.Duration(seconds * float64(time.Second))
}

// ReferenceReadbackVerifier ports `_verify_reference_readback` verbatim: a
// no-op when the populate summary claimed nothing to verify (a non-import-
// capable provider's no-op discovery must not depend on ClickHouse at all),
// otherwise polls until every claimed key is visible or the deadline elapses.
type ReferenceReadbackVerifier struct {
	checker ReadbackVerifier
	timeout time.Duration
	now     func() time.Time
	sleep   func(time.Duration)
}

func NewReferenceReadbackVerifier(checker ReadbackVerifier) (*ReferenceReadbackVerifier, error) {
	if checker == nil {
		return nil, ErrReferenceDiscoveryUnavailable
	}
	return &ReferenceReadbackVerifier{
		checker: checker,
		timeout: readbackTimeoutSeconds(),
		now:     time.Now,
		sleep:   time.Sleep,
	}, nil
}

func (verifier *ReferenceReadbackVerifier) Verify(ctx context.Context, orgID, provider string, summary map[string]any) error {
	if verifier == nil || verifier.checker == nil {
		return ErrReferenceDiscoveryUnavailable
	}
	expectedTeamKeys := stringsFromSummaryField(summary["reference_team_keys"])
	expectedSprintIDs := stringsFromSummaryField(summary["reference_sprint_ids"])
	if len(expectedTeamKeys) == 0 && len(expectedSprintIDs) == 0 {
		return nil
	}
	deadline := verifier.now().Add(verifier.timeout)
	for {
		missingTeams, err := verifier.checker.MissingTeamKeys(ctx, orgID, provider, expectedTeamKeys)
		if err != nil {
			return err
		}
		missingSprints, err := verifier.checker.MissingSprintIDs(ctx, orgID, provider, expectedSprintIDs)
		if err != nil {
			return err
		}
		if len(missingTeams) == 0 && len(missingSprints) == 0 {
			return nil
		}
		if !verifier.now().Before(deadline) {
			return fmt.Errorf("reference discovery readback failed: missing_teams=%v missing_sprints=%v", missingTeams, missingSprints)
		}
		verifier.sleep(250 * time.Millisecond)
	}
}
