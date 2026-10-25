//go:build integration

package providersync

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// This file authors NO DDL. The three tables come from the real migration
// chain (internal/chmigrate/history), applied by chschema through
// the project's own canonical entrypoint.
//
// The previous hand-typed CREATE TABLE constants were a second, unversioned
// copy of the schema, and the SHOW CREATE TABLE test below could only ever
// confirm what those constants declared. They had already drifted: the
// work_item_team_attributions copy carried the PRE-053 enums, missing the
// `issue_project` and `manual_fallback` source values and the `manual` and
// `none` confidence values that the production resolver genuinely emits. The
// unassigned fixture in this file had been written as confidence "low" -- a
// value the resolver never produces -- which is exactly what kept the stale
// enum from failing anything.
//
// The engines remain the whole point of this file, and now they are the
// migrations' engines rather than our restatement of them:
//   - estimate coverage is ReplacingMergeTree PARTITION BY toYYYYMM(day)
//   - state durations is PLAIN MergeTree, also partitioned, where FINAL does
//     nothing at all
//   - team attributions is ReplacingMergeTree with NO PARTITION BY

const githubDerivedIntegrationOrg = "org-acme"

func githubDerivedIntegrationConn(t *testing.T, ctx context.Context) driver.Conn {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := instance.Close(closeContext); err != nil {
			t.Errorf("terminate ClickHouse: %v", err)
		}
	})
	// Migrate BEFORE opening the Go connection, so a failure to apply the real
	// chain surfaces as a migration failure rather than as a confusing "table
	// does not exist" from the first query.
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// githubWorkItemDerivedEmittableSources and ...Confidences are every value the
// production resolver can put in these two enum columns
// (github_work_items_derivation_context.go:327,404,462,491,510,561 and
// confidenceForPrimary). "low" is deliberately ABSENT: the resolver never emits
// it, and the fixture in this file that used to claim it was written to fit a
// stale hand-typed enum rather than to describe anything production produces.
var (
	githubWorkItemDerivedEmittableSources = []string{
		"native_team", "issue_project", "project_ownership", "repo_ownership",
		"assignee_membership", "linked_issue", "author_membership",
		"manual_fallback", "unassigned",
	}
	githubWorkItemDerivedEmittableConfidences = []string{
		"high", "medium", "manual", "none",
	}
)

// TestGitHubWorkItemTeamAttributionEnumsAcceptEveryResolverValue closes the
// drift class directly, rather than trusting that sourcing the schema from the
// migrations happens to keep it closed.
//
// The hand-typed DDL this file used to create carried the PRE-053 enums, so
// `issue_project`, `manual_fallback`, `manual` and `none` -- all genuinely
// reachable resolver outputs -- would have been REJECTED with
// UNKNOWN_ELEMENT_OF_ENUM by the real column. Nothing failed, because no
// fixture ever used one.
//
// This asserts against the enum the MIGRATION CHAIN produced, and the value
// list comes from the producer, so it fails if either side moves: a new
// resolver source without a migration, or a migration that narrows an enum.
func TestGitHubWorkItemTeamAttributionEnumsAcceptEveryResolverValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)

	for _, tt := range []struct {
		column string
		values []string
	}{
		{"source", githubWorkItemDerivedEmittableSources},
		{"confidence", githubWorkItemDerivedEmittableConfidences},
	} {
		t.Run(tt.column, func(t *testing.T) {
			var columnType string
			if err := conn.QueryRow(ctx,
				"SELECT type FROM system.columns WHERE table = 'work_item_team_attributions' "+
					"AND name = ? AND database = currentDatabase()", tt.column,
			).Scan(&columnType); err != nil {
				t.Fatal(err)
			}
			if !strings.HasPrefix(columnType, "Enum") {
				t.Fatalf("%s is %s, not an Enum -- this guard assumes an enum and would "+
					"otherwise pass vacuously", tt.column, columnType)
			}
			for _, value := range tt.values {
				// Match the quoted enum element, not a bare substring: 'none'
				// must not be satisfied by a hypothetical 'none_given'.
				if !strings.Contains(columnType, "'"+value+"'") {
					t.Errorf("the resolver can emit %s=%q but the migrated column cannot "+
						"store it, so a real row would be rejected with "+
						"UNKNOWN_ELEMENT_OF_ENUM\ncolumn type: %s",
						tt.column, value, columnType)
				}
			}
		})
	}
}

var githubWorkItemDerivedSourceEnumElement = regexp.MustCompile(`'([^']+)'\s*=\s*(\d+)`)

// githubWorkItemDerivedParseEnum8Codes parses a ClickHouse
// `Enum8('name' = code, ...)` type string into name->code. It fails the test
// outright on a malformed type rather than returning a partial map, so a
// parsing gap can never masquerade as a passing code-mapping assertion.
func githubWorkItemDerivedParseEnum8Codes(t *testing.T, columnType string) map[string]int {
	t.Helper()
	matches := githubWorkItemDerivedSourceEnumElement.FindAllStringSubmatch(columnType, -1)
	if len(matches) == 0 {
		t.Fatalf("no Enum8 elements parsed from column type: %s", columnType)
	}
	codes := make(map[string]int, len(matches))
	for _, match := range matches {
		code, err := strconv.Atoi(match[2])
		if err != nil {
			t.Fatalf("non-numeric enum code %q in column type: %s", match[2], columnType)
		}
		codes[match[1]] = code
	}
	return codes
}

// TestGitHubWorkItemTeamAttributionSourceEnumCodesAreAppendedNotRenumbered
// pins the exact `source` Enum8 code for every value across migrations
// 051/053/078 (CHAOS-4244), not just that each name is PRESENT (the sibling
// test above). Enum8 storage codes are insertion order, NOT precedence --
// team-attribution.md s0 warns readers not to read them as the ladder -- but
// that only holds if migrations only ever APPEND a new code, never renumber
// an existing one. A migration that renumbers (e.g. slotting
// `author_membership` in the MIDDLE of the existing codes instead of
// appending it as 9) would silently reinterpret every already-written row's
// stored source. This test fails on exactly that class of migration bug,
// independent of `_SOURCE_ORDER` (which decides PRECEDENCE, a separate
// concern this test does not touch).
func TestGitHubWorkItemTeamAttributionSourceEnumCodesAreAppendedNotRenumbered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)

	var columnType string
	if err := conn.QueryRow(ctx,
		"SELECT type FROM system.columns WHERE table = 'work_item_team_attributions' "+
			"AND name = 'source' AND database = currentDatabase()",
	).Scan(&columnType); err != nil {
		t.Fatal(err)
	}
	got := githubWorkItemDerivedParseEnum8Codes(t, columnType)
	want := map[string]int{
		"native_team":         1,
		"linked_issue":        2,
		"project_ownership":   3,
		"repo_ownership":      4,
		"assignee_membership": 5,
		"unassigned":          6,
		"issue_project":       7,
		"manual_fallback":     8,
		"author_membership":   9,
	}
	for name, wantCode := range want {
		gotCode, ok := got[name]
		if !ok {
			t.Errorf("enum is missing %q entirely: %s", name, columnType)
			continue
		}
		if gotCode != wantCode {
			t.Errorf("%q stored as code %d, want %d (a migration renumbered an "+
				"existing code instead of appending) -- column type: %s",
				name, gotCode, wantCode, columnType)
		}
	}
	if len(got) != len(want) {
		t.Errorf("enum has %d elements, want exactly %d (an untracked value was "+
			"added or removed) -- got=%v want=%v", len(got), len(want), got, want)
	}
}

// githubDerivedSortingKeyColumns extracts the sorting key's top-level entries
// from a SHOW CREATE TABLE body.
//
// A substring test is NOT good enough here: `strings.Contains(orderBy, "day")`
// is satisfied by a column named `birthday`, by `toYYYYMM(day)` appearing in
// the PARTITION BY clause the slice may still include, and by a column named
// `day_bucket` -- none of which put `day` itself in the key. The whole point of
// the assertion is that the partition expression is derivable from the sorting
// key, so it has to compare real key entries.
func githubDerivedSortingKeyColumns(t *testing.T, ddl string) []string {
	t.Helper()
	marker := "\nORDER BY "
	start := strings.Index(ddl, marker)
	if start < 0 {
		t.Fatalf("no ORDER BY clause in DDL:\n%s", ddl)
	}
	clause := ddl[start+len(marker):]
	if end := strings.IndexAny(clause, "\n"); end >= 0 {
		clause = clause[:end]
	}
	clause = strings.TrimSpace(clause)
	// A single-column key has no parentheses; a tuple key does.
	if strings.HasPrefix(clause, "(") && strings.HasSuffix(clause, ")") {
		clause = clause[1 : len(clause)-1]
	}
	var columns []string
	depth, current := 0, strings.Builder{}
	flush := func() {
		if value := strings.TrimSpace(current.String()); value != "" {
			columns = append(columns, value)
		}
		current.Reset()
	}
	for _, symbol := range clause {
		switch {
		case symbol == '(':
			depth++
			current.WriteRune(symbol)
		case symbol == ')':
			depth--
			current.WriteRune(symbol)
		case symbol == ',' && depth == 0:
			// Only a top-level comma separates key entries; a comma inside
			// ifNull(team_id, '') does not.
			flush()
		default:
			current.WriteRune(symbol)
		}
	}
	flush()
	return columns
}

// TestGitHubDerivedTablesPartitionExpressionsAreDerivableFromTheSortingKey is
// the precondition every partition-fenced readback in this package rests on.
//
// It is only meaningful because the schema now comes from the real migration
// chain. While the tables were created from hand-typed constants in this file,
// reading them back via SHOW CREATE TABLE proved nothing beyond "ClickHouse
// stored what we just typed" -- the assertion and the input had the same
// author.
//
// The rule (cross-lane finding, measured on a real container): fencing a
// readback on the partition key is only safe when the partition expression is
// DERIVABLE from the sorting key, so a given key can occupy exactly one
// partition. If a table partitioned on a column outside its sorting key, the
// fence would be actively WRONG under the default server setting: FINAL merges
// across partitions to a single winner, the fence filters that winner out, a
// correctly superseded row reads Absent, and the committer rewrites forever.
//
// All three tables here satisfy the rule -- the two partitioned ones carry
// `day` in their sorting keys, and the third has no PARTITION BY at all -- but
// that is a fact to VERIFY, not to assume, because it is the migration
// authors' choice and not ours.
func TestGitHubDerivedTablesPartitionExpressionsAreDerivableFromTheSortingKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	conn := githubDerivedIntegrationConn(t, ctx)

	for _, tt := range []struct {
		table           string
		wantPartitioned bool
	}{
		{"estimate_coverage_metrics_daily", true},
		{"work_item_state_durations_daily", true},
		{"work_item_team_attributions", false},
	} {
		t.Run(tt.table, func(t *testing.T) {
			var ddl string
			if err := conn.QueryRow(ctx, "SHOW CREATE TABLE "+tt.table).Scan(&ddl); err != nil {
				t.Fatal(err)
			}
			partitioned := strings.Contains(ddl, "PARTITION BY")
			if partitioned != tt.wantPartitioned {
				t.Fatalf("%s: PARTITION BY present = %v, want %v -- the readback shape "+
					"chosen for this table assumes otherwise\nDDL: %s",
					tt.table, partitioned, tt.wantPartitioned, ddl)
			}
			if !partitioned {
				return
			}
			if !strings.Contains(ddl, "PARTITION BY toYYYYMM(day)") {
				t.Fatalf("%s: partition expression is not toYYYYMM(day); a fence on `day` "+
					"may no longer pin one partition\nDDL: %s", tt.table, ddl)
			}
			// `day` must be IN the sorting key, so toYYYYMM(day) is derivable
			// from it and one key cannot span two partitions.
			columns := githubDerivedSortingKeyColumns(t, ddl)
			present := false
			for _, column := range columns {
				if column == "day" {
					present = true
					break
				}
			}
			if !present {
				t.Fatalf("%s: `day` is NOT a column of the sorting key, so toYYYYMM(day) "+
					"is not derivable from it and fencing the readback on day is UNSAFE "+
					"under the default do_not_merge_across_partitions_select_final=0"+
					"\nsorting key columns: %q", tt.table, columns)
			}
		})
	}
}

func githubDerivedIntegrationLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

func githubDerivedIntegrationIdentity(destination string, rowCount int) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: githubDerivedIntegrationOrg, Provider: "github", Dataset: "work-items",
		Generation: "gen-1", Destination: destination, RowCount: rowCount,
	}
}
