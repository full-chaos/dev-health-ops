package syncdispatchruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type recordedStatement struct {
	sql  string
	args []any
}

// recordingClaimTx records every statement of a claim and answers each with no rows.
type recordingClaimTx struct {
	pgx.Tx
	statements []recordedStatement
}

func (tx *recordingClaimTx) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	tx.statements = append(tx.statements, recordedStatement{sql: sql, args: args})
	return emptyRows{}, nil
}

type emptyRows struct{}

func (emptyRows) Close()                                       {}
func (emptyRows) Err() error                                   { return nil }
func (emptyRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (emptyRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (emptyRows) Next() bool                                   { return false }
func (emptyRows) Scan(...any) error                            { return nil }
func (emptyRows) Values() ([]any, error)                       { return nil, nil }
func (emptyRows) RawValues() [][]byte                          { return nil }
func (emptyRows) Conn() *pgx.Conn                              { return nil }

func withClaimChunkSize(t *testing.T, size int) {
	t.Helper()
	previous := claimIDChunkSize
	claimIDChunkSize = size
	t.Cleanup(func() { claimIDChunkSize = previous })
}

func idList(prefix string, n int) []string {
	ids := make([]string, n)
	for index := range ids {
		ids[index] = fmt.Sprintf("%s%04d", prefix, index)
	}
	return ids
}

func idMap(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

func TestChunkIDsKeepsAtLeastOneChunkAndNeverExceedsTheSize(t *testing.T) {
	for _, n := range []int{0, 1, 6, 7, 8, 14, 15} {
		chunks := chunkIDs(idList("x", n), 7)
		total := 0
		for _, chunk := range chunks {
			if len(chunk) > 7 {
				t.Fatalf("N=%d: a chunk of %d ids, want at most 7", n, len(chunk))
			}
			total += len(chunk)
		}
		wantChunks := (n + 6) / 7
		if wantChunks == 0 {
			wantChunks = 1
		}
		if total != n || len(chunks) != wantChunks {
			t.Fatalf("N=%d: %d ids in %d chunks, want %d in %d", n, total, len(chunks), n, wantChunks)
		}
	}
}

// Every id bind of every claim statement is at most the chunk size, the authorized ids are covered exactly once across
// the executions of a statement, and every capped id is excluded in EVERY execution.
func TestClaimUnitsChunksEveryBindAndExcludesEveryCappedIDInEveryExecution(t *testing.T) {
	const size = 7
	withClaimChunkSize(t, size)
	for _, authorizedCount := range []int{size - 1, size, size + 1, 3*size + 2} {
		for _, cappedCount := range []int{0, size - 1, size, size + 1, 2*size + 3} {
			t.Run(fmt.Sprintf("authorized=%d capped=%d", authorizedCount, cappedCount), func(t *testing.T) {
				authorized := idList("a", authorizedCount)
				capped := idList("c", cappedCount)
				tx := &recordingClaimTx{}
				if _, _, err := claimUnits(context.Background(), tx, "run", idMap(authorized), idMap(capped), time.Unix(0, 0)); err != nil {
					t.Fatal(err)
				}
				wantExecutions := 3*((authorizedCount+size-1)/size) + 1 // three UPDATE statements per authorized chunk, then the probe
				if len(tx.statements) != wantExecutions {
					t.Fatalf("%d statements, want %d", len(tx.statements), wantExecutions)
				}
				covered := map[string]int{}
				for index, statement := range tx.statements {
					for _, arg := range statement.args {
						if ids, ok := arg.([]string); ok && len(ids) > size {
							t.Fatalf("statement %d binds %d ids, want at most %d", index, len(ids), size)
						}
					}
					if index == len(tx.statements)-1 {
						continue
					}
					excluded := map[string]bool{}
					last := len(statement.args) - 1
					for position := 4; position <= last; position++ { // after the four scalars: NOT binds, then the authorized chunk
						for _, id := range statement.args[position].([]string) {
							if position == last {
								covered[id]++
							} else {
								excluded[id] = true
							}
						}
					}
					for _, id := range capped {
						if !excluded[id] {
							t.Fatalf("statement %d does not exclude capped id %s", index, id)
						}
					}
				}
				for _, id := range authorized {
					if covered[id] != 3 { // once in each of the three UPDATE statements
						t.Fatalf("authorized id %s is covered %d times, want 3 (once per UPDATE)", id, covered[id])
					}
				}
				probe := tx.statements[len(tx.statements)-1]
				excludedByProbe := map[string]bool{}
				for _, arg := range probe.args[6:] {
					for _, id := range arg.([]string) {
						excludedByProbe[id] = true
					}
				}
				for _, id := range append(append([]string{}, authorized...), capped...) {
					if !excludedByProbe[id] {
						t.Fatalf("the probe does not exclude %s", id)
					}
				}
			})
		}
	}
}

// With the default size a small claim is the single-bind shape: one capped bind at $5 and one authorized bind at $6.
func TestClaimUnitsKeepsTheSingleBindShapeBelowTheChunkSize(t *testing.T) {
	tx := &recordingClaimTx{}
	if _, _, err := claimUnits(context.Background(), tx, "run", idMap(idList("a", 3)), idMap(idList("c", 2)), time.Unix(0, 0)); err != nil {
		t.Fatal(err)
	}
	if len(tx.statements) != 4 {
		t.Fatalf("%d statements, want 3 UPDATEs and the probe", len(tx.statements))
	}
	for index, statement := range tx.statements[:3] {
		if strings.Count(statement.sql, "NOT (id = ANY($5::uuid[]))") != 1 || strings.Count(statement.sql, "id = ANY($6::uuid[])") != 1 {
			t.Fatalf("statement %d is not the single-bind shape:\n%s", index, statement.sql)
		}
		if len(statement.args) != 6 {
			t.Fatalf("statement %d has %d args, want 6", index, len(statement.args))
		}
	}
}

// The runtime roles hold no TEMPORARY privilege on purpose, so no non-test source of the package may create a temp table.
func TestNoNonTestSourceCreatesATempTable(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no source files found (%v)", err)
	}
	pattern := regexp.MustCompile(`(?i)create\s+(global\s+|local\s+)?temp(orary)?\s`)
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if loc := pattern.FindIndex(raw); loc != nil {
			t.Errorf("%s creates a temporary table at byte %d: the runtime roles have TEMPORARY revoked", file, loc[0])
		}
	}
}
