package datahealth

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

var trailingLimit = regexp.MustCompile(`LIMIT (\d+)\s*$`)

// limitHonoringCH cuts the scripted rows to the statement's trailing LIMIT, as ClickHouse does.
type limitHonoringCH struct{ inner *routingCH }

func (c limitHonoringCH) Query(ctx context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	scanner, err := c.inner.Query(ctx, statement, b)
	if err != nil {
		return nil, err
	}
	if m := trailingLimit.FindStringSubmatch(statement); m != nil {
		limit, _ := strconv.Atoi(m[1])
		if fs, ok := scanner.(*fakeScanner); ok && len(fs.rows) > limit {
			fs.rows = fs.rows[:limit]
		}
	}
	return scanner, nil
}

func TestIdentityUnmappedCountIsNotCappedAtTheReadLimit(t *testing.T) {
	const unmappedIdentities = 101
	var observed [][]any
	for i := 0; i < unmappedIdentities; i++ {
		observed = append(observed, []any{"git", fmt.Sprintf("user%03d@example.invalid", i), "n", uint64(1000 - i)})
	}
	ch := limitHonoringCH{inner: &routingCH{rules: []chRule{{match: "FROM git_commits", rows: observed}}}}
	got := (&Reader{ClickHouse: ch}).IdentityMapping(context.Background(), "org-1", "")
	if got.UnmappedCount != unmappedIdentities {
		t.Fatalf("unmapped count = %d, want %d (101 unmapped identities observed)", got.UnmappedCount, unmappedIdentities)
	}
}
