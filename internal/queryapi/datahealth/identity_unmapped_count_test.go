package datahealth

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

var trailingLimit = regexp.MustCompile(`LIMIT (\d+)\s*$`)

// limitCH cuts the scripted rows to the statement's trailing LIMIT, as ClickHouse does, and records the work.
type limitCH struct {
	inner    *routingCH
	observed int
	returned int
}

func (c *limitCH) Query(ctx context.Context, statement string, b []clickhouse.Binding) (clickhouse.RowScanner, error) {
	scanner, err := c.inner.Query(ctx, statement, b)
	if err != nil {
		return nil, err
	}
	fs, ok := scanner.(*fakeScanner)
	if !ok {
		return scanner, nil
	}
	if m := trailingLimit.FindStringSubmatch(statement); m != nil {
		if limit, _ := strconv.Atoi(m[1]); len(fs.rows) > limit {
			fs.rows = fs.rows[:limit]
		}
	}
	if strings.Contains(statement, "FROM git_commits") {
		c.observed++
		c.returned += len(fs.rows)
	}
	return fs, nil
}

// observedRows are in observed_count order (the statement's ORDER BY); the first `mapped` are known to the org.
func observedRows(total, mapped int) (observed, known [][]any) {
	for i := 0; i < total; i++ {
		email := fmt.Sprintf("user%05d@example.invalid", i)
		observed = append(observed, []any{"git", email, "n", uint64(total - i)})
		if i < mapped {
			known = append(known, []any{fmt.Sprintf("c%d", i), email, "", `{}`, []string{}})
		}
	}
	return observed, known
}

func countWith(total, mapped int) (*limitCH, int, []string) {
	observed, known := observedRows(total, mapped)
	ch := &limitCH{inner: &routingCH{rules: []chRule{{match: "FROM git_commits", rows: observed}, {match: "FROM identities FINAL", rows: known}}}}
	got := (&Reader{ClickHouse: ch}).IdentityMapping(context.Background(), "org-1", "")
	var shown []string
	for _, id := range got.UnmappedIdentities {
		shown = append(shown, *id.Email)
	}
	return ch, got.UnmappedCount, shown
}

func TestIdentityUnmappedCountCountsEveryUnmappedGroup(t *testing.T) {
	_, count, _ := countWith(150, 30)
	if count != 120 {
		t.Fatalf("unmapped count = %d, want 120 (150 observed, 30 of them mapped)", count)
	}
}

func TestIdentityUnmappedBelowTheHundredthByCountIsCountedAndShown(t *testing.T) {
	_, count, shown := countWith(101, 100)
	if count != 1 || len(shown) != 1 || shown[0] != "user00100@example.invalid" {
		t.Fatalf("count=%d shown=%v, want the one unmapped identity ranked 101st by observed count", count, shown)
	}
}

func TestIdentityUnmappedCountIsOneStatementAndBoundedByTheRowsOfTheOrg(t *testing.T) {
	const groups = 10000
	ch, count, _ := countWith(groups, 0)
	if ch.observed != 1 || ch.returned != groups || count != groups {
		t.Fatalf("observed statements=%d rows returned=%d count=%d, want 1 / %d / %d", ch.observed, ch.returned, count, groups, groups)
	}
}

func TestIdentityUnmappedCountOverTheCeilingIsALowerBoundWithOneWarn(t *testing.T) {
	previous := observedGroupCeiling
	observedGroupCeiling = 5
	defer func() { observedGroupCeiling = previous }()
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(old)

	observed, _ := observedRows(20, 0)
	ch := &limitCH{inner: &routingCH{rules: []chRule{{match: "FROM git_commits", rows: observed}}}}
	got := (&Reader{ClickHouse: ch}).IdentityMapping(context.Background(), "c0ffee00-aaaa-bbbb-cccc-000000000001", "")
	if got.UnmappedCount != 5 {
		t.Fatalf("unmapped count = %d, want 5 (the first 5 groups: the ceiling)", got.UnmappedCount)
	}
	logged := buf.String()
	if n := strings.Count(logged, "level=WARN"); n != 1 || !strings.Contains(logged, "org_prefix=c0ffee00") || !strings.Contains(logged, "ceiling=5") ||
		!strings.Contains(logged, `msg="query-api: data health identity count is a lower bound, observed groups passed the ceiling"`) || strings.Contains(logged, "c0ffee00-aaaa") {
		t.Fatalf("want exactly one WARN carrying the org prefix only: %s", logged)
	}
}
