package security

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
)

// QueryClient is the narrow ClickHouse boundary this package needs.
type QueryClient interface {
	Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error)
}

// defaultFirst is the page size when the request carries no pagination.
const defaultFirst = 50

// decodeCursor turns an offset cursor into a non-negative offset. Anything
// that is not an integer decodes to 0, as does a negative value. Integers
// are read the way a decimal integer literal is read: surrounding
// whitespace, one optional sign, digits with single underscores between
// digits.
func decodeCursor(cursor *string) (int64, error) {
	if cursor == nil {
		return 0, nil
	}
	s := strings.TrimSpace(*cursor)
	neg := false
	if strings.HasPrefix(s, "+") || strings.HasPrefix(s, "-") {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" || s[0] == '_' || s[len(s)-1] == '_' || strings.Contains(s, "__") {
		return 0, nil
	}
	digits, ok := decimalDigits(s)
	if !ok {
		return 0, nil
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("security: cursor offset out of range")
	}
	if neg || n < 0 {
		return 0, nil
	}
	return n, nil
}

// decimalDigits maps a digit string with optional single underscores
// between digits to ASCII digits. Any Unicode decimal digit (category Nd)
// is accepted and mapped by its position in its own zero-based run of ten.
func decimalDigits(s string) (string, bool) {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r == '_':
			continue
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case unicode.IsDigit(r):
			start := r
			for unicode.IsDigit(start - 1) {
				start--
			}
			b.WriteRune('0' + (r-start)%10)
		default:
			return "", false
		}
	}
	return b.String(), b.Len() > 0
}

func encodeCursor(offset int64) string { return strconv.FormatInt(offset, 10) }

// nonEmpty maps the empty string to nil, so an empty text column reads as
// an absent value.
func nonEmpty(s *string) *string {
	if s == nil || *s == "" {
		return nil
	}
	return s
}

// ResolveAlerts returns one page of the org's alerts ordered by severity
// rank then creation time, newest first. One extra row is read to decide
// hasNextPage; totalCount is the offset plus the returned edges.
func ResolveAlerts(ctx context.Context, client QueryClient, orgID string, filters *model.SecurityAlertFilterInput, pagination *model.SecurityPaginationInput) (*model.SecurityAlertConnection, error) {
	if client == nil {
		return nil, errors.New("security: clickhouse client is required")
	}
	first := int64(defaultFirst)
	var after *string
	if pagination != nil {
		first = int64(pagination.First)
		after = pagination.After
	}
	offset, err := decodeCursor(after)
	if err != nil {
		return nil, err
	}

	f := buildFilter(orgID, filters)
	bindings := append(append([]clickhouse.Binding{}, f.bindings...),
		clickhouse.Binding{Name: "limit", Value: first + 1},
		clickhouse.Binding{Name: "offset", Value: offset},
	)
	query := `SELECT
    toString(sa.repo_id) AS repo_id,
    sa.alert_id,
    r.repo AS repo_name,
    sa.source,
    coalesce(sa.severity, 'unknown') AS severity,
    coalesce(sa.state, 'open') AS state,
    sa.package_name,
    sa.cve_id,
    sa.url,
    sa.title,
    sa.description,
    sa.created_at,
    sa.fixed_at,
    sa.dismissed_at
FROM security_alerts sa
INNER JOIN repos r ON sa.repo_id = r.id
` + f.sql + `
ORDER BY ` + severityRank + ` DESC, sa.created_at DESC
LIMIT {limit:Int64} OFFSET {offset:Int64}`

	rows, err := client.Query(ctx, query, bindings)
	if err != nil {
		return nil, fmt.Errorf("security: alerts query: %w", err)
	}
	defer rows.Close()

	var nodes []model.SecurityAlertNode
	for rows.Next() {
		var (
			repoID, alertID, repoName, source, severity, state string
			pkg, cve, url, title, description                  *string
			createdAt                                          time.Time
			fixedAt, dismissedAt                               *time.Time
		)
		if err := rows.Scan(&repoID, &alertID, &repoName, &source, &severity, &state,
			&pkg, &cve, &url, &title, &description, &createdAt, &fixedAt, &dismissedAt); err != nil {
			return nil, fmt.Errorf("security: alerts scan: %w", err)
		}
		nodes = append(nodes, model.SecurityAlertNode{
			AlertID:     alertID,
			RepoID:      repoID,
			RepoName:    repoName,
			Source:      source,
			Severity:    severity,
			State:       state,
			PackageName: nonEmpty(pkg),
			CveID:       nonEmpty(cve),
			URL:         nonEmpty(url),
			Title:       nonEmpty(title),
			Description: nonEmpty(description),
			CreatedAt:   createdAt,
			FixedAt:     fixedAt,
			DismissedAt: dismissedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("security: alerts rows: %w", err)
	}

	hasNext := int64(len(nodes)) > first
	kept := sliceTo(nodes, first)
	edges := make([]model.SecurityAlertEdge, 0, len(kept))
	for i := range kept {
		edges = append(edges, model.SecurityAlertEdge{
			Node:   &kept[i],
			Cursor: encodeCursor(offset + int64(i) + 1),
		})
	}
	page := &model.PageInfo{HasNextPage: hasNext, HasPreviousPage: offset > 0}
	if len(edges) > 0 {
		start, end := edges[0].Cursor, edges[len(edges)-1].Cursor
		page.StartCursor, page.EndCursor = &start, &end
	}
	return &model.SecurityAlertConnection{
		Edges:      edges,
		TotalCount: int(offset) + len(edges),
		PageInfo:   page,
	}, nil
}

// sliceTo returns rows[:n] with the semantics of a list slice: a negative
// n counts from the end, and n past the length keeps every row.
func sliceTo[T any](rows []T, n int64) []T {
	l := int64(len(rows))
	if n < 0 {
		n += l
		if n < 0 {
			n = 0
		}
	}
	if n > l {
		n = l
	}
	return rows[:n]
}
