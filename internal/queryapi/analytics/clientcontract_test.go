package analytics

import (
	"regexp"
	"strings"
	"testing"
)

// This file makes the test doubles enforce the REAL ClickHouse client's
// constraints, so violations fail in `go test` instead of silently
// degrading in production.
//
// Why it exists: the pinned dev-health-go v0.4.0 client rejects unsafe
// statements BEFORE they reach ClickHouse, and resolveSankey /
// resolveFlowMatrix SWALLOW that rejection into an empty result with no
// GraphQL error. So a rejected query is indistinguishable from an org
// with no data. Four of six flowMatrix templates led with `WITH` and were
// rejected outright -- every dimension returned empty -- and no unit test
// could detect it, because the fake client had no guard at all.
//
// The general lesson, learned the expensive way: a fake that is KINDER
// than its real counterpart cannot fail on the class of defect the real
// one enforces. Fixing the four templates was necessary and insufficient;
// without this file the fifth template added later reintroduces the bug
// silently. Enumerated deliberately from client.go rather than
// discovered one outage at a time.
//
// Mirrors clickhouse/client.go:185-197 (v0.4.0) EXACTLY. If the pin
// moves, re-read that function and update this together with it.
var (
	fakeExternalTableFunction = regexp.MustCompile(`(?i)\b(url|s3|remote|file|hdfs|mysql|postgresql|sqlite)\s*\(`)
	fakeOutputClause          = regexp.MustCompile(`(?i)\binto\s+(outfile|dumpfile)\b`)
)

// validateLikeRealClient replicates validateReadOnlyStatement. All four
// branches, not just the first-token one: a template with an embedded or
// trailing semicolon fails identically.
func validateLikeRealClient(statement string) error {
	if strings.Contains(statement, ";") {
		return errFakeUnsafeStatement{reason: "contains ';'"}
	}
	fields := strings.Fields(statement)
	first := ""
	if len(fields) > 0 {
		first = fields[0]
	}
	if !strings.EqualFold(first, "SELECT") {
		return errFakeUnsafeStatement{reason: "first token is " + quoteOrEmpty(first) + ", not SELECT (a leading CTE/WITH is rejected)"}
	}
	if fakeExternalTableFunction.MatchString(statement) {
		return errFakeUnsafeStatement{reason: "matches the external-table-function pattern"}
	}
	if fakeOutputClause.MatchString(statement) {
		return errFakeUnsafeStatement{reason: "matches the INTO OUTFILE/DUMPFILE pattern"}
	}
	return nil
}

func quoteOrEmpty(s string) string {
	if s == "" {
		return "(empty)"
	}
	return `"` + s + `"`
}

type errFakeUnsafeStatement struct{ reason string }

func (e errFakeUnsafeStatement) Error() string {
	return "clickhouse runtime: unsafe statement -- the REAL v0.4.0 client would reject this before it reached ClickHouse, and resolveSankey/resolveFlowMatrix would swallow it to an EMPTY result: " + e.reason
}

// TestEveryCompiledQueryPassesTheRealClientGuard is the regression guard
// for the whole class. It compiles every flowMatrix query this package
// can emit and checks each against the real client's rules.
func TestEveryCompiledQueryPassesTheRealClientGuard(t *testing.T) {
	templates := map[string]string{
		"flowMatrixTeamNodesTemplate":     flowMatrixTeamNodesTemplate,
		"flowMatrixTeamEdgesTemplate":     flowMatrixTeamEdgesTemplate,
		"flowMatrixRepoNodesTemplate":     flowMatrixRepoNodesTemplate,
		"flowMatrixRepoEdgesTemplate":     flowMatrixRepoEdgesTemplate,
		"flowMatrixWorkTypeNodesTemplate": flowMatrixWorkTypeNodesTemplate,
		"flowMatrixWorkTypeEdgesTemplate": flowMatrixWorkTypeEdgesTemplate,
	}
	for name, sql := range templates {
		if err := validateLikeRealClient(sql); err != nil {
			t.Errorf("%s would be REJECTED in production: %v", name, err)
		}
	}
}
