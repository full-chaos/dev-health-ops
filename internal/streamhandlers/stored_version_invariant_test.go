package streamhandlers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storedversion"
	"github.com/full-chaos/dev-health-ops/internal/storedversion/storedversiontest"
)

// streamHandlerWriters lists every stream-handler writer of each in-scope
// table, the set the invariant is enumerated over.
func streamHandlerWriters(t *testing.T) map[string][]storedversiontest.Writer {
	t.Helper()
	specs, err := StoredVersionSpecs()
	if err != nil {
		t.Fatal(err)
	}
	return specs
}

func TestStreamHandlerWritersKeepTheStoredVersionInvariant(t *testing.T) {
	writers := streamHandlerWriters(t)
	for _, table := range []string{"git_pull_requests", "git_pull_request_reviews", "git_commits", "deployments", "work_items", "repos", "identities"} {
		t.Run(table, func(t *testing.T) {
			cells := storedversiontest.Enumerate(t, writers[table], 3)
			t.Logf("%s: %d writers, %d cells", table, len(writers[table]), cells)
		})
	}
}

// The enumerated writer set equals the generated set of contract tables: every
// internal contract and every external kind × allowed system of an in-scope
// table.
func TestInvariantEnumeratesEveryStreamHandlerContract(t *testing.T) {
	enumerated := map[string]bool{}
	for _, writers := range streamHandlerWriters(t) {
		for _, writer := range writers {
			enumerated[storedversiontest.Fingerprint(writer.Contract)] = true
		}
	}
	generated := []storedversion.Contract{
		internalCommitContract, internalReviewContract, internalPullRequestContract,
		internalDeploymentContract, internalWorkItemContract,
	}
	for _, kind := range []string{"pull_request.v1", "review.v1", "commit.v1", "work_item.v1", "repository.v1", "identity.v1"} {
		for _, system := range []string{"github", "gitlab", "jira", "linear", "custom", "pagerduty", "atlassian"} {
			if _, allowed := externalAllowedKinds[system][kind]; !allowed {
				continue
			}
			contract, ok := externalContract(kind, system)
			if !ok {
				t.Fatalf("%s from %s has no contract", kind, system)
			}
			generated = append(generated, contract)
		}
	}
	for _, contract := range generated {
		if !enumerated[storedversiontest.Fingerprint(contract)] {
			t.Errorf("contract %s is not enumerated", storedversiontest.Fingerprint(contract))
		}
	}
}

// Every INSERT statement on an in-scope table in this package's production
// source is the insert of an enumerated writer: the writer set is discovered
// from the code, not listed by hand.
func TestEveryInScopeInsertInTheSourceIsAnEnumeratedWriter(t *testing.T) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate test")
	}
	files, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var inserts []string
	fset := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(parsed, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err == nil && strings.HasPrefix(strings.TrimSpace(text), "INSERT INTO ") {
				inserts = append(inserts, strings.TrimSpace(text))
			}
			return true
		})
	}
	if len(inserts) == 0 {
		t.Fatal("no INSERT statements found")
	}
	scoped := map[string]bool{"git_pull_requests": true, "git_pull_request_reviews": true, "git_commits": true, "deployments": true, "work_items": true, "repos": true, "identities": true}
	writers := streamHandlerWriters(t)
	found := 0
	for _, insert := range inserts {
		table := strings.TrimSpace(strings.TrimPrefix(insert[:strings.IndexByte(insert, '(')], "INSERT INTO "))
		if !scoped[table] {
			continue
		}
		found = found + 1
		base := strings.TrimSuffix(insert, ")")
		matched := false
		for _, writer := range writers[table] {
			if strings.HasPrefix(writer.Insert, base) {
				matched = true
			}
		}
		if !matched {
			t.Errorf("%s is written by %q, which no enumerated writer covers", table, insert)
		}
	}
	if found == 0 {
		t.Fatal("no in-scope INSERT statements found")
	}
}
