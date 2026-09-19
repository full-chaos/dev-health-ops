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

func internalWriter(name, insert string, contract storedversion.Contract) storedversiontest.Writer {
	return storedversiontest.Writer{
		Name: name, Contract: contract, Insert: insert, NullIsUnstated: true,
		Carry: func(payload map[string]any) map[string]bool { return internalCarry(contract, payload) },
	}
}

func externalWriter(t *testing.T, kind, system string) storedversiontest.Writer {
	t.Helper()
	contract, ok := externalContract(kind, system)
	if !ok {
		t.Fatalf("%s has no contract", kind)
	}
	query, err := externalInsertQuery(kind)
	if err != nil {
		t.Fatal(err)
	}
	extended, _, err := withContractColumns(query, contract)
	if err != nil {
		t.Fatal(err)
	}
	return storedversiontest.Writer{
		Name: "external " + kind + " " + system, Contract: contract, Insert: extended,
		Carry: func(payload map[string]any) map[string]bool { return externalCarry(contract, payload) },
	}
}

// StreamHandlerWriters lists every stream-handler writer of each in-scope
// table, the set the invariant is enumerated over.
func streamHandlerWriters(t *testing.T) map[string][]storedversiontest.Writer {
	t.Helper()
	return map[string][]storedversiontest.Writer{
		"git_pull_requests": {
			internalWriter("internal pull-requests", internalPullRequestInsert, internalPullRequestContract),
			externalWriter(t, "pull_request.v1", "github"),
		},
		"git_pull_request_reviews": {
			internalWriter("internal reviews", internalReviewInsert, internalReviewContract),
			externalWriter(t, "review.v1", "github"),
		},
		"git_commits": {
			internalWriter("internal commits", internalCommitInsert, internalCommitContract),
			externalWriter(t, "commit.v1", "github"),
		},
		"deployments": {
			internalWriter("internal deployments", internalDeploymentInsert, internalDeploymentContract),
		},
		"work_items": {
			internalWriter("internal work-items", internalWorkItemInsert, internalWorkItemContract),
			externalWriter(t, "work_item.v1", "jira"), externalWriter(t, "work_item.v1", "github"),
			externalWriter(t, "work_item.v1", "gitlab"), externalWriter(t, "work_item.v1", "linear"),
		},
		"repos":      {externalWriter(t, "repository.v1", "github")},
		"identities": {externalWriter(t, "identity.v1", "github")},
	}
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
