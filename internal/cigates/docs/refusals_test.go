package docs_test

import (
	"strings"
	"testing"
)

// Refusal inventory of the ported tests. Every Python raise, sys.exit, argparse error and refusing stdlib call in a ported
// file of tests/docs is listed with the Go row that refuses the same input; the test runs each row, so a Python refusal
// with no Go row fails here.
//
// Executed grep on the merge base c5007bdb (the tree before this change), over the nine ported files
// test_audience_navigation, test_design_tokens, test_ai_user_guide_source_accuracy, test_user_guides,
// test_user_guide_source_accuracy, test_jira_service_management_contract, test_built_site_links, test_docs_guards_workflow
// and test_guard_failures, and the helper user_guide_source_accuracy:
//   git show c5007bdb:tests/docs/<file>.py | grep -nE 'raise |sys\.exit|SystemExit|argparse|parser\.error|pytest\.raises|os\._exit|\bexit\('
// Hits: test_design_tokens.py:55 (`raise AssertionError`) and :113 (`pytest.raises(AssertionError)`). The other files are
// plain assertions on files and workflows and have no refusal path of their own.

type refusalRow struct {
	python string
	goRow  string
	run    func(t *testing.T) error
}

func TestEveryPythonRefusalOfThePortedTestsHasAGoRow(t *testing.T) {
	rows := []refusalRow{
		{
			python: `test_design_tokens.py:55 raise AssertionError(f"{source_path}: undeclared raw CSS value {raw_value}")`,
			goRow:  "designGuard: a raw colour outside the :root tokens of context-fabric.css names the value (plant: a raw colour outside the tokens)",
			run: func(t *testing.T) error {
				d := realTree(t).appended(t, "docs/stylesheets/context-fabric.css", "\n.fixture { color: #badc0d; }\n")
				got := strings.Join(designGuard(d), "\n")
				if !strings.Contains(got, "context-fabric.css: undeclared raw CSS value #badc0d") {
					t.Errorf("designGuard did not name the raw value: %q", got)
				}
				return nil
			},
		},
		{
			python: "test_design_tokens.py:113 with pytest.raises(AssertionError) as error (the fixture css is refused with the message above)",
			goRow:  "TestUndeclaredRawCSSValueIsRejected: undeclaredRawCSSValue returns the raw value of a fixture css, nothing for a declared token",
			run: func(t *testing.T) error {
				if got := undeclaredRawCSSValue(".fixture { color: #badc0d; }"); got != "#badc0d" {
					t.Errorf("undeclaredRawCSSValue = %q", got)
				}
				return nil
			},
		},
	}
	for _, r := range rows {
		r := r
		if r.python == "" || r.goRow == "" || r.run == nil {
			t.Fatalf("an inventory row is incomplete: %+v", r)
		}
		t.Run(r.python, func(t *testing.T) { _ = r.run(t) })
	}
	if len(rows) != 2 {
		t.Errorf("the inventory lists %d refusals; the executed grep found 2: add the row for any new Python refusal", len(rows))
	}
}
