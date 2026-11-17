package providersync

import (
	"errors"
	"testing"
	"time"
)

// CHAOS-13985: the shape of the acr go-coverage artifact header.
const githubTestsCoberturaDoctypeFixture = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE coverage SYSTEM "http://cobertura.sourceforge.net/xml/coverage-04.dtd">
<coverage line-rate="0.5" branch-rate="0" version="" timestamp="1794933870730" lines-covered="2" lines-valid="4" branches-covered="0" branches-valid="0" complexity="0">
  <sources><source>/work</source></sources>
  <packages>
    <package name="a" line-rate="0.5" branch-rate="0" complexity="0">
      <classes>
        <class name="-" filename="a/a.go" line-rate="0.5" branch-rate="0" complexity="0">
          <methods><method name="f" signature="" line-rate="0.5" branch-rate="0" complexity="0">
            <lines><line number="1" hits="1"></line><line number="2" hits="0"></line></lines>
          </method></methods>
          <lines><line number="1" hits="1"></line><line number="2" hits="0"></line></lines>
        </class>
        <class name="-" filename="a/b.go" line-rate="0.5" branch-rate="0" complexity="0">
          <lines><line number="1" hits="3"></line><line number="2" hits="0"></line></lines>
        </class>
      </classes>
    </package>
  </packages>
</coverage>`

var githubTestsDoctypeNow = time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)

func parseDoctypeArchive(t *testing.T, members map[string]string) githubTestsReportRows {
	t.Helper()
	rows, err := parseGitHubTestsArtifact(
		githubTestsZip(t, members),
		"artifact-1", "c7198fbc-1945-3717-05d8-eb78866b4e79", "9001", "org-a",
		nil, nil, githubTestsDoctypeNow,
	)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestGitHubTestsCoberturaWithExternalDoctypeParses(t *testing.T) {
	// Pure in-memory parse: the SYSTEM id is an unresolvable-by-design URL and
	// encoding/xml hands the DOCTYPE over as a directive without fetching it.
	rows := parseDoctypeArchive(t, map[string]string{"coverage.xml": githubTestsCoberturaDoctypeFixture})
	if rows.Skipped != 0 || len(rows.SkippedArtifacts) != 0 || len(rows.Coverage) != 1 {
		t.Fatalf("rows=%+v", rows)
	}
	coverage := rows.Coverage[0]
	if coverage.LinesTotal == nil || *coverage.LinesTotal != 4 || coverage.LinesCovered == nil || *coverage.LinesCovered != 2 ||
		coverage.LineCoveragePct == nil || *coverage.LineCoveragePct != 50 {
		t.Fatalf("coverage=%+v", coverage)
	}
}

func TestGitHubTestsJUnitWithPlainDoctypeParses(t *testing.T) {
	for name, doctype := range map[string]string{
		"system": `<!DOCTYPE testsuite SYSTEM "http://example.invalid/junit.dtd">`,
		"public": `<!DOCTYPE testsuite PUBLIC "-//X//DTD//EN" "http://example.invalid/junit.dtd">`,
		"bare":   `<!DOCTYPE testsuite>`,
	} {
		rows := parseDoctypeArchive(t, map[string]string{
			"junit.xml": `<?xml version="1.0"?>` + "\n" + doctype + `<testsuite name="x"><testcase name="t" classname="c"/></testsuite>`,
		})
		if rows.Skipped != 0 || len(rows.Suites) != 1 || len(rows.Cases) != 1 {
			t.Fatalf("%s: rows=%+v", name, rows)
		}
	}
}

func TestGitHubTestsUnsafeDoctypeIsOwnCause(t *testing.T) {
	for name, member := range map[string]string{
		"junit internal subset":     `<!DOCTYPE testsuite [ <!ELEMENT testsuite ANY> ]><testsuite name="x"><testcase name="t"/></testsuite>`,
		"junit entity":              `<!DOCTYPE testsuite><!ENTITY x "boom"><testsuite name="x"/>`,
		"cobertura internal subset": `<?xml version="1.0"?><!DOCTYPE coverage [ <!ELEMENT coverage ANY> ]><coverage lines-valid="1" lines-covered="1"></coverage>`,
		"cobertura entity":          `<?xml version="1.0"?><!DOCTYPE coverage SYSTEM "x"><!entity x "boom"><coverage lines-valid="1" lines-covered="1"></coverage>`,
	} {
		file := "r.xml"
		rows := parseDoctypeArchive(t, map[string]string{file: member})
		incomplete, optional := rows.optionalIncomplete()
		want := GitHubTestsIncomplete{Component: "report_member", Cause: "unsafe_doctype", Count: 1}
		if rows.Skipped != 1 || !optional || len(incomplete) != 1 || incomplete[0] != want ||
			len(rows.SkippedArtifacts) != 1 || rows.SkippedArtifacts[0].Cause != "unsafe_doctype" {
			t.Fatalf("%s: rows=%+v incomplete=%+v", name, rows, incomplete)
		}
		if _, ok := githubTestsWatermarkAdvancingPairs[want.Component][want.Cause]; !ok {
			t.Fatal("unsafe_doctype must advance the watermark like malformed")
		}
		if _, ok := githubTestsIncompleteVocabulary[want.Component][want.Cause]; !ok {
			t.Fatal("unsafe_doctype missing from the incomplete vocabulary")
		}
	}
}

func TestGitHubTestsBrokenXMLStaysMalformed(t *testing.T) {
	for name, member := range map[string]string{
		"junit":     `<testsuite name="x"><testcase></testsuite>`,
		"cobertura": `<?xml version="1.0"?><!DOCTYPE coverage SYSTEM "x"><coverage lines-valid="1"><packages>`,
	} {
		rows := parseDoctypeArchive(t, map[string]string{"r.xml": member})
		incomplete, _ := rows.optionalIncomplete()
		if len(incomplete) != 1 || incomplete[0].Cause != "malformed" {
			t.Fatalf("%s: incomplete=%+v", name, incomplete)
		}
	}
}

func TestGitHubTestsUnsafeXMLDirectivesParseErrorWrapsBothSentinels(t *testing.T) {
	_, err := parseCoberturaRow([]byte(`<!DOCTYPE coverage [<!ENTITY a "b">]><coverage/>`), "c.xml", "a", "r", "1", "o", githubTestsDoctypeNow)
	if !errors.Is(err, ErrGitHubTestsReportInvalid) || !errors.Is(err, ErrGitHubTestsUnsafeDoctype) {
		t.Fatalf("err=%v", err)
	}
}
