package providersync

import (
	"fmt"
	"strings"
	"testing"
)

// CHAOS-13985 round 1: the report type and every directive decision come from
// the XML tokens of the member, never from byte substrings or prefixes.

const prepassTrailer = `<packages><package><classes><class filename="a.go"><lines><line number="1" hits="1"/><line number="2" hits="0"/></lines></class></classes></package></packages>`

func prepassDoc(root, prefix, middle, suffix string) string {
	switch root {
	case "coverage":
		return prefix + `<coverage lines-valid="2" lines-covered="1">` + middle + prepassTrailer + `</coverage>` + suffix
	case "testsuite":
		return prefix + `<testsuite name="s">` + middle + `<testcase name="t" classname="c"/></testsuite>` + suffix
	default:
		return prefix + `<testsuites><testsuite name="s">` + middle + `<testcase name="t" classname="c"/></testsuite></testsuites>` + suffix
	}
}

func prepassCheck(t *testing.T, label, file, body, wantCause string) {
	t.Helper()
	rows := parseDoctypeArchive(t, map[string]string{file: body})
	incomplete, _ := rows.optionalIncomplete()
	if wantCause == "" {
		if rows.Skipped != 0 || len(incomplete) != 0 || len(rows.SkippedArtifacts) != 0 || len(rows.Coverage)+len(rows.Cases) != 1 {
			t.Fatalf("%s: want one row and no skip, rows=%+v incomplete=%+v", label, rows, incomplete)
		}
		return
	}
	if rows.Skipped != 1 || len(rows.Coverage)+len(rows.Cases) != 0 || len(incomplete) != 1 ||
		incomplete[0].Cause != wantCause || len(rows.SkippedArtifacts) != 1 || rows.SkippedArtifacts[0].Cause != wantCause {
		t.Fatalf("%s: want cause %s and no rows, rows=%+v incomplete=%+v", label, wantCause, rows, incomplete)
	}
}

func TestGitHubTestsXMLPrepassAcceptsAndRefusesByTokens(t *testing.T) {
	const decl = `<?xml version="1.0" encoding="UTF-8"?>` + "\n"
	long := "<!-- " + strings.Repeat("x", 3000) + " -->\n"
	for _, root := range []string{"coverage", "testsuite", "testsuites"} {
		sys := func(extra string) string {
			return fmt.Sprintf(`<!DOCTYPE %s SYSTEM "http://example.invalid/r.dtd%s">`, root, extra)
		}
		pub := fmt.Sprintf(`<!DOCTYPE %s PUBLIC "-//X//EN" "http://example.invalid/r.dtd">`, root)
		type cell struct{ name, file, body, want string }
		cells := []cell{
			{"plain", "r.xml", prepassDoc(root, "", "", ""), ""},
			{"decl+system", "r.xml", prepassDoc(root, decl+sys(""), "", ""), ""},
			{"decl+public", "r.xml", prepassDoc(root, decl+pub, "", ""), ""},
			{"bom xml", "r.xml", prepassDoc(root, "\xEF\xBB\xBF"+decl+sys(""), "", ""), ""},
			{"bom info", "r.info", prepassDoc(root, "\xEF\xBB\xBF"+decl+sys(""), "", ""), ""},
			{"root after 2KiB", "r.xml", prepassDoc(root, decl+long+sys(""), "", ""), ""},
			{"root at 2KiB cutoff info", "r.info", prepassDoc(root, decl+long+sys(""), "", ""), ""},
			{"other tag in comment", "r.xml", prepassDoc(root, decl, `<!-- <testsuite> <coverage> -->`, ""), ""},
			{"other tag in PI", "r.xml", prepassDoc(root, decl, `<?x <testsuite> <coverage> ?>`, ""), ""},
			{"other tag in CDATA", "r.xml", prepassDoc(root, decl, `<![CDATA[ <testsuite> <coverage> <!ENTITY a "b"> ]]>`, ""), ""},
			{"entity text in comment", "r.xml", prepassDoc(root, decl+sys(""), `<!-- <!ENTITY a "b"> -->`, ""), ""},
			{"doctype subset text in comment", "r.xml", prepassDoc(root, decl+sys(""), `<!-- <!DOCTYPE x [ <!ELEMENT a ANY> ]> -->`, ""), ""},
			{"entity text in PI", "r.xml", prepassDoc(root, decl, `<?x <!ENTITY a "b"> ?>`, ""), ""},
			{"entity text in CDATA", "r.xml", prepassDoc(root, decl, `<![CDATA[ <!ENTITY a "b"> <!DOCTYPE x [ ]> ]]>`, ""), ""},
			{"entity and subset text in SYSTEM literal", "r.xml", prepassDoc(root, decl+sys(`<!ENTITY[`), "", ""), ""},
			{"subset text in PUBLIC literals", "r.xml", prepassDoc(root, fmt.Sprintf(`<!DOCTYPE %s PUBLIC "-//<!ENTITY[//EN" 'http://x/[<!ENTITY'>`, root), "", ""), ""},
			{"unicode comment before doctype", "r.xml", prepassDoc(root, decl+"<!-- İßﬃΩ -->"+sys(`[<!ENTITY`), "", ""), ""},
			{"trailing whitespace comment PI", "r.xml", prepassDoc(root, decl+sys(""), "", " \n<!-- end -->\n<?done x?>\n"), ""},
			{"internal subset", "r.xml", prepassDoc(root, decl+fmt.Sprintf(`<!DOCTYPE %s [ <!ELEMENT a ANY> ]>`, root), "", ""), "unsafe_doctype"},
			{"internal subset with entity", "r.xml", prepassDoc(root, decl+fmt.Sprintf(`<!DOCTYPE %s [ <!ENTITY a "b"> ]>`, root), "", ""), "unsafe_doctype"},
			{"internal subset bom", "r.xml", prepassDoc(root, "\xEF\xBB\xBF"+fmt.Sprintf(`<!DOCTYPE %s [ <!ELEMENT a ANY> ]>`, root), "", ""), "unsafe_doctype"},
			{"internal subset long prologue xml", "r.xml", prepassDoc(root, decl+long+fmt.Sprintf(`<!DOCTYPE %s [ <!ELEMENT a ANY> ]>`, root), "", ""), "unsafe_doctype"},
			{"internal subset long prologue info", "r.info", prepassDoc(root, decl+long+fmt.Sprintf(`<!DOCTYPE %s [ <!ELEMENT a ANY> ]>`, root), "", ""), "unsafe_doctype"},
			{"empty internal subset", "r.xml", prepassDoc(root, decl+fmt.Sprintf(`<!DOCTYPE %s []>`, root), "", ""), "unsafe_doctype"},
			{"CI text before empty subset", "r.xml", prepassDoc(root, "CI: ok\n"+decl+fmt.Sprintf(`<!DOCTYPE %s []>`, root), "", ""), "unsafe_doctype"},
			{"CI text before subset with entity", "r.xml", prepassDoc(root, "CI: ok\n"+fmt.Sprintf(`<!DOCTYPE %s [ <!ENTITY a "b"> ]>`, root), "", ""), "unsafe_doctype"},
			{"top-level entity", "r.xml", prepassDoc(root, decl+sys("")+`<!ENTITY a "b">`, "", ""), "unsafe_doctype"},
			{"unfinished declaration after root", "r.xml", prepassDoc(root, decl, "", fmt.Sprintf(`<!DOCTYPE %s SYSTEM "x"`, root)), "malformed"},
			{"second root", "r.xml", prepassDoc(root, decl, "", prepassDoc(root, "", "", "")), "malformed"},
			{"truncated trailing comment", "r.xml", prepassDoc(root, decl, "", "<!-- never closed"), "malformed"},
		}
		for _, c := range cells {
			prepassCheck(t, root+"/"+c.name, c.file, c.body, c.want)
		}
	}
}

func TestGitHubTestsLCOVWithCoverageTextStaysLCOV(t *testing.T) {
	rows := parseDoctypeArchive(t, map[string]string{"r.info": "TN:<coverage>\nSF:a.go\nDA:1,1\nDA:2,0\nLF:2\nLH:1\nend_of_record\n"})
	if rows.Skipped != 0 || len(rows.Coverage) != 1 {
		t.Fatalf("rows=%+v", rows)
	}
}

// The cap went 8 -> 6 (CHAOS-13985): the 7th and later skips go only to the
// per-cause overflow and count, never into the sample.
func TestGitHubTestsSeventhSkippedArtifactOnlyCountsAndOverflows(t *testing.T) {
	var records []GitHubTestsSkippedArtifact
	var overflow int
	var causeOverflow map[string]bool
	var causeCount map[string]int
	for i := 0; i < githubTestsMaxSkippedArtifactRecords+1; i++ {
		records, overflow, causeOverflow, causeCount = appendGitHubTestsSkippedArtifact(
			records, overflow, causeOverflow, causeCount,
			GitHubTestsSkippedArtifact{RunID: "1", ArtifactID: "2", Cause: githubTestsUnsafeDoctypeCause},
		)
	}
	if githubTestsMaxSkippedArtifactRecords != 6 || len(records) != 6 || overflow != 1 ||
		!causeOverflow[githubTestsUnsafeDoctypeCause] || causeCount[githubTestsUnsafeDoctypeCause] != 7 {
		t.Fatalf("records=%d overflow=%d causeOverflow=%v causeCount=%v", len(records), overflow, causeOverflow, causeCount)
	}
	// incomplete[] still holds the cause for every one of them.
	var rows githubTestsReportRows
	for i := 0; i < 7; i++ {
		rows.recordSkipped(githubTestsUnsafeDoctypeCause, false)
	}
	incomplete, _ := rows.optionalIncomplete()
	if len(incomplete) != 1 || incomplete[0] != (GitHubTestsIncomplete{Component: "report_member", Cause: "unsafe_doctype", Count: 7}) {
		t.Fatalf("incomplete=%+v", incomplete)
	}
}
