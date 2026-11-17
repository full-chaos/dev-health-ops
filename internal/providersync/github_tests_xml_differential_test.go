package providersync

import (
	"fmt"
	"reflect"
	"testing"
)

// CHAOS-13985 contract: refuse only an ENTITY / internal subset (unsafe_doctype)
// and a second root or element after the root or markup that does not tokenize
// (malformed). Every other form is decoded exactly as main's decoder decodes it.
// Each cell runs the same input through main's frozen decoder (the oracle) and
// the tip; rows must be equal and the tip must carry no marker.
//
// Oracle choice per cell: "guard" is main as it ran (blanket DOCTYPE refusal);
// "noguard" is main's decoder without that guard. Equality is asserted against
// noguard; the table logs both. For a Cobertura root with text before it, main
// dispatched on a byte prefix (so it never reached its Cobertura decoder); the
// oracle there is main's Cobertura decoder itself, and the tip picks the type
// from the real root.

type diffResult struct {
	rows any
	err  bool
}

func diffRender(rows any, err error) diffResult {
	if err != nil {
		return diffResult{rows: "error", err: true}
	}
	return diffResult{rows: rows}
}

func TestGitHubTestsXMLTolerantFormsDecodeLikeMain(t *testing.T) {
	const decl = `<?xml version="1.0" encoding="UTF-8"?>`
	for _, root := range []string{"coverage", "testsuite", "testsuites"} {
		sys := fmt.Sprintf(`<!DOCTYPE %s SYSTEM "http://example.invalid/r.dtd">`, root)
		type form struct{ name, prefix, middle, suffix string }
		forms := []form{
			{"plain external DOCTYPE (main refused by guard only)", decl + "\n" + sys, "", ""},
			{"PUBLIC DOCTYPE (guard only)", fmt.Sprintf(`<!DOCTYPE %s PUBLIC "-//X//EN" "http://example.invalid/r.dtd">`, root), "", ""},
			{"CI status before root", "CI: status ok\n", "", ""},
			{"CI status before declaration", "CI: status ok\n" + decl, "", ""},
			{"CI status before DOCTYPE (guard only)", "CI: status ok\n" + decl + sys, "", ""},
			{"duplicate declaration", decl + "\n" + decl, "", ""},
			{"declaration after root", decl, "", "\n" + decl},
			{"declaration after whitespace and comment", "\n<!-- c -->\n" + decl, "", ""},
			{"declaration inside root", decl, decl, ""},
			{"xml target case variant after root", decl, "", `<?XML version="1.0"?>`},
			{"suffix CDATA whitespace", decl, "", `<![CDATA[ ]]>`},
			{"suffix empty CDATA", decl, "", `<![CDATA[]]>`},
			{"suffix character reference", decl, "", `&#10;`},
			{"suffix NBSP", decl, "", " "},
			{"suffix text", decl, "", "junk"},
			{"text before root", decl + "junk", "", ""},
			{"invalid DOCTYPE name start (guard only)", decl + `<!DOCTYPE 1r SYSTEM "x">`, "", ""},
			{"invalid DOCTYPE name char (guard only)", decl + `<!DOCTYPE r$x SYSTEM "x">`, "", ""},
			{"comment in DOCTYPE (guard only)", decl + `<!DOCTYPE r <!-- c --> SYSTEM "x">`, "", ""},
			{"lowercase doctype (guard only)", decl + `<!doctype r SYSTEM "x">`, "", ""},
			{"SYSTEM without id (guard only)", decl + `<!DOCTYPE r SYSTEM>`, "", ""},
			{"duplicate DOCTYPE (guard only)", decl + sys + sys, "", ""},
			{"DOCTYPE after root (guard only)", decl, "", sys},
			{"DOCTYPE inside root (guard only)", decl, sys, ""},
			{"other directive", decl + `<!ELEMENT a ANY>`, "", ""},
			{"BOM and declaration", "\xEF\xBB\xBF" + decl, "", ""},
		}
		for _, f := range forms {
			body := prepassDoc(root, f.prefix, f.middle, f.suffix)
			var guarded, oracle, tip diffResult
			if root == "coverage" {
				g, ge := oracleMainParseCoberturaRow(true, []byte(body), "r.xml", "artifact-1", "repo", "run", "org", githubTestsDoctypeNow)
				o, oe := oracleMainParseCoberturaRow(false, []byte(body), "r.xml", "artifact-1", "repo", "run", "org", githubTestsDoctypeNow)
				n, ne := parseCoberturaRow([]byte(body), "r.xml", "artifact-1", "repo", "run", "org", githubTestsDoctypeNow)
				guarded, oracle, tip = diffRender(g, ge), diffRender(o, oe), diffRender(n, ne)
			} else {
				gs, gc, gn, ge := oracleMainParseJUnitRows(true, []byte(body), "artifact-1", "repo", "run", "org", nil, nil, githubTestsDoctypeNow, map[string]int{})
				os, oc, on, oe := oracleMainParseJUnitRows(false, []byte(body), "artifact-1", "repo", "run", "org", nil, nil, githubTestsDoctypeNow, map[string]int{})
				ns, nc, nn, ne := parseJUnitRows([]byte(body), "artifact-1", "repo", "run", "org", nil, nil, githubTestsDoctypeNow, map[string]int{})
				guarded = diffRender([]any{gs, gc, gn}, ge)
				oracle = diffRender([]any{os, oc, on}, oe)
				tip = diffRender([]any{ns, nc, nn}, ne)
			}
			equal := reflect.DeepEqual(oracle, tip)
			label := func(r diffResult) string {
				if r.err {
					return "refused"
				}
				return "rows"
			}
			t.Logf("TABLE %-9s %-52s main=%-7s main-noguard=%-7s tip=%-7s equal=%t", root, f.name, label(guarded), label(oracle), label(tip), equal)
			if !equal {
				t.Errorf("%s/%s: tip differs from main's decoder: noguard err=%t tip err=%t", root, f.name, oracle.err, tip.err)
				continue
			}
			// Through the archive path the tip carries no marker when the decoder reads rows.
			rows := parseDoctypeArchive(t, map[string]string{"r.xml": body})
			incomplete, _ := rows.optionalIncomplete()
			if !oracle.err && (rows.Skipped != 0 || len(incomplete) != 0 || len(rows.Coverage)+len(rows.Cases) != 1) {
				t.Errorf("%s/%s: archive path lost the report: rows=%+v incomplete=%+v", root, f.name, rows, incomplete)
			}
		}
	}
}
