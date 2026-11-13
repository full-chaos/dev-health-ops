package main

import (
	"strings"
	"testing"
)

const chPostureHeader = "user_name\trole_name\taccess_type\tdatabase\ttable\tcolumn\tis_partial_revoke\tgrant_option\n"

const chPostureProdRows = "dho_app\t\\N\tSELECT\tdefault\t\\N\t\\N\t0\t0\n" +
	"dho_app\t\\N\tINSERT\tdefault\t\\N\t\\N\t0\t0\n" +
	"dho_app\t\\N\tALTER DELETE\tdefault\t\\N\t\\N\t0\t0\n" +
	"dho_app\t\\N\tOPTIMIZE\tdefault\t\\N\t\\N\t0\t0\n" +
	"dho_app\t\\N\tSELECT\tsystem\tcolumns\t\\N\t0\t0\n" +
	"dho_app\t\\N\tSELECT\tsystem\tparts\t\\N\t0\t0\n" +
	"dho_app\t\\N\tSELECT\tsystem\ttables\t\\N\t0\t0\n" +
	"dho_app\t\\N\tSELECT\tsystem\tview_refreshes\t\\N\t0\t0\n"

func TestChPostureCompare(t *testing.T) {
	dir := t.TempDir()
	pass := write(t, dir, "pass.tsv", chPostureHeader+chPostureProdRows)
	fail := write(t, dir, "fail.tsv", chPostureHeader+
		strings.Replace(chPostureProdRows, "dho_app\t\\N\tOPTIMIZE\tdefault\t\\N\t\\N\t0\t0\n", "", 1)+
		"dho_app\t\\N\tALTER UPDATE\tdefault\t\\N\t\\N\t0\t0\n")
	short := write(t, dir, "short.tsv", chPostureHeader+chPostureProdRows+"dho_app\t\\N\tSELECT\tdefault\t\\N\t0\t0\n")
	noHeader := write(t, dir, "noheader.tsv", chPostureProdRows)
	escaped := write(t, dir, "escaped.tsv", chPostureHeader+chPostureProdRows+"dho_app\t\\N\tSELECT\tdef\\tault\t\\N\t\\N\t0\t0\n")
	badFlag := write(t, dir, "badflag.tsv", chPostureHeader+strings.Replace(chPostureProdRows, "\t0\t0\n", "\t0\tfalse\n", 1))

	cases := []struct {
		name   string
		args   []string
		code   int
		stdout string
		stderr []string
	}{
		{"pass file", []string{"compare", "--user", "dho_app", "--grants", pass}, 0,
			"ch-posture: dho_app matches the declared posture (8 rows)\n", nil},
		{"flags in either order", []string{"compare", "--grants", pass, "--user", "dho_app"}, 0,
			"ch-posture: dho_app matches the declared posture (8 rows)\n", nil},
		{"fail file", []string{"compare", "--user", "dho_app", "--grants", fail}, 1, "",
			[]string{"MISMATCH", "extra privilege: ALTER UPDATE ON default.*", "missing privilege: OPTIMIZE ON default.*"}},
		{"wrong column count", []string{"compare", "--user", "dho_app", "--grants", short}, 2, "",
			[]string{"line 10: 7 fields, want 8"}},
		{"no header", []string{"compare", "--user", "dho_app", "--grants", noHeader}, 2, "",
			[]string{"line 1: header"}},
		{"escaped value", []string{"compare", "--user", "dho_app", "--grants", escaped}, 2, "",
			[]string{"line 10: database: escaped value"}},
		{"flag not 0 or 1", []string{"compare", "--user", "dho_app", "--grants", badFlag}, 2, "",
			[]string{"line 2: grant_option: \"false\", want 0 or 1"}},
		{"undeclared user", []string{"compare", "--user", "dho_api_ch", "--grants", pass}, 2, "",
			[]string{"user \"dho_api_ch\" has no declared posture"}},
		{"missing file", []string{"compare", "--user", "dho_app", "--grants", dir + "/absent.tsv"}, 2, "",
			[]string{"absent.tsv"}},
		{"no verb", nil, 2, "", []string{"usage: cigate ch-posture compare"}},
		{"unknown flag", []string{"compare", "--user", "dho_app", "--file", pass}, 2, "",
			[]string{"usage: cigate ch-posture compare"}},
		{"repeated flag", []string{"compare", "--user", "dho_app", "--user", "dho_app"}, 2, "",
			[]string{"usage: cigate ch-posture compare"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCaptured(t, append([]string{"ch-posture"}, tc.args...)...)
			if code != tc.code || out != tc.stdout {
				t.Fatalf("exit %d stdout %q, want exit %d stdout %q; stderr:\n%s", code, out, tc.code, tc.stdout, errOut)
			}
			for _, want := range tc.stderr {
				if !strings.Contains(errOut, want) {
					t.Errorf("stderr does not name %q:\n%s", want, errOut)
				}
			}
		})
	}
}
