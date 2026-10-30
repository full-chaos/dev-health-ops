package bigboy_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// The deploy copy of the file moves bugsink's credentials from three required variables to bugsink's own env file
// (CHAOS-11970). Until that deploy change merges, the BIGBOY_DIR tree may have either shape, and only these two.
var composeBugsinkVars = []string{"BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY"}

const composeBugsinkEnvFile = "./.bugsink-dev/bugsink.env"

// composeBugsinkShape returns a finding unless the file is in exactly one of the two shapes:
// old: the required variables are base plus the three BUGSINK_* and bugsink loads no .bugsink-dev env file;
// new: the required variables are base only, bugsink's env_file is exactly the required .bugsink-dev/bugsink.env, and the
// text interpolates no BUGSINK_* variable.
func composeBugsinkShape(required, base []string, bugsink doc, text string) []string {
	old := append(append([]string(nil), base...), composeBugsinkVars...)
	sort.Strings(old)
	files := list(bugsink["env_file"])
	ownFile := false
	for _, e := range files {
		if p := str(e); p == composeBugsinkEnvFile || str(mp(e)["path"]) == composeBugsinkEnvFile {
			ownFile = true
		}
	}
	exactFile := len(files) == 1 && str(mp(files[0])["path"]) == composeBugsinkEnvFile && mp(files[0])["required"] == true
	interpolated := false
	for _, v := range composeBugsinkVars {
		if strings.Contains(text, "${"+v) {
			interpolated = true
		}
	}
	switch {
	case equal(required, old) && !ownFile:
		return nil
	case equal(required, base) && exactFile && !interpolated:
		return nil
	}
	return []string{fmt.Sprintf("compose.bigboy.yml requires %v (bugsink env_file %v, BUGSINK_* interpolated %v): want the pins and DHO_API_CH_USERS_XML %v "+
		"plus either the three bugsink variables %v (old form) or none of them with bugsink's env_file exactly %s, required (new form)",
		required, files, interpolated, base, composeBugsinkVars, composeBugsinkEnvFile)}
}

func TestComposeBugsinkShapeCells(t *testing.T) {
	base := []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML"}
	withVars := []string{"BIGBOY_DHO_IMAGE", "BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY", "DHO_API_CH_USERS_XML"}
	oldText := "      DATABASE_URL: postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink\n"
	own := doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": true}}}
	cases := []struct {
		name     string
		required []string
		bugsink  doc
		text     string
		ok       bool
	}{
		{"old form: the three variables, no own env file", withVars, doc{}, oldText, true},
		{"new form: own env file, no variables", base, own, "", true},
		{"neither: no env file and no variables", base, doc{}, "", false},
		{"both: variables and the own env file", withVars, own, oldText, false},
		{"credentials from ops/.env", base, doc{"env_file": []any{doc{"path": "./ops/.env", "required": true}}}, "", false},
		{"own env file plus ops/.env", base, doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": true}, doc{"path": "./ops/.env", "required": true}}}, "", false},
		{"own env file not required", base, doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": false}}}, "", false},
		{"own env file in the string form", base, doc{"env_file": []any{composeBugsinkEnvFile}}, "", false},
		{"new form but a variable is still interpolated with a default", base, own, "      SECRET_KEY: ${BUGSINK_SECRET_KEY:-x}\n", false},
		{"old form missing one variable", []string{"BIGBOY_DHO_IMAGE", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY", "DHO_API_CH_USERS_XML"}, doc{}, oldText, false},
		{"old form with the own env file in its string form", withVars, doc{"env_file": []any{composeBugsinkEnvFile}}, oldText, false},
		{"new form with an extra required variable", []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML", "PROVE_ORG"}, own, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := composeBugsinkShape(c.required, base, c.bugsink, c.text)
			if c.ok && len(got) != 0 {
				t.Fatalf("want accepted, got %v", got)
			}
			if !c.ok && len(got) == 0 {
				t.Fatal("want a finding, got none")
			}
		})
	}
}
