package bigboy_test

import (
	"fmt"
	"sort"
	"testing"

	"gopkg.in/yaml.v3"
)

// The deploy copy of the file moves bugsink's credentials from three required variables to bugsink's own env file
// (CHAOS-11970). Until that deploy change merges, the BIGBOY_DIR tree may have either shape, and only these two.
var composeBugsinkVars = []string{"BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY"}

const composeBugsinkEnvFile = "./.bugsink-dev/bugsink.env"

// composeBugsinkShape returns a finding unless the file is in exactly one of the two shapes:
// old: the required variables are base plus the three BUGSINK_* and bugsink loads no .bugsink-dev env file;
// new: the required variables are base only, bugsink's env_file is exactly the required .bugsink-dev/bugsink.env, and no
// scalar of the file interpolates one of the three bugsink credential variables in any Compose form.
func composeBugsinkShape(required, base []string, bugsink doc, scalars []string) []string {
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
	for _, s := range scalars {
		for _, name := range composeInterpolatedNames(s) {
			for _, v := range composeBugsinkVars {
				if name == v {
					interpolated = true
				}
			}
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

// composeBugsinkFileFindings applies composeBugsinkShape (bugsink service + every scalar) and composeBugsinkPassThrough (every
// service) to the compose file the guard reads.
func composeBugsinkFileFindings(g *guard, required, base []string) []string {
	f := g.yamlFile(composeFile)
	services := mp(f["services"])
	return append(composeBugsinkShape(required, base, mp(services["bugsink"]), composeScalars(f)), composeBugsinkPassThrough(services)...)
}

// composeInterpolatedNames returns the variable names Compose interpolates in one scalar: $NAME, ${NAME} and ${NAME<op>...}
// for every operator (:- - :? ? :+ +), including a ${...} nested in a default. $$ is a literal dollar and interpolates nothing.
func composeInterpolatedNames(s string) []string {
	isStart := func(c byte) bool { return c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') }
	isName := func(c byte) bool { return isStart(c) || (c >= '0' && c <= '9') }
	var names []string
	for i := 0; i < len(s); i++ {
		if s[i] != '$' || i+1 >= len(s) {
			continue
		}
		j := i + 1
		if s[j] == '$' {
			i = j
			continue
		}
		if s[j] == '{' {
			j++
		}
		if j >= len(s) || !isStart(s[j]) {
			continue
		}
		k := j
		for k < len(s) && isName(s[k]) {
			k++
		}
		names = append(names, s[j:k])
		i = k - 1
	}
	return names
}

func composeScalarsOf(t *testing.T, text string) []string {
	t.Helper()
	var v any
	if err := yaml.Unmarshal([]byte(text), &v); err != nil {
		t.Fatalf("cell YAML: %v", err)
	}
	return composeScalars(v)
}

func TestComposeBugsinkShapeCells(t *testing.T) {
	base := []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML"}
	withVars := []string{"BIGBOY_DHO_IMAGE", "BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY", "DHO_API_CH_USERS_XML"}
	oldText := "environment:\n  DATABASE_URL: postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink\n"
	newText := "environment:\n  BASE_URL: ${BUGSINK_BASE_URL:-http://localhost:8800}\n  PORT: '8000'\n"
	own := doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": true}}}
	env := func(v string) string { return "environment:\n  SECRET_KEY: " + v + "\n" }
	cases := []struct {
		name     string
		required []string
		bugsink  doc
		text     string
		ok       bool
	}{
		{"old form: the three variables, no own env file", withVars, doc{}, oldText, true},
		{"new form: own env file, no variables", base, own, newText, true},
		{"neither: no env file and no variables", base, doc{}, newText, false},
		{"both: variables and the own env file", withVars, own, oldText, false},
		{"credentials from ops/.env", base, doc{"env_file": []any{doc{"path": "./ops/.env", "required": true}}}, newText, false},
		{"own env file plus ops/.env", base, doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": true}, doc{"path": "./ops/.env", "required": true}}}, newText, false},
		{"own env file not required", base, doc{"env_file": []any{doc{"path": composeBugsinkEnvFile, "required": false}}}, newText, false},
		{"own env file in the string form", base, doc{"env_file": []any{composeBugsinkEnvFile}}, newText, false},
		{"old form missing one variable", []string{"BIGBOY_DHO_IMAGE", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY", "DHO_API_CH_USERS_XML"}, doc{}, oldText, false},
		{"old form with the own env file in its string form", withVars, doc{"env_file": []any{composeBugsinkEnvFile}}, oldText, false},
		{"new form with an extra required variable", []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML", "PROVE_ORG"}, own, newText, false},
		{"new form, bare $VAR", base, own, env("$BUGSINK_SECRET_KEY"), false},
		{"new form, ${VAR}", base, own, env("${BUGSINK_SECRET_KEY}"), false},
		{"new form, ${VAR:-default}", base, own, env("${BUGSINK_SECRET_KEY:-x}"), false},
		{"new form, ${VAR-default}", base, own, env("${BUGSINK_SECRET_KEY-x}"), false},
		{"new form, ${VAR:?error}", base, own, env("'${BUGSINK_SECRET_KEY:?set it}'"), false},
		{"new form, ${VAR?error}", base, own, env("'${BUGSINK_SECRET_KEY?set it}'"), false},
		{"new form, ${VAR:+alt}", base, own, env("${BUGSINK_SECRET_KEY:+x}"), false},
		{"new form, nested in another default", base, own, env("${OTHER:-${BUGSINK_DB_PASSWORD}}"), false},
		{"new form, inside a URL", base, own, "environment:\n  DATABASE_URL: postgresql://bugsink:$BUGSINK_DB_PASSWORD@postgres:5432/bugsink_v2\n", false},
		{"new form, list-form environment", base, own, "environment:\n- CREATE_SUPERUSER=$BUGSINK_CREATE_SUPERUSER\n", false},
		{"new form, $$ then $VAR", base, own, env("$$$BUGSINK_SECRET_KEY"), false},
		{"new form, $$VAR is a literal", base, own, env("$$BUGSINK_SECRET_KEY"), true},
		{"new form, $${VAR} is a literal", base, own, env("$${BUGSINK_SECRET_KEY}"), true},
		{"new form, a comment naming the variable", base, own, "# $BUGSINK_SECRET_KEY moved to the env file\n" + newText, true},
		{"new form, a longer name is another variable", base, own, env("$BUGSINK_SECRET_KEY_OLD"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := composeBugsinkShape(c.required, base, c.bugsink, composeScalarsOf(t, c.text))
			if c.ok && len(got) != 0 {
				t.Fatalf("want accepted, got %v", got)
			}
			if !c.ok && len(got) == 0 {
				t.Fatal("want a finding, got none")
			}
		})
	}
}

func TestComposeInterpolatedNamesCells(t *testing.T) {
	for in, want := range map[string]string{
		"$A":            "A",
		"${A}":          "A",
		"${A:-d}":       "A",
		"${A-d}":        "A",
		"${A:?e}":       "A",
		"${A?e}":        "A",
		"${A:+r}":       "A",
		"${A+r}":        "A",
		"${A:-${B}}":    "A B",
		"x$A_1y":        "A_1y",
		"$$A":           "",
		"$${A}":         "",
		"$$$A":          "A",
		"$":             "",
		"$1":            "",
		"${":            "",
		"plain text":    "",
		"a$A and ${B}c": "A B",
	} {
		if got := fmt.Sprint(composeInterpolatedNames(in)); got != "["+want+"]" {
			t.Errorf("%q: names %s, want [%s]", in, got, want)
		}
	}
}

func TestComposeBugsinkFileFindingsReadsTheFile(t *testing.T) {
	base := []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML"}
	head := "services:\n  bugsink:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n    environment:\n      PORT: '8000'\n"
	for _, c := range []struct {
		name, extra string
		ok          bool
	}{
		{"new form", "", true},
		{"new form plus a bare credential variable", "      SECRET_KEY: $BUGSINK_SECRET_KEY\n", false},
		{"new form plus a credential variable in another service", "  web:\n    environment:\n    - X=${BUGSINK_DB_PASSWORD}\n", false},
		{"new form plus a null credential key in another service", "  web:\n    environment:\n      BUGSINK_SECRET_KEY: null\n", false},
		{"new form plus another service loading the bugsink env file", "  web:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, composeFile, head+c.extra)
			g := &guard{root: root, tmp: t.TempDir()}
			got := composeBugsinkFileFindings(g, base, base)
			if len(g.v) != 0 {
				t.Fatalf("guard errors: %v", g.v)
			}
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
		})
	}
}
