package bigboy_test

import (
	"fmt"
	"testing"

	"gopkg.in/yaml.v3"
)

// The deploy copy of the file keeps bugsink's credentials in bugsink's own env file, never in compose variables (CHAOS-11970).
var composeBugsinkVars = []string{"BUGSINK_CREATE_SUPERUSER", "BUGSINK_DB_PASSWORD", "BUGSINK_SECRET_KEY"}

const composeBugsinkEnvFile = "./.bugsink-dev/bugsink.env"

// composeBugsinkShape returns a finding unless the file is in the one accepted shape: the required variables are base only,
// bugsink's env_file is exactly the required .bugsink-dev/bugsink.env, and no scalar of the file interpolates one of the
// three bugsink credential variables in any Compose form. (The old three-variable shape was accepted during the move only.)
func composeBugsinkShape(required, base []string, bugsink doc, scalars []string) []string {
	files := list(bugsink["env_file"])
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
	if equal(required, base) && exactFile && !interpolated {
		return nil
	}
	return []string{fmt.Sprintf("compose.bigboy.yml requires %v (bugsink env_file %v, BUGSINK_* interpolated %v): want only the pins and DHO_API_CH_USERS_XML %v "+
		"and bugsink's env_file exactly %s, required, with none of the bugsink variables %v interpolated",
		required, files, interpolated, base, composeBugsinkEnvFile, composeBugsinkVars)}
}

// composeBugsinkFileFindings applies composeBugsinkShape (bugsink service + every scalar) and composeBugsinkLeaks (every way
// another service can get a credential) to the compose file the guard reads.
func composeBugsinkFileFindings(g *guard, required, base []string) []string {
	f := g.yamlFile(composeFile)
	return append(composeBugsinkShape(required, base, mp(mp(f["services"])["bugsink"]), composeScalars(f)), composeBugsinkLeaks(f)...)
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
	oldUse := doc{"environment": map[string]any{"DATABASE_URL": "postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink",
		"SECRET_KEY": "${BUGSINK_SECRET_KEY:?set}", "CREATE_SUPERUSER": "${BUGSINK_CREATE_SUPERUSER:?set}"}}
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
		{"old form: the three variables, no own env file (dropped after the deploy move)", withVars, oldUse, oldText, false},
		{"old form: the three names required only in an unused extension field, bugsink static", withVars,
			doc{"environment": map[string]any{"SECRET_KEY": "static-secret", "CREATE_SUPERUSER": "static-admin", "DATABASE_URL": "postgresql://x"}},
			"x-unused: ${BUGSINK_DB_PASSWORD:?a} ${BUGSINK_SECRET_KEY:?b} ${BUGSINK_CREATE_SUPERUSER:?c}\n", false},
		{"old form: bugsink names the three only in a label, its environment static", withVars,
			doc{"labels": map[string]any{"x": "${BUGSINK_DB_PASSWORD:?a} ${BUGSINK_SECRET_KEY:?b} ${BUGSINK_CREATE_SUPERUSER:?c}"},
				"environment": map[string]any{"SECRET_KEY": "static-secret", "CREATE_SUPERUSER": "static-admin", "DATABASE_URL": "postgresql://x"}},
			oldText, false},
		{"old form: bugsink uses two of the three", withVars,
			doc{"environment": map[string]any{"DATABASE_URL": "postgresql://devhealth:${BUGSINK_DB_PASSWORD:?set}@postgres:5432/bugsink", "SECRET_KEY": "${BUGSINK_SECRET_KEY:?set}", "CREATE_SUPERUSER": "static-admin"}},
			oldText, false},
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

func TestComposeRequiredVariablesReachTheShapeCheck(t *testing.T) {
	base := []string{"BIGBOY_DHO_IMAGE", "DHO_API_CH_USERS_XML"}
	head := "services:\n  bugsink:\n    env_file:\n    - path: " + composeBugsinkEnvFile + "\n      required: true\n  web:\n    environment:\n" +
		"      A: ${BIGBOY_DHO_IMAGE:?set}\n      B: ${DHO_API_CH_USERS_XML:?set}\n"
	for _, c := range []struct {
		name, extra string
		ok          bool
	}{
		{"pins only", "", true},
		{"extra required with colon", "      C: ${PROVE_ORG:?m}\n", false},
		{"extra required without colon", "      C: ${PROVE_ORG?m}\n", false},
		{"extra required with a digit in the name, colon", "      C: ${PROVE_ORG2:?m}\n", false},
		{"extra required with a digit in the name, no colon", "      C: ${PROVE_ORG2?m}\n", false},
		{"default with colon is not required", "      C: ${PROVE_ORG:-d}\n", true},
		{"default without colon is not required", "      C: ${PROVE_ORG-d}\n", true},
		{"plain braced variable is not required", "      C: ${PROVE_ORG}\n", true},
		{"escaped required without colon is a literal", "      C: $${PROVE_ORG?m}\n", true},
		{"escaped required with colon is a literal", "      C: $${PROVE_ORG:?m}\n", true},
		{"escaped required with a digit in the name is a literal", "      C: $${PROVE_ORG2?m}\n", true},
		{"required after an escaped dollar is a reference", "      C: $$${PROVE_ORG?m}\n", false},
		{"required without escape is a reference", "      C: ${PROVE_ORG?m}\n", false},
	} {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			writeFile(t, root, composeFile, head+c.extra)
			g := &guard{root: root, tmp: t.TempDir()}
			got := composeBugsinkFileFindings(g, composeRequired(g), base)
			if len(g.v) != 0 {
				t.Fatalf("guard errors: %v", g.v)
			}
			if c.ok != (len(got) == 0) {
				t.Fatalf("want accepted=%v, findings %v", c.ok, got)
			}
		})
	}
}
