package bigboyrender

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// ProjectScopeEnv is go-api's flag for project_scope on the ACR access read (apiservice.EnvACRAccessProjectScope,
// CHAOS-12224). With it on, go-api's ClickHouse readiness requires APIPostureFor(db, true): APIPosture plus
// ProjectScopeGrant, exact both ways. So the bigboy users.d render and the grants check must know the value go-api
// runs with (CHAOS-12268); `go-api-project-scope` reads it from go-api's env file.
const ProjectScopeEnv = "ACR_ACCESS_PROJECT_SCOPE"

// projectScopeOption is the render-ch-users and grants-compare option for the flag-on posture.
const projectScopeOption = "--project-scope"

// projectScopeMarker opens the manifest function whose one row is the flag-on grant.
const projectScopeMarker = "func ProjectScopeGrant("

// ProjectScopeValue is go-api's reading of the flag value (apiservice.ParseACRAccessProjectScope): empty after
// TrimSpace is off, a strconv.ParseBool value is that value, anything else is refused. An apiservice test is the
// differential check that the two agree.
func ProjectScopeValue(raw string) (bool, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return false, nil
	}
	on, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", ProjectScopeEnv)
	}
	return on, nil
}

// projectScopeFromEnvFile reads the flag from a Compose env file's text. Absent is off, as go-api reads an unset
// flag. It accepts only the lines whose value cannot read differently under Compose's own env-file parser:
// `ACR_ACCESS_PROJECT_SCOPE=value` at the start of a line, the value bare or in one pair of matching quotes, and the
// value then a boolean go-api accepts. A repeated key, an `export` prefix, a space before `=` and an indented key are
// refused; an inline comment, an unbalanced quote or an interpolation leaves a value that is not a boolean, so it is
// refused too. Errors name the line number, never the line.
func projectScopeFromEnvFile(text string) (bool, error) {
	found, line := "", 0
	for i, raw := range strings.Split(text, "\n") {
		l := strings.TrimSuffix(raw, "\r")
		trimmed := strings.TrimSpace(l)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		key, value, hasEq := strings.Cut(l, "=")
		if !strings.Contains(key, ProjectScopeEnv) {
			continue
		}
		bare := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(key), "export "))
		if bare != ProjectScopeEnv {
			continue // another key that only contains the name (ACR_ACCESS_PROJECT_SCOPE_X)
		}
		if !hasEq || key != ProjectScopeEnv {
			return false, fmt.Errorf("%s: line %d sets it in a form Compose may read differently (export, indent or a space before =)", ProjectScopeEnv, i+1)
		}
		if line != 0 {
			return false, fmt.Errorf("%s: set twice (line %d and line %d)", ProjectScopeEnv, line, i+1)
		}
		v := strings.TrimSpace(value)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		// An inline comment, an interpolation or an unbalanced quote leaves a value that is not a boolean, so
		// ProjectScopeValue refuses it below.
		found, line = v, i+1
	}
	on, err := ProjectScopeValue(found)
	if err != nil {
		return false, fmt.Errorf("%s (line %d)", err, line)
	}
	return on, nil
}

// goAPIProjectScope: `go-api-project-scope <go-api env file>` prints true or false, the project-scope flag go-api
// runs with; exit 1 when the file cannot be read or the flag cannot be read unambiguously, 2 on a usage error.
func goAPIProjectScope(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 {
		fmt.Fprintln(stderr, "usage: cigate bigboy go-api-project-scope <go-api env file>")
		return 2
	}
	path := args[0]
	info, err := os.Stat(path)
	if err != nil {
		fmt.Fprintf(stderr, "go-api env file %s cannot be read: %v\n", path, err)
		return 1
	}
	if !info.Mode().IsRegular() {
		fmt.Fprintf(stderr, "go-api env file %s is not a file\n", path)
		return 1
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		fmt.Fprintf(stderr, "go-api env file %s cannot be read: %v\n", path, err)
		return 1
	}
	on, err := projectScopeFromEnvFile(string(raw))
	if err != nil {
		fmt.Fprintf(stderr, "go-api env file %s: %v\n", path, err)
		return 1
	}
	fmt.Fprintln(stdout, strconv.FormatBool(on))
	return 0
}

// takeProjectScopeOption removes a leading --project-scope from args and reports whether it was there.
func takeProjectScopeOption(args []string) ([]string, bool) {
	if len(args) > 0 && args[0] == projectScopeOption {
		return args[1:], true
	}
	return args, false
}
