package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// shimBasePath is basePath with the python shims first: a child that runs with the fixed system PATH must still meet them.
func shimBasePath() string { return "PATH=" + withShims("/usr/bin:/bin:/usr/local/bin") }

// shimBasePathWith is shimBasePath with first (when set) in front and, after it, a directory that holds only links to the named
// tools as this test process finds them. A skip guard that finds a tool on the test PATH and the script it runs then resolve the
// same binary: ARC runners install helm under _work/_tool, outside the fixed base PATH (CHAOS-13084). A tool the process does not
// find gets no link, and the script refuses it as before.
func shimBasePathWith(t *testing.T, first string, tools ...string) string {
	t.Helper()
	links := t.TempDir()
	for _, tool := range tools {
		p, err := exec.LookPath(tool)
		if err != nil {
			continue
		}
		if p, err = filepath.Abs(p); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(p, filepath.Join(links, tool)); err != nil {
			t.Fatal(err)
		}
	}
	front := links + string(os.PathListSeparator)
	if first != "" {
		front = first + string(os.PathListSeparator) + front
	}
	return strings.Replace(shimBasePath(), "PATH=", "PATH="+front, 1)
}

// fakeEnvsubst stands in for envsubst next to a fake kubectl, so a test that fakes the cluster does not depend on the host having
// gettext (CHAOS-13084). It replaces ${NAME} for each NAME its SHELL-FORMAT argument lists (every ${NAME} in the input when there
// is no argument), as envsubst does, and keeps the input's trailing newlines. FAKE_ENVSUBST_MARK, when set, names a file it creates.
const fakeEnvsubst = `#!/usr/bin/env bash
[ -z "${FAKE_ENVSUBST_MARK:-}" ] || : >"${FAKE_ENVSUBST_MARK}"
in="$(cat; echo .)"; in="${in%.}"
for v in $(grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' <<<"${1-$in}" | sort -u); do
  n="${v:2:${#v}-3}"
  in="${in//"$v"/"${!n:-}"}"
done
printf '%s' "$in"
`
