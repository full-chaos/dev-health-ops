package checkgo_test

import (
	"strings"
	"testing"
)

// The fake go of the scope pass cell: the no-tag `go list` discovery pass fails, every other go call is the real go.
const scopePassFailFakeGo = `#!/bin/sh
if [ "$1" = list ]; then
  for a; do
    case "$a" in -tags=) echo "fake go: the no-tag discovery pass fails" >&2; exit 1 ;; esac
  done
fi
exec "$FAKE_REAL_GO" "$@"
`

func TestAuthScopeSelftestFailsWhenAnEarlierDiscoveryPassFails(t *testing.T) {
	t.Parallel()
	code, errText, _ := runCheckGoWithFakeGo(t, []string{"auth", "scope-selftest"}, nil, scopePassFailFakeGo)
	if !strings.Contains(errText, "fake go: the no-tag discovery pass fails") {
		t.Fatalf("the fake go never failed the no-tag pass; rc %d, stderr: %s", code, tail(errText, 800))
	}
	if code != 2 || !strings.Contains(errText, "auth scope: the scope selftest refused") {
		t.Fatalf("a failed discovery pass must fail the scope selftest: rc %d, want 2; stderr: %s", code, tail(errText, 800))
	}
}
