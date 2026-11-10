package checkgo_test

import (
	"strings"
	"testing"
)

// The fake go of the scope pass cell answers every discovery pass: the no-tag pass fails, the others print one canned
// package. It logs each scope-verdict call to the file the harness returns; `go list -m` and every other call go to the
// real go, so the cell runs no real package discovery.
const scopePassFailFakeGo = `#!/bin/sh
if [ "$1" = list ]; then
  json=no; tags=unset
  for a; do
    case "$a" in -m) exec "$FAKE_REAL_GO" "$@" ;; -json) json=yes ;; -tags=*) tags="${a#-tags=}" ;; esac
  done
  if [ "$json" = yes ]; then
    if [ "$tags" = "" ]; then echo "fake go: the no-tag discovery pass fails" >&2; exit 1; fi
    echo '{"ImportPath":"github.com/full-chaos/dev-health-ops/internal/auth/fakepkg"}'
    exit 0
  fi
  echo github.com/full-chaos/dev-health-ops/internal/auth/fakepkg
  exit 0
fi
for a; do case "$a" in scope-verdict) echo "$*" >> "$FAKE_DIR/test-args" ;; esac; done
exec "$FAKE_REAL_GO" "$@"
`

func TestAuthScopeSelftestFailsWhenAnEarlierDiscoveryPassFails(t *testing.T) {
	t.Parallel()
	code, errText, calls := runCheckGoWithFakeGo(t, []string{"auth", "scope-selftest"}, nil, scopePassFailFakeGo)
	if !strings.Contains(errText, "fake go: the no-tag discovery pass fails") {
		t.Fatalf("the fake go never failed the no-tag pass; rc %d, stderr: %s", code, tail(errText, 800))
	}
	if code != 2 || !strings.Contains(errText, "auth scope: the scope selftest refused") {
		t.Fatalf("a failed discovery pass must fail the scope selftest: rc %d, want 2; stderr: %s", code, tail(errText, 800))
	}
	// The refusal must come from the failed pass, not from a verdict over the passes that did run.
	if strings.Contains(calls, "scope-verdict") {
		t.Fatalf("the verdict ran after a failed discovery pass: %s", tail(calls, 400))
	}
}
