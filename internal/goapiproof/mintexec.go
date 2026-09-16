package goapiproof

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// This file exists because go.lang.security.audit.dangerous-exec-command
// flags ANY exec.Command/exec.CommandContext call whose first argument is
// not a Go string literal at the call site -- confirmed empirically: a
// value looked up from a map of literals, or a variable already checked
// against an allowlist, still trips the rule; only a literal written
// directly in the call expression clears it. A per-caller exec.Command
// over an operator-supplied path (go-api-rest-prove's own earlier shape)
// can therefore never satisfy it without a `nosemgrep` suppression, which
// this repo's own rule forbids for an alert like this one. This file is
// the one place that owns the literal-per-case shape, so every credential
// minter in this service goes through it instead of re-deriving it.
//
// mintHelperMaxBytes bounds a minting helper's captured stdout -- a
// runaway or misbehaving helper must not be read without limit.
const mintHelperMaxBytes = 8 << 10

// MintViaAllowlistedHelper runs one of a fixed, compiled-in set of
// credential-minting helpers and returns its trimmed stdout.
//
// helperName SELECTS a helper; it is never a path, and it is never what
// reaches exec.CommandContext. Every case below names its executable with
// a Go string literal -- extending the set is a code-reviewed,
// recompiled change, never something an operator's flag value can steer.
// Both paths are the dev-health-go-api-tools image's own baked-in
// locations (docs/operate/runbooks/query-api-bootstrap.md and
// docs/contribute/architecture/go-api-wave-0-proof-infrastructure.md both
// document them at these exact paths), built from cmd/mint-envelope and
// cmd/mint-edge-token respectively.
//
// args are passed through unchanged as the helper's own argv[1:] -- they
// are operator-supplied data for the helper to interpret (e.g. "-org",
// "<org>"), never a second executable path, so nothing about them can
// steer WHICH binary runs.
//
// The helper's stderr and a failure's stdout are never reported by the
// returned error, matching go-api-prove's own minting contract: a helper
// that printed a secret to either stream on failure must not leak it into
// this tool's own output.
func MintViaAllowlistedHelper(ctx context.Context, helperName string, args []string) (string, error) {
	var cmd *exec.Cmd
	switch helperName {
	case "mint-envelope":
		cmd = exec.CommandContext(ctx, "/usr/local/bin/mint-envelope", args...)
	case "mint-edge-token":
		cmd = exec.CommandContext(ctx, "/usr/local/bin/mint-edge-token", args...)
	default:
		return "", fmt.Errorf("goapiproof: %q is not an allowlisted minting helper -- expected \"mint-envelope\" or \"mint-edge-token\"", helperName)
	}
	var stdout bytes.Buffer
	cmd.Stdout = &mintLimitedWriter{w: &stdout, max: mintHelperMaxBytes}
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("goapiproof: run minting helper %q: %w", helperName, err)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// mintLimitedWriter caps how many bytes a subprocess's stdout is captured
// to, silently dropping bytes past the cap rather than erroring: the cap
// exists to bound memory, not to judge the helper's output.
type mintLimitedWriter struct {
	w   io.Writer
	max int
	n   int
}

func (l *mintLimitedWriter) Write(p []byte) (int, error) {
	if l.n >= l.max {
		return len(p), nil
	}
	remaining := l.max - l.n
	if len(p) > remaining {
		p = p[:remaining]
	}
	n, err := l.w.Write(p)
	l.n += n
	return len(p), err
}
