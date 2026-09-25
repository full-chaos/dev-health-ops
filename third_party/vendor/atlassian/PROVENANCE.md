# Vendored: github.com/full-chaos/atlassian (go/ directory)

Source: https://github.com/full-chaos/atlassian at cb7c3665cb90 (2026-07-16).
Copied unmodified: go/go.mod, go/atlassian/ (the Go module's library tree), LICENSE. Tests and tools directories are not copied.

Why vendored: the upstream go.mod declares `module atlassian`, which the Go proxy refuses when fetched as
github.com/full-chaos/atlassian/go. Ops requires it as `atlassian` and points it here with a replace directive.
Replace this directory with a normal tagged require once upstream declares its real module path.

Location: the path contains a `vendor` element on purpose: ci/check_go.sh skips `*/vendor/*` when it discovers Go
modules and when it checks formatting, so third-party code is neither vetted nor gofmt-checked as ops code. The root
module reaches it only through the replace directive in go.mod.
