package renderdiff

import _ "embed"

// kitUsage is the kit tool's module docstring (full-render-diff.py, sha256 ecf6771eb509): usage() prints it, so the
// port prints the same bytes. It lives in usage.txt, not in a Go string literal, because the text names SQL words.
//
//go:embed usage.txt
var kitUsage string
