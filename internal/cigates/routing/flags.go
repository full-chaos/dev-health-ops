package routing

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// The query-api env generator (CHAOS-6967(b), CHAOS-6987/D2728): query-api's bigboy env derived from deploy's
// values.prod.yaml ops.queryApi.extraEnv, never a hand-copied list. Two classes are emitted:
//   - GO_API_*_ENABLED with the literal value "true", emitted as "true" (they gate REST/GraphQL operations at boot);
//   - an entry with valueFrom.secretKeyRef, emitted as a compose ${KEY} substitution of the Secret's OWN key name.
//
// The rendered text goes to --out with one file write and NEVER to stdout (CodeQL #2424-#2426: printing an env-var NAME
// a taint rule reads as secret-shaped). The text is reference names and ${...} syntax only, never a secret value: this
// process never reads one. compose.bigboy.yml is never written; bigboy-cut.sh diffs the output against it by name.

const queryAPIFlagsUsage = `usage: cigate query-api-flags <deploy values.prod.yaml> --out <path> [--format yaml|compose-env]
  yaml (default): an environment: mapping fragment (6-space indent, compose.bigboy.yml's style).
  compose-env: NAME=value-or-${REF} lines.
  The text goes to --out only; nothing is printed to stdout.
`

// skipNames are literal entries that are prod-topology-specific or set by another bigboy mechanism:
// OTEL_* point at prod's collector; QUERY_API_INTERNAL_ADDR is the internal listener bigboy does not split out.
var skipNames = map[string]bool{"OTEL_ENABLED": true, "OTEL_EXPORTER_OTLP_ENDPOINT": true, "QUERY_API_INTERNAL_ADDR": true}

type envRef struct{ name, label string }

// queryAPIEnv is (flags, refs), each sorted; a ref's label is the Secret's key name, a reference, never a value.
func queryAPIEnv(doc any) ([]envRef, []envRef) {
	env := item(item(item(doc, "ops"), "queryApi"), "extraEnv")
	var flags, refs []envRef
	for _, e := range iter(env) {
		get(e, "name")
		nameValue, found := e.(*pyMap).lookup("name")
		if !found {
			nameValue = ""
		}
		if s, ok := nameValue.(string); ok && skipNames[s] {
			continue
		}
		name := str(nameValue, "name")
		if strings.HasPrefix(name, "GO_API_") && strings.HasSuffix(name, "_ENABLED") && pyStr(get(e, "value")) == "true" {
			flags = append(flags, envRef{name, "true"})
			continue
		}
		source := get(e, "valueFrom")
		if truthy(source) && contains(source, "secretKeyRef") {
			label := get(item(source, "secretKeyRef"), "key")
			if truthy(label) {
				refs = append(refs, envRef{name, pyStr(label)})
			}
		}
	}
	if len(flags) == 0 {
		raise("generate-query-api-enabled-flags: found ZERO GO_API_*_ENABLED=true entries under ops.queryApi.extraEnv -- this almost certainly means the values.prod.yaml shape changed (key renamed/moved) rather than that every route was disabled. Refusing to emit an empty set silently; update this script's path (ops.queryApi.extraEnv) to match.")
	}
	if len(refs) == 0 {
		raise("generate-query-api-enabled-flags: found ZERO valueFrom.secretKeyRef entries under ops.queryApi.extraEnv -- this almost certainly means the values.prod.yaml shape changed rather than that query-api now needs no secrets at all. Refusing to emit an empty set silently; update this script's path (ops.queryApi.extraEnv) to match.")
	}
	byNameThenLabel := func(s []envRef) {
		sort.SliceStable(s, func(i, j int) bool {
			if s[i].name != s[j].name {
				return s[i].name < s[j].name
			}
			return s[i].label < s[j].label
		})
	}
	byNameThenLabel(flags)
	byNameThenLabel(refs)
	return flags, refs
}

func renderOverlay(flags, refs []envRef, format string) string {
	var lines []string
	if format == "compose-env" {
		for _, f := range flags {
			lines = append(lines, f.name+"="+f.label)
		}
		for _, r := range refs {
			lines = append(lines, r.name+"=${"+r.label+"}")
		}
	} else {
		for _, f := range flags {
			lines = append(lines, "      "+f.name+`: "`+f.label+`"`)
		}
		for _, r := range refs {
			lines = append(lines, "      "+r.name+": ${"+r.label+"}")
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

// RunQueryAPIFlags is `cigate query-api-flags`. Exit 0 written; 1 a values shape it cannot read (named on stderr);
// 2 usage. The stderr summary names counts and the out path only.
func RunQueryAPIFlags(args []string, deps Deps, stdout, stderr io.Writer) int {
	if len(args) < 1 {
		fmt.Fprint(stderr, queryAPIFlagsUsage)
		return 2
	}
	path, format, out := args[0], "yaml", ""
	rest := args[1:]
	for i := 0; i < len(rest); {
		switch {
		case rest[i] == "--format" && i+1 < len(rest):
			format = rest[i+1]
			i += 2
		case rest[i] == "--out" && i+1 < len(rest):
			out = rest[i+1]
			i += 2
		default:
			i++
		}
	}
	if out == "" {
		fmt.Fprintln(stderr, "generate-query-api-enabled-flags: --out <path> is required (this tool writes its rendered overlay to a file, never to stdout -- see this module's own doc comment).")
		return 2
	}
	return guarded(stderr, func() int {
		raw, err := deps.Files.ReadFile(path)
		if err != nil {
			raise("%v", err)
		}
		flags, refs := queryAPIEnv(mustLoad(raw, false))
		if err := deps.Files.WriteFile(out, []byte(renderOverlay(flags, refs, format))); err != nil {
			raise("%v", err)
		}
		fmt.Fprintf(stderr, "# %d flags, %d secret refs -> %s\n", len(flags), len(refs), out)
		return 0
	})
}
