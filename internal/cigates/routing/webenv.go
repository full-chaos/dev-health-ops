package routing

import (
	"fmt"
	"io"
	"strings"
)

// The web env-name parity check (CHAOS-6987, D2736): names only, prod against bigboy. Prod's web env is deploy
// values.prod.yaml ops.web.env + ops.web.extraEnv; bigboy's bigboy-specific values are compose.bigboy.yml
// web.environment. The list stays hand-maintained (one name does not earn a generator); this check makes it LOUD:
//   - every prod web name must be CLASSIFIED below: a new prod name fails, named;
//   - every `overlay` name must be set in compose.bigboy.yml's web.environment;
//   - every classified name must still exist in prod: a stale entry fails, named;
//   - with --live-names FILE (container-env-names.sh output), every prod web name must be in the running container.
//
// Values are never read from the running stack; the overlay values checked are public URLs committed in the file.

const webEnvParityUsage = `usage: cigate web-env-parity <values.prod.yaml> <compose.bigboy.yml> [--live-names FILE]
Exit: 0 parity; 1 drift (each finding on stderr as "DRIFT: ..."); 2 usage/shape error.
`

type classified struct{ name, class string }

// webClassified is name -> "overlay" (compose.bigboy.yml sets the bigboy value) or "base: <reason>" (bigboy's own
// compose.yml sets a venue-local value on purpose). Order is the order findings are reported in.
var webClassified = []classified{
	{"BACKEND_URL", "overlay"},
	{"AUTH_URL", "overlay"},
	{"TRUST_PROXY", "overlay"},
	{"TRUSTED_PROXY_HOPS", "overlay"},
	{"ACR_API_ORIGIN", "base: compose.yml sets venue-local ACR wiring; ACR is not a bigboy parity target"},
	{"ACR_WEB_ASSERTION_ISSUER", "base: venue-local ACR wiring (compose.yml)"},
	{"ACR_WEB_ASSERTION_AUDIENCE", "base: venue-local ACR wiring (compose.yml)"},
	{"ACR_WEB_ASSERTION_KID", "base: venue-local ACR wiring (compose.yml)"},
	{"ACR_WEB_ASSERTION_KEY_FILE", "base: venue-local ACR wiring (compose.yml)"},
	// CHAOS-10137: social sign-in names from deploy #537/#538 (CHAOS-9847); bigboy's web gets them from compose.yml.
	{"AUTH_GITHUB_ID", "base: social sign-in credential set by compose.yml (CHAOS-9847)"},
	{"AUTH_GITHUB_SECRET", "base: social sign-in credential set by compose.yml (CHAOS-9847)"},
	{"AUTH_GOOGLE_ID", "base: social sign-in credential set by compose.yml (CHAOS-9847)"},
	{"AUTH_GOOGLE_SECRET", "base: social sign-in credential set by compose.yml (CHAOS-9847)"},
}

// webExpectedOverlay is the public, non-secret values the overlay must carry (D2724 router target, D2736 origin;
// CHAOS-7205: traefik is bigboy's one trusted proxy, so prod's TRUST_PROXY literals hold).
var webExpectedOverlay = []struct{ name, value string }{
	{"BACKEND_URL", "http://traefik:3000"},
	{"AUTH_URL", "https://www.commanderkeen.dev"},
	{"TRUST_PROXY", "true"},
	{"TRUSTED_PROXY_HOPS", "1"},
}

func isClassified(name any) bool {
	for _, c := range webClassified {
		if name == c.name {
			return true
		}
	}
	return false
}

func prodWebNames(doc any) []any {
	web, ok := subscript(doc, "ops")
	if ok {
		web, ok = subscript(web, "web")
	}
	if !ok {
		raise("check-web-env-parity: values has no ops.web block (shape changed?)")
	}
	var names []any
	env := or(get(web, "env"), &pyMap{})
	if m, isMap := env.(*pyMap); isMap {
		names = append(names, m.keys...)
	} else {
		for _, e := range iter(env) {
			names = append(names, item(e, "name"))
		}
	}
	for _, e := range iter(or(get(web, "extraEnv"), []any{})) {
		names = append(names, item(e, "name"))
	}
	if len(names) == 0 {
		raise("check-web-env-parity: prod web has ZERO env names -- refusing to pass on an empty set")
	}
	return names
}

// subscript is v[k] where a KeyError or TypeError is "not there", as `except (KeyError, TypeError)` read it.
func subscript(v, k any) (any, bool) {
	switch x := v.(type) {
	case *pyMap:
		return x.lookup(k)
	case []any:
		return nil, false
	case string:
		return nil, false
	}
	return nil, false
}

type overlayEnv struct {
	keys []string
	vals map[string]string
}

func overlayWebEnv(doc any) overlayEnv {
	env := or(get(or(get(or(get(or(doc, &pyMap{}), "services"), &pyMap{}), "web"), &pyMap{}), "environment"), &pyMap{})
	out := overlayEnv{vals: map[string]string{}}
	add := func(k, v string) {
		if _, seen := out.vals[k]; !seen {
			out.keys = append(out.keys, k)
		}
		out.vals[k] = v
	}
	if list, ok := env.([]any); ok {
		pairs := &pyMap{}
		for _, e := range list {
			k, v, found := strings.Cut(str(e, "environment item"), "=")
			if !found {
				raise("ValueError: dictionary update sequence element has length 1; 2 is required")
			}
			pairs.set(k, v)
		}
		env = pairs
	}
	m, ok := env.(*pyMap)
	if !ok {
		raise("AttributeError: '%s' object has no attribute 'items'", pyTypeName(env))
	}
	for i, k := range m.keys {
		add(pyStr(k), pyStr(m.vals[i]))
	}
	return out
}

func webEnvFindings(values, overlay any, live map[string]bool) []string {
	var findings []string
	prod := prodWebNames(values)
	env := overlayWebEnv(overlay)
	inProd := func(name string) bool {
		for _, p := range prod {
			if p == name {
				return true
			}
		}
		return false
	}
	for _, name := range prod {
		if !isClassified(name) {
			findings = append(findings, fmt.Sprintf("prod web env name %s is not classified -- decide overlay vs base in check-web-env-parity.py and set it on bigboy if it applies", pyStr(name)))
		}
	}
	for _, c := range webClassified {
		if !inProd(c.name) {
			findings = append(findings, fmt.Sprintf("classified name %s no longer exists in prod web env -- drop it from CLASSIFIED", c.name))
		}
		if _, set := env.vals[c.name]; c.class == "overlay" && !set {
			findings = append(findings, fmt.Sprintf("overlay name %s missing from compose.bigboy.yml web.environment", c.name))
		}
	}
	for _, want := range webExpectedOverlay {
		if got, set := env.vals[want.name]; set && got != want.value {
			findings = append(findings, fmt.Sprintf("overlay %s is not the expected public value %s", want.name, want.value))
		}
	}
	if live != nil {
		for _, name := range prod {
			if s, ok := name.(string); !ok || !live[s] {
				findings = append(findings, fmt.Sprintf("running web container has no env var named %s", pyStr(name)))
			}
		}
	}
	return findings
}

// pySplitlines is str.splitlines(): every line boundary Python knows, no trailing empty line.
func pySplitlines(s string) []string {
	var out []string
	start := 0
	runes := []rune(s)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case '\n', '\v', '\f', '\x1c', '\x1d', '\x1e', '\u0085', ' ', ' ':
			out = append(out, string(runes[start:i]))
			start = i + 1
		case '\r':
			out = append(out, string(runes[start:i]))
			if i+1 < len(runes) && runes[i+1] == '\n' {
				i++
			}
			start = i + 1
		}
	}
	if start < len(runes) {
		out = append(out, string(runes[start:]))
	}
	return out
}

// RunWebEnvParity is `cigate web-env-parity`. Exit 0 parity; 1 drift (each finding on stderr as `DRIFT: ...`); 2 usage
// or an empty --live-names file.
func RunWebEnvParity(args []string, deps Deps, stdout, stderr io.Writer) int {
	return guarded(stderr, func() int {
		rest := append([]string(nil), args...)
		var live map[string]bool
		for i, a := range rest {
			if a != "--live-names" {
				continue
			}
			if i+1 >= len(rest) {
				fmt.Fprintln(stderr, "usage: --live-names needs a file")
				return 2
			}
			raw, err := deps.Files.ReadFile(rest[i+1])
			if err != nil {
				raise("%v", err)
			}
			live = map[string]bool{}
			for _, ln := range pySplitlines(string(raw)) {
				if name := pyStrip(ln); name != "" {
					live[name] = true
				}
			}
			if len(live) == 0 {
				fmt.Fprintln(stderr, "check-web-env-parity: --live-names file is empty -- a names read that did not happen is not a pass")
				return 2
			}
			rest = append(rest[:i], rest[i+2:]...)
			break
		}
		if len(rest) != 2 {
			fmt.Fprint(stderr, webEnvParityUsage)
			return 2
		}
		valuesRaw, err := deps.Files.ReadFile(rest[0])
		if err != nil {
			raise("%v", err)
		}
		values := mustLoad(valuesRaw, false)
		prodWebNames(values)
		overlayRaw, err := deps.Files.ReadFile(rest[1])
		if err != nil {
			raise("%v", err)
		}
		findings := webEnvFindings(values, mustLoad(overlayRaw, true), live)
		for _, f := range findings {
			fmt.Fprintf(stderr, "DRIFT: %s\n", f)
		}
		if len(findings) > 0 {
			return 1
		}
		return 0
	})
}
