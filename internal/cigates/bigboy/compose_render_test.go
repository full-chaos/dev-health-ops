package bigboy_test

import (
	"fmt"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// No docker on the runners: composeRender does what `docker compose config` does, on the parsed file.

const (
	composeFile          = "ci/bigboy/compose.bigboy.yml"
	composePins          = "ci/bigboy/images.env"
	composeProject       = "/bigboy-root"
	composeOperatorImage = "ghcr.io/full-chaos/dev-health-go-operator@sha256:0000000000000000000000000000000000000000000000000000000000000000"
)

var composeStackServices = strings.Fields(`bugsink clickhouse go-api go-contractcheck go-reconciler go-river-provision go-scheduler go-stream-external
go-stream-ingest go-stream-pagerduty go-sync-dispatch-route-activate go-sync-finalize-route-activate go-sync-post-route-activate
go-sync-reference-route-activate go-worker go-worker-consumers-ready go-worker-heavy go-worker-migrate go-worker-ops go-worker-ready
go-worker-route-activate mailpit migrate pgbouncer pgbouncer-river-coordinator pgbouncer-river-queue postgres query-api riverui
traefik valkey web`)

var composeAllOneOffs = []string{"smoke", "venue", "venue-hook", "venue-river"}

type composeIn struct {
	profiles []string
	env      map[string]string
	pins     func(string) string
	opsEnv   string
}

type composeOut struct {
	top      doc
	services map[string]doc
	images   []string
	err      string
}

func composeEnv(extra map[string]string) map[string]string {
	env := map[string]string{
		"BIGBOY_OPERATOR_IMAGE": composeOperatorImage,
		"DHO_API_CH_USERS_XML":  composeProject + "/.go-api-dev/dho_api_ch.xml",
	}
	for k, v := range extra {
		env[k] = v
	}
	return env
}

func composeEnvFile(text string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(strings.TrimPrefix(line, "export "), "=")
		if !ok {
			continue
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' && v[len(v)-1] == '"' || v[0] == '\'' && v[len(v)-1] == '\'') {
			v = v[1 : len(v)-1]
		}
		out[strings.TrimSpace(k)] = v
	}
	return out
}

func composePinsMap(text string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(text, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, _ := strings.Cut(line, "=")
		out[k] = v
	}
	return out
}

func composeDropLine(prefix string) func(string) string {
	return func(text string) string {
		var keep []string
		for _, line := range strings.Split(text, "\n") {
			if !strings.HasPrefix(line, prefix) {
				keep = append(keep, line)
			}
		}
		return strings.Join(keep, "\n")
	}
}

func composeIsName(c byte, first bool) bool {
	return c == '_' || c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || !first && c >= '0' && c <= '9'
}

func composeInterpolate(s string, env map[string]string) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); {
		if s[i] != '$' || i+1 >= len(s) {
			b.WriteByte(s[i])
			i++
			continue
		}
		switch n := s[i+1]; {
		case n == '$':
			b.WriteByte('$')
			i += 2
		case n == '{':
			end := composeBraceEnd(s, i+2)
			if end < 0 {
				return "", fmt.Errorf("invalid template: %q", s)
			}
			v, err := composeExpand(s[i+2:end], env)
			if err != nil {
				return "", err
			}
			b.WriteString(v)
			i = end + 1
		case composeIsName(n, true):
			j := i + 1
			for j < len(s) && composeIsName(s[j], false) {
				j++
			}
			b.WriteString(env[s[i+1:j]])
			i = j
		default:
			b.WriteByte('$')
			i++
		}
	}
	return b.String(), nil
}

func composeBraceEnd(s string, from int) int {
	depth := 0
	for j := from; j < len(s); j++ {
		switch {
		case s[j] == '$' && j+1 < len(s) && s[j+1] == '{':
			depth++
			j++
		case s[j] == '}':
			if depth == 0 {
				return j
			}
			depth--
		}
	}
	return -1
}

func composeExpand(body string, env map[string]string) (string, error) {
	n := 0
	for n < len(body) && composeIsName(body[n], n == 0) {
		n++
	}
	name, rest := body[:n], body[n:]
	if name == "" {
		return "", fmt.Errorf("invalid interpolation format for %q", body)
	}
	val, set := env[name]
	full := set && val != ""
	sub := func(p string) (string, error) { return composeInterpolate(rest[len(p):], env) }
	switch {
	case rest == "":
		return val, nil
	case strings.HasPrefix(rest, ":-"):
		if !full {
			return sub(":-")
		}
	case strings.HasPrefix(rest, "-"):
		if !set {
			return sub("-")
		}
	case strings.HasPrefix(rest, ":?"), strings.HasPrefix(rest, "?"):
		if strings.HasPrefix(rest, ":?") && !full || !set {
			msg, _ := sub(rest[:strings.Index(rest, "?")+1])
			return "", fmt.Errorf("required variable %s is missing a value: %s", name, msg)
		}
	case strings.HasPrefix(rest, ":+"):
		if full {
			return sub(":+")
		}
		return "", nil
	case strings.HasPrefix(rest, "+"):
		if set {
			return sub("+")
		}
		return "", nil
	default:
		return "", fmt.Errorf("invalid interpolation format for %q", body)
	}
	return val, nil
}

func composeWalk(v any, env map[string]string, errs *[]string) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = composeWalk(e, env, errs)
		}
	case []any:
		for i, e := range x {
			x[i] = composeWalk(e, env, errs)
		}
	case string:
		out, err := composeInterpolate(x, env)
		if err != nil {
			*errs = append(*errs, err.Error())
		}
		return out
	}
	return v
}

func composeRender(g *guard, in composeIn) composeOut {
	pins := g.read(composePins)
	if in.pins != nil {
		pins = in.pins(pins)
	}
	env := composeEnvFile(in.opsEnv)
	for k, v := range composeEnvFile(pins) {
		env[k] = v
	}
	proc := in.env
	if proc == nil {
		proc = composeEnv(nil)
	}
	for k, v := range proc {
		env[k] = v
	}
	var raw doc
	if err := yaml.Unmarshal([]byte(g.read(composeFile)), &raw); err != nil {
		return composeOut{err: err.Error()}
	}
	var errs []string
	composeWalk(raw, env, &errs)
	if len(errs) > 0 {
		sort.Strings(errs)
		return composeOut{err: strings.Join(errs, "\n")}
	}
	out := composeOut{top: raw, services: map[string]doc{}}
	for name, s := range mp(raw["services"]) {
		svc := mp(s)
		if !composeActive(strs(svc["profiles"]), in.profiles) {
			continue
		}
		svc = composeNormalise(svc, env)
		out.services[name] = svc
		out.images = append(out.images, str(svc["image"]))
	}
	for _, sec := range mp(raw["secrets"]) {
		if m := mp(sec); m != nil && m["file"] != nil {
			m["file"] = composeAbs(str(m["file"]))
		}
	}
	sort.Strings(out.images)
	return out
}

func composeActive(own, selected []string) bool {
	if len(own) == 0 {
		return true
	}
	for _, p := range own {
		for _, s := range selected {
			if p == s {
				return true
			}
		}
	}
	return false
}

func composeAbs(p string) string {
	if p == "" || strings.HasPrefix(p, "/") {
		return p
	}
	return path.Join(composeProject, p)
}

func composeNormalise(s doc, env map[string]string) doc {
	if e, ok := s["environment"]; ok {
		s["environment"] = composeEnvMap(e, env)
	}
	var ports []any
	for _, p := range list(s["ports"]) {
		ports = append(ports, composePort(p))
	}
	if ports != nil {
		s["ports"] = ports
	}
	for _, v := range list(s["volumes"]) {
		if m := mp(v); m != nil && str(m["type"]) == "bind" {
			m["source"] = composeAbs(str(m["source"]))
		}
	}
	if l, ok := s["depends_on"].([]any); ok {
		deps := doc{}
		for _, d := range l {
			deps[str(d)] = doc{"condition": "service_started", "required": true}
		}
		s["depends_on"] = deps
	}
	if l, ok := s["networks"].([]any); ok {
		nets := doc{}
		for _, n := range l {
			nets[str(n)] = nil
		}
		s["networks"] = nets
	}
	if l, ok := s["secrets"].([]any); ok {
		var secs []any
		for _, x := range l {
			if m := mp(x); m != nil {
				secs = append(secs, m)
			} else {
				secs = append(secs, doc{"source": str(x)})
			}
		}
		s["secrets"] = secs
	}
	return s
}

func composeEnvMap(v any, env map[string]string) doc {
	out := doc{}
	put := func(k string, val any, has bool) {
		if has && val != nil {
			out[k] = str(val)
		} else if e, ok := env[k]; ok {
			out[k] = e
		}
	}
	switch x := v.(type) {
	case []any:
		for _, item := range x {
			k, val, has := strings.Cut(str(item), "=")
			put(k, val, has)
		}
	case map[string]any:
		for k, val := range x {
			put(k, val, true)
		}
	}
	return out
}

var composePortRe = regexp.MustCompile(`^(?:(.+):)?([0-9]+):([0-9]+)(?:/(tcp|udp))?$`)

func composePort(p any) doc {
	if m := mp(p); m != nil {
		t, _ := strconv.Atoi(str(m["target"]))
		m["target"], m["published"] = t, str(m["published"])
		return m
	}
	parts := composePortRe.FindStringSubmatch(str(p))
	if parts == nil {
		return doc{"invalid": str(p)}
	}
	t, _ := strconv.Atoi(parts[3])
	proto := parts[4]
	if proto == "" {
		proto = "tcp"
	}
	return doc{"mode": "ingress", "host_ip": parts[1], "published": parts[2], "target": t, "protocol": proto}
}

var composeRequiredRe = regexp.MustCompile(`\$\{([A-Za-z_]+):\?`)

func composeRequired(g *guard) []string {
	seen := map[string]bool{}
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			for _, e := range x {
				walk(e)
			}
		case []any:
			for _, e := range x {
				walk(e)
			}
		case string:
			for _, m := range composeRequiredRe.FindAllStringSubmatch(x, -1) {
				seen[m[1]] = true
			}
		}
	}
	walk(g.yamlFile(composeFile))
	return composeSorted(seen)
}

func composeScalars(v any) []string {
	var out []string
	switch x := v.(type) {
	case map[string]any:
		for _, e := range x {
			out = append(out, composeScalars(e)...)
		}
	case []any:
		for _, e := range x {
			out = append(out, composeScalars(e)...)
		}
	case string:
		out = append(out, x)
	}
	return out
}

func composeSorted[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func composeSet(items ...string) map[string]bool {
	out := map[string]bool{}
	for _, i := range items {
		out[i] = true
	}
	return out
}

func composeAdded(base, more composeOut) []string {
	var out []string
	for name := range more.services {
		if _, ok := base.services[name]; !ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func composeMust(g *guard, in composeIn) composeOut {
	out := composeRender(g, in)
	if out.err != "" {
		g.failf("compose config (profiles %v) does not render: %s", in.profiles, out.err)
	}
	return out
}

func composeSortedStrs(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
