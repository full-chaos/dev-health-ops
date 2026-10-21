package routing

import (
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// The plane-split router generator (CHAOS-6987, D2724): bigboy's traefik router derived from deploy's values.prod.yaml
// ingress.goApiPaths + ingress.queryApiPaths, never hand-maintained, so the router cannot drift from what prod's
// ingress does.
//
// Bigboy has no k8s Ingress; traefik is the router. Web's own catch-all Host() rule has a low priority; this generator
// emits higher-priority Host()+PathRegexp() rules for go-api and query-api, so matching paths reach those planes and
// everything else falls through to web (which calls its BACKEND_URL, this router, on the internal host name).
//
// Path syntax: goApiPaths use {param} whole-segment templates; queryApiPaths use pathType Exact (literal) or
// ImplementationSpecific (an inline [^/]+ placeholder). Both become one traefik PathRegexp per plane.
//
// An entry of the ops chart's allow-list (ops.ingress.pythonAllowList) goes to the Python api unless it names its
// backend with `service: query-api` (prod changes a path's backend INSIDE the Ingress object that holds it). Such an
// entry is a query-api path of this router. An ANCHORED allow-list entry (`/literal$`) matches case-insensitively
// (CHAOS-7925), as ingress-nginx does in regex mode; every other path is case-sensitive.
//
// Pinned mode (R467): the values come from values.prod.yaml AT --deploy-sha, and the run REFUSES (exit 3) unless that
// deploy commit's vendor/dev-health-ops pin is --expect-ops-sha, the build bigboy runs.

const planeSplitUsage = `usage: cigate plane-split-router <values.prod.yaml> [--format labels|dynamic]
       cigate plane-split-router --deploy-repo <deploy repo> --deploy-sha <sha> --expect-ops-sha <ops sha>
                                 [--values-out FILE] [--format labels|dynamic]
  labels (default): the docker-compose label lines for go-api's and query-api's services.
  dynamic: the traefik file-provider config (what bigboy loads from .traefik-dynamic/planes.yml).
  Pinned mode (R467): the values are values.prod.yaml AT --deploy-sha; exit 3 unless that deploy commit's
  vendor/dev-health-ops pin is --expect-ops-sha. --values-out also writes those values to FILE.
`

// HostRule is the router's only host. `traefik` ONLY, never the public host (D2733): a rule that matched the public
// host let a browser request skip web and its proxy.ts (the only place a session cookie becomes a bearer) and land on
// a plane with no credential. `traefik` is web's own server-side BACKEND_URL target, whose calls carry the bearer.
const HostRule = "Host(`traefik`)"

const (
	wildcard      = "\x00"
	wildcardRegex = "[^/]+"
	sharedList    = "ops.ingress.pythonAllowList"
	// BigboyLocalSource names the bigboy-local Python paths. CHAOS-8361: there are none (bigboy runs no Python api).
	BigboyLocalSource = "the bigboy-local Python paths (BIGBOY_LOCAL_PYTHON_PATHS in this script)"
)

var (
	paramRe = regexp.MustCompile(`\{[a-zA-Z0-9_]+\}`)
	// The anchored shape the ops chart accepts: literal segments, `\.` for a dot, exactly one trailing `$`.
	anchoredEntry = regexp.MustCompile(`^(?:/(?:[A-Za-z0-9_-]|\\\.)+)+\$$`)
	hexRe         = regexp.MustCompile(`^[0-9a-f]+$`)
)

// Segments is a Go plane path: per segment, the literal parts around its wildcards (each one or more characters of
// ONE segment). The emitted regex and the two-backends check are both derived from it.
type Segments [][]string

type entry struct{ path, pathType any }

func segmentsOf(text string) Segments {
	var out Segments
	for _, seg := range strings.Split(text, "/") {
		out = append(out, strings.Split(seg, wildcard))
	}
	return out
}

func goapiPathSegments(path any) Segments {
	return segmentsOf(paramRe.ReplaceAllLiteralString(str(path, "re.sub() string"), wildcard))
}

func queryapiEntrySegments(path, pathType any) Segments {
	p := str(path, "path")
	if pathType == "Exact" {
		return segmentsOf(p)
	}
	return segmentsOf(strings.ReplaceAll(p, wildcardRegex, wildcard))
}

// pyEscape is Python's re.escape (3.7+): a backslash before each of ()[]{}?*+-|^$\.&~# and whitespace \t\n\r\v\f.
func pyEscape(s string) string {
	var b strings.Builder
	for _, r := range s {
		if strings.ContainsRune("()[]{}?*+-|^$\\.&~# \t\n\r\v\f", r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

// unescapeHyphen drops re.escape's needless `\-`: it is not a YAML double-quoted escape.
func unescapeHyphen(s string) string { return strings.ReplaceAll(s, `\-`, "-") }

func segmentsRegex(segs Segments) string {
	out := make([]string, len(segs))
	for i, seg := range segs {
		parts := make([]string, len(seg))
		for j, p := range seg {
			parts[j] = unescapeHyphen(pyEscape(p))
		}
		out[i] = strings.Join(parts, wildcardRegex)
	}
	return strings.Join(out, "/")
}

// CombinedRegex is one plane's PathRegexp body.
func CombinedRegex(paths []string) string { return "^(" + strings.Join(paths, "|") + ")$" }

// pinError is a pinned deploy sha the running build cannot use.
type pinError struct{ msg string }

func runGit(g Git, repo string, args ...string) (string, *pinError) {
	out, err := g.Run(repo, args...)
	if err != nil {
		ge, ok := err.(*GitError)
		if !ok {
			raise("%v", err)
		}
		msg := ge.Stderr
		head := args
		if len(head) > 2 {
			head = head[:2]
		}
		return "", &pinError{fmt.Sprintf("git %s failed in %s: %s", strings.Join(head, " "), repo, firstRunes(pyStrip(msg), 200))}
	}
	// Python read git's stdout as text: universal newlines.
	return strings.ReplaceAll(strings.ReplaceAll(out, "\r\n", "\n"), "\r", "\n"), nil
}

func firstRunes(s string, n int) string {
	if utf8.RuneCountInString(s) <= n {
		return s
	}
	return string([]rune(s)[:n])
}

// valuesAtSHA is (full deploy sha, vendored ops sha, values.prod.yaml text); it refuses a vendor pin mismatch.
func valuesAtSHA(g Git, repo, deploySHA, expectOpsSHA string) (string, string, string, *pinError) {
	full, perr := runGit(g, repo, "rev-parse", "--verify", deploySHA+"^{commit}")
	if perr != nil {
		return "", "", "", perr
	}
	full = pyStrip(full)
	tree, perr := runGit(g, repo, "ls-tree", full, "vendor/dev-health-ops")
	if perr != nil {
		return "", "", "", perr
	}
	fields := strings.FieldsFunc(tree, pyIsSpace)
	if len(fields) < 3 || fields[1] != "commit" {
		return "", "", "", &pinError{fmt.Sprintf("deploy %s has no vendor/dev-health-ops gitlink", firstRunes(full, 12))}
	}
	vendored := fields[2]
	want := strings.ToLower(pyStrip(expectOpsSHA))
	if utf8.RuneCountInString(want) < 7 || !hexRe.MatchString(want) || !strings.HasPrefix(vendored, want) {
		shown := firstRunes(want, 12)
		if shown == "" {
			shown = "<empty>"
		}
		return "", "", "", &pinError{fmt.Sprintf("deploy %s vendors ops %s, but the running build is %s -- refusing to route paths the running build may not serve (R467)", firstRunes(full, 12), firstRunes(vendored, 12), shown)}
	}
	text, perr := runGit(g, repo, "show", full+":values.prod.yaml")
	if perr != nil {
		return "", "", "", perr
	}
	return full, vendored, text, nil
}

// pathsFromDoc is the go-api and query-api path regexes of the values.
func pathsFromDoc(doc any) ([]string, []string) {
	ingress := item(doc, "ingress")
	var goPaths, queryPaths []string
	for _, e := range iter(item(ingress, "goApiPaths")) {
		goPaths = append(goPaths, segmentsRegex(goapiPathSegments(item(e, "path"))))
	}
	for _, e := range iter(item(ingress, "queryApiPaths")) {
		path := item(e, "path")
		queryPaths = append(queryPaths, segmentsRegex(queryapiEntrySegments(path, item(e, "pathType"))))
	}
	// An allow-list entry that names query-api is a query-api path of this router, from whichever list in use it is
	// on; each is routed once. CHAOS-7925: an anchored entry matches case-insensitively.
	for _, qa := range queryAPIAllowListEntries(doc) {
		re := "(?i:" + segmentsRegex(qa.segments) + ")"
		if !containsString(queryPaths, re) {
			queryPaths = append(queryPaths, re)
		}
	}
	if len(goPaths) == 0 {
		raise("generate-plane-split-router: found ZERO goApiPaths entries -- refusing to emit an empty router (values.prod.yaml shape may have changed)")
	}
	if len(queryPaths) == 0 {
		raise("generate-plane-split-router: found ZERO queryApiPaths entries -- refusing to emit an empty router (values.prod.yaml shape may have changed)")
	}
	return goPaths, queryPaths
}

func containsString(list []string, s string) bool {
	for _, e := range list {
		if e == s {
			return true
		}
	}
	return false
}

// EmitLabels is the docker-compose label lines for the two planes.
func EmitLabels(goRegex, queryRegex string) string {
	return strings.Join([]string{
		"      # go-api service labels (add to its compose block):",
		`      traefik.enable: "true"`,
		"      traefik.scope: dev-health",
		`      traefik.http.routers.go-api-paths.rule: "` + HostRule + " && PathRegexp(`" + goRegex + "`)\"",
		"      traefik.http.routers.go-api-paths.entrypoints: web",
		`      traefik.http.routers.go-api-paths.priority: "1000"`,
		`      traefik.http.services.go-api-paths.loadbalancer.server.port: "8000"`,
		"",
		"      # query-api service labels (add to its compose block):",
		`      traefik.enable: "true"`,
		"      traefik.scope: dev-health",
		`      traefik.http.routers.query-api-paths.rule: "` + HostRule + " && PathRegexp(`" + queryRegex + "`)\"",
		"      traefik.http.routers.query-api-paths.entrypoints: web",
		`      traefik.http.routers.query-api-paths.priority: "1000"`,
		`      traefik.http.services.query-api-paths.loadbalancer.server.port: "8090"`,
	}, "\n")
}

// entryService is the backend an allow-list entry names: api (also with no `service` key) or query-api.
func entryService(e any, source string) string {
	service := any("api")
	if contains(e, "service") {
		service = item(e, "service")
	}
	if service != "api" && service != "query-api" {
		raise("generate-plane-split-router: %s entry %s has service %s: an allow-list entry routes to api (the Python api, the default) or to query-api", source, pyRepr(get(e, "path")), pyRepr(service))
	}
	return service.(string)
}

type namedList struct {
	name    string
	entries any
}

func opsIngress(doc any) any { return or(get(or(get(doc, "ops"), &pyMap{}), "ingress"), &pyMap{}) }

// allowLists is every allow-list of the values: the shared one, then each host's own.
func allowLists(doc any) []namedList {
	ingress := opsIngress(doc)
	lists := []namedList{{sharedList, or(get(ingress, "pythonAllowList"), []any{})}}
	for _, host := range iter(or(get(ingress, "hosts"), []any{})) {
		if _, ok := host.(*pyMap); !ok {
			continue
		}
		if own, ok := get(host, "pythonAllowList").([]any); ok {
			lists = append(lists, namedList{hostListName(host), own})
		}
	}
	return lists
}

func opsHosts(doc any) []any {
	var out []any
	for _, host := range iter(or(get(opsIngress(doc), "hosts"), []any{})) {
		if _, ok := host.(*pyMap); ok {
			out = append(out, host)
		}
	}
	return out
}

func hostListName(host any) string {
	return "ops.ingress.hosts[" + pyStr(get(host, "host")) + "].pythonAllowList"
}

// allowListsInUse is the lists the ops chart renders rules from: the shared one when a host opts into it
// (`pythonAllowList: true`) or no host is listed, and each host's own.
func allowListsInUse(doc any) []namedList {
	ingress := opsIngress(doc)
	hosts := opsHosts(doc)
	var lists []namedList
	optIn := len(hosts) == 0
	for _, host := range hosts {
		if get(host, "pythonAllowList") == true {
			optIn = true
		}
	}
	if optIn {
		lists = append(lists, namedList{sharedList, or(get(ingress, "pythonAllowList"), []any{})})
	}
	for _, host := range hosts {
		if own, ok := get(host, "pythonAllowList").([]any); ok {
			lists = append(lists, namedList{hostListName(host), own})
		}
	}
	return lists
}

type queryAPIEntry struct {
	source         string
	path, pathType any
	segments       Segments
}

// queryAPIAllowListEntries is every entry that names query-api on an allow-list in use. Such an entry must be ANCHORED
// (ImplementationSpecific `/literal$`), the one shape that is one path on every host; any other shape is refused.
func queryAPIAllowListEntries(doc any) []queryAPIEntry {
	var found []queryAPIEntry
	for _, l := range allowListsInUse(doc) {
		for _, e := range iter(l.entries) {
			if entryService(e, l.name) != "query-api" {
				continue
			}
			path, pathType := item(e, "path"), item(e, "pathType")
			if !(pathType == "ImplementationSpecific" && anchoredEntry.MatchString(str(path, "expected string"))) {
				raise("generate-plane-split-router: %s entry {path: %s, pathType: %s} names query-api but is not an anchored entry (pathType ImplementationSpecific, path `/literal$`): write it anchored, the one shape that is one path on every host", l.name, pyStr(path), pyStr(pathType))
			}
			literal := allowListLiteral(path, pathType)
			for _, seg := range strings.Split(literal, "/") {
				if seg == "." || seg == ".." {
					raise("generate-plane-split-router: %s entry {path: %s, pathType: %s} names query-api but has a `.` or `..` segment, which the ops chart refuses", l.name, pyStr(path), pyStr(pathType))
				}
			}
			found = append(found, queryAPIEntry{l.name, path, pathType, segmentsOf(literal)})
		}
	}
	return found
}

// queryAPIPathsThatDifferByHost is one line per host of the ops Ingress that does not send to query-api a path an
// allow-list entry sends there. Each host must carry the entry on the list it uses with no path of its own beside
// `/`, or be a web host (all its paths to web).
func queryAPIPathsThatDifferByHost(doc any) []string {
	var order []any
	carried := map[string][]string{}
	for _, qa := range queryAPIAllowListEntries(doc) {
		key := pyRepr(qa.path)
		if _, seen := carried[key]; !seen {
			order = append(order, qa.path)
		}
		carried[key] = append(carried[key], qa.source)
	}
	var found []string
	for _, host := range opsHosts(doc) {
		own := get(host, "pythonAllowList")
		var source string
		if _, ok := own.([]any); ok {
			source = hostListName(host)
		} else if own == true {
			source = sharedList
		} else {
			set := map[string]bool{}
			for _, p := range iter(or(get(host, "paths"), []any{})) {
				if _, ok := p.(*pyMap); ok {
					set[pyStr(get(p, "service"))] = true
				}
			}
			services := sortedKeys(set)
			if !(len(services) == 1 && services[0] == "web") {
				shown := strings.Join(services, ", ")
				if shown == "" {
					shown = "none"
				}
				for _, path := range order {
					found = append(found, fmt.Sprintf("%s is sent to query-api by %s, and host %s has no allow-list: its own paths answer it (services: %s)", pyStr(path), strings.Join(carried[pyRepr(path)], ", "), pyStr(get(host, "host")), shown))
				}
			}
			continue
		}
		for _, path := range order {
			have := carried[pyRepr(path)]
			if !containsString(have, source) {
				found = append(found, fmt.Sprintf("%s is sent to query-api by %s and not by %s (host %s)", pyStr(path), strings.Join(have, ", "), source, pyStr(get(host, "host"))))
			}
		}
		var ownPaths []string
		for _, p := range iter(or(get(host, "paths"), []any{})) {
			if _, ok := p.(*pyMap); ok && get(p, "path") != "/" {
				ownPaths = append(ownPaths, pyStr(get(p, "path")))
			}
		}
		if len(ownPaths) > 0 {
			for _, path := range order {
				found = append(found, fmt.Sprintf("%s is sent to query-api by %s, and host %s has paths of its own beside `/`: %s", pyStr(path), strings.Join(carried[pyRepr(path)], ", "), pyStr(get(host, "host")), strings.Join(ownPaths, ", ")))
			}
		}
	}
	return found
}

// pythonAllowListFromDoc is the shared list's entries the Python api answers; nil when the values have no shared
// list (the default list, empty since CHAOS-6263).
func pythonAllowListFromDoc(doc any) []entry {
	entries := get(opsIngress(doc), "pythonAllowList")
	if entries == nil {
		return []entry{}
	}
	out := []entry{}
	for _, e := range iter(entries) {
		if entryService(e, sharedList) == "api" {
			out = append(out, entry{item(e, "path"), item(e, "pathType")})
		}
	}
	return out
}

// allowListLiteral is the literal path an entry names (an anchored entry without its `$`, `\.` as `.`).
func allowListLiteral(path, pathType any) string {
	p := str(path, "path")
	if pathType == "ImplementationSpecific" {
		return strings.ReplaceAll(strings.TrimSuffix(p, "$"), `\.`, ".")
	}
	return p
}

type namedEntries struct {
	name    string
	entries []entry
}

// pythonSources is every list that puts a path on the Python api: the shared list, each host's own (what prod's
// ingress routes on that host), and the bigboy-local paths (none).
func pythonSources(doc any) []namedEntries {
	sources := []namedEntries{{sharedList, pythonAllowListFromDoc(doc)}}
	for _, l := range allowLists(doc)[1:] {
		list := []entry{}
		for _, e := range iter(l.entries) {
			if entryService(e, l.name) == "api" {
				list = append(list, entry{item(e, "path"), item(e, "pathType")})
			}
		}
		sources = append(sources, namedEntries{l.name, list})
	}
	return append(sources, namedEntries{BigboyLocalSource, []entry{}})
}

type claimedPath struct {
	plane    string
	path     any
	segments Segments
}

func pathTables(doc any) []claimedPath {
	ingress := item(doc, "ingress")
	var claimed []claimedPath
	for _, e := range iter(item(ingress, "goApiPaths")) {
		claimed = append(claimed, claimedPath{"ingress.goApiPaths", item(e, "path"), goapiPathSegments(item(e, "path"))})
	}
	for _, e := range iter(item(ingress, "queryApiPaths")) {
		claimed = append(claimed, claimedPath{"ingress.queryApiPaths", item(e, "path"), queryapiEntrySegments(item(e, "path"), item(e, "pathType"))})
	}
	return claimed
}

// goPlanePaths is every path a Go plane claims: both path tables, and each allow-list entry that names query-api.
func goPlanePaths(doc any) []claimedPath {
	claimed := pathTables(doc)
	for _, qa := range queryAPIAllowListEntries(doc) {
		claimed = append(claimed, claimedPath{qa.source + " (service: query-api)", qa.path, qa.segments})
	}
	return claimed
}

// pathsInTwoObjects is one line per allow-list entry that names query-api while a path table also claims its path:
// prod renders them in different Ingress objects, and the admission webhook denies two objects for one host and path.
func pathsInTwoObjects(doc any) []string {
	tables := pathTables(doc)
	var found []string
	for _, qa := range queryAPIAllowListEntries(doc) {
		for _, t := range tables {
			if pythonEntryOverlaps(qa.path, qa.pathType, t.segments) {
				found = append(found, fmt.Sprintf("%s entry {path: %s, pathType: %s, service: query-api} and %s entry %s", qa.source, pyStr(qa.path), pyStr(qa.pathType), t.plane, pyStr(t.path)))
			}
		}
	}
	return found
}

func segmentPattern(parts []string) string {
	quoted := make([]string, len(parts))
	for i, p := range parts {
		quoted[i] = regexp.QuoteMeta(p)
	}
	return strings.Join(quoted, wildcardRegex)
}

// segmentIs reports whether a Go plane segment matches exactly this text.
func segmentIs(parts []string, text string) bool {
	return regexp.MustCompile(`^(?:` + segmentPattern(parts) + `)$`).MatchString(text)
}

// segmentCanStartWith reports whether a Go plane segment matches some text that starts with head.
func segmentCanStartWith(parts []string, head string) bool {
	if strings.HasPrefix(parts[0], head) {
		return true
	}
	return len(parts) > 1 && strings.HasPrefix(head, parts[0])
}

// pythonEntryOverlaps reports whether some request path is matched by BOTH this Python entry and this Go plane path.
// Exact and anchored entries match one path; a Prefix is read at its widest (every path that starts with its text,
// what ingress-nginx renders in regex mode). A Go wildcard never crosses a "/", so the answer is exact.
func pythonEntryOverlaps(path, pathType any, segs Segments) bool {
	if pathType == "Prefix" {
		want := strings.Split(str(path, "path"), "/")
		if len(segs) < len(want) {
			return false
		}
		for i := 0; i < len(want)-1; i++ {
			if !segmentIs(segs[i], want[i]) {
				return false
			}
		}
		return segmentCanStartWith(segs[len(want)-1], want[len(want)-1])
	}
	want := strings.Split(allowListLiteral(path, pathType), "/")
	if len(segs) != len(want) {
		return false
	}
	for i, text := range want {
		if !segmentIs(segs[i], text) {
			return false
		}
	}
	return true
}

// pathsOnTwoPlanes is one line per Python entry a Go plane path overlaps, and per entry of a shape that cannot be
// checked. One path on two backends is a tie-break, not a route: bigboy would prove a route prod may not have.
func pathsOnTwoPlanes(doc any) []string {
	claimed := goPlanePaths(doc)
	var found []string
	for _, src := range pythonSources(doc) {
		for _, e := range src.entries {
			desc := fmt.Sprintf("%s entry {path: %s, pathType: %s}", src.name, pyStr(e.path), pyStr(e.pathType))
			if !(e.pathType == "Prefix" || e.pathType == "Exact" || e.pathType == "ImplementationSpecific") ||
				(e.pathType == "ImplementationSpecific" && !anchoredEntry.MatchString(str(e.path, "expected string"))) {
				found = append(found, desc+" is not a shape the ops chart accepts (Prefix, Exact, or an anchored ImplementationSpecific `/literal$`), so it cannot be checked against the Go planes")
				continue
			}
			for _, c := range claimed {
				if pythonEntryOverlaps(e.path, e.pathType, c.segments) {
					found = append(found, fmt.Sprintf("%s and %s entry %s", desc, c.plane, pyStr(c.path)))
				}
			}
		}
	}
	return found
}

// traefikPathRule is the Python router's path terms: Prefix = the path or anything under it; an anchored entry is one
// case-insensitive whole-path regex (CHAOS-7243, CHAOS-7925), its dot written `[.]` (no `\.` in a YAML double-quoted
// scalar).
func traefikPathRule(entries []entry) string {
	var terms []string
	for _, e := range entries {
		if e.pathType == "ImplementationSpecific" {
			literal := allowListLiteral(e.path, e.pathType)
			body := strings.ReplaceAll(unescapeHyphen(pyEscape(literal)), `\.`, "[.]")
			terms = append(terms, "PathRegexp(`(?i)^"+body+"$`)")
			continue
		}
		terms = append(terms, "Path(`"+pyStr(e.path)+"`)")
		if e.pathType == "Prefix" {
			terms = append(terms, "PathPrefix(`"+strings.TrimRight(str(e.path, "path"), "/")+"/`)")
		}
	}
	return "(" + strings.Join(terms, " || ") + ")"
}

// EmitDynamicConfig is the traefik FILE-PROVIDER config: the planes by address only, so applying it touches traefik
// (which hot-reloads the file) and nothing else. CHAOS-8361: the Python router and service are written only when an
// allow-list entry names the Python api.
func EmitDynamicConfig(goRegex, queryRegex string, pythonAllow []entry) string {
	pythonRouter, pythonService := "", ""
	if len(pythonAllow) > 0 {
		pythonRouter = `    # CHAOS-7047: the paths the Python api answers (an entry of the ops chart's
    # ingress.pythonAllowList that names it), below the two plane routers, above the default backend.
    python-allowlist:
      rule: "` + HostRule + " && " + traefikPathRule(pythonAllow) + `"
      entryPoints: [web]
      priority: 500
      service: python-allowlist
`
		pythonService = `    python-allowlist:
      loadBalancer:
        servers:
          - url: "http://api:8000"
`
	}
	return `# GENERATED by generate-plane-split-router.py from deploy/values.prod.yaml's
# ingress.goApiPaths + ingress.queryApiPaths -- do not hand-edit the rule values, regenerate.
# Traefik file-provider dynamic config (mounted into the traefik container, hot-reloaded).
http:
  routers:
    go-api-paths:
      rule: "` + HostRule + " && PathRegexp(`" + goRegex + "`)" + `"
      entryPoints: [web]
      priority: 1000
      service: go-api-paths
    query-api-paths:
      rule: "` + HostRule + " && PathRegexp(`" + queryRegex + "`)" + `"
      entryPoints: [web]
      priority: 1000
      service: query-api-paths
` + pythonRouter + `    # Everything else on the internal traefik hostname (web's own server-side BACKEND_URL calls)
    # falls through to the GO api (its native 404 answers an unknown path), mirroring prod's
    # default backend. Without a rule, a non-split path called via BACKEND_URL=http://traefik:3000
    # would 404 at traefik itself, with no plane header.
    api-internal-catchall:
      rule: "Host(` + "`traefik`" + `)"
      entryPoints: [web]
      priority: 1
      service: api-internal-catchall
  services:
    go-api-paths:
      loadBalancer:
        servers:
          - url: "http://go-api:8000"
    query-api-paths:
      loadBalancer:
        servers:
          - url: "http://query-api:8090"
` + pythonService + `    api-internal-catchall:
      loadBalancer:
        servers:
          - url: "http://go-api:8000"
`
}

// RunPlaneSplitRouter is `cigate plane-split-router`. Exit 0 emitted; 1 a values shape it cannot read; 2 usage;
// 3 pinned deploy sha refused; 4 one path on two backends, hosts or Ingress objects; 5 a path no rule can carry.
func RunPlaneSplitRouter(args []string, deps Deps, stdout, stderr io.Writer) int {
	opts, pos, err := parseArgs(args, []string{"--format", "--deploy-repo", "--deploy-sha", "--expect-ops-sha", "--values-out"})
	if err == nil && len(pos) > 1 {
		err = fmt.Errorf("unrecognized arguments: %s", strings.Join(pos[1:], " "))
	}
	format := "labels"
	if f, ok := opts["--format"]; ok {
		format = f
		if err == nil && f != "labels" && f != "dynamic" {
			err = fmt.Errorf("argument --format: invalid choice: %s (choose from 'labels', 'dynamic')", pyRepr(f))
		}
	}
	if err != nil {
		return usageError(stdout, stderr, planeSplitUsage, "cigate plane-split-router", err)
	}
	values := ""
	if len(pos) == 1 {
		values = pos[0]
	}
	repo, sha, expect := opts["--deploy-repo"], opts["--deploy-sha"], opts["--expect-ops-sha"]
	anyPinned, allPinned := repo != "" || sha != "" || expect != "", repo != "" && sha != "" && expect != ""
	if values != "" && anyPinned {
		fmt.Fprintln(stderr, "give EITHER a values file OR --deploy-repo/--deploy-sha/--expect-ops-sha")
		return 2
	}
	if values == "" && !allPinned {
		fmt.Fprint(stderr, planeSplitUsage)
		return 2
	}
	return guarded(stderr, func() int {
		var doc any
		var goPaths, queryPaths []string
		var pythonAllow []entry
		if values != "" {
			raw, err := deps.Files.ReadFile(values)
			if err != nil {
				raise("%v", err)
			}
			doc = mustLoad(raw, false)
			goPaths, queryPaths = pathsFromDoc(doc)
			pythonAllow = pythonAllowListFromDoc(doc)
		} else {
			full, vendored, text, perr := valuesAtSHA(deps.Git, repo, sha, expect)
			if perr != nil {
				fmt.Fprintf(stderr, "REFUSED: %s\n", perr.msg)
				return 3
			}
			doc = mustLoad([]byte(text), false)
			goPaths, queryPaths = pathsFromDoc(doc)
			pythonAllow = pythonAllowListFromDoc(doc)
			if out := opts["--values-out"]; out != "" {
				if err := deps.Files.WriteFile(out, []byte(text)); err != nil {
					raise("%v", err)
				}
			}
			fmt.Fprintf(stderr, "# deploy_sha=%s ops_vendor=%s\n", full, vendored)
		}
		return emitRouter(doc, goPaths, queryPaths, pythonAllow, format, stdout, stderr)
	})
}

func mustLoad(raw []byte, composeTags bool) any {
	doc, err := loadYAML(raw, composeTags)
	if err != nil {
		raise("yaml: %v", err)
	}
	return doc
}

func emitRouter(doc any, goPaths, queryPaths []string, pythonAllow []entry, format string, stdout, stderr io.Writer) int {
	// The rule sits in a double-quoted YAML scalar, which cannot carry a regex escape: the file would not load and
	// traefik would keep its old router with no failing step. Refuse instead.
	var unwritable []string
	for _, c := range goPlanePaths(doc) {
		if strings.Contains(segmentsRegex(c.segments), `\`) {
			unwritable = append(unwritable, c.plane+" entry "+pyStr(c.path))
		}
	}
	if len(unwritable) > 0 {
		fmt.Fprintln(stderr, "REFUSED: no valid rule can be emitted for "+strings.Join(unwritable, ", ")+": the path has a character that needs a regex escape (a dot, for one), and the emitted rule cannot carry an escape. This generator does not support such a path yet.")
		return 5
	}
	goRegex, queryRegex := CombinedRegex(goPaths), CombinedRegex(queryPaths)
	if both := pathsOnTwoPlanes(doc); len(both) > 0 {
		fmt.Fprintln(stderr, "REFUSED: one path, two backends. A path the Python api is given and a path a Go plane is given can match the same request. Take the path off every Python list in the same values change that moves it to the Go plane:\n  "+strings.Join(both, "\n  "))
		return 4
	}
	if perHost := queryAPIPathsThatDifferByHost(doc); len(perHost) > 0 {
		fmt.Fprintln(stderr, "REFUSED: one path, a backend that differs by host. An allow-list entry sends the path to query-api, and a host of the ops Ingress answers it from another backend. This router has one host and cannot show both. Every host must carry the entry on the allow-list it uses (the shared one or its own) and have no path of its own beside `/`, or be a web host (all paths to web):\n  "+strings.Join(perHost, "\n  "))
		return 4
	}
	if twice := pathsInTwoObjects(doc); len(twice) > 0 {
		fmt.Fprintln(stderr, "REFUSED: one path, two Ingress objects. An allow-list entry that names query-api and an entry of ingress.goApiPaths or ingress.queryApiPaths can match the same request. Prod's ingress admission denies two live Ingress objects for one host and path. Keep the path in ONE place:\n  "+strings.Join(twice, "\n  "))
		return 4
	}
	var out string
	if format == "dynamic" {
		out = EmitDynamicConfig(goRegex, queryRegex, pythonAllow)
	} else {
		out = EmitLabels(goRegex, queryRegex)
	}
	fmt.Fprintln(stdout, out)
	tables := length(item(item(doc, "ingress"), "queryApiPaths"))
	fmt.Fprintf(stderr, "# %d goApiPaths, %d query-api paths (%d queryApiPaths, %d allow-list entries that name query-api)\n", len(goPaths), len(queryPaths), tables, len(queryPaths)-tables)
	return 0
}

func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
