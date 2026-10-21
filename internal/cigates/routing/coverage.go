package routing

import (
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// The route coverage probe (R467): every path the plane-split router sends to a Go plane must have a handler in the
// RUNNING build. Deploy values can run ahead of the deployed image; traffic for such a path would get a 404.
//
// One request per routed path with the non-standard method ROUTEPROBE. Both Go planes use net/http's method-aware
// mux: a path with a handler answers 405 and a path with none answers 404, before any handler, auth or database code
// runs, so the probe executes nothing and needs no credential. A handler behind auth middleware may answer 401 first;
// that also proves a handler is registered.
//
// LIMIT: a registered STUB is indistinguishable from a real handler. The control for "values ahead of the build" is
// the pinned deploy sha (plane-split-router --deploy-sha); this check adds the 404 class on top.

const routeCoverageUsage = `usage: cigate route-coverage <values.prod.yaml> [--go-api-url http://127.0.0.1:8093]
                            [--query-api-url http://127.0.0.1:8091]
Exit: 0 every routed path registered; 1 any path missing/unexpected (named on stderr); 2 usage.
`

const (
	probeMethod = "ROUTEPROBE"
	placeholder = "routeprobe"
)

var probeParamRe = regexp.MustCompile(`\{[A-Za-z0-9_]+\}`)

func probePaths(doc any) ([]string, []string) {
	ingress := item(doc, "ingress")
	var goPaths, queryPaths []string
	for _, e := range iter(item(ingress, "goApiPaths")) {
		goPaths = append(goPaths, probeParamRe.ReplaceAllLiteralString(str(item(e, "path"), "path"), placeholder))
	}
	for _, e := range iter(item(ingress, "queryApiPaths")) {
		queryPaths = append(queryPaths, strings.ReplaceAll(str(item(e, "path"), "path"), wildcardRegex, placeholder))
	}
	if len(goPaths) == 0 || len(queryPaths) == 0 {
		raise("check-route-coverage: values has ZERO goApiPaths or queryApiPaths -- refusing to pass")
	}
	return goPaths, queryPaths
}

// target is a plane URL's host and port; only http on 127.0.0.1 or localhost, with no path, is accepted.
func target(raw string) (string, int) {
	refuse := func() { raise("check-route-coverage: plane URL must be http://127.0.0.1:<port>, got %s", pyRepr(raw)) }
	u, err := url.Parse(raw)
	if err != nil {
		refuse()
	}
	host := strings.ToLower(u.Hostname())
	if strings.ToLower(u.Scheme) != "http" || (host != "127.0.0.1" && host != "localhost") || (u.Path != "" && u.Path != "/") {
		refuse()
	}
	port := 80
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 65535 {
			raise("ValueError: Port out of range 0-65535")
		}
		// Python read the port as `parts.port or 80`: an explicit :0 is port 80 too.
		if n != 0 {
			port = n
		}
	}
	return host, port
}

// RunRouteCoverage is `cigate route-coverage`. Exit 0 every routed path registered; 1 any path missing or answering an
// unexpected status (each named on stderr); 2 usage.
func RunRouteCoverage(args []string, deps Deps, stdout, stderr io.Writer) int {
	opts, pos, err := parseArgs(args, []string{"--go-api-url", "--query-api-url"})
	if err == nil && len(pos) != 1 {
		if len(pos) == 0 {
			err = fmt.Errorf("the following arguments are required: values")
		} else {
			err = fmt.Errorf("unrecognized arguments: %s", strings.Join(pos[1:], " "))
		}
	}
	if err != nil {
		return usageError(stdout, stderr, routeCoverageUsage, "cigate route-coverage", err)
	}
	goURL, queryURL := "http://127.0.0.1:8093", "http://127.0.0.1:8091"
	if v, ok := opts["--go-api-url"]; ok {
		goURL = v
	}
	if v, ok := opts["--query-api-url"]; ok {
		queryURL = v
	}
	return guarded(stderr, func() int {
		raw, err := deps.Files.ReadFile(pos[0])
		if err != nil {
			raise("%v", err)
		}
		goPaths, queryPaths := probePaths(mustLoad(raw, false))
		// Python's http.client refused a request target with a non-ASCII character (UnicodeEncodeError) or a control
		// or space character (InvalidURL). Such a path is refused here too, before any probe (D6129).
		for _, p := range append(append([]string{}, goPaths...), queryPaths...) {
			if !sendablePath(p) {
				raise("check-route-coverage: routed path %s cannot be sent as a request target (a non-ASCII, control or space character): refusing before any probe", pyRepr(p))
			}
		}
		failed := false
		for _, plane := range []struct {
			name, url string
			paths     []string
		}{{"go-api", goURL, goPaths}, {"query-api", queryURL, queryPaths}} {
			host, port := target(plane.url)
			// Python kept the statuses in a dict by path: a path routed twice is probed twice and counted once.
			var order []string
			statuses := map[string]int{}
			for _, p := range plane.paths {
				status, err := deps.Prober.Probe(host, port, probeMethod, p)
				if err != nil {
					raise("%v", err)
				}
				if _, seen := statuses[p]; !seen {
					order = append(order, p)
				}
				statuses[p] = status
			}
			var missing, unexpected []string
			for _, p := range order {
				if statuses[p] == 404 {
					missing = append(missing, p)
				}
			}
			for _, p := range order {
				switch statuses[p] {
				case 401, 404, 405:
				default:
					unexpected = append(unexpected, fmt.Sprintf("%s=%d", p, statuses[p]))
				}
			}
			for _, p := range missing {
				fmt.Fprintf(stderr, "DRIFT: %s has NO handler for routed path %s (404) -- values run ahead of the running build\n", plane.name, p)
			}
			for _, u := range unexpected {
				fmt.Fprintf(stderr, "DRIFT: %s answered an unexpected status for %s (want 405)\n", plane.name, u)
			}
			fmt.Fprintf(stdout, "ROUTE_COVERAGE plane=%s routed=%d registered=%d missing=%d unexpected=%d\n", plane.name, len(plane.paths), len(plane.paths)-len(missing)-len(unexpected), len(missing), len(unexpected))
			failed = failed || len(missing) > 0 || len(unexpected) > 0
		}
		if failed {
			return 1
		}
		return 0
	})
}

// sendablePath reports whether Python's http.client would put the path on a request line: ASCII only, and none of
// the characters it rejects in a URL (0x00-0x20 and 0x7f).
func sendablePath(p string) bool {
	for i := 0; i < len(p); i++ {
		if c := p[i]; c <= 0x20 || c >= 0x7f {
			return false
		}
	}
	return true
}
