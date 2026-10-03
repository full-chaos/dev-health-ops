package ingressplanes

import (
	"bytes"
	"os"
	"sort"
	"strings"
	"testing"
)

// planeOfUpstream is written here, not taken from the generator: the address
// each plane has in the compose stack, as the checked-in file must name it.
var planeOfUpstream = map[string]string{
	"go-api:8000":    PlaneGoAPI,
	"query-api:8090": PlaneQueryAPI,
}

func checkedInRouterConfig(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(repoFile(t, RouterConfigPath))
	if err != nil {
		t.Fatalf("read the checked-in router configuration: %v", err)
	}
	return data
}

// TestCheckedInRouterConfigIsWhatTheContractGenerates is the regeneration
// check: generating again gives the checked-in bytes. It fails on a hand edit
// of the file and on a contract change that was not followed by a generate.
func TestCheckedInRouterConfigIsWhatTheContractGenerates(t *testing.T) {
	generated, err := Render(checkedInContract(t), DefaultOptions())
	if err != nil {
		t.Fatalf("render the checked-in contract: %v", err)
	}
	if onDisk := checkedInRouterConfig(t); !bytes.Equal(onDisk, generated) {
		t.Fatalf("%s is not what %s generates (%d bytes on disk, %d generated). Run from the repository root:\n  go run ./internal/ingressplanes/cmd/routerconf",
			RouterConfigPath, ContractPath, len(onDisk), len(generated))
	}
}

// TestCheckedInRouterConfigAgreesWithTheContract is the parse-and-compare
// check. It reads the checked-in file with the test's own nginx reading and
// compares the locations with the contract's rules: one location per rule and
// no other, each a case-insensitive regex of the rule's path, each to the
// upstream of the rule's plane, in the order the longest path comes first.
// It does not call the generator, so a generator that writes a wrong file
// (and so passes the regeneration check) fails here.
func TestCheckedInRouterConfigAgreesWithTheContract(t *testing.T) {
	contract := checkedInContract(t)
	file := readRouterFile(t, string(checkedInRouterConfig(t)))

	type entry struct{ modifier, pattern, plane string }
	var want []entry
	for _, r := range contract.Rules {
		want = append(want, entry{"~*", "^" + r.Path, r.Plane})
	}
	var got []entry
	for _, location := range file.locations {
		plane, known := planeOfUpstream[location.upstream]
		if !known {
			t.Errorf("location %s proxies to %q, which is not a plane of the compose stack (%v)", location.pattern, location.upstream, planeOfUpstream)
		}
		got = append(got, entry{location.modifier, location.pattern, plane})
	}

	// The same set: every rule has its location, and no location is without a rule.
	count := func(entries []entry) map[entry]int {
		counts := map[entry]int{}
		for _, e := range entries {
			counts[e]++
		}
		return counts
	}
	wantCount, gotCount := count(want), count(got)
	for e, n := range wantCount {
		if gotCount[e] != n {
			t.Errorf("the contract has rule %+v %d time(s), the file has it %d time(s)", e, n, gotCount[e])
		}
	}
	for e, n := range gotCount {
		if wantCount[e] == 0 {
			t.Errorf("the file has location %+v (%d), the contract has no such rule", e, n)
		}
	}

	// The order: nginx takes the first regex location that matches, so the
	// file must hold the longest path first and the default rule last.
	sort.SliceStable(want, func(i, j int) bool { return want[i].pattern > want[j].pattern })
	sort.SliceStable(want, func(i, j int) bool { return len(want[i].pattern) > len(want[j].pattern) })
	if len(got) == len(want) {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("location %d is %+v, want %+v: the locations are not in ingress-nginx's order", i, got[i], want[i])
			}
		}
	}
	if last := got[len(got)-1]; last.pattern != "^/" || last.plane != contract.DefaultPlane() {
		t.Errorf("the last location must be the default rule to %s, got %+v", contract.DefaultPlane(), last)
	}
}

// TestRouterConfigAddsNoCredential pins what keeps web's server-side proxy the
// only place that turns the session cookie into a bearer: the router has no
// directive that sets, reads or checks a credential, it proxies to the two
// planes only (never to web), and every directive it holds is one this test
// knows. A new directive must be added here by someone who read the README.
// The headers the router does set are held by TestRouterOwnsTheForwardedHeaders.
func TestRouterConfigAddsNoCredential(t *testing.T) {
	text := string(checkedInRouterConfig(t))
	top, err := parseNginx(text)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"worker_processes": true, "error_log": true, "pid": true, "events": true, "worker_connections": true,
		"http": true, "log_format": true, "access_log": true, "server_tokens": true, "resolver": true,
		"client_body_temp_path": true, "proxy_temp_path": true, "fastcgi_temp_path": true, "uwsgi_temp_path": true, "scgi_temp_path": true,
		"server": true, "listen": true, "server_name": true, "client_max_body_size": true,
		"proxy_http_version": true, "proxy_buffering": true, "proxy_read_timeout": true, "proxy_send_timeout": true,
		"proxy_set_header": true, "set": true, "location": true, "proxy_pass": true,
	}
	seen := 0
	var walk func([]directive)
	walk = func(directives []directive) {
		for _, d := range directives {
			seen++
			if !allowed[d.name] {
				t.Errorf("directive %q is not on the list of this test: say in the README why it adds no credential path, then add it", d.name)
			}
			for _, word := range append([]string{d.name}, d.args...) {
				lower := strings.ToLower(word)
				for _, banned := range []string{"authorization", "cookie", "auth_request", "auth_basic", "auth_jwt", "$http_auth", "$arg_", "bearer"} {
					if strings.Contains(lower, banned) {
						t.Errorf("directive %s %v names %q: the router must not set, read or check a credential", d.name, d.args, banned)
					}
				}
			}
			walk(d.block)
		}
	}
	walk(top)
	if seen < 20 {
		t.Fatalf("only %d directives were read: the file was not checked", seen)
	}
	upstreams := map[string]bool{}
	for _, location := range readRouterFile(t, text).locations {
		upstreams[location.upstream] = true
	}
	for upstream := range upstreams {
		if _, plane := planeOfUpstream[upstream]; !plane {
			t.Errorf("the router proxies to %q: only the two planes are allowed (a route to web here would let a browser request reach a plane with no bearer)", upstream)
		}
	}
	if len(upstreams) != len(planeOfUpstream) {
		t.Errorf("the router proxies to %v, want both planes %v", upstreams, planeOfUpstream)
	}
}

// TestRouterOwnsTheForwardedHeaders holds the header table of the checked-in
// file to the one ingress-nginx sends for a direct client with its default
// configuration (nginx.tmpl of controller-v1.14.5, with use-forwarded-headers
// and compute-full-forwarded-for off: $pass_access_scheme is $scheme,
// $pass_port is $server_port, $best_http_host is $http_host). The table is
// written here, not read from the generator. Every forwarded header comes from
// the router's own connection, so a value a client sent for one of them never
// reaches a plane; a client's X-Forwarded-For is replaced, not added to.
func TestRouterOwnsTheForwardedHeaders(t *testing.T) {
	want := [][2]string{
		{"Host", "$http_host"},
		{"X-Real-IP", "$remote_addr"},
		{"X-Forwarded-For", "$remote_addr"},
		{"X-Forwarded-Host", "$http_host"},
		{"X-Forwarded-Port", "$server_port"},
		{"X-Forwarded-Proto", "$scheme"},
		{"X-Forwarded-Scheme", "$scheme"},
		{"X-Scheme", "$scheme"},
		{"X-Original-Forwarded-For", "$http_x_forwarded_for"},
		{"X-Original-Forwarded-Host", "$http_x_forwarded_host"},
	}
	file := readRouterFile(t, string(checkedInRouterConfig(t)))
	var got [][2]string
	for _, header := range children(file.server, "proxy_set_header") {
		if len(header.args) != 2 {
			t.Fatalf("proxy_set_header with %d arguments: %v", len(header.args), header.args)
		}
		got = append(got, [2]string{header.args[0], header.args[1]})
	}
	if len(got) != len(want) {
		t.Fatalf("the server sets %d headers, want %d:\n got %v\nwant %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("header %d: got %v, want %v", i, got[i], want[i])
		}
	}
	// nginx gives a location the server's headers only when the location sets
	// none of its own, so no other block may hold the directive.
	top, err := parseNginx(string(checkedInRouterConfig(t)))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	var count func([]directive)
	count = func(directives []directive) {
		for _, d := range directives {
			if d.name == "proxy_set_header" {
				total++
			}
			count(d.block)
		}
	}
	count(top)
	if total != len(want) {
		t.Errorf("the file holds %d proxy_set_header directives, %d of them in the server block: a location with its own would drop the server's", total, len(want))
	}
}

// TestPlaneUpstreamsAreConstants holds that no request can choose where the
// router proxies to. A location proxies to a variable only so that nginx
// resolves the plane's name when a request comes (the router then starts while
// a plane is not there yet). So: the file has exactly two set directives, both
// in the server block, each gives a plane variable one literal host:port with
// no variable in it; every proxy_pass is one of those two variables and has no
// URI part; and no other directive of the file can give a variable a value.
func TestPlaneUpstreamsAreConstants(t *testing.T) {
	text := string(checkedInRouterConfig(t))
	top, err := parseNginx(text)
	if err != nil {
		t.Fatal(err)
	}
	file := readRouterFile(t, text)
	want := map[string]string{"$plane_go_api": "go-api:8000", "$plane_query_api": "query-api:8090"}

	inServer := children(file.server, "set")
	got := map[string]string{}
	for _, set := range inServer {
		if len(set.args) != 2 {
			t.Fatalf("set with %d arguments: %v", len(set.args), set.args)
		}
		if _, twice := got[set.args[0]]; twice {
			t.Errorf("%s is set more than once", set.args[0])
		}
		got[set.args[0]] = set.args[1]
		if strings.Contains(set.args[1], "$") {
			t.Errorf("set %s %q: the value holds a variable; a plane's address must be a literal", set.args[0], set.args[1])
		}
	}
	if len(got) != len(want) {
		t.Errorf("the server sets %v, want exactly %v", got, want)
	}
	for name, value := range want {
		if got[name] != value {
			t.Errorf("set %s is %q, want the literal %q", name, got[name], value)
		}
	}

	// Directives that give a variable a value. Only set is in the file, and
	// only in the server block.
	assigning := map[string]bool{
		"set": true, "map": true, "geo": true, "split_clients": true, "if": true, "rewrite": true,
		"auth_request_set": true, "perl_set": true, "js_set": true, "set_by_lua": true, "set_by_lua_block": true,
	}
	sets, passes := 0, 0
	var walk func(directives []directive, inLocation bool)
	walk = func(directives []directive, inLocation bool) {
		for _, d := range directives {
			if assigning[d.name] {
				if d.name != "set" {
					t.Errorf("directive %s %v can give a variable a value from the request", d.name, d.args)
				}
				if inLocation {
					t.Errorf("%s %v is inside a location: a plane variable is set once, in the server block", d.name, d.args)
				}
				sets++
			}
			if d.name == "proxy_pass" {
				passes++
				if len(d.args) != 1 {
					t.Fatalf("proxy_pass with %d arguments", len(d.args))
				}
				target, ok := strings.CutPrefix(d.args[0], "http://")
				if _, plane := want[target]; !ok || !plane {
					t.Errorf("proxy_pass %s: want http:// and one of the two plane variables, nothing after it", d.args[0])
				}
			}
			walk(d.block, inLocation || d.name == "location")
		}
	}
	walk(top, false)
	if sets != len(inServer) {
		t.Errorf("the file holds %d assigning directives, %d of them are the server's set directives", sets, len(inServer))
	}
	if passes != len(file.locations) || passes == 0 {
		t.Errorf("%d proxy_pass directives for %d locations", passes, len(file.locations))
	}
}
