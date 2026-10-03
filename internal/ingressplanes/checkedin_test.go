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
	headers := map[string]string{}
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
			if d.name == "proxy_set_header" && len(d.args) == 2 {
				headers[d.args[0]] = d.args[1]
			}
			walk(d.block)
		}
	}
	walk(top)
	if seen < 20 {
		t.Fatalf("only %d directives were read: the file was not checked", seen)
	}
	wantHeaders := map[string]string{
		"Host":            "$http_host",
		"X-Real-IP":       "$remote_addr",
		"X-Forwarded-For": "$proxy_add_x_forwarded_for",
	}
	if len(headers) != len(wantHeaders) {
		t.Errorf("the router sets headers %v, want exactly %v", headers, wantHeaders)
	}
	for name, value := range wantHeaders {
		if headers[name] != value {
			t.Errorf("proxy_set_header %s is %q, want %q", name, headers[name], value)
		}
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
