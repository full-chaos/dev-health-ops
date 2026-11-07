package shadowvenue

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// StubHosts are the outside hosts the venue pins to the stub (cmd/venue-extstub Hosts).
var StubHosts = []string{"api.github.com", "api.pagerduty.com", "api.stripe.com", "github.com", "identity.pagerduty.com"}

// egressKeys are the keys of an egress record, each exactly once: the read's place (before, between, after), and the
// facts the venue reads from the running go-api (ADDENDUM-13267-1 P1-P4).
var egressKeys = []string{"when", "network_internal", "networks", "default_route", "public_tcp", "public_dns", "stub_tcp", "hosts_pinned", "image"}

var imageDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// EgressRead is one egress record file as the judge got it: its name, its key=value lines, or the error that made it
// unreadable (a void, not a usage error).
type EgressRead struct {
	Name   string
	Fields map[string]string
	Err    error
}

// ParseEgress reads one egress record: key=value lines of egressKeys, each at most once, no other key, no empty value.
// A key that is missing fails checkEgress.
func ParseEgress(name string, raw []byte) EgressRead {
	read := EgressRead{Name: name, Fields: map[string]string{}}
	for n, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		switch {
		case !ok || value == "":
			read.Err = fmt.Errorf("line %d is not key=value", n+1)
		case !slices.Contains(egressKeys, key):
			read.Err = fmt.Errorf("line %d: unknown key %q", n+1, key)
		case read.Fields[key] != "":
			read.Err = fmt.Errorf("line %d: key %s twice", n+1, key)
		}
		if read.Err != nil {
			return read
		}
		read.Fields[key] = value
	}
	return read
}

// checkEgress holds the egress reads of a run to the no-egress form: exactly the reads named by want, in that order,
// each readable and saying the go-api is on one internal network with no default route, a refused public TCP connect,
// a failing public DNS lookup, a reachable stub and the stub hosts pinned; the image digest and the network equal in
// every read. Each problem is a void.
func checkEgress(reads []EgressRead, want []string) []string {
	var voids []string
	if len(reads) != len(want) {
		return []string{fmt.Sprintf("%d egress records, want %d (%s): the no-egress form is not proven", len(reads), len(want), strings.Join(want, ", "))}
	}
	hosts := slices.Clone(StubHosts)
	slices.Sort(hosts)
	expect := map[string]string{"network_internal": "true", "default_route": "none", "public_tcp": "refused", "public_dns": "fail", "stub_tcp": "ok",
		"hosts_pinned": strings.Join(hosts, ",")}
	for i, r := range reads {
		if r.Err != nil {
			voids = append(voids, fmt.Sprintf("egress record %s is unreadable: %v", r.Name, r.Err))
			continue
		}
		if r.Fields["when"] != want[i] {
			voids = append(voids, fmt.Sprintf("egress record %s is the %q read, want %q", r.Name, r.Fields["when"], want[i]))
		}
		for _, key := range egressKeys {
			if w, fixed := expect[key]; fixed && r.Fields[key] != w {
				voids = append(voids, fmt.Sprintf("egress record %s: %s=%s, want %s", r.Name, key, r.Fields[key], w))
			}
		}
		if strings.Contains(r.Fields["networks"], ",") {
			voids = append(voids, fmt.Sprintf("egress record %s: go-api is on networks %s, want one internal network", r.Name, r.Fields["networks"]))
		}
		if !imageDigest.MatchString(r.Fields["image"]) {
			voids = append(voids, fmt.Sprintf("egress record %s: image %q is not a sha256 digest", r.Name, r.Fields["image"]))
		}
		if i > 0 && reads[0].Err == nil {
			for _, key := range []string{"image", "networks"} {
				if r.Fields[key] != reads[0].Fields[key] {
					voids = append(voids, fmt.Sprintf("egress record %s: %s %s differs from %s's %s", r.Name, key, r.Fields[key], reads[0].Name, reads[0].Fields[key]))
				}
			}
		}
	}
	return voids
}
