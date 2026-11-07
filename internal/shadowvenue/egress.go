package shadowvenue

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
)

// StubHosts are the outside hosts the venue pins to the stub (cmd/venue-extstub Hosts).
var StubHosts = []string{"api.github.com", "api.pagerduty.com", "api.stripe.com", "github.com", "identity.pagerduty.com"}

// egressKeys are the keys of an egress record, each exactly once: the read's place (before, between, after), and the
// facts the venue reads from the running go-api (ADDENDUM-13267-1 P1-P4).
// The stub_* keys are the stub's own status file (cmd/venue-extstub -status): its mode, its answer-table digest and its
// start time; the run needs the frozen run mode in every read and one stub process across the reads.
var egressKeys = []string{"when", "network_internal", "networks", "default_route", "public_tcp", "public_dns", "stub_tcp", "hosts_pinned", "image",
	"stub_mode", "stub_config", "stub_started"}

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
		"hosts_pinned": strings.Join(hosts, ","), "stub_mode": "run"}
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
		if !imageDigest.MatchString(r.Fields["stub_config"]) {
			voids = append(voids, fmt.Sprintf("egress record %s: stub_config %q is not a sha256 digest", r.Name, r.Fields["stub_config"]))
		}
		if i > 0 && reads[0].Err == nil {
			for _, key := range []string{"image", "networks", "stub_config", "stub_started"} {
				if r.Fields[key] != reads[0].Fields[key] {
					voids = append(voids, fmt.Sprintf("egress record %s: %s %s differs from %s's %s", r.Name, key, r.Fields[key], reads[0].Name, reads[0].Fields[key]))
				}
			}
		}
	}
	return voids
}

// checkSeedLog voids a run when the stub's seed-mode log was not given while an external row ran, or when one of its
// lines falls inside the drive window [first sent start - skew, last sent end + grace]: a seed call during a run.
func checkSeedLog(slots []Slot, seed SeedLog, grace time.Duration) []string {
	if !seed.Given {
		return []string{"external rows were sent but the stub's seed-mode log was not given (-seed-log; an empty file is a log)"}
	}
	if seed.Err != nil {
		return []string{fmt.Sprintf("the seed-mode log is unreadable: %v", seed.Err)}
	}
	first, last := driveSpan(slots)
	var voids []string
	for _, hit := range seed.Hits {
		if !hit.At.Before(first.Add(-windowSkew)) && !hit.At.After(last.Add(grace)) {
			voids = append(voids, fmt.Sprintf("a seed-mode stub line at %s (%s %s %s%s) lies inside the drive window: seeding ran during the run",
				hit.At.Format(time.RFC3339Nano), hit.Service, hit.Method, hit.Host, hit.Path))
		}
	}
	return voids
}

// SeedLog is the stub's seed-mode log as the judge got it.
type SeedLog struct {
	Given bool
	Hits  []StubHit
	Err   error
}
