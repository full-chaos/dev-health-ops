package shadowvenue

import (
	"fmt"
	"net"
	"regexp"
	"slices"
	"strings"
	"time"
)

// StubHosts is the frozen list of the outside hosts the venue pins to the stub: the five it serves (cmd/venue-extstub
// Hosts) and every other code-constant host go-api can dial (UnservedHosts there; ADDENDUM-13267-3 section 2, acp Q-C1).
// The egress record's hosts_pinned must equal it.
var StubHosts = []string{
	"accounts.google.com", "api.anthropic.com", "api.atlassian.com", "api.eu.pagerduty.com", "api.github.com", "api.linear.app",
	"api.openai.com", "api.pagerduty.com", "api.resend.com", "api.stripe.com", "api.typesafe.ai", "app.launchdarkly.com",
	"connect.stripe.com", "files.stripe.com", "generativelanguage.googleapis.com", "github.com", "gitlab.com",
	"identity.pagerduty.com", "meter-events.stripe.com", "oauth2.googleapis.com", "www.googleapis.com",
}

// egressKeys are the keys of an egress record, each exactly once: the read's place (before, between, after), and the
// facts the venue reads from the running go-api (ADDENDUM-13267-1 P1-P4).
// dynamic_hosts=unset states that GITHUB_URL/GITLAB_URL, the credential base URLs, OIDC/SAML URLs and the BYO LLM
// base_url are unset for org E and the T pool (acp Q-C1 C hosts). The stub_* keys are the stub's own status file (cmd/venue-extstub -status): its mode, its answer-table digest and its
// start time; the run needs the frozen run mode in every read and one stub process across the reads.
var egressKeys = []string{"when", "network_internal", "networks", "default_route", "public_tcp", "public_dns", "stub_tcp", "hosts_pinned", "image",
	"stub_mode", "stub_config", "stub_started", "goapi_ip", "run_id", "dynamic_hosts"}

// ingressKey is the one optional key, at most once: none, or the comma list of the venue-ingress IPs a drive may sign
// in through (CheckVenueBase). A record without it parses as before.
const ingressKey = "ingress_ips"

// noRun is the run_id of the read after the last drive, which precedes no drive.
const noRun = "none"

var imageDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// EgressRead is one egress record file as the judge got it: its name, its key=value lines, or the error that made it
// unreadable (a void, not a usage error).
type EgressRead struct {
	Name   string
	Fields map[string]string
	Err    error
}

// ParseEgress reads one egress record: key=value lines, every key of egressKeys exactly once, no other key, no empty
// value. run_id names the drive the read precedes (the driver's -run-id), or none for the read after the last drive.
func ParseEgress(name string, raw []byte) EgressRead {
	read := EgressRead{Name: name, Fields: map[string]string{}}
	seen := map[string]bool{}
	for n, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		key, value, ok := strings.Cut(line, "=")
		switch {
		case !ok:
			read.Err = fmt.Errorf("line %d is not key=value", n+1)
		case !slices.Contains(egressKeys, key) && key != ingressKey:
			read.Err = fmt.Errorf("line %d: unknown key %q", n+1, key)
		case seen[key]:
			read.Err = fmt.Errorf("line %d: key %s twice", n+1, key)
		case value == "":
			read.Err = fmt.Errorf("line %d: key %s has an empty value", n+1, key)
		}
		if read.Err != nil {
			return read
		}
		seen[key] = true
		read.Fields[key] = value
	}
	for _, key := range egressKeys {
		if !seen[key] {
			read.Err = fmt.Errorf("key %s missing", key)
			return read
		}
	}
	if value, ok := read.Fields[ingressKey]; ok {
		if _, err := parseIngressIPs(value); err != nil {
			read.Err = fmt.Errorf("key %s: %w", ingressKey, err)
		}
	}
	return read
}

// parseIngressIPs reads an ingress_ips value: none, or distinct literal IPs in their plain form, none of them loopback
// or unspecified; no CIDR, host name or empty entry.
func parseIngressIPs(value string) ([]net.IP, error) {
	if value == "none" {
		return nil, nil
	}
	var ips []net.IP
	seen := map[string]bool{}
	for _, entry := range strings.Split(value, ",") {
		ip := net.ParseIP(entry)
		switch {
		case ip == nil || ip.String() != entry:
			return nil, fmt.Errorf("%q is not a literal IP in its plain form", entry)
		case ip.IsLoopback() || ip.IsUnspecified():
			return nil, fmt.Errorf("%s is loopback or unspecified", entry)
		case seen[entry]:
			return nil, fmt.Errorf("%s twice", entry)
		}
		seen[entry] = true
		ips = append(ips, ip)
	}
	return ips, nil
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
		"hosts_pinned": strings.Join(hosts, ","), "stub_mode": "run", "dynamic_hosts": "unset"}
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
		if (want[i] == "after") != (r.Fields["run_id"] == noRun) {
			voids = append(voids, fmt.Sprintf("egress record %s: run_id %q: the %s read must name %s", r.Name, r.Fields["run_id"], want[i],
				map[bool]string{true: "none", false: "the drive it precedes"}[want[i] == "after"]))
		}
		for j := 0; j < i; j++ {
			if reads[j].Err == nil && r.Fields["run_id"] == reads[j].Fields["run_id"] {
				voids = append(voids, fmt.Sprintf("egress record %s names run %q like %s: each read precedes its own drive", r.Name, r.Fields["run_id"], reads[j].Name))
			}
		}
		if !net.ParseIP(r.Fields["goapi_ip"]).IsPrivate() {
			voids = append(voids, fmt.Sprintf("egress record %s: goapi_ip %q is not a private IP", r.Name, r.Fields["goapi_ip"]))
		}
		if i > 0 && reads[0].Err == nil {
			for _, key := range []string{"image", "networks", "stub_config", "stub_started", "goapi_ip", ingressKey} {
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
