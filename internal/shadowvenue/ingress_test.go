package shadowvenue

import (
	"strings"
	"testing"
)

const testIngressIPs = "10.199.86.2,10.199.86.10"

func withIngress(when, value string) EgressRead {
	return ParseEgress(when+".txt", []byte(egressText(when)+"ingress_ips="+value+"\n"))
}

// A base host that is exactly one of the record's ingress_ips is accepted, like goapi_ip and loopback.
func TestCheckVenueBaseAcceptsAListedIngressAddress(t *testing.T) {
	rec := withIngress("before", testIngressIPs)
	if rec.Err != nil {
		t.Fatalf("a record with ingress_ips: %v", rec.Err)
	}
	for _, base := range []string{"http://10.199.86.2:8000", "http://10.199.86.10:8000", "http://" + testGoAPIIP + ":8000"} {
		if u, err := CheckVenueBase(base, rec, testRunID); err != nil || "http://"+u.Host != strings.TrimSuffix(base, "/") {
			t.Errorf("%s: %v %v", base, u, err)
		}
	}
	if _, err := CheckVenueBase("http://127.0.0.1:8093", EgressRead{}, ""); err != nil {
		t.Errorf("loopback with no record: %v", err)
	}
}

// Only an exact listed address passes: no subnet or prefix match, no list = today's rule, none = no ingress, and the
// record's other checks still hold for an ingress host.
func TestCheckVenueBaseRefusesAnUnlistedIngressAddress(t *testing.T) {
	set := func(rec string, key, value string) EgressRead {
		lines := strings.Split(rec, "\n")
		for i, l := range lines {
			if strings.HasPrefix(l, key+"=") {
				lines[i] = key + "=" + value
			}
		}
		return ParseEgress("before.txt", []byte(strings.Join(lines, "\n")))
	}
	listed := egressText("before") + "ingress_ips=10.199.86.2\n"
	cases := map[string]struct {
		base   string
		record EgressRead
	}{
		"an unlisted host in the same /29":  {"http://10.199.86.9:8000", ParseEgress("before.txt", []byte(listed))},
		"the subnet's network address":      {"http://10.199.86.0:8000", ParseEgress("before.txt", []byte(listed))},
		"no ingress_ips key":                {"http://10.199.86.2:8000", ParseEgress("before.txt", []byte(egressText("before")))},
		"ingress_ips none":                  {"http://10.199.86.2:8000", withIngress("before", "none")},
		"a malformed list":                  {"http://10.199.86.2:8000", withIngress("before", "10.199.86.0/29")},
		"network not internal":              {"http://10.199.86.2:8000", set(listed, "network_internal", "false")},
		"a default route":                   {"http://10.199.86.2:8000", set(listed, "default_route", "via 10.199.86.1")},
		"a record of another drive":         {"http://10.199.86.2:8000", set(listed, "run_id", "13267-4b")},
		"a host name":                       {"http://venue-ingress:8000", withIngress("before", "10.199.86.2")},
		"a listed ip under a path":          {"http://10.199.86.2:8000/api", ParseEgress("before.txt", []byte(listed))},
		"a listed public ip":                {"http://8.8.8.8:8000", withIngress("before", "8.8.8.8")},
		"a hand-built record, bad list":     {"http://10.199.86.2:8000", EgressRead{Name: "x", Fields: map[string]string{"network_internal": "true", "default_route": "none", "goapi_ip": testGoAPIIP, "run_id": testRunID, "ingress_ips": "10.199.86.0/29"}}},
		"a hand-built record, list of two ": {"http://10.199.86.3:8000", EgressRead{Name: "x", Fields: map[string]string{"network_internal": "true", "default_route": "none", "goapi_ip": testGoAPIIP, "run_id": testRunID, "ingress_ips": "10.199.86.2,10.199.86.4"}}},
	}
	for name, c := range cases {
		if _, err := CheckVenueBase(c.base, c.record, testRunID); err == nil {
			t.Errorf("%s: %s accepted", name, c.base)
		}
	}
}

// ingress_ips is optional: a record without it parses exactly as before; a given list must be literal IPs in their
// plain form, none loopback or unspecified, no CIDR, no duplicate, or the record is a parse error.
func TestParseEgressIngressIPs(t *testing.T) {
	plain := ParseEgress("before.txt", []byte(egressText("before")))
	if plain.Err != nil || len(plain.Fields) != len(egressKeys) {
		t.Fatalf("a record without ingress_ips: %v, %d fields", plain.Err, len(plain.Fields))
	}
	for _, ok := range []string{"none", "10.199.86.2", testIngressIPs, "10.199.86.2,fd00::2"} {
		if rec := withIngress("before", ok); rec.Err != nil || rec.Fields["ingress_ips"] != ok {
			t.Errorf("%q refused: %v", ok, rec.Err)
		}
	}
	for _, bad := range []string{"10.199.86.0/29", "10.199.86.2,", ",10.199.86.2", "10.199.86.2,,10.199.86.10", "10.199.86.2,10.199.86.2",
		"127.0.0.1", "::1", "0.0.0.0", "::", "venue-ingress", " 10.199.86.2", "10.199.86.2 ", "::ffff:10.199.86.2", "010.199.86.2", "10.199.86",
		"FD00::2", "none,10.199.86.2", "10.199.86.2;10.199.86.10"} {
		if rec := withIngress("before", bad); rec.Err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
	if rec := ParseEgress("before.txt", []byte(egressText("before")+"ingress_ips=10.199.86.2\ningress_ips=10.199.86.2\n")); rec.Err == nil {
		t.Error("ingress_ips twice accepted")
	}
	if rec := ParseEgress("before.txt", []byte(egressText("before")+"ingress_ips=\n")); rec.Err == nil {
		t.Error("an empty ingress_ips accepted")
	}
}

// The judge holds ingress_ips equal across the run's reads, like goapi_ip.
func TestCheckEgressHoldsIngressIPsEqualAcrossReads(t *testing.T) {
	want := []string{"before", "between"}
	if voids := checkEgress([]EgressRead{withIngress("before", testIngressIPs), withIngress("between", testIngressIPs)}, want); len(voids) != 0 {
		t.Fatalf("equal lists: %v", voids)
	}
	for name, reads := range map[string][]EgressRead{
		"lists differ":    {withIngress("before", testIngressIPs), withIngress("between", "10.199.86.2")},
		"between lacks":   {withIngress("before", testIngressIPs), egressReads("between")[0]},
		"before lacks":    {egressReads("before")[0], withIngress("between", testIngressIPs)},
		"a malformed one": {withIngress("before", testIngressIPs), withIngress("between", "10.199.86.2,10.199.86.2")},
	} {
		if voids := checkEgress(reads, want); !strings.Contains(strings.Join(voids, "\n"), "ingress_ips") {
			t.Errorf("%s: %v", name, voids)
		}
	}
}
