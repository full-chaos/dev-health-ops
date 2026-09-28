package config

import (
	"net"
	"strings"
	"testing"
)

func queryAPISpec(values map[string]string) Spec {
	return Spec{
		Service:   QueryAPIServiceName,
		LookupEnv: lookup(values),
	}
}

// D2953: unset (the common case) parses to nil and keeps today's behaviour --
// the internal listener accepts every peer.
func TestQueryAPIInternalAllowedCIDRsUnsetIsNil(t *testing.T) {
	t.Parallel()
	cfg, err := Load(queryAPISpec(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.QueryAPIInternalAllowedCIDRs != nil {
		t.Fatalf("QueryAPIInternalAllowedCIDRs = %v, want nil when unset", cfg.QueryAPIInternalAllowedCIDRs)
	}
}

// D2953: a well-formed list, IPv4 and IPv6 mixed, parses into the exact CIDRs
// named, in order.
func TestQueryAPIInternalAllowedCIDRsParsesIPv4AndIPv6(t *testing.T) {
	t.Parallel()
	cfg, err := Load(queryAPISpec(map[string]string{
		"QUERY_API_INTERNAL_ALLOWED_CIDRS": "10.0.0.0/8, ::1/128 ,172.16.0.0/12",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.QueryAPIInternalAllowedCIDRs) != 3 {
		t.Fatalf("got %d entries, want 3: %v", len(cfg.QueryAPIInternalAllowedCIDRs), cfg.QueryAPIInternalAllowedCIDRs)
	}
	if !cfg.QueryAPIInternalAllowedCIDRs[0].Contains(net.ParseIP("10.1.2.3")) {
		t.Errorf("entry 0 does not contain 10.1.2.3: %v", cfg.QueryAPIInternalAllowedCIDRs[0])
	}
	if !cfg.QueryAPIInternalAllowedCIDRs[1].Contains(net.ParseIP("::1")) {
		t.Errorf("entry 1 does not contain ::1: %v", cfg.QueryAPIInternalAllowedCIDRs[1])
	}
	if !cfg.QueryAPIInternalAllowedCIDRs[2].Contains(net.ParseIP("172.16.5.5")) {
		t.Errorf("entry 2 does not contain 172.16.5.5: %v", cfg.QueryAPIInternalAllowedCIDRs[2])
	}
}

// A trailing comma or a whitespace-only entry is dropped, not an error --
// the same tolerance parseCORSOrigins already gives its own comma list.
func TestQueryAPIInternalAllowedCIDRsToleratesTrailingCommaAndBlankEntries(t *testing.T) {
	t.Parallel()
	cfg, err := Load(queryAPISpec(map[string]string{
		"QUERY_API_INTERNAL_ALLOWED_CIDRS": "10.0.0.0/8, ,172.16.0.0/12,",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.QueryAPIInternalAllowedCIDRs) != 2 {
		t.Fatalf("got %d entries, want 2 (blank entries dropped): %v", len(cfg.QueryAPIInternalAllowedCIDRs), cfg.QueryAPIInternalAllowedCIDRs)
	}
}

// D2953: a malformed entry anywhere in the list is a startup error -- Load
// refuses, never applying a partial list.
func TestQueryAPIInternalAllowedCIDRsMalformedRefusesStart(t *testing.T) {
	t.Parallel()
	for name, raw := range map[string]string{
		"not a CIDR at all":  "not-a-cidr",
		"bare IP, no prefix": "10.0.0.1",
		"one good, one bad":  "10.0.0.0/8,nope",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := Load(queryAPISpec(map[string]string{"QUERY_API_INTERNAL_ALLOWED_CIDRS": raw}))
			if err == nil {
				t.Fatalf("Load succeeded with a malformed QUERY_API_INTERNAL_ALLOWED_CIDRS=%q, want a refusal", raw)
			}
			if !strings.Contains(err.Error(), "QUERY_API_INTERNAL_ALLOWED_CIDRS") {
				t.Fatalf("error does not name the setting: %v", err)
			}
		})
	}
}
