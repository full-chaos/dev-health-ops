package config

import (
	"strings"
	"testing"
)

// TestBillingEdgeAddressResolution holds the opt-in billing-edge listener's
// address (CHAOS-6520): empty is off, a set value is a host:port that differs
// from the api and operator addresses, and every other service ignores it.
func TestBillingEdgeAddressResolution(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		overrides map[string]string
		want      string
		err       string
	}{
		{name: "default is off"},
		{name: "blank is off", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": "   "}},
		{name: "env", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": " :8010 "}, want: ":8010"},
		{name: "flag wins over env", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": ":8010"},
			overrides: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": ":8011"}, want: ":8011"},
		{name: "not host:port", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": "8010"}, err: "(DEV_HEALTH_API_BILLING_EDGE_ADDR) must be a host:port"},
		{name: "same as the api default", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": ":8000"}, err: "must differ from"},
		{name: "same as the api address", env: map[string]string{"DEV_HEALTH_API_ADDR": "127.0.0.1:9000", "DEV_HEALTH_API_BILLING_EDGE_ADDR": "127.0.0.1:9000"}, err: "DEV_HEALTH_API_ADDR"},
		{name: "same as the operator address", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": ":8080"}, err: "DEV_HEALTH_HTTP_ADDR"},
		{name: "port zero never collides", env: map[string]string{"DEV_HEALTH_API_ADDR": "127.0.0.1:0", "DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0", "DEV_HEALTH_API_BILLING_EDGE_ADDR": "127.0.0.1:0"}, want: "127.0.0.1:0"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Load(Spec{Service: APIServiceName, LookupEnv: lookupFrom(test.env), Overrides: test.overrides})
			if test.err != "" {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("error %v, want %q", err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.APIBillingEdgeAddress != test.want {
				t.Fatalf("APIBillingEdgeAddress = %q, want %q", cfg.APIBillingEdgeAddress, test.want)
			}
			reported := false
			for _, attr := range cfg.SafeAttrs() {
				if attr.Key == "api_billing_edge_address" && attr.Value.String() == test.want {
					reported = true
				}
			}
			if !reported {
				t.Fatal("SafeAttrs must report the billing-edge address")
			}
		})
	}
}

// TestBillingEdgeAddressIsAnAPIOnlyOption: no other service advertises or
// parses it, even when the shared environment carries a malformed value.
func TestBillingEdgeAddressIsAnAPIOnlyOption(t *testing.T) {
	if help := HelpText(APIServiceName, false); !strings.Contains(help, "--api-billing-edge-addr") {
		t.Fatalf("api help lacks the flag:\n%s", help)
	}
	malformed := map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": "nohostport"}
	for _, service := range []string{"dev-health-worker", "dev-health-scheduler", "dev-health-reconciler", "dev-health-stream-runner"} {
		requireQueues := service == "dev-health-worker"
		if strings.Contains(HelpText(service, requireQueues), "--api-billing-edge-addr") {
			t.Errorf("%s advertises the api-only flag", service)
		}
		if requireQueues {
			continue // queue selection is required input unrelated to this test
		}
		cfg, err := Load(Spec{Service: service, LookupEnv: lookupFrom(malformed)})
		if err != nil {
			t.Errorf("%s failed on a setting meant for the api: %v", service, err)
			continue
		}
		if cfg.APIBillingEdgeAddress != "" {
			t.Errorf("%s parsed the billing-edge address %q", service, cfg.APIBillingEdgeAddress)
		}
	}
}
