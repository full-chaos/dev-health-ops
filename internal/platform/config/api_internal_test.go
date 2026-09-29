package config

import (
	"strings"
	"testing"
)

// TestInternalAddressResolution: the internal listener (CHAOS-7181) is off
// when empty, otherwise a host:port that overlaps no other api listener, and
// only the api service advertises it.
func TestInternalAddressResolution(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
		err  string
	}{
		{name: "default is off"},
		{name: "blank is off", env: map[string]string{"DEV_HEALTH_API_INTERNAL_ADDR": "  "}},
		{name: "env", env: map[string]string{"DEV_HEALTH_API_INTERNAL_ADDR": " :8091 "}, want: ":8091"},
		{name: "not host:port", env: map[string]string{"DEV_HEALTH_API_INTERNAL_ADDR": "8091"}, err: "must be a host:port"},
		{name: "same as api", env: map[string]string{"DEV_HEALTH_API_INTERNAL_ADDR": ":8000"}, err: "DEV_HEALTH_API_ADDR"},
		{name: "same as operator", env: map[string]string{"DEV_HEALTH_API_INTERNAL_ADDR": ":8080"}, err: "DEV_HEALTH_HTTP_ADDR"},
		{name: "same as billing edge", env: map[string]string{"DEV_HEALTH_API_BILLING_EDGE_ADDR": ":8010", "DEV_HEALTH_API_INTERNAL_ADDR": "127.0.0.1:8010"}, err: "DEV_HEALTH_API_BILLING_EDGE_ADDR"},
		{name: "port zero never collides", env: map[string]string{"DEV_HEALTH_API_ADDR": "127.0.0.1:0", "DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0", "DEV_HEALTH_API_INTERNAL_ADDR": "127.0.0.1:0"}, want: "127.0.0.1:0"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Load(Spec{Service: APIServiceName, LookupEnv: lookupFrom(test.env)})
			if test.err != "" {
				if err == nil || !strings.Contains(err.Error(), test.err) {
					t.Fatalf("error %v, want %q", err, test.err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if cfg.APIInternalAddress != test.want {
				t.Fatalf("APIInternalAddress = %q, want %q", cfg.APIInternalAddress, test.want)
			}
		})
	}
}

func TestInternalAddressIsAnAPIOnlyOption(t *testing.T) {
	if help := HelpText(APIServiceName, false); !strings.Contains(help, "--api-internal-addr") {
		t.Fatal("dho api --help omits --api-internal-addr")
	}
	for _, service := range []string{"dev-health-scheduler", "dev-health-reconciler", "dev-health-stream-runner"} {
		if strings.Contains(HelpText(service, false), "--api-internal-addr") {
			t.Errorf("%s advertises --api-internal-addr", service)
		}
	}
}

func TestACRPublicCompatResolution(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
		want bool
		err  bool
	}{
		{name: "default off"},
		{name: "on", env: map[string]string{"DEV_HEALTH_API_ACR_PUBLIC_COMPAT": "true"}, want: true},
		{name: "off", env: map[string]string{"DEV_HEALTH_API_ACR_PUBLIC_COMPAT": "false"}},
		{name: "malformed", env: map[string]string{"DEV_HEALTH_API_ACR_PUBLIC_COMPAT": "maybe"}, err: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg, err := Load(Spec{Service: APIServiceName, LookupEnv: lookupFrom(test.env)})
			if (err != nil) != test.err {
				t.Fatalf("err = %v, want error %t", err, test.err)
			}
			if err == nil && cfg.APIACRPublicCompat != test.want {
				t.Fatalf("APIACRPublicCompat = %t, want %t", cfg.APIACRPublicCompat, test.want)
			}
		})
	}
}
