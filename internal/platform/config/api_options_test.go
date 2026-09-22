package config

import (
	"reflect"
	"strings"
	"testing"
)

func lookupFrom(env map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { value, ok := env[key]; return value, ok }
}

// TestAPISettingsAreScopedToTheAPIService: only dev-health-api advertises and
// parses the api settings; every other service ignores them even when the
// shared environment carries malformed values.
func TestAPISettingsAreScopedToTheAPIService(t *testing.T) {
	if help := HelpText(APIServiceName, false); !strings.Contains(help, "--api-addr") || !strings.Contains(help, "--cors-allowed-origins") {
		t.Fatal("the api must advertise its settings")
	}
	malformed := map[string]string{"DEV_HEALTH_API_ADDR": "nohostport", "CORS_ALLOWED_ORIGINS": ",,,"}
	for _, service := range []string{"dev-health-worker", "dev-health-scheduler", "dev-health-reconciler", "dev-health-stream-runner"} {
		requireQueues := service == "dev-health-worker"
		help := HelpText(service, requireQueues)
		if strings.Contains(help, "--api-addr") || strings.Contains(help, "--cors-allowed-origins") {
			t.Errorf("%s advertises api settings", service)
		}
		if requireQueues {
			continue // queue selection is required input unrelated to this test
		}
		cfg, err := Load(Spec{Service: service, LookupEnv: lookupFrom(malformed)})
		if err != nil {
			t.Errorf("%s failed on api settings meant for the api: %v", service, err)
			continue
		}
		if cfg.APIAddress != "" || cfg.CORSAllowedOrigins != nil {
			t.Errorf("%s parsed api settings: %q %q", service, cfg.APIAddress, cfg.CORSAllowedOrigins)
		}
	}
}

func TestAPISettingsResolution(t *testing.T) {
	cases := []struct {
		name      string
		env       map[string]string
		overrides map[string]string
		address   string
		origins   []string
		err       string
	}{
		{name: "defaults", address: ":8000", origins: []string{"http://localhost:3000"}},
		{name: "env", env: map[string]string{"DEV_HEALTH_API_ADDR": "0.0.0.0:9000", "CORS_ALLOWED_ORIGINS": " https://a , ,https://b,"},
			address: "0.0.0.0:9000", origins: []string{"https://a", "https://b"}},
		{name: "flag wins over env", env: map[string]string{"DEV_HEALTH_API_ADDR": ":9000"},
			overrides: map[string]string{"DEV_HEALTH_API_ADDR": ":9100"}, address: ":9100", origins: []string{"http://localhost:3000"}},
		{name: "empty origin list", env: map[string]string{"CORS_ALLOWED_ORIGINS": " , "}, address: ":8000", origins: []string{}},
		{name: "blank address falls back to the default", env: map[string]string{"DEV_HEALTH_API_ADDR": "  "}, address: ":8000", origins: []string{"http://localhost:3000"}},
		{name: "star kept verbatim", env: map[string]string{"CORS_ALLOWED_ORIGINS": "*"}, address: ":8000", origins: []string{"*"}},
		{name: "not host:port", env: map[string]string{"DEV_HEALTH_API_ADDR": "8000"}, err: "DEV_HEALTH_API_ADDR"},
		{name: "same as operator listener", env: map[string]string{"DEV_HEALTH_API_ADDR": ":8080"}, err: "must differ"},
		{name: "same explicit operator listener", env: map[string]string{"DEV_HEALTH_API_ADDR": "127.0.0.1:7000", "DEV_HEALTH_HTTP_ADDR": "127.0.0.1:7000"}, err: "must differ"},
		{name: "both on port zero", env: map[string]string{"DEV_HEALTH_API_ADDR": "127.0.0.1:0", "DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0"},
			address: "127.0.0.1:0", origins: []string{"http://localhost:3000"}},
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
			if cfg.APIAddress != test.address || !reflect.DeepEqual(cfg.CORSAllowedOrigins, test.origins) {
				t.Fatalf("got %q %q, want %q %q", cfg.APIAddress, cfg.CORSAllowedOrigins, test.address, test.origins)
			}
			found := false
			for _, attr := range cfg.SafeAttrs() {
				if attr.Key == "api_address" && attr.Value.String() == test.address {
					found = true
				}
			}
			if !found {
				t.Fatal("SafeAttrs must report the api address")
			}
		})
	}
}
