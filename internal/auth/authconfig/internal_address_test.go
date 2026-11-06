package authconfig

import (
	"strings"
	"testing"
)

const internalAddressEnv = "AUTH_SERVICE_INTERNAL_HTTP_ADDR"

// The internal listener is off unless its address is set, and its address is
// checked like the other two: host:port, and never one of theirs.
func TestTheInternalAddressIsCheckedLikeTheOtherListeners(t *testing.T) {
	cases := map[string]struct {
		set  map[string]string
		want string
	}{
		"not host:port": {map[string]string{internalAddressEnv: "8097"}, internalAddressEnv + " must be a host:port address"},
		"same as api":   {map[string]string{EnvAPIAddress: ":9000", internalAddressEnv: ":9000"}, internalAddressEnv + " must not be the same address"},
		"same as operator": {
			map[string]string{EnvOperatorAddress: ":9001", internalAddressEnv: ":9001"}, internalAddressEnv + " must not be the same address",
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			for k, v := range c.set {
				env[k] = v
			}
			_, err := Load(Spec{LookupEnv: lookupFrom(env)})
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("Load = %v, want an error naming %q", err, c.want)
			}
		})
	}
}

// Off by default: no address, and the start line says so.
func TestTheInternalListenerIsOffByDefaultAndTheStartLineNamesIt(t *testing.T) {
	cfg, err := Load(Spec{LookupEnv: lookupFrom(baseEnv())})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, attr := range cfg.SafeAttrs() {
		if attr.Key == "internal_address" {
			found = true
			if attr.Value.String() != "" {
				t.Errorf("internal_address = %q by default, want empty", attr.Value.String())
			}
		}
	}
	if !found {
		t.Fatal("the start line has no internal_address field")
	}
	env := baseEnv()
	env[internalAddressEnv] = "127.0.0.1:19097"
	if cfg, err = Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
		t.Fatal(err)
	}
	for _, attr := range cfg.SafeAttrs() {
		if attr.Key == "internal_address" && attr.Value.String() != "127.0.0.1:19097" {
			t.Errorf("internal_address = %q, want the configured address", attr.Value.String())
		}
	}
}

// The same listener in another spelling is still the same listener: a
// leading-zero port, and the wildcard host in any of its forms.
func TestTheInternalAddressRefusesAnotherSpellingOfAnotherListenersAddress(t *testing.T) {
	type pair struct{ other, internal string }
	spellings := []pair{
		{":8095", ":08095"},
		{":8095", "0.0.0.0:8095"},
		{"0.0.0.0:8095", ":8095"},
		{":8095", "[::]:8095"},
		{"[::]:8095", ":8095"},
		{"0.0.0.0:8095", "[::]:8095"},
		{"0.0.0.0:8095", "127.0.0.1:8095"},
		{"127.0.0.1:8095", ":8095"},
		{"127.0.0.1:8095", "127.0.0.1:08095"},
		{"[0:0:0:0:0:0:0:1]:8095", "[::1]:08095"},
		{"127.0.0.1:8095", "[0:0:0:0:0:0:0:0]:08095"},
		{"127.0.0.1:8095", "[::ffff:127.0.0.1]:8095"},
		{"127.0.0.1:8095", "localhost:8095"},
		{"[::1]:8095", "localhost:8095"},
		{"[fe80::1]:8095", "[fe80::1%eth0]:8095"},
	}
	for _, listener := range []string{EnvAPIAddress, EnvOperatorAddress} {
		for _, p := range spellings {
			t.Run(listener+"/"+p.other+"/"+p.internal, func(t *testing.T) {
				env := baseEnv()
				env[EnvAPIAddress] = "10.0.0.1:9000"
				env[EnvOperatorAddress] = "10.0.0.1:9001"
				env[listener] = p.other
				env[internalAddressEnv] = p.internal
				_, err := Load(Spec{LookupEnv: lookupFrom(env)})
				want := internalAddressEnv + " must not be the same address"
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("Load = %v, want an error naming %q", err, want)
				}
			})
		}
	}
}

func TestTheInternalAddressAcceptsWhatDoesNotShareAListener(t *testing.T) {
	cases := map[string]struct{ api, operator, internal string }{
		"different port":                {":8095", ":8096", ":8097"},
		"specific host, other port":     {":8095", ":8096", "127.0.0.1:8097"},
		"different specific hosts":      {"127.0.0.1:8095", "127.0.0.1:8096", "127.0.0.2:8095"},
		"port zero beside port zero":    {":0", ":8096", ":0"},
		"port zero wildcard vs another": {"0.0.0.0:0", ":8096", ":0"},
		"port zero in zero spelling":    {":8095", ":0", ":00"},
		"port zero wildcard in v6 form": {":8095", ":0", "[::]:00"},
		"two specific hosts, one port":  {"10.0.0.1:8095", "10.0.0.1:8096", "10.0.0.2:8095"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			env[EnvAPIAddress] = c.api
			env[EnvOperatorAddress] = c.operator
			env[internalAddressEnv] = c.internal
			if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
				t.Fatalf("Load = %v, want accepted", err)
			}
		})
	}
}
