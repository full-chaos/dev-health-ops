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
