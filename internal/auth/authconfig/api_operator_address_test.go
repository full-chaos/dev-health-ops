package authconfig

import (
	"strings"
	"testing"
)

// The API and operator addresses are one listener when the socket they bind is the same, whatever the spelling.
func TestTheAPIAndOperatorAddressesRefuseAnotherSpellingOfOneListener(t *testing.T) {
	cases := map[string]struct{ api, operator string }{
		"identical":                   {":8095", ":8095"},
		"leading-zero port":           {":8095", ":08095"},
		"leading-zero port, api side": {"127.0.0.1:08095", "127.0.0.1:8095"},
		"empty host vs ipv4 wildcard": {":8095", "0.0.0.0:8095"},
		"ipv4 wildcard vs empty host": {"0.0.0.0:8095", ":8095"},
		"empty host vs ipv6 wildcard": {":8095", "[::]:8095"},
		"two wildcards":               {"0.0.0.0:8095", "[::]:8095"},
		"wildcard vs specific host":   {":8095", "127.0.0.1:8095"},
		"specific host vs wildcard":   {"127.0.0.1:8095", "0.0.0.0:8095"},
		"localhost vs loopback":       {"localhost:8095", "127.0.0.1:8095"},
		"ipv4-mapped ipv6 vs ipv4":    {"[::ffff:127.0.0.1]:8095", "127.0.0.1:8095"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			env[EnvAPIAddress] = c.api
			env[EnvOperatorAddress] = c.operator
			_, err := Load(Spec{LookupEnv: lookupFrom(env)})
			want := EnvAPIAddress + " and " + EnvOperatorAddress + " must not be the same address"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("Load = %v, want an error naming %q", err, want)
			}
		})
	}
}

func TestTheAPIAndOperatorAddressesAcceptWhatDoesNotShareAListener(t *testing.T) {
	cases := map[string]struct{ api, operator string }{
		"defaults":                 {defaultAPIAddress, defaultOperatorAddress},
		"different port":           {":8095", ":8096"},
		"wildcard, other port":     {"0.0.0.0:8095", ":8096"},
		"different specific hosts": {"127.0.0.1:8095", "127.0.0.2:8095"},
		"port 0 on both":           {":0", ":0"},
		"port 0 vs specific host":  {"127.0.0.1:0", ":0"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			env := baseEnv()
			env[EnvAPIAddress] = c.api
			env[EnvOperatorAddress] = c.operator
			if _, err := Load(Spec{LookupEnv: lookupFrom(env)}); err != nil {
				t.Fatalf("Load = %v, want accepted", err)
			}
		})
	}
}
