package sso

import "testing"

// The legacy yardstick of the JIT parity cells: an empty or NULL allowed_domains is no domain filter.
func TestCheckAllowedDomainWithNoListAllowsEveryDomain(t *testing.T) {
	empty, jsonNull := "[]", "null"
	for name, raw := range map[string]*string{"empty": &empty, "json null": &jsonNull, "null": nil} {
		t.Run(name, func(t *testing.T) {
			if err := checkAllowedDomain(&providerRow{AllowedDomains: raw}, "p@example.com"); err != nil {
				t.Fatalf("checkAllowedDomain = %v, want nil", err)
			}
		})
	}
}
