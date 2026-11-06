package serviceaccount

import "testing"

func TestNewKeyResolverRefusesAnIncompleteConfig(t *testing.T) {
	if _, err := NewKeyResolver(KeyResolverConfig{}); err == nil {
		t.Fatal("a resolver without a pool and a schema was built")
	}
}
