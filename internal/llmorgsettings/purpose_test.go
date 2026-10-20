package llmorgsettings

import (
	"reflect"
	"testing"
)

func TestParsePurposeIsTheFixedList(t *testing.T) {
	for _, name := range []string{"investigate", "explain"} {
		if got, ok := ParsePurpose(name); !ok || string(got) != name {
			t.Errorf("ParsePurpose(%q) = %q, %v", name, got, ok)
		}
	}
	for _, name := range []string{"", "Explain", "default", "summarize", " explain", "categorize"} {
		if _, ok := ParsePurpose(name); ok {
			t.Errorf("ParsePurpose(%q) accepted", name)
		}
	}
}

func TestPurposeKeysLayout(t *testing.T) {
	want := []string{"purpose:explain:provider", "purpose:explain:model", "purpose:explain:api_key", "purpose:explain:base_url"}
	if got := PurposeKeys(PurposeExplain); !reflect.DeepEqual(got, want) {
		t.Fatalf("PurposeKeys = %v, want %v", got, want)
	}
}

// Resolution per purpose: the override when stored, else the default row, else
// nothing (the platform model).
func TestProjectResolutionOrder(t *testing.T) {
	def := orgSettings{"provider": "anthropic", "model": "big", "api_key": "sk-default", "base_url": "https://default.example.com"}
	withOverrides := func(purpose Purpose, extra map[string]string) orgSettings {
		out := orgSettings{}
		for k, v := range def {
			out[k] = v
		}
		for k, v := range extra {
			out[PurposeKey(purpose, k)] = v
		}
		return out
	}
	for _, purpose := range Purposes {
		t.Run(string(purpose)+"/override wins and never borrows a default field", func(t *testing.T) {
			got := project(withOverrides(purpose, map[string]string{"provider": "openai", "model": "small"}), purpose)
			want := orgSettings{"provider": "openai", "model": "small"}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
		t.Run(string(purpose)+"/no override falls back to the default row", func(t *testing.T) {
			if got := project(def, purpose); !reflect.DeepEqual(got, def) {
				t.Fatalf("got %v, want %v", got, def)
			}
		})
		t.Run(string(purpose)+"/no default and no override is empty", func(t *testing.T) {
			if got := project(orgSettings{}, purpose); len(got) != 0 {
				t.Fatalf("got %v, want empty", got)
			}
		})
		t.Run(string(purpose)+"/another purpose's override is ignored", func(t *testing.T) {
			other := PurposeExplain
			if purpose == PurposeExplain {
				other = PurposeInvestigate
			}
			got := project(withOverrides(other, map[string]string{"provider": "openai", "model": "small"}), purpose)
			if !reflect.DeepEqual(got, withOverrides(other, map[string]string{"provider": "openai", "model": "small"})) || got[keyProvider] != "anthropic" {
				t.Fatalf("got %v", got)
			}
		})
	}
	t.Run("no purpose reads the default only", func(t *testing.T) {
		in := withOverrides(PurposeExplain, map[string]string{"provider": "openai", "model": "small"})
		if got := project(in, ""); got[keyProvider] != "anthropic" {
			t.Fatalf("got %v", got)
		}
	})
}

func TestProjectKeyInheritance(t *testing.T) {
	base := func(overrideProvider string, overrideKey string) orgSettings {
		in := orgSettings{"provider": "anthropic", "model": "big", "api_key": "sk-default",
			"purpose:explain:provider": overrideProvider, "purpose:explain:model": "small"}
		if overrideKey != "" {
			in["purpose:explain:api_key"] = overrideKey
		}
		return in
	}
	t.Run("same provider, no own key: uses the default key", func(t *testing.T) {
		got := project(base("Anthropic", ""), PurposeExplain)
		want := orgSettings{"provider": "Anthropic", "model": "small", "api_key": "sk-default"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	})
	t.Run("same provider, own key: own key wins", func(t *testing.T) {
		if got := project(base("anthropic", "sk-own"), PurposeExplain); got[keyAPIKey] != "sk-own" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("different provider never inherits the key", func(t *testing.T) {
		if got := project(base("openai", ""), PurposeExplain); got[keyAPIKey] != "" {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("another purpose does not inherit through this one", func(t *testing.T) {
		if got := project(base("anthropic", ""), PurposeInvestigate); got[keyModel] != "big" {
			t.Fatalf("got %v", got)
		}
	})
}

func TestSourceKeys(t *testing.T) {
	cfg := func(kv ...string) (func(string) bool, func(string) string) {
		m := map[string]string{}
		for i := 0; i < len(kv); i += 2 {
			m[kv[i]] = kv[i+1]
		}
		return func(k string) bool { return m[k] != "" }, func(k string) string { return m[k] }
	}
	check := func(name string, want []string, purpose Purpose, kv ...string) {
		t.Helper()
		stored, value := cfg(kv...)
		if got := sourceKeys(stored, value, purpose); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: %v, want %v", name, got, want)
		}
	}
	check("default", Fields, PurposeInvestigate, "provider", "x")
	check("override", PurposeKeys(PurposeInvestigate), PurposeInvestigate,
		"provider", "a", "api_key", "k", "purpose:investigate:provider", "b", "purpose:investigate:api_key", "k2")
	check("other purpose's override", Fields, PurposeInvestigate, "purpose:explain:provider", "b")
	check("no purpose", Fields, "", "purpose:explain:provider", "b")
	check("inherits default key", append(PurposeKeys(PurposeInvestigate), "provider", "api_key"), PurposeInvestigate,
		"provider", "A", "api_key", "k", "purpose:investigate:provider", "a")
	check("different provider does not inherit", PurposeKeys(PurposeInvestigate), PurposeInvestigate,
		"provider", "a", "api_key", "k", "purpose:investigate:provider", "b")
}
