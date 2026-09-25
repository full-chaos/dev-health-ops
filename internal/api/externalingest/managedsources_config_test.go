package externalingest

import "testing"

// pyConfig reads a config the way Python does: lazily, per field, skipping the
// values that are not the wanted type. Python's json.loads parses 1e400 as inf
// and 1e-400 as 0.0; a typed decode of the whole object fails on the first and
// used to answer 500 for every host (CHAOS-6748 r2).
func TestPyConfigReadsFieldsLazilyLikePython(t *testing.T) {
	config, err := decodePyConfig([]byte(`{"url":1e400,"base_url":"https://ghe.acme.test","nested":{"n":[1e999,{}]},"big":123456789012345678901234567890}`))
	if err != nil {
		t.Fatalf("a value the reader does not want must not fail the config: %v", err)
	}
	if got, ok := config.str("base_url"); !ok || got != "https://ghe.acme.test" {
		t.Fatalf("base_url = (%q, %t), want the sibling key to survive", got, ok)
	}
	if _, ok := config.str("url"); ok {
		t.Fatalf("a number is not a str: skipped")
	}
	if !config.truthy("url") || !config.truthy("big") || !config.truthy("nested") {
		t.Fatalf("1e400 (inf), a big int and a non-empty object are truthy")
	}
	if config.truthy("absent") {
		t.Fatalf("an absent key is None: falsy")
	}
}

func TestPyConfigTopLevelTruthiness(t *testing.T) {
	for raw, wantTruthy := range map[string]bool{
		`1e400`: true, `-1e400`: true, `5`: true, `0.5`: true, `123456789012345678901234567890`: true,
		`true`: true, `"x"`: true, `[1]`: true,
		`0`: false, `-0`: false, `0.0`: false, `-0.0`: false, `0e5`: false, `1e-400`: false,
		`false`: false, `null`: false, `""`: false, `[]`: false, `{}`: false,
	} {
		config, err := decodePyConfig([]byte(raw))
		if raw == `{}` {
			if err != nil || len(config) != 0 {
				t.Errorf("config %s: got (%v, %v), want empty", raw, config, err)
			}
			continue
		}
		if wantTruthy {
			if err != errCredentialConfigNotObject {
				t.Errorf("config %s: err = %v, want errCredentialConfigNotObject (truthy non-object)", raw, err)
			}
			continue
		}
		if err != nil || config != nil {
			t.Errorf("config %s: got (%v, %v), want (nil, nil) (falsy is {})", raw, config, err)
		}
	}
}

func TestConfiguredHostFirstTruthyKeyWins(t *testing.T) {
	for _, test := range []struct {
		name, raw, want string
	}{
		{"underflow instance url is falsy, falls to url", `{"github_instance_url":1e-400,"github_url":"https://a.acme.test"}`, "https://a.acme.test"},
		{"overflow instance url is truthy and not a str, url ignored", `{"github_instance_url":1e400,"github_url":"https://a.acme.test"}`, ""},
		{"instance url str wins", `{"github_instance_url":"https://i.acme.test","github_url":"https://a.acme.test"}`, "https://i.acme.test"},
		{"blank url", `{"github_url":"   "}`, ""},
	} {
		config, err := decodePyConfig([]byte(test.raw))
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		host, ok := configuredHost(config, "github")
		if host != test.want || ok != (test.want != "") {
			t.Errorf("%s: got (%q, %t), want %q", test.name, host, ok, test.want)
		}
	}
}

// A source's metadata is read for two string keys only; a number Go's float64
// cannot hold elsewhere in it must not lose those keys.
func TestDecodeMetadataKeepsStringKeysBesideAnOverflowNumber(t *testing.T) {
	metadata := decodeMetadata([]byte(`{"path_with_namespace":"group/project","weight":1e400}`))
	if got, _ := metadata["path_with_namespace"].(string); got != "group/project" {
		t.Fatalf("path_with_namespace = %q, want it kept beside 1e400 (metadata %v)", got, metadata)
	}
}
