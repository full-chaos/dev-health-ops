package loadharness

import (
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestCheckHostDefaultDeny(t *testing.T) {
	ok := []string{"http://localhost:8080/x", "http://127.0.0.1:1", "http://[::1]:80", "http://auth-service:8080", "http://LOCALHOST"}
	for _, u := range ok {
		if err := CheckHost(u, nil); err != nil {
			t.Fatalf("%s refused: %v", u, err)
		}
	}
	bad := []string{"https://app.example.com", "http://10.0.0.5", "http://localhost.evil.com", "http://auth-service.prod.svc", "ftp://localhost", "localhost", "http://"}
	for _, u := range bad {
		if err := CheckHost(u, nil); err == nil {
			t.Fatalf("%s accepted", u)
		}
	}
	if err := CheckHost("http://bigboy-dev:8080", []string{"bigboy-dev"}); err != nil {
		t.Fatal(err)
	}
	if err := CheckHost("http://bigboy-dev:8080", []string{"bigboy"}); err == nil {
		t.Fatal("allow-host must match exactly")
	}
}

func TestParseConfigValidation(t *testing.T) {
	good := `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","method":"GET","path":"/x"}]}`
	cfg, err := ParseConfig([]byte(good), env(nil))
	if err != nil || cfg.Targets[0].Weight != 1 || cfg.Targets[0].ExpectStatus[0] != 200 {
		t.Fatalf("%+v %v", cfg, err)
	}
	bad := map[string]string{
		"noname":    `{"base_url":"http://127.0.0.1:1","targets":[{"method":"GET","path":"/x"}]}`,
		"dupname":   `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"/x"},{"name":"a","path":"/y"}]}`,
		"notargs":   `{"base_url":"http://127.0.0.1:1","targets":[]}`,
		"abspath":   `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"http://evil/x"}]}`,
		"unknown":   `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"/x","bogus":1}]}`,
		"envinpath": `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"/${ENV:T}"}]}`,
		"unsetenv":  `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"/x","header_env":{"Authorization":{"env":"NOPE"}}}]}`,
		"badweight": `{"base_url":"http://127.0.0.1:1","targets":[{"name":"a","path":"/x","weight":-1}]}`,
	}
	for name, doc := range bad {
		if _, err := ParseConfig([]byte(doc), env(nil)); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestExpandPlaceholders(t *testing.T) {
	e := env(map[string]string{"P": "u1,u2", "S": "sec"})
	got, err := expand(`{"a":"${ENV:S}","b":"${POOL:P}","n":${SEQ}}`, 3, e)
	if err != nil || got != `{"a":"sec","b":"u2","n":3}` {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := expand("${ENV:MISSING}", 0, e); err == nil {
		t.Fatal("unset env accepted")
	}
}

func TestProfiles(t *testing.T) {
	n, err := ProfileByName("normal")
	p, err2 := ProfileByName("peak")
	if err != nil || err2 != nil || p.Rate <= n.Rate || p.Concurrency < n.Concurrency {
		t.Fatalf("%+v %+v", n, p)
	}
	if _, err := ProfileByName("x"); err == nil || !strings.Contains(err.Error(), "normal") {
		t.Fatalf("%v", err)
	}
}
