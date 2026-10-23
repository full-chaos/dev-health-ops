package devhealth_test

import (
	"encoding/json"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/token"
)

var keySetBase = []string{
	"--set", "authService.enabled=true",
	"--set", "authService.database.secretName=auth-db",
	"--set", "authService.migrate.secretName=auth-migrate-db",
	"--set", "authService.migrate.secretKey=MIGRATION_URI",
	"--set-json", `authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]`,
}

func keySetArgs(keys string, extra ...string) []string {
	args := append([]string{}, keySetBase...)
	args = append(args, "--set", "authService.keySet.enabled=true", "--set-json", "authService.keySet.keys="+keys)
	return append(args, extra...)
}

const twoKeys = `[{"kid":"auth-2026-10","state":"active","file":"auth-2026-10.pem","secretName":"key-a","secretKey":"signing-key.pem"},` +
	`{"kid":"auth-2026-09","state":"retiring","file":"auth-2026-09.pem","secretName":"key-b","secretKey":"signing-key.pem"}]`

func TestAuthKeySetRendersManifestAndStaging(t *testing.T) {
	docs, out, err := renderAuth(t, keySetArgs(twoKeys)...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.600s", err, out)
	}
	cm := authDoc(docs, "ConfigMap", "a-dev-health-auth-service-keyset")
	if cm == nil {
		t.Fatal("no key-set ConfigMap")
	}
	raw, _ := at(cm, "data", "keyset.json").(string)
	var manifest struct {
		Keys []map[string]string `json:"keys"`
	}
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		t.Fatalf("manifest is not JSON: %v", err)
	}
	if len(manifest.Keys) != 2 {
		t.Fatalf("manifest keys = %v", manifest.Keys)
	}
	for _, key := range manifest.Keys {
		if len(key) != 3 || key["kid"] == "" || key["state"] == "" || key["file"] == "" {
			t.Errorf("manifest entry is not exactly kid, state, file: %v", key)
		}
		if !token.ValidKeyID(key["kid"]) || !token.State(key["state"]).Valid() {
			t.Errorf("manifest entry is outside the keystore schema: %v", key)
		}
	}
	if strings.Contains(raw, "PRIVATE KEY") || strings.Contains(raw, "key-a") || strings.Contains(raw, "secret") {
		t.Errorf("manifest carries more than kid, state and file: %s", raw)
	}
	if len(raw) > keySetManifestLimit(t) {
		t.Errorf("manifest is %d bytes, over the keystore limit", len(raw))
	}

	deployment := authDoc(docs, "Deployment", "a-dev-health-auth-service")
	pod := at(deployment, "spec", "template", "spec")
	args := strings.Join(toStrings(at(pod, "containers", 0, "args")), " ")
	if !strings.Contains(args, "--signing-keyset-file=/run/auth/signing/keyset.json") {
		t.Errorf("auth-service args lack the key-set flag: %s", args)
	}
	for _, forbidden := range []string{"--signing-key-file", "--signing-key-id"} {
		if strings.Contains(args, forbidden) {
			t.Errorf("auth-service args carry %s beside the key set: %s", forbidden, args)
		}
	}
	script := strings.Join(toStrings(at(pod, "initContainers", 0, "command")), " ")
	for _, want := range []string{"install -m 0400 /in/auth-2026-10.pem /out/auth-2026-10.pem", "install -m 0400 /in/auth-2026-09.pem /out/auth-2026-09.pem", "install -m 0400 /in/keyset.json /out/keyset.json"} {
		if !strings.Contains(script, want) {
			t.Errorf("init container does not run %q: %s", want, script)
		}
	}
	volumes, _ := at(pod, "volumes").([]any)
	var source any
	for _, v := range volumes {
		if at(v, "name") == "signing-key-source" {
			source = v
		}
	}
	if at(source, "projected", "defaultMode") != 288 {
		t.Errorf("source volume is not projected 0440: %v", source)
	}
	sources, _ := at(source, "projected", "sources").([]any)
	if len(sources) != 3 {
		t.Errorf("source volume has %d sources, want 2 Secrets and the ConfigMap: %v", len(sources), sources)
	}
	for _, v := range volumes {
		if at(v, "name") == "signing-key" && at(v, "emptyDir", "medium") != "Memory" {
			t.Errorf("staged copy is not memory-backed: %v", v)
		}
	}
	if strings.Contains(out, "--signing-key-id") {
		t.Error("the render still carries a single key id")
	}
}

func keySetManifestLimit(t *testing.T) int {
	t.Helper()
	raw, err := os.ReadFile("../../../internal/auth/keystore/custody.go")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`MaxManifestBytes\s*=\s*int64\((\d+) << 10\)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("keystore.MaxManifestBytes not found")
	}
	n, _ := strconv.Atoi(string(m[1]))
	return n << 10
}

func TestAuthKeySetOffRendersNoKeySet(t *testing.T) {
	docs, out, err := renderAuth(t, authBase...)
	if err != nil {
		t.Fatalf("render failed: %v\n%.400s", err, out)
	}
	if authDoc(docs, "ConfigMap", "a-dev-health-auth-service-keyset") != nil || strings.Contains(out, "keyset") {
		t.Error("the single-key render carries key-set staging")
	}
}

func TestAuthKeySetRefusals(t *testing.T) {
	active := func(kid, file string) string {
		return `{"kid":"` + kid + `","state":"active","file":"` + file + `","secretName":"s","secretKey":"k"}`
	}
	cases := map[string]struct {
		args []string
		want string
	}{
		"no keys":          {keySetArgs(`[]`), "authService.keySet.keys must list at least one key"},
		"no active":        {keySetArgs(`[{"kid":"a","state":"pending","file":"a.pem","secretName":"s","secretKey":"k"}]`), "exactly one key must be active"},
		"two active":       {keySetArgs(`[` + active("a", "a.pem") + `,` + active("b", "b.pem") + `]`), "exactly one key must be active"},
		"bad state":        {keySetArgs(`[{"kid":"a","state":"revoked","file":"a.pem","secretName":"s","secretKey":"k"}]`), "state must be one of"},
		"bad kid":          {keySetArgs(`[{"kid":"a b","state":"active","file":"a.pem","secretName":"s","secretKey":"k"}]`), "kid must match"},
		"duplicate kid":    {keySetArgs(`[` + active("a", "a.pem") + `,{"kid":"a","state":"retired","file":"b.pem","secretName":"s","secretKey":"k"}]`), "kid must be unique"},
		"duplicate file":   {keySetArgs(`[` + active("a", "a.pem") + `,{"kid":"b","state":"retired","file":"a.pem","secretName":"s","secretKey":"k"}]`), "file must be unique"},
		"path in file":     {keySetArgs(`[` + active("a", "d/a.pem") + `]`), "file must be a bare name"},
		"dotdot file":      {keySetArgs(`[` + active("a", "..") + `]`), "file must be a bare name"},
		"manifest clash":   {keySetArgs(`[` + active("a", "keyset.json") + `]`), "file must not be keyset.json"},
		"kid without file": {keySetArgs(`[{"kid":"a","state":"active","file":"a.pem"}]`), "needs secretName and secretKey"},
		"with signingKey":  {keySetArgs(`[`+active("a", "a.pem")+`]`, "--set", "authService.signingKey.secretName=k"), "authService.keySet replaces authService.signingKey"},
		"manifest too big": {keySetArgs(bigKeys()), "manifest is larger than"},
		"with key id":      {keySetArgs(`[`+active("a", "a.pem")+`]`, "--set", "authService.signingKey.keyId=a"), "authService.keySet replaces authService.signingKey"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, out, err := renderAuth(t, tc.args...)
			if err == nil {
				t.Fatal("the render succeeded; it must refuse")
			}
			if !strings.Contains(out, tc.want) {
				t.Fatalf("the refusal does not say %q: %.500s", tc.want, out)
			}
		})
	}
}

func TestAuthKeySetStillRefusesHandSetEnv(t *testing.T) {
	args := keySetArgs(twoKeys, "--set", "authService.extraEnv[0].name=AUTH_SIGNING_KEYSET_FILE", "--set", "authService.extraEnv[0].value=/k/keyset.json")
	_, out, err := renderAuth(t, args...)
	if err == nil || !strings.Contains(out, "AUTH_SIGNING_KEYSET_FILE") {
		t.Fatalf("hand-set AUTH_SIGNING_KEYSET_FILE was not refused: %.300s", out)
	}
}

func bigKeys() string {
	var b strings.Builder
	b.WriteString("[")
	for i := 0; i < 200; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		state := "retired"
		if i == 0 {
			state = "active"
		}
		b.WriteString(`{"kid":"auth-key-` + strconv.Itoa(i) + `","state":"` + state + `","file":"auth-key-` + strconv.Itoa(i) + `.pem","secretName":"s","secretKey":"k"}`)
	}
	return b.String() + "]"
}
