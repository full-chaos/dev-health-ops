package billing

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonHelpersProgram runs the REAL Python helpers the routes decide
// with, over the same inputs the Go side sees.
const pythonHelpersProgram = `
import json, os, sys
from fastapi import HTTPException
from dev_health_ops.api.billing._helpers import normalize_billing_tier
from dev_health_ops.api.billing.plan_sync_service import _slugify
import importlib
router = importlib.import_module("dev_health_ops.api.billing.router")
cases = json.loads(sys.stdin.read())
out = {"tier": [normalize_billing_tier(v) for v in cases["tier"]], "slug": [_slugify(v) for v in cases["slug"]], "url": []}
os.environ["APP_BASE_URL"] = cases["app_base_url"]
os.environ["ALLOWED_CHECKOUT_DOMAINS"] = cases["allowed"]
for value in cases["url"]:
    try:
        router._validate_checkout_url(value)
        out["url"].append("ok")
    except HTTPException as exc:
        out["url"].append(exc.detail)
print(json.dumps(out))
`

func TestBillingHelpersMatchLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	tiers := []string{"team", " Team ", "ENTERPRISE", "community\n", "gold", "", "\u00a0team\u00a0", "TEAM\u200b", "İ", "Community"}
	slugs := []string{"Team Plan", "  --Pro!!  ", "Ünïcode Pro!", "İstanbul", "a__b", "", "___", "ẞig", "K\u212aelvin", "tab\tsep"}
	urls := []string{"/ok", "//evil.test/x", "https://app.venue.test/x", "https://app.venue.test", "https://app.venue.testing/x",
		"ftp:/x", "mailto:x@y", "https://evil.test/x", " https://app.venue.test/x", "\thttps://app.venue.test/x", "https:///x",
		"https://alt.venue.test", "https://x.test/pathz", "https://x.test/pat", "HTTPS://APP.VENUE.TEST/x", "1http://a.b/", "h:tp://a/",
		"http://a", "a+b-c.d://host/p", "https://app.venue.test\n/x", "", "x", "https:app.venue.test/x", "https://?q", "https://#f"}
	appBase, allowed := " https://app.venue.test/ ", " https://alt.venue.test , ,https://x.test/path"
	input, _ := json.Marshal(map[string]any{"tier": tiers, "slug": slugs, "url": urls, "app_base_url": appBase, "allowed": allowed})
	command := exec.Command(python, "-c", pythonHelpersProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct{ Tier, Slug, URL []string }
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for index, value := range tiers {
		if got := normalizeTier(value); got != want.Tier[index] {
			t.Errorf("normalizeTier(%q) = %q, python %q", value, got, want.Tier[index])
		}
	}
	for index, value := range slugs {
		if got := slugify(value); got != want.Slug[index] {
			t.Errorf("slugify(%q) = %q, python %q", value, got, want.Slug[index])
		}
	}
	cfg, err := config.Load(config.Spec{Service: config.APIServiceName, LookupEnv: func(key string) (string, bool) {
		switch key {
		case "APP_BASE_URL":
			return appBase, true
		case "ALLOWED_CHECKOUT_DOMAINS":
			return allowed, true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	h := handlers{config: cfg.APIBilling}
	for index, value := range urls {
		got := "ok"
		if answer := h.validateCheckoutURL(value); answer != nil {
			object, isObject := answer.body.(*pyjson.Object)
			if !isObject {
				t.Fatalf("validateCheckoutURL(%q) answered %T", value, answer.body)
			}
			text, _ := object.Get("detail")
			got, _ = text.(string)
		}
		if got != want.URL[index] {
			t.Errorf("validateCheckoutURL(%q) = %q, python %q", value, got, want.URL[index])
		}
	}
	proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR")
	if proof == "" {
		t.Fatal("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR is required")
	}
	if err := os.WriteFile(filepath.Join(proof, "api-billing-helpers"), []byte("executed"), 0o600); err != nil {
		t.Fatal(err)
	}
}
