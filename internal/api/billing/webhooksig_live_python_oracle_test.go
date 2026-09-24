package billing

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// pythonSignatureProgram classifies each [payload b64, header b64] with the
// real stripe-python WebhookSignature.verify_header (default tolerance):
// "ok", "sig" (SignatureVerificationError) or "crash" (any other
// exception). The header bytes reach it latin-1 decoded, as Starlette
// hands them over.
const pythonSignatureProgram = `
import base64, json, sys
from stripe import SignatureVerificationError
from stripe._webhook import WebhookSignature, Webhook
cases = json.loads(sys.stdin.read())
out = []
for payload, header in cases["cases"]:
    try:
        WebhookSignature.verify_header(base64.b64decode(payload), base64.b64decode(header).decode("latin-1"), cases["secret"], Webhook.DEFAULT_TOLERANCE)
        out.append("ok")
    except SignatureVerificationError:
        out.append("sig")
    except Exception as exc:
        out.append("crash:" + type(exc).__name__)
print(json.dumps(out))
`

func sign(secret, timestamp string, payload []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp + "."))
	mac.Write(payload)
	return hex.EncodeToString(mac.Sum(nil))
}

// TestStripeSignatureMatchesLivePython holds verifyStripeSignature to
// stripe-python's verify_header over header shapes, timestamp spellings,
// signature lists and bodies.
func TestStripeSignatureMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	const secret = "whsec_venue_oracle"
	now := time.Now().Unix()
	body := []byte(`{"id":"evt_1","object":"event","type":"invoice.paid","data":{"object":{"id":"in_1","metadata":{"org_id":"x"}}}}`)
	good := func(ts int64) string { return sign(secret, fmt.Sprint(ts), body) }
	fresh, old, future := now, now-1000, now+1000
	type sigCase struct {
		payload []byte
		header  string
	}
	cases := []sigCase{
		{body, fmt.Sprintf("t=%d,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v1=%s", old, good(old))},
		{body, fmt.Sprintf("t=%d,v1=%s", future, good(future))},
		{body, fmt.Sprintf("t=%d,v1=%s", now-299, good(now-299))},
		{body, fmt.Sprintf("t=%d,v1=%s", now-310, good(now-310))},
		{body, fmt.Sprintf("v1=%s,t=%d", good(fresh), fresh)},
		{body, fmt.Sprintf("t=%d,v1=deadbeef,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v0=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v1=%s", fresh, strings.ToUpper(good(fresh)))},
		{body, fmt.Sprintf("t=%d,v1=%s=extra", fresh, good(fresh))},
		{body, fmt.Sprintf("t=+%d,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t= %d ,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=\xa0%d\xa0,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%s_%s,v1=%s", fmt.Sprint(fresh)[:4], fmt.Sprint(fresh)[4:], good(fresh))},
		{body, fmt.Sprintf("t=0%d,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,t=%d,v1=%s", fresh, old, good(fresh))},
		{body, fmt.Sprintf("t=%d,t,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v1,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=abc,v1=%s", good(fresh))},
		{body, fmt.Sprintf("v1=%s", good(fresh))},
		{body, fmt.Sprintf("t=%d", fresh)},
		{body, ""},
		{body, ","},
		{body, "garbage"},
		{body, fmt.Sprintf(" t=%d,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d, v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v1=\xe9\xe9,v1=%s", fresh, good(fresh))},
		{body, fmt.Sprintf("t=%d,v1=%s,v1=\xe9", fresh, good(fresh))},
		{[]byte("\xff\xfe not utf-8"), fmt.Sprintf("t=%d,v1=%s", fresh, sign(secret, fmt.Sprint(fresh), []byte("\xff\xfe not utf-8")))},
		{[]byte(""), fmt.Sprintf("t=%d,v1=%s", fresh, sign(secret, fmt.Sprint(fresh), nil))},
		{[]byte("\ufeff{}"), fmt.Sprintf("t=%d,v1=%s", fresh, sign(secret, fmt.Sprint(fresh), []byte("\ufeff{}")))},
		{body, fmt.Sprintf("t=%d,v1=%s", fresh, sign("whsec_other", fmt.Sprint(fresh), body))},
	}
	encoded := make([][2]string, len(cases))
	for index, c := range cases {
		encoded[index] = [2]string{base64.StdEncoding.EncodeToString(c.payload), base64.StdEncoding.EncodeToString([]byte(c.header))}
	}
	input, _ := json.Marshal(map[string]any{"cases": encoded, "secret": secret})
	command := exec.Command(python, "-c", pythonSignatureProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	checkedAt := time.Now()
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(want) != len(cases) {
		t.Fatalf("python answered %d of %d", len(want), len(cases))
	}
	verdicts := map[string]int{}
	for index, c := range cases {
		got := "ok"
		switch err := verifyStripeSignature(c.payload, c.header, secret, checkedAt); err {
		case nil:
		case errSignature:
			got = "sig"
		default:
			got = "crash"
		}
		wanted := want[index]
		if strings.HasPrefix(wanted, "crash:") {
			wanted = "crash"
		}
		verdicts[wanted]++
		if got != wanted {
			t.Errorf("case %d header %q: go %s, python %s", index, c.header, got, want[index])
		}
	}
	for _, verdict := range []string{"ok", "sig", "crash"} {
		if verdicts[verdict] == 0 {
			t.Errorf("no case reached %q: the comparison did not cover that outcome", verdict)
		}
	}
	t.Logf("%d cases: %v", len(cases), verdicts)
	if proof := os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"); proof != "" {
		if err := os.WriteFile(filepath.Join(proof, "api-billing-webhook-signature"), []byte("executed"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}
