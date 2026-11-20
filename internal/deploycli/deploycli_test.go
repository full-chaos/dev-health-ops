package deploycli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

const (
	commit = "23bea3be509e238eecbf9ab85200a6af24ac3a4b"
	zero64 = "0000000000000000000000000000000000000000000000000000000000000000"
	good   = "DHO_DEPLOY_INPUTS=1\nTARGET=dev-health\nRUN_TAG=s41-a\nAUTHOR=fcsup\nSTEP=41\n" +
		"SVC_SHA=1111111111111111111111111111111111111111\nROLLBACK_SHA=033140968a4fd14b31cb3606a7fe1ec749f5b8ba\n" +
		"EXP_START_REV=449\nGUARD_LIVE_STEP=6\nPGVER=0066,0162\nRENDER_SHA256=" + zero64 + "\nPODSNAP_TARGETS=dev-health-ops-web\n" +
		"PODSNAP_SCALERS=none\nPGF_LIB_SHA256=" + zero64 + "\nSCRAPE=no\nMIGJOBLOG=yes\nROLLBACK_REV=449\nEXPECT_NEWEST_REV=450\n" +
		"IMGROW=Deployment/dev-health-ops-web .spec.template.spec.containers[0].image 2c87c3613bca 3c61345532cc\n"
	canary = "canary_pass"
)

// pinnedHost is a host whose running binary is pinned: a file, a link standing
// in for /proc/self/exe, and a pin file.
func pinnedHost(t *testing.T) host {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	bin, exe, pin := filepath.Join(dir, "dho"), filepath.Join(dir, "self-exe"), filepath.Join(dir, "dho-deploy.pin")
	body := []byte("the binary bytes")
	sum := sha256.Sum256(body)
	if err := os.WriteFile(bin, body, 0o500); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bin, exe); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pin, []byte(hex.EncodeToString(sum[:])+" "+commit+" "+bin+"\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	return host{pinPath: pin, selfExe: exe, commit: commit}
}

func unpinnedHost(t *testing.T) host {
	h := pinnedHost(t)
	h.commit = strings.Repeat("b", 40)
	return h
}

func exec(t *testing.T, h host, stdin string, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{command(h)}, cli.Env{
		Args: args, Stdin: strings.NewReader(stdin), Stdout: &out, Stderr: &errOut,
		Lookup: func(string) (string, bool) { return "", false },
	})
	return out.String(), errOut.String(), code
}

func sha(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestSelfCheckPrintsLineOneAndPasses(t *testing.T) {
	out, _, code := exec(t, pinnedHost(t), "", "deploy", "self-check")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	want := regexp.MustCompile(`^DHO-DEPLOY 23bea3be509e sha256\(dho\)=[0-9a-f]{64} pin=ok inputs=- sha256\(inputs\)=- cmd=self-check$`)
	if code != cli.ExitOK || !want.MatchString(lines[0]) || lines[len(lines)-1] != "DHO-DEPLOY END rc=0" {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestSelfCheckWithInputsHashesAndParsesStdin(t *testing.T) {
	out, _, code := exec(t, pinnedHost(t), good, "deploy", "self-check", "-")
	if code != cli.ExitOK || !strings.Contains(out, "inputs=stdin sha256(inputs)="+sha(good)+" cmd=self-check -") ||
		!strings.Contains(out, "INPUTS: PASS target=dev-health run_tag=s41-a author=fcsup step=41") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	bad := good + "UNKNOWN_KEY=" + canary + "\n"
	out, _, code = exec(t, pinnedHost(t), bad, "deploy", "self-check", "-")
	if code != cli.ExitRefused || !strings.Contains(out, "sha256(inputs)="+sha(bad)) || !strings.Contains(out, "INPUTS: REFUSED: inputs line 20: unknown key for this target") ||
		strings.Contains(out, canary) || !strings.HasSuffix(out, "DHO-DEPLOY END rc=3\n") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestSelfCheckRefusesAnUnpinnedBinary(t *testing.T) {
	out, _, code := exec(t, unpinnedHost(t), "", "deploy", "self-check")
	if code != cli.ExitRefused || !strings.Contains(out, "pin=refused") || !strings.Contains(out, "PIN: REFUSED: the build commit is not the pinned commit") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
}

func TestArgvIsAFixedGrammar(t *testing.T) {
	for name, args := range map[string][]string{
		"self-check extra word":        {"self-check", "-", "x"},
		"self-check not dash":          {"self-check", canary},
		"roll no words":                {"roll"},
		"roll unknown target":          {"roll", "prod", "pre", "-"},
		"roll unknown phase":           {"roll", "dev-health", "upgrade", "-"},
		"roll ingress wait":            {"roll", "ingress-nginx", "wait", "-"},
		"roll no dash":                 {"roll", "dev-health", "pre"},
		"roll a path for inputs":       {"roll", "dev-health", "pre", "/var/lib/oci-cache/lane-scratch/x"},
		"roll extra word":              {"roll", "dev-health", "pre", "-", canary},
		"unknown flag":                 {"roll", "-" + canary, "dev-health", "pre", "-"},
		"where-it-acts flag":           {"self-check", "--org=" + canary},
		"self-check end-of-flags":      {"self-check", "--"},
		"self-check end-of-flags dash": {"self-check", "--", "-"},
		"roll end-of-flags":            {"roll", "--", "dev-health", "pre", "-"},
		"roll end-of-flags later":      {"roll", "dev-health", "--", "pre", "-"},
		"help with a trailing word":    {"self-check", "--help", canary},
		"help with a value":            {"roll", "--help=" + canary},
		"help after the words":         {"roll", "dev-health", "pre", "-", "-h"},
	} {
		t.Run(name, func(t *testing.T) {
			out, errOut, code := exec(t, pinnedHost(t), good, append([]string{"deploy"}, args...)...)
			if code != cli.ExitUsage || strings.Contains(out+errOut, canary) || strings.Contains(out, "DHO-DEPLOY ") {
				t.Fatalf("exit %d, stdout %q, stderr %q", code, out, errOut)
			}
		})
	}
}

func TestRollIsRefusedAfterItsGates(t *testing.T) {
	out, _, code := exec(t, pinnedHost(t), good, "deploy", "roll", "dev-health", "pre", "-")
	if code != cli.ExitRefused || !strings.Contains(out, "cmd=roll dev-health pre -") || !strings.Contains(out, "INPUTS: PASS") ||
		!strings.Contains(out, "roll is not built in this binary") || !strings.HasSuffix(out, "DHO-DEPLOY END rc=3\n") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	out, _, code = exec(t, pinnedHost(t), good, "deploy", "roll", "acr", "pre", "-")
	if code != cli.ExitRefused || !strings.Contains(out, "INPUTS: REFUSED: TARGET is not the target of the command") {
		t.Fatalf("exit %d:\n%s", code, out)
	}
	out, _, code = exec(t, unpinnedHost(t), good, "deploy", "roll", "dev-health", "pre", "-")
	if code != cli.ExitRefused || !strings.Contains(out, "PIN: REFUSED") || strings.Contains(out, "INPUTS:") {
		t.Fatalf("an unpinned binary must stop before the parse; exit %d:\n%s", code, out)
	}
}

func TestHelpExitsZero(t *testing.T) {
	for _, verb := range []string{"self-check", "roll"} {
		for _, h := range []string{"--help", "-help", "-h"} {
			out, errOut, code := exec(t, pinnedHost(t), "", "deploy", verb, h)
			if code != cli.ExitOK || !strings.Contains(errOut, "usage: dho deploy "+verb) || out != "" {
				t.Fatalf("%s %s: exit %d, %q %q", verb, h, code, out, errOut)
			}
		}
	}
}

// (c): a refusal holds no byte of the input value: not the kit-only TARGET,
// not the unknown key's name, not its value.
func TestRefusalsHoldNoInputValueBytes(t *testing.T) {
	cases := map[string][]string{}
	for _, target := range []string{"ingress-nginx", "acr", "f1pipe"} {
		cases[strings.Replace(good, "TARGET=dev-health", "TARGET="+target, 1)] = []string{target}
	}
	cases[good+"ZZ_CANARY_KEY=canary_unknown_value\n"] = []string{"ZZ_CANARY_KEY", "canary_unknown_value"}
	for buf, values := range cases {
		out, errOut, code := exec(t, pinnedHost(t), buf, "deploy", "self-check", "-")
		if code != cli.ExitRefused || !strings.Contains(out, "INPUTS: REFUSED") {
			t.Fatalf("exit %d:\n%s", code, out)
		}
		for _, v := range values {
			if strings.Contains(out+errOut, v) {
				t.Fatalf("the output holds the input bytes %q:\n%s%s", v, out, errOut)
			}
		}
	}
}
