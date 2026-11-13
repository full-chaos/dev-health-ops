package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/shadowvenue"
)

func egressFile(t *testing.T, extra string) string {
	t.Helper()
	text := "when=before\nnetwork_internal=true\nnetworks=venue-int\ndefault_route=none\npublic_tcp=refused\npublic_dns=fail\nstub_tcp=ok\n" +
		"hosts_pinned=" + strings.Join(shadowvenue.StubHosts, ",") + "\nimage=sha256:" + strings.Repeat("1", 64) + "\nstub_mode=run\n" +
		"stub_config=sha256:" + strings.Repeat("3", 64) + "\nstub_started=2026-11-07T15:00:00Z\ngoapi_ip=10.199.82.20\nrun_id=13267-4a\ndynamic_hosts=unset\n" + extra
	path := filepath.Join(t.TempDir(), "egress-before.txt")
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// drive and mint take the same base rule: a listed ingress IP passes the check (drive then stops at -out, mint at its
// empty name list, so no request is sent); an unlisted one, or any ingress IP of a record with no ingress_ips, is
// refused before any request.
func TestDriveAndMintTakeTheIngressRule(t *testing.T) {
	listed := egressFile(t, "ingress_ips=10.199.86.2\n")
	plain := egressFile(t, "")
	for _, c := range []struct {
		name, base, record, want string
		code                     int
	}{
		{"drive listed", "http://10.199.86.2:8000", listed, "drive: -out is required", exitUsage},
		{"drive unlisted", "http://10.199.86.9:8000", listed, "is not go-api's venue address", exitUsage},
		{"drive no key", "http://10.199.86.2:8000", plain, "is not go-api's venue address", exitUsage},
		{"mint listed", "http://10.199.86.2:8000", listed, "mint: no principal name", exitFail},
		{"mint unlisted", "http://10.199.86.9:8000", listed, "is not go-api's venue address", exitUsage},
		{"mint no key", "http://10.199.86.2:8000", plain, "is not go-api's venue address", exitUsage},
	} {
		t.Run(c.name, func(t *testing.T) {
			args := []string{"-principals", principalsFile(t), "-base", c.base, "-egress", c.record, "-run-id", "13267-4a"}
			if strings.HasPrefix(c.name, "drive") {
				args = append([]string{"drive", "-only-rows", "2"}, args...)
			} else {
				args = append([]string{"mint", "-out-dir", t.TempDir()}, args...)
			}
			var out, errOut bytes.Buffer
			if code := run(context.Background(), args, &out, &errOut); code != c.code || !strings.Contains(errOut.String(), c.want) {
				t.Fatalf("code %d, stderr %q; want %d and %q", code, errOut.String(), c.code, c.want)
			}
		})
	}
}
