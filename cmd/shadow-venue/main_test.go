package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func principalsFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	var b strings.Builder
	b.WriteString("kind\tname\temail\tuuid\torg\trole\tpassword_file\n")
	b.WriteString("org\tA\t-\t11111111-1111-4111-8111-111111111111\tA\t-\t-\n")
	b.WriteString("org\tB\t-\t22222222-2222-4222-8222-222222222222\tB\t-\t-\n")
	b.WriteString("org\tC\t-\t33333333-3333-4333-8333-333333333333\tC\t-\t-\n")
	for i, p := range []string{"member-A\tA\tmember", "admin-A\tA\towner", "drift-A\tA\towner", "superuser\t-\tplatform_admin", "owner-C\tC\towner"} {
		f := strings.Split(p, "\t")
		pw := filepath.Join(dir, f[0]+".pw")
		if err := os.WriteFile(pw, []byte("fake\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		b.WriteString("principal\t" + f[0] + "\t" + f[0] + "@example.test\t4444444" + string(rune('0'+i)) + "-4444-4444-8444-444444444444\t" + f[1] + "\t" + f[2] + "\t" + pw + "\n")
	}
	for _, s := range []string{"team", "integration", "dataset", "feature_override"} {
		b.WriteString(s + "\t" + s + "\t-\t55555555-5555-4555-8555-555555555555\tA\t-\t-\n")
	}
	b.WriteString("setting\tgeneral/p_key\t-\t-\tA\t-\t-\n")
	path := filepath.Join(dir, "principals.tsv")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunExitCodes(t *testing.T) {
	principals := principalsFile(t)
	var out, errOut bytes.Buffer
	if code := run(context.Background(), nil, &out, &errOut); code != exitUsage {
		t.Fatalf("no step: %d", code)
	}
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-destructive"}, &out, &errOut); code != exitOK {
		t.Fatalf("plan: %d %s", code, errOut.String())
	}
	if !strings.HasPrefix(out.String(), "1\tC1\tadmin\tdrift-A\tk=1\tGET /api/v1/admin/teams") {
		t.Fatalf("plan does not start with control C1: %.200s", out.String())
	}
	last := strings.Split(strings.TrimSpace(out.String()), "\n")
	if l := last[len(last)-1]; !strings.Contains(l, "\t13\tsuperuser\t") || !strings.Contains(l, "DELETE /api/v1/admin/orgs/33333333") {
		t.Fatalf("the last slot is not the superuser's org C delete: %s", l)
	}
	errOut.Reset()
	slots := filepath.Join(t.TempDir(), "slots.tsv")
	code := run(context.Background(), []string{"drive", "-principals", principals, "-base", "https://api.fullchaos.dev", "-out", slots}, &out, &errOut)
	if code != exitUsage || !strings.Contains(errOut.String(), "loopback") {
		t.Fatalf("a non-loopback base: %d %s", code, errOut.String())
	}
	if _, err := os.Stat(slots); !os.IsNotExist(err) {
		t.Fatal("drive made its slots file before it refused the base")
	}
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-throwaway-org", "A"}, &out, &errOut); code != exitUsage {
		t.Fatalf("throwaway org A: %d", code)
	}
}

// The driver's client answers a redirect itself (CheckRedirect returns http.ErrUseLastResponse): the other origin
// sees no request and no bearer, and the transport has no environment proxy.
func TestTheDriverClientNeverFollowsARedirect(t *testing.T) {
	var seen int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { seen++ }))
	defer other.Close()
	venue := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/stolen", http.StatusTemporaryRedirect)
	}))
	defer venue.Close()
	client := newClient()
	if err := client.CheckRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
	if transport, ok := client.Transport.(*http.Transport); !ok || transport.Proxy != nil {
		t.Fatalf("the transport %T reads an environment proxy", client.Transport)
	}
	req, _ := http.NewRequest(http.MethodGet, venue.URL+"/api/v1/admin/teams", nil)
	req.Header.Set("Authorization", "Bearer venue-token")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect || seen != 0 {
		t.Fatalf("status %d, other origin saw %d requests", resp.StatusCode, seen)
	}
}

// A failed close of an output file is a lost write: it turns a passing step into a failed one and says so.
func TestFinishFailsAStepWhoseOutputFileDidNotClose(t *testing.T) {
	file, err := os.Create(filepath.Join(t.TempDir(), "out.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	var errOut bytes.Buffer
	if code := finish(file, exitOK, &errOut); code != exitOK {
		t.Fatalf("a clean close changed the code to %d", code)
	}
	errOut.Reset()
	if code := finish(file, exitOK, &errOut); code != exitFail || !strings.Contains(errOut.String(), "close") {
		t.Fatalf("a failed close gave code %d, stderr %q", code, errOut.String())
	}
	if code := finish(file, exitUsage, &errOut); code != exitUsage {
		t.Fatalf("an earlier failure was replaced by %d", code)
	}
}
