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
	for i, p := range []string{"member-A\tA\tmember", "admin-A\tA\towner", "drift-S\tA\tplatform_admin", "superuser\t-\tplatform_admin", "owner-C\tC\towner"} {
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
	for n := 1; n <= 3; n++ {
		label := "T" + string(rune('0'+n))
		b.WriteString("org\t" + label + "\t-\t" + poolUUID(n, 0) + "\t" + label + "\t-\t-\n")
		for i, role := range []string{"owner", "admin", "member"} {
			pw := filepath.Join(dir, label+role+".pw")
			if err := os.WriteFile(pw, []byte("fake\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			b.WriteString("principal\t" + role + "-" + label + "\t" + role + "-" + label + "@example.test\t" + poolUUID(n, i+1) + "\t" + label + "\t" + role + "\t" + pw + "\n")
		}
	}
	path := filepath.Join(dir, "principals.tsv")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// poolUUID is the id of pool org n (i 0) or of its i-th principal.
func poolUUID(n, i int) string {
	return "e" + string(rune('0'+n)) + string(rune('0'+i)) + "00000-0000-4000-8000-000000000000"
}

// idsFile is the frozen throwaway ids of the pool T1..T3 of principalsFile.
func idsFile(t *testing.T) string {
	t.Helper()
	var b strings.Builder
	b.WriteString("kind\tlabel\tuuid\n")
	for n := 1; n <= 3; n++ {
		b.WriteString("org\tT" + string(rune('0'+n)) + "\t" + poolUUID(n, 0) + "\n")
		for i := 1; i <= 3; i++ {
			b.WriteString("user\tu\t" + poolUUID(n, i) + "\n")
		}
	}
	path := filepath.Join(t.TempDir(), "ids.tsv")
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
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-destructive"}, &out, &errOut); code != exitUsage {
		t.Fatalf("destructive with no pool: %d", code)
	}
	out.Reset()
	errOut.Reset()
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-destructive", "-throwaway-pool", "T1,T2,T3", "-throwaway-ids", idsFile(t)}, &out, &errOut); code != exitOK {
		t.Fatalf("plan: %d %s", code, errOut.String())
	}
	if !strings.HasPrefix(out.String(), "1\tC1\tsuperuser\tdrift-S\tk=1\tGET /api/v1/admin/teams") {
		t.Fatalf("plan does not start with control C1: %.200s", out.String())
	}
	last := strings.Split(strings.TrimSpace(out.String()), "\n")
	if l := last[len(last)-1]; !strings.Contains(l, "\t13\tsuperuser\t") || !strings.Contains(l, "k=3\tDELETE /api/v1/admin/orgs/"+poolUUID(3, 0)) {
		t.Fatalf("the last slot is not the superuser's delete of pool org T3: %s", l)
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
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-only-rows", "1,999"}, &out, &errOut); code != exitUsage {
		t.Fatalf("-only-rows with an unknown id: %d", code)
	}
	out.Reset()
	if code := run(context.Background(), []string{"plan", "-principals", principals, "-only-rows", "22"}, &out, &errOut); code != exitOK {
		t.Fatalf("-only-rows 22: %d %s", code, errOut.String())
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if row := strings.Split(line, "\t")[1]; row != "C1" && row != "C2" && row != "22" {
			t.Fatalf("-only-rows 22 planned row %s", row)
		}
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

func TestJudgeTakesTheTwoNoOrgScrapesTogetherOrNotAtAll(t *testing.T) {
	dir := t.TempDir()
	one := filepath.Join(dir, "before.prom")
	if err := os.WriteFile(one, []byte("# HELP dev_health_authz_enforcement_unjudged_total x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	args := []string{"judge", "-slots", filepath.Join(dir, "nope"), "-log", filepath.Join(dir, "nope"), "-out", filepath.Join(dir, "t.tsv"), "-noorg-before", one}
	if code := run(context.Background(), args, &out, &errOut); code != exitUsage {
		t.Fatalf("one scrape only: code %d", code)
	}
	if !strings.Contains(errOut.String(), "go together") {
		t.Fatalf("stderr %q", errOut.String())
	}
}

func TestJudgeEnforcedNeedsEveryInput(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"judge-enforced", "-out", filepath.Join(t.TempDir(), "t.tsv")}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "is required") {
		t.Fatalf("no inputs: code %d, stderr %q", code, errOut.String())
	}
}

// -login-only signs each principal in once and sends no row request and writes no slots file.
func TestDriveLoginOnlySendsOnlySignIns(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.Method+" "+r.URL.Path)
		_, _ = w.Write([]byte(`{"access_token":"tok"}`))
	}))
	defer srv.Close()
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"drive", "-login-only", "-base", srv.URL, "-principals", principalsFile(t)}, &out, &errOut)
	if code != exitOK || !strings.Contains(out.String(), "4 sign-ins, no row request") {
		t.Fatalf("code %d out %q err %q", code, out.String(), errOut.String())
	}
	if len(paths) != 4 {
		t.Fatalf("%d requests: %v", len(paths), paths)
	}
	for _, p := range paths {
		if !strings.HasPrefix(p, "POST /api/v1/auth/") {
			t.Errorf("a request that is no sign-in: %s", p)
		}
	}
}
