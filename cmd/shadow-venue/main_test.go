package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/shadowvenue"
)

func principalsFile(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	var b strings.Builder
	b.WriteString("kind\tname\temail\tuuid\torg\trole\tpassword_file\n")
	b.WriteString("org\tA\t-\t11111111-1111-4111-8111-111111111111\tA\t-\t-\n")
	b.WriteString("org\tB\t-\t22222222-2222-4222-8222-222222222222\tB\t-\t-\n")
	b.WriteString("org\tC\t-\t33333333-3333-4333-8333-333333333333\tC\t-\t-\n")
	b.WriteString("org\tE\t-\t66666666-6666-4666-8666-666666666666\tE\t-\t-\n")
	for i, p := range []string{"member-A\tA\tmember", "admin-A\tA\towner", "drift-S\tA\tplatform_admin", "superuser\t-\tplatform_admin", "owner-C\tC\towner", "member-E\tE\tmember", "admin-E\tE\tadmin"} {
		f := strings.Split(p, "\t")
		pw := filepath.Join(dir, f[0]+".pw")
		if err := os.WriteFile(pw, []byte("fake\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		b.WriteString("principal\t" + f[0] + "\t" + f[0] + "@example.test\t4444444" + string(rune('0'+i)) + "-4444-4444-8444-444444444444\t" + f[1] + "\t" + f[2] + "\t" + pw + "\n")
	}
	b.WriteString("invoice_open\tinv\t-\t77777777-7777-4777-8777-777777777777\tE\t-\t-\n")
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

// CHAOS-13673 (V6): the record line names org_required and every other reason summed; the retired no_org and
// served_org have no field of their own, so a series of either is only part of "other".
func TestJudgeEnforcedRecordLineHasNoRetiredReason(t *testing.T) {
	series := func(route, reason, n string) string {
		return `dev_health_authz_enforcement_unjudged_total{reason="` + reason + `",route="` + route + `"} ` + n
	}
	scrape := func(lines ...string) []byte {
		return []byte("# HELP dev_health_authz_enforcement_unjudged_total x\n# TYPE dev_health_authz_enforcement_unjudged_total counter\n" +
			strings.Join(lines, "\n") + "\n")
	}
	before := scrape(series("GET /a", "org_required", "1"), series("GET /b", "no_org", "2"))
	after := scrape(series("GET /a", "org_required", "4"), series("GET /b", "no_org", "3"), series("GET /b", "served_org", "2"), series("GET /c", "impersonation", "1"))
	line, ok := unjudgedRecordLine(before, after)
	if !ok || line != "judge-enforced: unjudged org_required 3, other 4" {
		t.Fatalf("record line %q (%v)", line, ok)
	}
	if strings.Contains(line, "no_org") || strings.Contains(line, "served_org") {
		t.Fatalf("the record line names a retired reason: %q", line)
	}
	if _, ok := unjudgedRecordLine(after, before); ok {
		t.Fatal("a counter that went down gave a record line")
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

// -states is an owner-only file: a group- or world-readable one is refused before any plan.
func TestPlanRefusesAStatesFileOthersCanRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "states.tsv")
	if err := os.WriteFile(path, []byte("row\tcaller\tk\tstate\n34\tadmin\t1\tst-a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"plan", "-principals", principalsFile(t), "-states", path}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "owner-only") {
		t.Fatalf("a 0644 states file: code %d, stderr %q", code, errOut.String())
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	errOut.Reset()
	if code := run(context.Background(), []string{"plan", "-principals", principalsFile(t), "-states", path}, &out, &errOut); code != exitOK || strings.Contains(errOut.String()+out.String(), "st-a") {
		t.Fatalf("a 0600 states file: code %d, stderr %q", code, errOut.String())
	}
}

// A drive to a non-loopback base without the egress record of the read before it is refused before any request.
func TestDriveRefusesAVenueIPWithoutItsEgressRecord(t *testing.T) {
	var out, errOut bytes.Buffer
	slots := filepath.Join(t.TempDir(), "slots.tsv")
	code := run(context.Background(), []string{"drive", "-principals", principalsFile(t), "-base", "http://172.30.0.5:8000", "-out", slots}, &out, &errOut)
	if code != exitUsage || !strings.Contains(errOut.String(), "-egress") {
		t.Fatalf("code %d, stderr %q", code, errOut.String())
	}
	if _, err := os.Stat(slots); !os.IsNotExist(err) {
		t.Fatal("drive made its slots file before it refused the base")
	}
}

// The reviewer's form: a -rows table with every external row's ext set to - is refused by plan (and so by drive)
// before any request, naming each row.
func TestPlanRefusesARowTableThatReclassifiesARow(t *testing.T) {
	var lines, external []string
	for _, line := range strings.Split(string(shadowvenue.DefaultRows), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 10 && f[5] != "-" && f[5] != "ext" && f[4] == "no" {
			f[5] = "-"
			external = append(external, f[0])
			line = strings.Join(f, "\t")
		}
		lines = append(lines, line)
	}
	path := filepath.Join(t.TempDir(), "rows.tsv")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"plan", "-principals", principalsFile(t), "-rows", path}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "class") {
		t.Fatalf("plan with the external rows made internal: code %d, stderr %q", code, errOut.String())
	}
	if len(external) != 20 {
		t.Fatalf("%d external rows changed, want 20", len(external))
	}
	for _, id := range external {
		if !strings.Contains(errOut.String(), "row "+id+":") {
			t.Errorf("row %s is not named in the refusal", id)
		}
	}
}

// tableWith writes the embedded table with one row's column col set to value and returns its path.
func tableWith(t *testing.T, id string, col int, value string) string {
	t.Helper()
	var lines []string
	for _, line := range strings.Split(string(shadowvenue.DefaultRows), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 10 && f[0] == id {
			f[col] = value
			line = strings.Join(f, "\t")
		}
		lines = append(lines, line)
	}
	path := filepath.Join(t.TempDir(), "rows.tsv")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// The CLI forms: plan refuses C1 moved onto the org-delete route; judge and judge-enforced void (exit 3, a class line)
// on a -rows table with row 13's destructive flag dropped.
func TestCLIHoldsEveryRowToItsPinnedClass(t *testing.T) {
	var lines []string
	for _, line := range strings.Split(string(shadowvenue.DefaultRows), "\n") {
		if f := strings.Split(line, "\t"); len(f) == 10 && f[0] == "C1" {
			f[1], f[2], f[8] = "DELETE", "/api/v1/admin/orgs/{org_id}", "/api/v1/admin/orgs/{org}"
			line = strings.Join(f, "\t")
		}
		lines = append(lines, line)
	}
	moved := filepath.Join(t.TempDir(), "rows.tsv")
	if err := os.WriteFile(moved, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"plan", "-principals", principalsFile(t), "-rows", moved}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "row C1") {
		t.Fatalf("plan with C1 on the org delete: code %d, stderr %q", code, errOut.String())
	}
	dir := t.TempDir()
	write := func(name, text string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	slots := write("slots.tsv", slotsHeaderV2+"\n"+
		"1\tC1\tsuperuser\tdrift-S\tfalse\tmismatch\t1\tGET\t/api/v1/admin/teams\t/api/v1/admin/teams\t2026-11-07T12:00:00Z\t2026-11-07T12:00:00.1Z\t200\t-\t-\t-\t-\t-\t-\t-\t-\t-\n")
	log := write("go-api.log", "{}\n")
	mismatch := tableWith(t, "13", 4, "no")
	out.Reset()
	errOut.Reset()
	if code := run(context.Background(), []string{"judge", "-slots", slots, "-log", log, "-rows", mismatch, "-out", filepath.Join(dir, "t1.tsv"), "-k", "1"}, &out, &errOut); code != 3 || !strings.Contains(out.String(), "row 13:") {
		t.Fatalf("judge with row 13 destructive=no: code %d, stdout %q, stderr %q", code, out.String(), errOut.String())
	}
	yard := write("yard.tsv", "seq\trow\tcaller\tk\tkey\tstatus\tbody_norm_sha\tverdict\n1\tC1\tsuperuser\t1\tGET /api/v1/admin/teams\t200\t-\tMISMATCH\n")
	d := write("d.txt", strings.Repeat("a", 64)+"\n")
	m := write("m.prom", "# none\n")
	out.Reset()
	errOut.Reset()
	if code := run(context.Background(), []string{"judge-enforced", "-slots", slots, "-yardstick", yard, "-yardstick-digest", d, "-yardstick-digest-after", d,
		"-digest-before", d, "-digest-after", d, "-metrics-before", m, "-metrics-after", m, "-rows", mismatch, "-out", filepath.Join(dir, "t2.tsv"), "-k", "1"}, &out, &errOut); code != 3 || !strings.Contains(out.String(), "row 13:") {
		t.Fatalf("judge-enforced with row 13 destructive=no: code %d, stdout %q, stderr %q", code, out.String(), errOut.String())
	}
}

const slotsHeaderV2 = "seq\trow\tcaller\tprincipal\tcross_org\texpect\tk\tmethod\troute\tpath\tt_start\tt_end\tstatus\tnote\twant_sub\twant_org\tclaim_sub\tclaim_org\tclaim_exp\tbody_len\tbody_sha\tbody_norm_sha"

func slotLine(seq int, norm, claimSub string) string {
	const sha = "ab"
	hex := strings.Repeat(sha, 32)
	return strings.Join([]string{fmt.Sprint(seq), "58", "member", "member-A", "false", "match", "1", "GET", "/api/v1/admin/teams", "/api/v1/admin/teams",
		"2026-11-08T12:00:00Z", "2026-11-08T12:00:01Z", "200", "-", "prn_AAAAAAAAAAAAAAAAAAAAAA", "-", claimSub, "-", "2030-01-01T00:00:00Z", "2", hex, norm}, "\t")
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBodydiffListsTheCellsWhoseNormalizedBodyDiffers(t *testing.T) {
	same := strings.Repeat("cd", 32)
	other := strings.Repeat("ef", 32)
	a := writeFile(t, "a.tsv", slotsHeaderV2+"\n"+slotLine(1, same, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n")
	b := writeFile(t, "b.tsv", slotsHeaderV2+"\n"+slotLine(1, other, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n")
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"bodydiff", "-a", a, "-b", a}, &out, &errOut); code != exitOK || !strings.Contains(out.String(), "0 cells differ") {
		t.Fatalf("equal files: %d %q %q", code, out.String(), errOut.String())
	}
	out.Reset()
	if code := run(context.Background(), []string{"bodydiff", "-a", a, "-b", b}, &out, &errOut); code != exitFail || !strings.Contains(out.String(), "row 58") || !strings.Contains(out.String(), "1 cells differ") {
		t.Fatalf("a differing body: %d %q", code, out.String())
	}
	two := writeFile(t, "two.tsv", slotsHeaderV2+"\n"+slotLine(1, same, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n"+strings.Replace(slotLine(2, same, "prn_AAAAAAAAAAAAAAAAAAAAAA"), "\t58\t", "\t59\t", 1)+"\n")
	if code := run(context.Background(), []string{"bodydiff", "-a", a, "-b", two}, &out, &errOut); code != exitUsage {
		t.Fatalf("files of two sizes were compared: %d", code)
	}
	v1 := writeFile(t, "v1.tsv", "seq\trow\n")
	if code := run(context.Background(), []string{"bodydiff", "-a", a, "-b", v1}, &out, &errOut); code != exitUsage {
		t.Fatalf("a v1 file: %d", code)
	}
}

func TestJudgeIsStrictAndVoidsASlotsFileWithNoClaimReadBack(t *testing.T) {
	dir := t.TempDir()
	slots := filepath.Join(dir, "slots.tsv")
	line := slotLine(1, strings.Repeat("cd", 32), "-")
	if err := os.WriteFile(slots, []byte(slotsHeaderV2+"\n"+line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "go-api.log")
	if err := os.WriteFile(log, []byte(`{"time":"2026-11-08T12:00:00.5Z","msg":"api shadow pdp: result","route":"GET /api/v1/admin/teams","caller":"member","result":"match","decision_id":"dec_x"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"judge", "-slots", slots, "-log", log, "-out", filepath.Join(dir, "t.tsv"), "-k", "1"}, &out, &errOut)
	if code != 3 || !strings.Contains(out.String(), "claim read-back") {
		t.Fatalf("a slots file with no claim read-back: code %d out %q err %q", code, out.String(), errOut.String())
	}
	if !strings.Contains(out.String(), "judge: schedule sha12 ") {
		t.Fatalf("judge prints no schedule sha12 line: %q", out.String())
	}
}

func TestDriveRefusesATokenFileThatNoSlotUsesOrThatIsOpen(t *testing.T) {
	principals := principalsFile(t)
	dir := t.TempDir()
	open := filepath.Join(dir, "open.token")
	if err := os.WriteFile(open, []byte("x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	for name, arg := range map[string]string{"an unknown principal": "ghost=" + open, "an open file": "member-A=" + open, "no path": "member-A", "an empty name": "=" + open} {
		var out, errOut bytes.Buffer
		code := run(context.Background(), []string{"drive", "-base", "http://127.0.0.1:1", "-principals", principals, "-k", "1", "-out", filepath.Join(t.TempDir(), "s.tsv"), "-token-file", arg}, &out, &errOut)
		if code != exitUsage {
			t.Errorf("%s: code %d stderr %q", name, code, errOut.String())
		}
	}
	var out, errOut bytes.Buffer
	args := []string{"drive", "-base", "http://127.0.0.1:1", "-principals", principals, "-k", "1", "-out", filepath.Join(t.TempDir(), "s.tsv"), "-token-file", "member-A=" + open, "-token-file", "member-A=" + open}
	if code := run(context.Background(), args, &out, &errOut); code != exitUsage {
		t.Errorf("a name twice: code %d", code)
	}
}

func TestMintStepWritesTokenFilesAndPrintsNoToken(t *testing.T) {
	const user, orgA = "44444440-4444-4444-8444-444444444444", "11111111-1111-4111-8111-111111111111"
	token := func(org string) string {
		enc := base64.RawURLEncoding
		raw, _ := hex.DecodeString(strings.ReplaceAll(user, "-", ""))
		header := enc.EncodeToString([]byte(`{"alg":"EdDSA","typ":"at+jwt"}`))
		payload := enc.EncodeToString([]byte(`{"sub":"prn_` + enc.EncodeToString(raw) + `","org_id":"` + org + `","exp":1893456000}`))
		return header + "." + payload + "." + enc.EncodeToString([]byte("sig"))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"` + token(orgA) + `"}`))
	}))
	defer srv.Close()
	outDir := filepath.Join(t.TempDir(), "tok")
	if err := os.Mkdir(outDir, 0o700); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"mint", "-base", srv.URL, "-principals", principalsFile(t), "-names", "member-A", "-out-dir", outDir}, &out, &errOut)
	if code != exitOK {
		t.Fatalf("code %d stderr %q", code, errOut.String())
	}
	raw, err := os.ReadFile(filepath.Join(outDir, "member-A.token"))
	if err != nil || strings.Contains(out.String()+errOut.String(), strings.TrimSpace(string(raw))) {
		t.Fatalf("the token file is missing or its token was printed: %v", err)
	}
	if !strings.HasPrefix(out.String(), "member-A\tprn_") {
		t.Fatalf("mint line %q", out.String())
	}
}

// Both judging commands set Strict on the options they build: a run without claim read-back and body digests is never
// judged. The pin reads the source, so a command that drops the field fails here.
func TestBothJudgingCommandsSetStrict(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || (sel.Sel.Name != "JudgeOptions" && sel.Sel.Name != "EnforcedInput") {
			return true
		}
		for _, elt := range lit.Elts {
			if kv, ok := elt.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok && key.Name == "Strict" {
					if val, ok := kv.Value.(*ast.Ident); ok && val.Name == "true" {
						found[sel.Sel.Name] = true
					}
				}
			}
		}
		return true
	})
	if !found["JudgeOptions"] || !found["EnforcedInput"] {
		t.Fatalf("Strict: true is not set on both: %v", found)
	}
}

func TestBodykeyWritesAnOwnerOnlyKeyAndNeverOverwrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "key")
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"bodykey", "-out", path}, &out, &errOut); code != exitOK {
		t.Fatalf("code %d %q", code, errOut.String())
	}
	raw, _ := os.ReadFile(path)
	if strings.Contains(out.String(), strings.TrimSpace(string(raw))) {
		t.Fatal("the key was printed")
	}
	if code := run(context.Background(), []string{"bodykey", "-out", path}, &out, &errOut); code != exitUsage {
		t.Fatalf("an existing key file: code %d", code)
	}
	if code := run(context.Background(), []string{"bodykey"}, &out, &errOut); code != exitUsage {
		t.Fatalf("no -out: code %d", code)
	}
}

func TestDriveTakesTheSideFileAndItsKeyTogetherAndOwnerOnly(t *testing.T) {
	principals := principalsFile(t)
	dir := t.TempDir()
	good := filepath.Join(dir, "key")
	open := filepath.Join(dir, "open")
	if err := os.WriteFile(open, []byte(strings.Repeat("ab", 32)+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code := run(context.Background(), []string{"bodykey", "-out", good}, io.Discard, io.Discard); code != exitOK {
		t.Fatal("bodykey")
	}
	existing := filepath.Join(dir, "side")
	if err := os.WriteFile(existing, []byte("keep\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	base := []string{"drive", "-base", "http://127.0.0.1:1", "-principals", principals, "-k", "1", "-out"}
	for name, extra := range map[string][]string{
		"key without side file": {"-bodykeys-key", good},
		"side file without key": {"-bodykeys-out", filepath.Join(dir, "s1")},
		"an open key":           {"-bodykeys-key", open, "-bodykeys-out", filepath.Join(dir, "s2")},
		"an existing side file": {"-bodykeys-key", good, "-bodykeys-out", existing},
	} {
		var out, errOut bytes.Buffer
		args := append(append(append([]string{}, base...), filepath.Join(t.TempDir(), "slots.tsv")), extra...)
		if code := run(context.Background(), args, &out, &errOut); code != exitUsage {
			t.Errorf("%s: code %d stderr %q", name, code, errOut.String())
		}
	}
	if kept, _ := os.ReadFile(existing); string(kept) != "keep\n" {
		t.Fatal("an existing side file was overwritten")
	}
}

func TestBodydiffWithKeysListsLeafPathsOnly(t *testing.T) {
	same := strings.Repeat("cd", 32)
	other := strings.Repeat("ef", 32)
	slotsA := writeFile(t, "a.tsv", slotsHeaderV2+"\n"+slotLine(1, same, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n")
	slotsB := writeFile(t, "b.tsv", slotsHeaderV2+"\n"+slotLine(1, other, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n")
	keysA := writeFile(t, "ka.tsv", "seq\tpath\thmac12\n1\t$.t\taaaaaaaaaaaa\n1\t$.id\t111111111111\n")
	keysB := writeFile(t, "kb.tsv", "seq\tpath\thmac12\n1\t$.t\tbbbbbbbbbbbb\n1\t$.id\t111111111111\n")
	var out, errOut bytes.Buffer
	code := run(context.Background(), []string{"bodydiff", "-a", slotsA, "-b", slotsB, "-keys-a", keysA, "-keys-b", keysB}, &out, &errOut)
	if code != exitFail || !strings.Contains(out.String(), "$.t") || strings.Contains(out.String(), "$.id") || strings.Contains(out.String(), "aaaaaaaaaaaa") {
		t.Fatalf("code %d out %q", code, out.String())
	}
	errOut.Reset()
	if code := run(context.Background(), []string{"bodydiff", "-a", slotsA, "-b", slotsB, "-keys-a", keysA}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "go together") {
		t.Fatalf("one keys file only: code %d %q", code, errOut.String())
	}
}

func TestBodydiffWithKeysFailsOnADifferingLeafEvenWhenTheNormalizedDigestsAreEqual(t *testing.T) {
	same := strings.Repeat("cd", 32)
	slots := writeFile(t, "s.tsv", slotsHeaderV2+"\n"+slotLine(1, same, "prn_AAAAAAAAAAAAAAAAAAAAAA")+"\n")
	keysA := writeFile(t, "ka.tsv", "seq\tpath\thmac12\n1\t$.t\taaaaaaaaaaaa\n")
	keysB := writeFile(t, "kb.tsv", "seq\tpath\thmac12\n1\t$.t\tbbbbbbbbbbbb\n")
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"bodydiff", "-a", slots, "-b", slots, "-keys-a", keysA, "-keys-b", keysB}, &out, &errOut); code != exitFail || !strings.Contains(out.String(), "$.t") {
		t.Fatalf("code %d out %q", code, out.String())
	}
}

const sideHeaderForTest = "seq\tpath\thmac12"

func writeOwnerOnly(t *testing.T, name, content string, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReadLeafParityTakesThreeFilesTogetherAndAnOwnerOnlyFrozenFile(t *testing.T) {
	side := writeOwnerOnly(t, "side.tsv", sideHeaderForTest+"\n1\t$.a\taaaaaaaaaaaa\n", 0o600)
	frozen := writeOwnerOnly(t, "frozen.tsv", "1\t$.ts\n", 0o600)
	if p, err := readLeafParity("", "", ""); p != nil || err != nil {
		t.Fatalf("no flags: %v %v", p, err)
	}
	for name, args := range map[string][3]string{
		"only the frozen file":       {frozen, "", ""},
		"only the yardstick side":    {"", side, ""},
		"no enforced side":           {frozen, side, ""},
		"no frozen file":             {"", side, side},
		"a frozen file mode 0644":    {writeOwnerOnly(t, "f.tsv", "1\t$.ts\n", 0o644), side, side},
		"a frozen file with a junk":  {writeOwnerOnly(t, "g.tsv", "1\t$.ts\n# note\n", 0o600), side, side},
		"a frozen file not a file":   {t.TempDir(), side, side},
		"a side file that is absent": {frozen, side, filepath.Join(t.TempDir(), "none")},
	} {
		if p, err := readLeafParity(args[0], args[1], args[2]); err == nil || p != nil {
			t.Errorf("%s: accepted (%v)", name, err)
		}
		if strings.HasPrefix(name, "only") || name == "no enforced side" || name == "no frozen file" {
			if _, err := readLeafParity(args[0], args[1], args[2]); !strings.Contains(err.Error(), "go together") {
				t.Errorf("%s: %v, want the three-together refusal", name, err)
			}
		}
	}
	p, err := readLeafParity(frozen, side, side)
	if err != nil || p == nil || len(p.Volatile.SHA12) != 12 || !p.Volatile.ByRow["1"]["$.ts"] {
		t.Fatalf("a good set: %+v %v", p, err)
	}
}

func TestBodydiffPathsNeedsTheSideFiles(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(context.Background(), []string{"bodydiff", "-a", "x", "-b", "y", "-paths"}, &out, &errOut); code != exitUsage || !strings.Contains(errOut.String(), "-paths needs") {
		t.Fatalf("code %d stderr %q", code, errOut.String())
	}
}

func TestLeafPathLinesAreRowTabPathAndNothingElse(t *testing.T) {
	a := writeOwnerOnly(t, "a.tsv", sideHeaderForTest+"\n1\t$.ts\taaaaaaaaaaaa\n1\t$.id\tbbbbbbbbbbbb\n", 0o600)
	b := writeOwnerOnly(t, "b.tsv", sideHeaderForTest+"\n5\t$.ts\tcccccccccccc\n5\t$.id\tbbbbbbbbbbbb\n", 0o600)
	sets := [2][]shadowvenue.Slot{{{Seq: 1, Row: "7", Caller: "admin", K: 1}}, {{Seq: 5, Row: "7", Caller: "admin", K: 1}}}
	lines, err := leafPathLines(sets, a, b)
	if err != nil || len(lines) != 1 || lines[0] != "7\t$.ts" {
		t.Fatalf("%q %v", lines, err)
	}
	var out, errOut bytes.Buffer
	if code := bodydiffPaths(sets, a, b, &out, &errOut); code != exitFail || out.String() != "7\t$.ts\n" || !strings.Contains(errOut.String(), "1 differing leaf paths") {
		t.Fatalf("stdout must be the path lines only: code %d stdout %q stderr %q", code, out.String(), errOut.String())
	}
}
