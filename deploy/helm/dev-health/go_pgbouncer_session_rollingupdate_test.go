package devhealth_test

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"strings"
	"testing"
)

var sessionPoolers = map[string]string{"queueSession": queuePooler, "coordinatorSession": coordinatorPool}

func sessionRU(pooler, v string) []string {
	return []string{"--set-json", sessionRollingUpdateKey(pooler) + "=" + v}
}

func zeroSurge(pooler string) []string { return sessionRU(pooler, `{"maxSurge":0,"maxUnavailable":1}`) }

var goldenRenderArgs = []string{"--set", "web.env.BACKEND_URL=http://backend.example", "--set", "web.authSecret.secretName=web-auth"}

var goldenProdArgs = append(append([]string{}, goldenRenderArgs...),
	"--set", "authService.phase=1", "--set", "authService.database.secretName=auth-db",
	"--set", "authService.signingKey.secretName=auth-key", "--set", "authService.signingKey.keyId=auth-2026-10",
	"--set", "authService.migrate.secretName=auth-migrate-db", "--set", "authService.migrate.secretKey=MIGRATION_URI",
	"--set-json", `authService.networkPolicy.egress.database.to=[{"podSelector":{"matchLabels":{"app":"auth-postgres"}}}]`,
	"--set", "goWorkers.pgbouncer.transaction.poolSize=16", "--set", "goWorkers.pgbouncer.queueSession.poolSize=36",
	"--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10")

const goldenBaseSha = "97c204c79a34175b7ecb81240539bd1bf1dd8cb3"

// CHAOS-11583: with rollingUpdate unset on the session poolers the whole render is
// byte-equal to the base chart. The base chart is taken from git history (the base
// sha must be an ancestor of HEAD) and rendered with the SAME helm binary as the tip,
// so a helm version change cannot make the two differ.
func TestSessionRollingUpdateUnsetRendersByteEqual(t *testing.T) {
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm is not installed")
	}
	if skip, err := checkGoldenBase("../../..", goldenBaseSha); skip != "" {
		t.Skip(skip)
	} else if err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	tarball, err := exec.Command("git", "-C", "../../..", "archive", goldenBaseSha, "deploy/helm/dev-health").Output()
	if err != nil {
		t.Fatalf("git archive %s: %v", goldenBaseSha, err)
	}
	untar := exec.Command("tar", "-x", "-C", baseDir)
	untar.Stdin = bytes.NewReader(tarball)
	if out, err := untar.CombinedOutput(); err != nil {
		t.Fatalf("tar: %v %s", err, out)
	}
	for name, args := range map[string][]string{"default": goldenRenderArgs, "prod-like": goldenProdArgs} {
		render := func(dir string) string {
			cmd := exec.Command("helm", append([]string{"template", "a", "."}, args...)...)
			cmd.Dir = dir
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s render in %s: %v\n%.300s", name, dir, err, out)
			}
			return string(out)
		}
		base, tip := render(baseDir+"/deploy/helm/dev-health"), render(".")
		if base == tip {
			continue
		}
		bl, tl := strings.Split(base, "\n"), strings.Split(tip, "\n")
		for i := 0; i < len(bl) || i < len(tl); i++ {
			var b, c string
			if i < len(bl) {
				b = bl[i]
			}
			if i < len(tl) {
				c = tl[i]
			}
			if b != c {
				t.Errorf("%s render differs from base %s at line %d:\n  base: %s\n  tip:  %s", name, goldenBaseSha[:10], i+1, b, c)
				break
			}
		}
	}
}

func TestSessionRollingUpdateSchemaAcceptsAndRefuses(t *testing.T) {
	for pooler := range sessionPoolers {
		for name, v := range map[string]string{
			"surge 0 unavailable 1": `{"maxSurge":0,"maxUnavailable":1}`,
			"surge 1 unavailable 0": `{"maxSurge":1,"maxUnavailable":0}`,
			"surge 2":               `{"maxSurge":2}`,
			"empty":                 `{}`,
		} {
			if _, out, err := renderAuth(t, authWith(sessionRU(pooler, v)...)...); err != nil {
				t.Errorf("%s %s must render: %v\n%.400s", pooler, name, err, out)
			}
		}
		for name, v := range map[string]string{
			"surge 0 alone": `{"maxSurge":0}`,
			"both zero":     `{"maxSurge":0,"maxUnavailable":0}`,
			"percentage":    `{"maxSurge":"25%"}`,
			"too large":     `{"maxSurge":11}`,
			"unknown key":   `{"minReady":1}`,
		} {
			_, out, err := renderAuth(t, authWith(sessionRU(pooler, v)...)...)
			if err == nil || !strings.Contains(out, schemaRefusal) || !strings.Contains(out, pooler+"/rollingUpdate") {
				t.Errorf("%s %s: the SCHEMA must refuse: err=%v out=%.400s", pooler, name, err, out)
			}
		}
	}
}

func TestSessionRollingUpdateRendersStrategyOnThatPoolerOnly(t *testing.T) {
	for pooler, name := range sessionPoolers {
		docs, out, err := renderAuth(t, authWith(zeroSurge(pooler)...)...)
		if err != nil {
			t.Fatalf("%s: %v\n%.300s", pooler, err, out)
		}
		ru := at(authDoc(docs, "Deployment", name), "spec", "strategy", "rollingUpdate")
		if (at(ru, "maxSurge") != float64(0) && at(ru, "maxSurge") != 0) || (at(ru, "maxUnavailable") != float64(1) && at(ru, "maxUnavailable") != 1) {
			t.Errorf("%s Deployment must carry maxSurge 0 / maxUnavailable 1, got %v", pooler, ru)
		}
		for _, other := range []string{transactionPooler, queuePooler, coordinatorPool} {
			if other == name {
				continue
			}
			if s := at(authDoc(docs, "Deployment", other), "spec", "strategy"); s != nil {
				t.Errorf("%s: %s must keep the default strategy, got %v", pooler, other, s)
			}
		}
	}
}

// Prod shape: txn (16 + KEDA 4) = 20, queue 36, coordinator 10, direct 24. Every
// pooler at maxSurge 0 counts 90. One session pooler at surge 1 adds its pool.
func TestSessionSurgeIsCountedPerPooler(t *testing.T) {
	allZero := append(append(zeroSurge("queueSession"), zeroSurge("coordinatorSession")...),
		"--set-json", txnSurgeKey+`={"maxSurge":0,"maxUnavailable":1}`)
	pools := []string{"--set", "goWorkers.pgbouncer.queueSession.poolSize=36", "--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10"}
	with := func(budget string, extra ...string) []string {
		return authWith(append(append(append(append([]string{}, pools...), allZero...), "--set", "authService.database.connectionBudget="+budget), extra...)...)
	}
	if _, out, err := renderAuth(t, with("90")...); err != nil {
		t.Fatalf("all three at maxSurge 0 count 90 and must fit 90: %v\n%.400s", err, out)
	}
	if _, out, err := renderAuth(t, with("89")...); err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "90") {
		t.Fatalf("90 over 89 must fail: err=%v out=%.400s", err, out)
	}
	for pooler, want := range map[string]int{"queueSession": 126, "coordinatorSession": 100} {
		one := sessionRU(pooler, `{"maxSurge":1}`)
		if _, out, err := renderAuth(t, with(itoa(want), one...)...); err != nil {
			t.Errorf("%s surge 1 counts %d and must fit %d: %v\n%.300s", pooler, want, want, err, out)
		}
		_, out, err := renderAuth(t, with(itoa(want-1), one...)...)
		if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, itoa(want)) {
			t.Errorf("%s surge 1 counts %d and must fail %d: err=%v out=%.300s", pooler, want, want-1, err, out)
		}
	}
}

// Option B (acp, CHAOS-11583): an UNSET session surge counts one pod, the
// Kubernetes default, so the budget cannot pass a roll that exceeds the limit.
func TestSessionSurgeUnsetCountsOnePod(t *testing.T) {
	// 2 x 20 + 2 x 36 + 2 x 10 + 24 = 156 at prod pool sizes with every pooler unset.
	args := authWith("--set", "goWorkers.pgbouncer.queueSession.poolSize=36", "--set", "goWorkers.pgbouncer.coordinatorSession.poolSize=10",
		"--set", "goWorkers.pgbouncer.transaction.poolSize=16", "--set", "authService.database.connectionBudget=97")
	_, out, err := renderAuth(t, args...)
	if err == nil || !strings.Contains(out, budgetRefusal) || !strings.Contains(out, "156") ||
		!strings.Contains(out, "queue-session pool 36 x (1 + 1 surge pod) = 72") || !strings.Contains(out, "coordinator-session pool 10 x (1 + 1 surge pod) = 20") {
		t.Fatalf("unset session surge must count one pod each (156): err=%v out=%.600s", err, out)
	}
	// chart defaults: 2 x 20 + 2 x 50 + 2 x 14 + 24 = 192.
	if _, out, err := renderAuth(t, authWith("--set", "authService.database.connectionBudget=97")...); err == nil || !strings.Contains(out, "192") {
		t.Fatalf("chart defaults count 192: err=%v out=%.400s", err, out)
	}
}

func TestSessionRollingUpdateHelperRefusesWithoutSchema(t *testing.T) {
	for pooler := range sessionPoolers {
		for name, v := range map[string]string{
			"percentage":    `{"maxSurge":"25%"}`,
			"negative":      `{"maxSurge":-1}`,
			"too large":     `{"maxSurge":11}`,
			"fraction":      `{"maxUnavailable":0.5}`,
			"both zero":     `{"maxSurge":0,"maxUnavailable":0}`,
			"surge 0 alone": `{"maxSurge":0}`,
			"unknown key":   `{"minReady":1}`,
		} {
			_, out, err := renderAuth(t, authWith(append(sessionRU(pooler, v), "--skip-schema-validation")...)...)
			if err == nil || !strings.Contains(out, "goWorkers.pgbouncer."+pooler+".rollingUpdate") {
				t.Errorf("%s %s: want the helper refusal naming the key: err=%v out=%.300s", pooler, name, err, out)
			}
		}
	}
}

// The transaction messages did not change when the helper was generalised.
func TestTransactionRollingUpdateMessagesAreByteIdentical(t *testing.T) {
	for v, want := range map[string]string{
		`{"maxSurge":"25%"}`:      "goWorkers.pgbouncer.transaction.rollingUpdate.maxSurge must be a whole number from 0 to 10: the connection budget counts surge pods, a percentage cannot be counted",
		`{"minReady":1}`:          "goWorkers.pgbouncer.transaction.rollingUpdate.minReady is not a known key: use maxSurge and maxUnavailable",
		`{"maxSurge":0}`:          "goWorkers.pgbouncer.transaction.rollingUpdate: maxSurge 0 needs maxUnavailable of 1 or more (Kubernetes refuses a rolling update that may neither surge nor remove a pod)",
		`{"maxUnavailable":"1%"}`: "goWorkers.pgbouncer.transaction.rollingUpdate.maxUnavailable must be a whole number from 0 to 10: the connection budget counts surge pods, a percentage cannot be counted",
	} {
		_, out, err := renderAuth(t, authWith("--set-json", txnSurgeKey+"="+v, "--skip-schema-validation")...)
		if err == nil || !strings.Contains(out, want) {
			t.Errorf("%s: want exactly %q: err=%v out=%.400s", v, want, err, out)
		}
	}
}

const goldenNoGitSkip = "no .git: the golden base sha cannot be checked as an ancestor of HEAD; the go-quality legs check out full history (fetch-depth 0) and run this test there"

// checkGoldenBase returns a skip message ONLY when repo has no .git at all. A sha
// missing from the history (a shallow clone) is an error, never a skip.
func checkGoldenBase(repo, sha string) (skip string, err error) {
	if _, e := os.Stat(repo + "/.git"); e != nil {
		if errors.Is(e, fs.ErrNotExist) {
			return goldenNoGitSkip, nil
		}
		return "", fmt.Errorf("golden base check: cannot stat %s/.git: %v", repo, e)
	}
	if out, e := exec.Command("git", "-C", repo, "cat-file", "-e", sha+"^{commit}").CombinedOutput(); e != nil {
		return "", fmt.Errorf("golden base %s not in history: run with full history (%v %s)", sha, e, strings.TrimSpace(string(out)))
	}
	if out, e := exec.Command("git", "-C", repo, "merge-base", "--is-ancestor", sha, "HEAD").CombinedOutput(); e != nil {
		return "", fmt.Errorf("golden base sha %s is not an ancestor of HEAD (set goldenBaseSha to a main sha that HEAD contains): %v %s", sha, e, out)
	}
	return "", nil
}

func TestGoldenBaseCheckFailsOnAbsentShaAndNonAncestorAndSkipsOnlyWithoutGit(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	repo := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", repo, "-c", "user.name=t", "-c", "user.email=t@example.com", "-c", "commit.gpgsign=false"}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q", "-b", "main")
	git("commit", "-q", "--allow-empty", "-m", "base")
	base := git("rev-parse", "HEAD")
	git("checkout", "-q", "-b", "side")
	git("commit", "-q", "--allow-empty", "-m", "side")
	side := git("rev-parse", "HEAD")
	git("checkout", "-q", "main")
	git("commit", "-q", "--allow-empty", "-m", "main2")

	if skip, err := checkGoldenBase(repo, base); skip != "" || err != nil {
		t.Errorf("an ancestor must pass: skip=%q err=%v", skip, err)
	}
	absent := strings.Repeat("ab", 20)
	skip, err := checkGoldenBase(repo, absent)
	if skip != "" || err == nil || !strings.HasPrefix(err.Error(), "golden base "+absent+" not in history: run with full history") {
		t.Errorf("an absent sha must FAIL with the exact message, never skip: skip=%q err=%v", skip, err)
	}
	skip, err = checkGoldenBase(repo, side)
	if skip != "" || err == nil || !strings.HasPrefix(err.Error(), "golden base sha "+side+" is not an ancestor of HEAD") {
		t.Errorf("a present non-ancestor sha must FAIL: skip=%q err=%v", skip, err)
	}
	skip, err = checkGoldenBase(t.TempDir(), base)
	if skip != goldenNoGitSkip || err != nil {
		t.Errorf("no .git must skip with the named message: skip=%q err=%v", skip, err)
	}
	notDir := t.TempDir() + "/file"
	if e := os.WriteFile(notDir, nil, 0o600); e != nil {
		t.Fatal(e)
	}
	skip, err = checkGoldenBase(notDir, base)
	if skip != "" || err == nil || !strings.HasPrefix(err.Error(), "golden base check: cannot stat "+notDir+"/.git:") {
		t.Errorf("a stat error other than not-exist must FAIL, never skip: skip=%q err=%v", skip, err)
	}
}

// A non-map rollingUpdate (false, a string, a list) must reach the helper and be
// refused, for every pooler: the chart default must not replace an explicit value.
func TestPgbouncerRollingUpdateNonMapIsRefusedByTheHelper(t *testing.T) {
	for _, pooler := range []string{"transaction", "queueSession", "coordinatorSession"} {
		for name, v := range map[string]string{"false": `false`, "string": `"x"`, "list": `[1]`, "number": `5`} {
			set := []string{"--set-json", "goWorkers.pgbouncer." + pooler + ".rollingUpdate=" + v, "--skip-schema-validation"}
			_, out, err := renderAuth(t, authWith(set...)...)
			want := "goWorkers.pgbouncer." + pooler + ".rollingUpdate must be a map of maxSurge and maxUnavailable, or unset"
			if err == nil || !strings.Contains(out, want) {
				t.Errorf("%s %s: want the helper refusal %q: err=%v out=%.300s", pooler, name, want, err, out)
			}
		}
	}
}

// Without --skip-schema-validation the same non-map values are refused by the schema.
func TestPgbouncerRollingUpdateNonMapIsRefusedBySchema(t *testing.T) {
	for _, pooler := range []string{"transaction", "queueSession", "coordinatorSession"} {
		for name, v := range map[string]string{"false": `false`, "string": `"x"`, "list": `[1]`, "number": `5`} {
			_, out, err := renderAuth(t, authWith("--set-json", "goWorkers.pgbouncer."+pooler+".rollingUpdate="+v)...)
			if err == nil || !strings.Contains(out, schemaRefusal) || !strings.Contains(out, pooler+"/rollingUpdate") {
				t.Errorf("%s %s: the SCHEMA must refuse: err=%v out=%.300s", pooler, name, err, out)
			}
		}
	}
}

// An explicit null is "unset": it renders the same as no key, with and without the schema.
func TestPgbouncerRollingUpdateExplicitNullRendersAsUnset(t *testing.T) {
	_, want, err := renderAuth(t, authWith()...)
	if err != nil {
		t.Fatalf("unset render: %v\n%.300s", err, want)
	}
	for _, pooler := range []string{"transaction", "queueSession", "coordinatorSession"} {
		for _, skip := range []bool{false, true} {
			args := authWith("--set-json", "goWorkers.pgbouncer."+pooler+".rollingUpdate=null")
			if skip {
				args = append(args, "--skip-schema-validation")
			}
			_, out, err := renderAuth(t, args...)
			if err != nil {
				t.Errorf("%s null (skip schema %v) must render: %v\n%.300s", pooler, skip, err, out)
			} else if out != want {
				t.Errorf("%s null (skip schema %v) must render byte-equal to unset", pooler, skip)
			}
		}
	}
}
