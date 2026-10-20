package bigboy_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const cutRepinWeb = "ci/bigboy/bigboy-repin-web.sh"

var (
	cutRWOld    = "sha256:" + strings.Repeat("a", 64)
	cutRWNew    = "sha256:" + strings.Repeat("b", 64)
	cutRWHead   = strings.Repeat("1", 40)
	cutRWOther  = strings.Repeat("2", 40)
	cutRWParent = strings.Repeat("3", 40)
)

type cutRWOpts struct {
	repin   string // "-" leaves WEB_REPIN unset
	images  []string
	history []string
	changed []string
	extra   []string
}

type cutRWResult struct {
	scriptRun
	overlay, calls string
}

func cutRunRepinWeb(g *guard, o cutRWOpts) cutRWResult {
	work := cutTemp(g, "repin-web")
	root := filepath.Join(work, "root")
	overlay := filepath.Join(root, "_records", "images.env")
	cutWrite(g, overlay, "BIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web@"+cutRWOld+"\n", 0o644)
	bin := cutMkdir(g, work, "bin")
	log := filepath.Join(work, "calls.log")
	history := o.history
	if history == nil {
		history = []string{cutRWHead}
	}
	cutWrite(g, filepath.Join(work, "history.txt"), strings.Join(history, "\n")+"\n", 0o644)
	changed := ""
	for _, f := range o.changed {
		changed += f + "\n"
	}
	cutWrite(g, filepath.Join(work, "changed.txt"), changed, 0o644)
	cutStub(g, bin, "gh", fmt.Sprintf(`echo "gh $*" >> %q
case "$2" in
  */commits/main) echo %s ;;
  */commits?sha=*) cat %q ;;
  */compare/*) cat %q ;;
esac
`, log, cutRWHead, filepath.Join(work, "history.txt"), filepath.Join(work, "changed.txt")))
	var cases []string
	for _, c := range o.images {
		cases = append(cases, fmt.Sprintf(`*"dev-health-web:sha-%s "*`, c[:7]))
	}
	if len(cases) == 0 {
		cases = []string{"__none__"}
	}
	raw := `{"manifests":[{"digest":"` + cutRWNew + `","platform":{"os":"linux","architecture":"arm64"}}]}`
	cutStub(g, bin, "docker", fmt.Sprintf(`echo "docker $*" >> %q
case "$* " in %s) if [[ "$*" == *--raw* ]]; then echo '%s'; else echo '{"digest":"%s"}'; fi ;; esac
`, log, strings.Join(cases, "|"), raw, cutRWNew))
	env := []string{"PATH=" + bin + ":" + os.Getenv("PATH"), "HOME=" + work, "BIGBOY_ROOT=" + root,
		"WEB_REPIN_WAIT_TRIES=1", "WEB_REPIN_WAIT_SECS=0"}
	if o.repin != "-" {
		env = append(env, "WEB_REPIN="+o.repin)
	}
	env = append(env, o.extra...)
	r := cutExec(g, work, env, "bash", g.path(cutRepinWeb))
	return cutRWResult{scriptRun: r, overlay: cutFileText(overlay), calls: cutFileText(log)}
}

func (r cutRWResult) unchanged() bool {
	return strings.Contains(r.overlay, cutRWOld) && !strings.Contains(r.overlay, cutRWNew)
}

func (r cutRWResult) pinned() bool {
	return strings.Contains(r.overlay, cutRWNew) && !strings.Contains(r.overlay, cutRWOld)
}

func (r cutRWResult) dump() string {
	return fmt.Sprintf("rc=%d stdout=%q stderr=%q calls=%q overlay=%q", r.code, r.stdout, r.stderr, r.calls, r.overlay)
}

func cutIsMigrate(c cutCall) bool { return c.compose("run", "--rm", "--no-deps", "-T", "migrate") }

func cutCheckWebRepinOrder(g *guard) {
	r := cutRunCut(g, cutOpts{})
	repin := cutFind(r.calls, 0, cutTool("bigboy-repin.sh"))
	web := cutFind(r.calls, repin, cutTool("bigboy-repin-web.sh"))
	migrate := cutFind(r.calls, 0, cutIsMigrate)
	if repin < 0 || web < 0 || migrate < 0 || web > migrate {
		g.failf("the web repin (call %d) must run after the ops image repin (call %d) and before migrate (call %d): %s", web, repin, migrate, r.dump())
	}
}

func cutCheckUpRecreatesWeb(g *guard) {
	r := cutRunCut(g, cutOpts{})
	i := cutFind(r.calls, 0, func(c cutCall) bool { return c.compose("up", "query-api", "go-api") })
	if i < 0 {
		g.failf("no `up` of query-api and go-api: %s", r.dump())
		return
	}
	g.expect(r.calls[i].has("web"), "`web` is not recreated in the same up invocation as query-api/go-api: %s", r.calls[i].line())
	g.expect(r.calls[i].has("--no-build"), "the up of query-api/go-api/web must pass --no-build: %s", r.calls[i].line())
}

func cutCheckRepinWebNeverBuilds(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", images: []string{cutRWHead}})
	g.expect(r.code == 0, "the head repin failed: %s", r.dump())
	for _, ln := range strings.Split(r.calls, "\n") {
		if !strings.HasPrefix(ln, "docker ") {
			continue
		}
		for _, w := range strings.Fields(ln) {
			if w == "build" || strings.HasPrefix(w, "--build") {
				g.failf("bigboy-repin-web.sh invoked a host build: %s", ln)
			}
		}
	}
	g.expect(strings.Contains(r.calls, "docker buildx imagetools inspect"), "the digest must be resolved via imagetools: %q", r.calls)
	g.expect(r.pinned(), "the pinned digest is not the one imagetools resolved (hand-typed digest?): %q", r.overlay)
}

func cutCheckRepinWebFromWebMain(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", images: []string{cutRWHead}})
	g.expect(strings.HasPrefix(r.calls, "gh api repos/full-chaos/dev-health-web/commits/main"),
		"the web image must be resolved from dev-health-web/commits/main, its own main HEAD: %q", r.calls)
}

func cutCheckRepinSkip(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "skip", images: []string{cutRWHead}})
	g.expect(r.code == 0, "WEB_REPIN=skip failed: %s", r.dump())
	g.expect(strings.Contains(r.stdout, "SKIPPED") && strings.Contains(r.stdout, cutRWOld), "WEB_REPIN=skip must say SKIPPED and name the kept digest: %s", r.stdout)
	g.expect(r.unchanged(), "WEB_REPIN=skip changed the overlay: %q", r.overlay)
	g.expect(r.calls == "", "WEB_REPIN=skip must not call gh or docker: %q", r.calls)
}

func cutCheckRepinExplicitSHA(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: cutRWOther, images: []string{cutRWOther}})
	g.expect(r.code == 0, "an explicit sha failed: %s", r.dump())
	g.expect(r.pinned(), "an explicit sha did not pin its image: %q", r.overlay)
	g.expect(!strings.Contains(r.calls, "gh "), "an explicit sha must not resolve web main HEAD: %q", r.calls)
	g.expect(strings.Contains(r.calls, "sha-"+cutRWOther[:7]), "an explicit sha must look up its own image tag: %q", r.calls)
}

func cutCheckRepinDefaultHead(g *guard) {
	for _, repin := range []string{"-", "head"} {
		r := cutRunRepinWeb(g, cutRWOpts{repin: repin, images: []string{cutRWHead}})
		g.expect(r.code == 0, "WEB_REPIN=%s failed: %s", repin, r.dump())
		g.expect(strings.Contains(r.overlay, cutRWNew), "WEB_REPIN=%s did not pin web main HEAD's image: %q", repin, r.overlay)
		g.expect(strings.HasPrefix(r.calls, "gh api repos/full-chaos/dev-health-web/commits/main"), "WEB_REPIN=%s does not start from web main HEAD: %q", repin, r.calls)
	}
}

func cutCheckRepinRefusesOthers(g *guard) {
	for _, bad := range []string{"SKIP", "main", "abc123", cutRWOther[:39], strings.ReplaceAll(strings.ToUpper(cutRWOther), "2", "A")} {
		r := cutRunRepinWeb(g, cutRWOpts{repin: bad, images: []string{cutRWHead}})
		g.expect(r.code == 2, "WEB_REPIN=%q must be refused rc=2, got rc=%d: %s", bad, r.code, r.stdout)
		g.expect(r.unchanged(), "WEB_REPIN=%q changed the overlay: %q", bad, r.overlay)
		g.expect(r.calls == "", "WEB_REPIN=%q must be refused before any call: %q", bad, r.calls)
	}
}

func cutCheckRepinExplicitNoImage(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: cutRWOther, images: []string{cutRWHead}})
	g.expect(r.code == 3, "an explicit sha without an image must fail rc=3, got rc=%d: %s", r.code, r.stdout)
	g.expect(strings.Contains(r.stdout, "no CI image") && strings.Contains(r.stdout, "WEB_REPIN="), "the missing image is not named: %s", r.stdout)
	g.expect(r.unchanged(), "an explicit sha without an image changed the overlay: %q", r.overlay)
}

func cutCheckRepinWaitBounds(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", images: []string{cutRWHead}, extra: []string{"WEB_REPIN_WAIT_TRIES=forty"}})
	g.expect(r.code == 2 && r.calls == "", "a non-numeric wait bound must be refused rc=2 before any call: %s", r.dump())
	g.expect(strings.Contains(r.overlay, cutRWOld), "a non-numeric wait bound changed the overlay: %q", r.overlay)
}

// The documentation of a choice is comment text, so this one stays a line scan.
func cutCheckCutDocumentsWebRepin(g *guard) {
	lines := strings.Split(g.read(cutScript), "\n")
	for i, ln := range lines {
		if strings.Contains(ln, "bigboy-repin-web.sh") && !strings.HasPrefix(strings.TrimLeft(ln, " \t"), "#") {
			for _, above := range lines[max(0, i-4):i] {
				if strings.Contains(above, "WEB_REPIN") {
					return
				}
			}
			g.failf("bigboy-cut.sh must document WEB_REPIN beside the repin-web call (line %d)", i+1)
			return
		}
	}
	g.failf("bigboy-cut.sh never calls bigboy-repin-web.sh")
}

func cutCheckCIOnlyHead(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", history: []string{cutRWHead, cutRWParent}, images: []string{cutRWParent},
		changed: []string{".github/workflows/live-e2e.yml", ".github/scripts/x.sh"}})
	g.expect(r.code == 0, "a CI-only HEAD must take the newest imaged commit: %s", r.dump())
	g.expect(r.pinned(), "a CI-only HEAD did not pin the newest imaged commit: %q", r.overlay)
	rec := fmt.Sprintf("repin-web record: mode=head web_head=%s image_commit=%s digest=%s", cutRWHead, cutRWParent, cutRWNew)
	g.expect(strings.Contains(r.stdout, rec), "no record line %q: %s", rec, r.stdout)
	g.expect(strings.Contains(r.calls, "sha-"+cutRWHead[:7]) && strings.Contains(r.calls, "sha-"+cutRWParent[:7]), "HEAD and its parent were not both looked up: %q", r.calls)
	g.expect(!strings.Contains(r.stdout, "waiting for HEAD"), "a CI-only HEAD waited for HEAD's image: %s", r.stdout)
}

func cutCheckImagePathChangeFails(g *guard) {
	for _, changed := range [][]string{{"src/app/page.tsx"}, {".github/workflows/x.yml", "public/logo.svg"}, {"Dockerfile"}, {"README.md"}} {
		r := cutRunRepinWeb(g, cutRWOpts{repin: "head", history: []string{cutRWHead, cutRWParent}, images: []string{cutRWParent}, changed: changed})
		g.expect(r.code == 3, "a HEAD without an image that changes %v must fail rc=3, got rc=%d: %s", changed, r.code, r.stdout)
		g.expect(strings.Contains(r.stdout, "waiting for HEAD's own image") && strings.Contains(r.stdout, "no CI image"), "%v: the wait for HEAD's own image is not named: %s", changed, r.stdout)
		g.expect(r.unchanged(), "%v: the overlay changed: %q", changed, r.overlay)
	}
}

func cutCheckNoImagedCommit(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", history: []string{cutRWHead, cutRWParent}, images: []string{}})
	g.expect(r.code == 3, "no imaged commit in the history must fail rc=3, got rc=%d: %s", r.code, r.stdout)
	g.expect(r.unchanged(), "no imaged commit in the history changed the overlay: %q", r.overlay)
}

func cutCheckFullComparePage(g *guard) {
	var changed []string
	for i := range 300 {
		changed = append(changed, fmt.Sprintf(".github/f%d.yml", i))
	}
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", history: []string{cutRWHead, cutRWParent}, images: []string{cutRWParent}, changed: changed})
	g.expect(r.code == 3, "a full compare page must be read as an image path (rc=3), got rc=%d: %s", r.code, r.stdout)
}

func cutCheckHeadWithOwnImage(g *guard) {
	r := cutRunRepinWeb(g, cutRWOpts{repin: "head", history: []string{cutRWHead, cutRWParent}, images: []string{cutRWHead, cutRWParent}})
	g.expect(r.code == 0, "a HEAD with its own image failed: %s", r.dump())
	g.expect(strings.Contains(r.overlay, cutRWNew), "a HEAD with its own image was not pinned: %q", r.overlay)
	rec := fmt.Sprintf("web_head=%s image_commit=%s digest=%s", cutRWHead, cutRWHead, cutRWNew)
	g.expect(strings.Contains(r.stdout, rec), "a HEAD with its own image is not recorded as %q: %s", rec, r.stdout)
}

func cutRunCutWebRepin(g *guard, webImage bool) cutResult {
	ds := cutDefaultDigests()
	if !webImage {
		ds = cutWithout(ds, "dev-health-web:sha-")
	}
	return cutRunCut(g, cutOpts{keep: []string{"bigboy-repin.sh", "bigboy-repin-web.sh"}, digests: ds})
}

func cutCheckCutStopsOnWebRepinFailure(g *guard) {
	r := cutRunCutWebRepin(g, false)
	g.expect(strings.Contains(r.stdout, "STEP repin-web rc=3"), "no `STEP repin-web rc=3`: %s", r.stdout)
	g.expect(r.code == 1, "a failed web re-pin must fail the cut rc=1, got rc=%d", r.code)
	g.expect(strings.Contains(r.stdout, "cut stops: web re-pin failed rc=3"), "the stop is not named: %s", r.stdout)
	g.expect(!strings.Contains(r.stdout, "STEP migrate") && cutFind(r.calls, 0, cutIsMigrate) < 0, "the cut ran migrate after a failed web re-pin: %s", r.dump())
}

func cutCheckCutGoesOnAfterWebRepin(g *guard) {
	r := cutRunCutWebRepin(g, true)
	g.expect(strings.Contains(r.stdout, "STEP repin-web rc=0"), "no `STEP repin-web rc=0`: %s", r.dump())
	g.expect(!strings.Contains(r.stdout, "cut stops: web re-pin failed"), "the cut stopped after a good web re-pin: %s", r.stdout)
	g.expect(strings.Contains(r.stdout, "STEP migrate rc="), "the cut did not go on to migrate after a good web re-pin: %s", r.stdout)
}

func TestCutWebRepin(t *testing.T) {
	runReal(t,
		check{"web_repin_runs_after_repin_and_before_migrate_up", cutCheckWebRepinOrder},
		check{"up_recreates_web_alongside_the_other_pinned_images", cutCheckUpRecreatesWeb},
		check{"repin_web_script_never_builds_on_the_host", cutCheckRepinWebNeverBuilds},
		check{"repin_web_resolves_from_web_repo_main_not_the_ops_sha", cutCheckRepinWebFromWebMain},
		check{"repin_web_skip_changes_nothing_and_calls_nothing", cutCheckRepinSkip},
		check{"repin_web_explicit_sha_uses_that_commit_and_never_asks_for_main_head", cutCheckRepinExplicitSHA},
		check{"repin_web_default_is_still_web_main_head", cutCheckRepinDefaultHead},
		check{"repin_web_refuses_other_values_before_any_call_or_write", cutCheckRepinRefusesOthers},
		check{"repin_web_explicit_sha_without_an_image_fails_rc3_and_changes_nothing", cutCheckRepinExplicitNoImage},
		check{"repin_web_wait_bounds_must_be_numbers", cutCheckRepinWaitBounds},
		check{"cut_documents_the_web_repin_choice_next_to_the_call", cutCheckCutDocumentsWebRepin},
		check{"a_ci_only_head_takes_the_newest_imaged_commit_without_waiting", cutCheckCIOnlyHead},
		check{"a_head_without_image_that_changes_an_image_path_fails_rc3", cutCheckImagePathChangeFails},
		check{"no_imaged_commit_in_the_history_fails_rc3_and_changes_nothing", cutCheckNoImagedCommit},
		check{"a_full_compare_page_is_read_as_an_image_path", cutCheckFullComparePage},
		check{"a_head_with_its_own_image_is_pinned_and_recorded", cutCheckHeadWithOwnImage},
		check{"the_cut_stops_before_migrate_when_the_web_repin_fails", cutCheckCutStopsOnWebRepinFailure},
		check{"the_cut_goes_on_to_migrate_when_the_web_repin_succeeds", cutCheckCutGoesOnAfterWebRepin},
	)
}

const (
	cutWebLine     = `"$HERE"/bigboy-repin-web.sh > "$REC".repin-web.out 2>&1; rc_web=$?; st repin-web $rc_web`
	cutMigrateLine = `"${DC[@]}" run --rm --no-deps -T migrate > "$REC"/migrate.out 2>&1; st migrate $?`
	cutWebGate     = `[ $rc_web = 0 ] ||`
)

func TestCutWebRepinPlanted(t *testing.T) {
	cp := []string{"ci/bigboy"}
	rw := []string{cutRepinWeb, "ci/bigboy/arm64-child-digest.sh"}
	runPlanted(t, []planted{
		{
			name: "web repin moved after migrate", copy: cp, check: cutCheckWebRepinOrder,
			mutate: all(mut(cutScript, cutWebLine, "rc_web=0"), mut(cutScript, cutMigrateLine, cutMigrateLine+"\n"+cutWebLine)),
			want:   "must run after the ops image repin",
		},
		{
			name: "web dropped from the up", copy: cp, check: cutCheckUpRecreatesWeb,
			mutate: mut(cutScript, "up -d --no-deps --no-build query-api go-api web", "up -d --no-deps --no-build query-api go-api"),
			want:   "`web` is not recreated",
		},
		{
			name: "host build in repin-web", copy: rw, check: cutCheckRepinWebNeverBuilds,
			mutate: mut(cutRepinWeb, "WEB_S7=${WEB_SHA:0:7}", "docker compose build web\nWEB_S7=${WEB_SHA:0:7}"),
			want:   "invoked a host build",
		},
		{
			name: "hand-typed web digest", copy: rw, check: cutCheckRepinWebNeverBuilds,
			mutate: mut(cutRepinWeb, `${WEB_DIGEST#sha256:}#g`, strings.Repeat("e", 64)+"#g"),
			want:   "hand-typed digest",
		},
		{
			name: "web resolved from the ops repo", copy: rw, check: cutCheckRepinWebFromWebMain,
			mutate: mut(cutRepinWeb, "repos/full-chaos/dev-health-web/commits/main", "repos/full-chaos/dev-health-ops/commits/main"),
			want:   "dev-health-web/commits/main",
		},
		{
			name: "skip branch gone", copy: rw, check: cutCheckRepinSkip,
			mutate: mut(cutRepinWeb, "  skip)\n", "  skip-off)\n"),
			want:   "must say SKIPPED",
		},
		{
			name: "explicit sha asks for main HEAD", copy: rw, check: cutCheckRepinExplicitSHA,
			mutate: mut(cutRepinWeb, "WEB_SHA=$WEB_REPIN ;;", "WEB_SHA=$(gh api repos/full-chaos/dev-health-web/commits/main --jq .sha); WEB_SHA=$WEB_REPIN ;;"),
			want:   "must not resolve web main HEAD",
		},
		{
			name: "default became skip", copy: rw, check: cutCheckRepinDefaultHead,
			mutate: mut(cutRepinWeb, "WEB_REPIN=${WEB_REPIN:-head}", "WEB_REPIN=${WEB_REPIN:-skip}"),
			want:   "did not pin web main HEAD's image",
		},
		{
			name: "loose sha shape", copy: rw, check: cutCheckRepinRefusesOthers,
			mutate: mut(cutRepinWeb, `[[ $WEB_REPIN =~ ^[0-9a-f]{40}$ ]]`, `[[ $WEB_REPIN =~ ^[0-9a-zA-Z]+$ ]]`),
			want:   "must be refused rc=2",
		},
		{
			name: "missing image exits 0", copy: rw, check: cutCheckRepinExplicitNoImage,
			mutate: mut(cutRepinWeb, `after waiting"; exit 3; }`, `after waiting"; exit 0; }`),
			want:   "must fail rc=3",
		},
		{
			name: "wait bounds unchecked", copy: rw, check: cutCheckRepinWaitBounds,
			mutate: mut(cutRepinWeb, `[[ $WAIT_TRIES =~ ^[0-9]+$ && $WAIT_SECS =~ ^[0-9]+$ ]] ||`, `true ||`),
			want:   "non-numeric wait bound",
		},
		{
			name: "WEB_REPIN undocumented at the call", copy: []string{cutScript}, check: cutCheckCutDocumentsWebRepin,
			mutate: all(mut(cutScript, "script: WEB_REPIN=skip (web", "script: skip (web"), mut(cutScript, "# WEB_REPIN=<40-hex", "# <40-hex"),
				mut(cutScript, "image; WEB_REPIN=skip avoids", "image; skip avoids")),
			want: "must document WEB_REPIN",
		},
		{
			name: ".github enters the image", copy: rw, check: cutCheckCIOnlyHead,
			mutate: mut(cutRepinWeb, `^(\.git|\.github|`, `^(\.git|`),
			want:   "must take the newest imaged commit",
		},
		{
			name: "image paths never force the wait", copy: rw, check: cutCheckImagePathChangeFails,
			mutate: mut(cutRepinWeb, `\.DS_Store)$ ]] || IN_IMAGE=1`, `\.DS_Store)$ ]] || true`),
			want:   "must fail rc=3",
		},
		{
			name: "no-image guard removed", copy: rw, check: cutCheckNoImagedCommit,
			mutate: mut(cutRepinWeb, `[[ ${WEB_DIGEST:-} == sha256:* ]] || { echo "FAIL: no CI image`, `true || { echo "FAIL: no CI image`),
			want:   "must fail rc=3",
		},
		{
			name: "full compare page ignored", copy: rw, check: cutCheckFullComparePage,
			mutate: mut(cutRepinWeb, "[[ ${#CHANGED[@]} -ge 300 ]] && IN_IMAGE=1", ":"),
			want:   "full compare page",
		},
		{
			name: "history scan takes the oldest image", copy: rw, check: cutCheckHeadWithOwnImage,
			mutate: mut(cutRepinWeb, "IMG_SHA=$c; break", "IMG_SHA=$c"),
			want:   "is not recorded as",
		},
		{
			name: "web repin failure does not stop the cut", copy: cp, check: cutCheckCutStopsOnWebRepinFailure,
			mutate: mut(cutScript, cutWebGate, "true ||"),
			want:   "ran migrate after a failed web re-pin",
		},
		{
			name: "web repin success stops the cut", copy: cp, check: cutCheckCutGoesOnAfterWebRepin,
			mutate: mut(cutScript, cutWebGate, "[ $rc_web = 1 ] ||"),
			want:   "did not go on to migrate",
		},
	})
}
