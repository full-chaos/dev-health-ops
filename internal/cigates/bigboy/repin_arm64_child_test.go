package bigboy_test

import (
	"path/filepath"
	"strings"
	"testing"
)

const (
	repinScript    = "ci/bigboy/bigboy-repin.sh"
	repinWebScript = "ci/bigboy/bigboy-repin-web.sh"
	repinHelper    = "ci/bigboy/arm64-child-digest.sh"
	repinCut       = "ci/bigboy/bigboy-cut.sh"
)

func repinRunRepin(g *guard, root, path string, args ...string) scriptRun {
	return repinRun(g, map[string]string{
		"PATH": path, "BIGBOY_ROOT": root, "WEB_REPIN_WAIT_TRIES": "1", "WEB_REPIN_WAIT_SECS": "0",
	}, append([]string{g.path(repinScript)}, args...)...)
}

func repinCheckPin(g *guard, pins map[string]string, image, sha7 string) {
	v := pins[repinVar[image]]
	switch {
	case strings.HasSuffix(v, "@"+repinIndexDigest(image, sha7)):
		g.failf("images.env %s pins the multi-arch index digest of %s:sha-%s, not its linux/arm64 child", repinVar[image], image, sha7)
	case v != repinGHCR+image+"@"+repinArm64Digest(image, sha7):
		g.failf("images.env %s=%q, want the linux/arm64 child %s", repinVar[image], v, repinArm64Digest(image, sha7))
	}
}

func checkRepinPinsArm64Children(g *guard) {
	_, path := repinBin(g, map[string]string{"docker": repinIndexDocker(repinRepinRefs())})
	root := repinFixture(g, repinArm64Digest("dev-health-go-dho", repinOld8[:7]))
	r := repinRunRepin(g, root, path, repinOld8, repinNew)
	if r.code != 0 {
		g.failf("repin exited %d, want 0 when OLD8's arm64 child is the pinned child:\n%s", r.code, r.repinOut())
		return
	}
	pins := repinEnv(repinReadFile(g, filepath.Join(root, "_records", "images.env")))
	for _, image := range repinImages {
		repinCheckPin(g, pins, image, repinNew[:7])
	}
}

func checkRepinRejectsOld8ChildMismatch(g *guard) {
	_, path := repinBin(g, map[string]string{"docker": repinIndexDocker(repinRepinRefs())})
	root := repinFixture(g, repinDigestOf('9'))
	r := repinRunRepin(g, root, path, repinOld8, repinNew)
	g.expect(r.code == 4, "repin accepted an OLD8 whose linux/arm64 child is not the pinned child: rc=%d, want 4:\n%s", r.code, r.repinOut())
}

func checkRepinFailsWithoutArm64Child(g *guard) {
	stub := strings.ReplaceAll(repinIndexDocker(repinRepinRefs()), `"architecture":"arm64"`, `"architecture":"s390x"`)
	_, path := repinBin(g, map[string]string{"docker": stub})
	root := repinFixture(g, repinArm64Digest("dev-health-go-dho", repinOld8[:7]))
	ov := filepath.Join(root, "_records", "images.env")
	before := repinReadFile(g, ov)
	r := repinRunRepin(g, root, path, repinOld8, repinNew)
	g.expect(r.code != 0, "repin succeeded although the index has no linux/arm64 child:\n%s", r.repinOut())
	g.expect(repinReadFile(g, ov) == before, "images.env was rewritten although the index has no linux/arm64 child")
}

func checkRepinWebPinsArm64Child(g *guard) {
	_, path := repinBin(g, map[string]string{"docker": repinIndexDocker([]repinRef{{"dev-health-web", repinWebSHA[:7]}})})
	root := repinFixture(g, repinDigestOf('1'))
	r := repinRun(g, map[string]string{
		"PATH": path, "BIGBOY_ROOT": root, "WEB_REPIN_WAIT_TRIES": "1", "WEB_REPIN_WAIT_SECS": "0", "WEB_REPIN": repinWebSHA,
	}, g.path(repinWebScript))
	if r.code != 0 {
		g.failf("repin-web exited %d, want 0:\n%s", r.code, r.repinOut())
		return
	}
	repinCheckPin(g, repinEnv(repinReadFile(g, filepath.Join(root, "_records", "images.env"))), "dev-health-web", repinWebSHA[:7])
}

var (
	repinCutOld       = repinDigestOf('1')
	repinCutNew       = repinDigestOf('2')
	repinCutWrongOld  = repinDigestOf('9')
	repinCutIndex     = repinDigestOf('e')
	repinCutWebOld    = repinDigestOf('3')
	repinCutWebNew    = repinDigestOf('4')
	repinMigrateCalls = "--no-deps migrate"
)

// OLD8's dho tag resolves to a child that is not the pinned one (repin rc 4); only --raw carries the arm64 child.
func repinCutDocker() string {
	answer := func(child, descriptor string) string {
		return "  if [[ \"$args\" == *\"--raw\"* ]]; then\n" +
			"    echo '{\"manifests\":[{\"digest\":\"" + child + "\",\"platform\":{\"os\":\"linux\",\"architecture\":\"arm64\"}}]}'; exit 0\n" +
			"  fi\n  echo '{\"digest\":\"" + descriptor + "\"}'; exit 0\n"
	}
	var b strings.Builder
	b.WriteString("set -u\nargs=\"$*\"\n")
	b.WriteString("if [[ \"$args\" == *\"" + repinMigrateCalls + "\"* ]]; then exit 9; fi\n")
	b.WriteString("if [[ \"$args\" == *\"dev-health-go-dho:sha-" + repinOld8[:7] + "\"* ]]; then\n" + answer(repinCutWrongOld, repinCutWrongOld) + "fi\n")
	for _, image := range repinImages {
		b.WriteString("if [[ \"$args\" == *\"" + image + ":sha-\"* ]]; then\n" + answer(repinCutNew, repinCutIndex) + "fi\n")
	}
	b.WriteString("if [[ \"$args\" == *\"dev-health-web:sha-\"* ]]; then\n" + answer(repinCutWebNew, repinCutWebNew) + "fi\n")
	b.WriteString("if [[ \"$args\" == *\"venue-tools\"* ]]; then echo 'GOAPI_ROUTING_JSON {\"reason\": \"carried\"}'; exit 0; fi\nexit 0\n")
	return b.String()
}

func repinRunCut(g *guard) (scriptRun, string) {
	root := repinDir(g, "bigboy-root")
	rec := filepath.Join(root, "_records")
	var pins strings.Builder
	for _, image := range repinImages {
		pins.WriteString(repinVar[image] + "=" + repinGHCR + image + "@" + repinCutOld + "\n")
	}
	pins.WriteString("BIGBOY_WEB_IMAGE=" + repinGHCR + "dev-health-web@" + repinCutWebOld + "\n")
	repinWrite(g, filepath.Join(rec, "images.env"), pins.String(), 0o644)
	repinWrite(g, filepath.Join(root, "ops", ".env"), "", 0o644)
	repinWrite(g, filepath.Join(rec, "bigboy-"+repinOld8, "bigboy-rest-commands.sh"),
		"#!/usr/bin/env bash\nSTEP=placeholder\nB="+strings.Repeat("0", 40)+"\n", 0o644)
	repinWrite(g, filepath.Join(rec, "bigboy-"+repinNew[:8], ".keep"), "", 0o644)
	ops := repinDir(g, "ops-repo")
	repinWrite(g, filepath.Join(ops, ".git", "HEAD"), "ref: refs/heads/main\n", 0o644)
	_, path := repinBin(g, map[string]string{"docker": repinCutDocker(), "gh": "echo " + repinWebSHA + "\n"})
	r := repinRun(g, map[string]string{
		"PATH": path, "BIGBOY_ROOT": root, "BIGBOY_OPS_REPO": ops, "WEB_REPIN_WAIT_TRIES": "1", "WEB_REPIN_WAIT_SECS": "0",
	}, g.path(repinCut), repinOld8, repinNew)
	return r, root
}

func repinCheckOperator(g *guard, root string) {
	got := strings.TrimSpace(repinReadFile(g, filepath.Join(root, "_records", "bigboy-"+repinNew[:8]+".operator.txt")))
	switch got {
	case "operator=" + repinCutNew:
	case "operator=" + repinCutIndex:
		g.failf("cut resolved the operator to the multi-arch index digest, not through the arm64 child helper")
	default:
		g.failf("cut recorded %q, want operator=%s (the linux/arm64 child)", got, repinCutNew)
	}
}

func checkRepinCutOperatorArm64(g *guard) {
	_, root := repinRunCut(g)
	repinCheckOperator(g, root)
}

func checkRepinCutStopsOnFailedRepin(g *guard) {
	r, root := repinRunCut(g)
	g.expect(strings.Contains(r.stdout, "STEP repin rc=4"), "cut did not report STEP repin rc=4:\n%s", r.stdout)
	g.expect(r.code != 0, "cut exited 0 after a failed repin")
	g.expect(!strings.Contains(r.stdout, "STEP repin-web"), "cut ran STEP repin-web after a failed repin")
	g.expect(!strings.Contains(r.stdout, "STEP migrate"), "cut ran STEP migrate after a failed repin")
	repinCheckOperator(g, root)
}

func TestRepinArm64Child(t *testing.T) {
	runReal(t,
		check{"repin-pins-arm64-children", checkRepinPinsArm64Children},
		check{"repin-rejects-old8-child-mismatch", checkRepinRejectsOld8ChildMismatch},
		check{"repin-fails-without-arm64-child", checkRepinFailsWithoutArm64Child},
		check{"repin-web-pins-arm64-child", checkRepinWebPinsArm64Child},
		check{"cut-operator-through-arm64-helper", checkRepinCutOperatorArm64},
		check{"cut-stops-before-migrate-on-failed-repin", checkRepinCutStopsOnFailedRepin},
	)
}

func TestRepinArm64ChildPlanted(t *testing.T) {
	repin := []string{repinScript, repinHelper}
	indexLookup := `docker buildx imagetools inspect "ghcr.io/full-chaos/$1:sha-$S7" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true`
	runPlanted(t, []planted{
		{
			name:   "repin-resolves-index-digest",
			copy:   repin,
			mutate: repinMut(repinScript, `arm64_child_digest "ghcr.io/full-chaos/$1:sha-$S7" || true`, indexLookup),
			check:  checkRepinPinsArm64Children,
			want:   "pins the multi-arch index digest",
		},
		{
			name:   "repin-old8-mismatch-not-refused",
			copy:   repin,
			mutate: repinMut(repinScript, "  exit 4\nfi\ndig() {", "fi\ndig() {"),
			check:  checkRepinRejectsOld8ChildMismatch,
			want:   "repin accepted an OLD8 whose linux/arm64 child is not the pinned child",
		},
		{
			name:   "helper-takes-any-non-amd64-child",
			copy:   repin,
			mutate: repinMut(repinHelper, `.platform.architecture == "arm64"`, `.platform.architecture != "amd64"`),
			check:  checkRepinFailsWithoutArm64Child,
			want:   "repin succeeded although the index has no linux/arm64 child",
		},
		{
			name: "repin-web-resolves-index-digest",
			copy: []string{repinWebScript, repinHelper},
			mutate: repinMut(repinWebScript, `WEB_DIGEST=$(arm64_child_digest "ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7" || true)`,
				`WEB_DIGEST=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-web:sha-$WEB_S7" --format '{{json .Manifest}}' 2>/dev/null | jq -r .digest || true)`),
			check: checkRepinWebPinsArm64Child,
			want:  "pins the multi-arch index digest of dev-health-web",
		},
		{
			name: "cut-operator-from-index-descriptor",
			copy: []string{"ci/bigboy"},
			mutate: repinMut(repinCut, `BIGBOY_OPERATOR_DIGEST=$(arm64_child_digest ghcr.io/full-chaos/dev-health-go-operator:sha-"$S7")`,
				`BIGBOY_OPERATOR_DIGEST=$(docker buildx imagetools inspect ghcr.io/full-chaos/dev-health-go-operator:sha-"$S7" --format '{{json .Manifest}}' | jq -r .digest)`),
			check: checkRepinCutOperatorArm64,
			want:  "cut resolved the operator to the multi-arch index digest",
		},
		{
			name:   "cut-continues-after-failed-repin",
			copy:   []string{"ci/bigboy"},
			mutate: repinMut(repinCut, `[ $rc_repin = 0 ] || { echo "cut stops: re-pin failed rc=$rc_repin (see $REC.repin.out)"; exit 1; }`, ""),
			check:  checkRepinCutStopsOnFailedRepin,
			want:   "cut ran STEP repin-web after a failed repin",
		},
	})
}
