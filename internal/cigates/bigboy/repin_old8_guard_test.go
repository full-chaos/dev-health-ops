package bigboy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const repinShapeMsg = "is not exactly 8 lowercase hex characters"

// A full fixture in which a well-formed OLD8 would succeed, so a moved or neutered shape check shows up as a write.
func repinOld8Run(g *guard, old8 string) (scriptRun, string, string, []string) {
	_, path := repinBin(g, map[string]string{"docker": repinIndexDocker(repinRepinRefs())})
	root := repinFixture(g, repinArm64Digest("dev-health-go-dho", repinOld8[:7]))
	repinWrite(g, filepath.Join(root, "_records", "bigboy-"+old8, "bigboy-rest-commands.sh"), "#!/usr/bin/env bash\nSTEP=x\n", 0o644)
	ov := filepath.Join(root, "_records", "images.env")
	before, tree := repinReadFile(g, ov), repinTree(root)
	return repinRunRepin(g, root, path, old8, repinNew), root, before, tree
}

func checkRepinOld8ShapeBeforeAnyWrite(g *guard) {
	r, root, before, tree := repinOld8Run(g, repinOld8+"9")
	g.expect(r.code == 4, "repin exited %d for a malformed OLD8, want 4:\n%s", r.code, r.repinOut())
	g.expect(strings.Contains(r.repinOut(), repinShapeMsg), "repin did not name the malformed OLD8 with %q:\n%s", repinShapeMsg, r.repinOut())
	g.expect(repinReadFile(g, filepath.Join(root, "_records", "images.env")) == before, "images.env was rewritten for a malformed OLD8")
	after := repinTree(root)
	g.expect(equal(after, tree), "repin wrote files before rejecting a malformed OLD8: tree %v, was %v", after, tree)
}

func checkRepinOld8RejectedBeforeWriteMinimal(g *guard) {
	root := repinDir(g, "root")
	ov := filepath.Join(root, "_records", "images.env")
	repinWrite(g, ov, "placeholder\n", 0o644)
	_, path := repinBin(g, map[string]string{"docker": "exit 1\n"})
	r := repinRunRepin(g, root, path, "bd25cd0a9", repinNew)
	g.expect(r.code != 0, "repin exited 0 for the D2801 typo OLD8 bd25cd0a9")
	g.expect(strings.Contains(r.repinOut(), repinShapeMsg), "repin did not reject OLD8 bd25cd0a9 by its shape (%q):\n%s", repinShapeMsg, r.repinOut())
	g.expect(repinReadFile(g, ov) == "placeholder\n", "images.env was touched for a rejected OLD8")
	entries, _ := os.ReadDir(filepath.Join(root, "_records"))
	for _, e := range entries {
		g.expect(!(e.IsDir() && strings.HasPrefix(e.Name(), "bigboy-")), "repin created _records/%s for a rejected OLD8", e.Name())
	}
}

func checkRepinOld8ExactlyEightHex(g *guard) {
	root := repinDir(g, "root")
	repinWrite(g, filepath.Join(root, "_records", "images.env"), "placeholder\n", 0o644)
	_, path := repinBin(g, map[string]string{"docker": "exit 1\n"})
	for old8, reject := range map[string]bool{
		"bd25cd0a9": true, "bd25cd0": true, "BD25CD0A": true, "bd25cd0g": true, "bd25cd0a": false,
	} {
		r := repinRunRepin(g, root, path, old8, repinNew)
		named := strings.Contains(r.repinOut(), repinShapeMsg)
		if reject {
			g.expect(named && r.code == 4, "OLD8 %q (%d chars) was not rejected by the shape check: rc=%d\n%s", old8, len(old8), r.code, r.repinOut())
		} else {
			g.expect(!named, "OLD8 %q (exactly 8 lowercase hex) was rejected by the shape check", old8)
		}
	}
}

func repinMismatchRun(g *guard) (scriptRun, []string) {
	_, path := repinBin(g, map[string]string{"docker": repinIndexDocker(repinRepinRefs())})
	root := repinFixture(g, repinDigestOf('9'))
	log := filepath.Join(g.tmp, "docker-calls-"+filepath.Base(root))
	r := repinRun(g, map[string]string{"PATH": path, "BIGBOY_ROOT": root, "REPIN_DOCKER_LOG": log}, g.path(repinScript), repinOld8, repinNew)
	calls, _ := os.ReadFile(log)
	return r, strings.Split(strings.TrimSpace(string(calls)), "\n")
}

func checkRepinOld8CrossCheckedBeforeNewResolution(g *guard) {
	r, calls := repinMismatchRun(g)
	g.expect(r.code == 4, "repin exited %d on an OLD8/pinned-digest mismatch, want 4", r.code)
	g.expect(strings.Contains(r.repinOut(), "currently pins"), "repin did not compare OLD8 with the digest images.env currently pins:\n%s", r.repinOut())
	resolvedOld := false
	for _, c := range calls {
		resolvedOld = resolvedOld || strings.Contains(c, "dev-health-go-dho:sha-"+repinOld8[:7])
		g.expect(!strings.Contains(c, ":sha-"+repinNew[:7]), "repin resolved a NEW image before the OLD8 cross-check refused: docker %s", c)
	}
	g.expect(resolvedOld, "repin never resolved OLD8 against the registry: calls %v", calls)
}

func checkRepinMismatchFailsLoudly(g *guard) {
	r, _ := repinMismatchRun(g)
	g.expect(r.code == 4, "an OLD8/live-digest mismatch exited %d, want 4 (never a silent no-op)", r.code)
	g.expect(strings.Contains(r.repinOut(), "does not name the ACTUALLY-running build"), "the mismatch refusal does not say OLD8 does not name the ACTUALLY-running build:\n%s", r.repinOut())
}

func checkRepinOld8LookupUsesSevenCharTag(g *guard) {
	root := repinDir(g, "root")
	repinWrite(g, filepath.Join(root, "_records", "images.env"), "image: "+repinGHCR+"dev-health-go-dho@"+repinDigestOf('c')+"\n", 0o644)
	log := filepath.Join(g.tmp, "docker-calls-7")
	_, path := repinBin(g, map[string]string{"docker": `echo "$*" >> "` + log + `"
case "$*" in
  *dev-health-go-dho:sha-f03f57d) echo '{"manifests":[{"digest":"` + repinDigestOf('a') + `","platform":{"os":"linux","architecture":"arm64"}}]}' ;;
  *) exit 1 ;;
esac
`})
	r := repinRunRepin(g, root, path, "f03f57d5", repinNew)
	calls, _ := os.ReadFile(log)
	g.expect(!strings.Contains(r.repinOut(), "does not resolve to a real dev-health-go-dho image"), "the OLD8 lookup used a tag the registry does not have:\n%s", r.repinOut())
	g.expect(strings.Contains(r.repinOut(), "currently pins"), "the OLD8 lookup did not reach the pinned-digest cross-check:\n%s", r.repinOut())
	g.expect(!strings.Contains(string(calls), "dev-health-go-dho:sha-f03f57d5"), "repin asked the registry for the 8-character tag sha-f03f57d5")
}

func TestRepinOld8Guard(t *testing.T) {
	runReal(t,
		check{"old8-shape-checked-before-any-file-is-touched", checkRepinOld8ShapeBeforeAnyWrite},
		check{"old8-shape-rejects-bad-old8-before-any-write", checkRepinOld8RejectedBeforeWriteMinimal},
		check{"old8-shape-rejects-nine-characters", checkRepinOld8ExactlyEightHex},
		check{"old8-cross-checked-before-new-resolution", checkRepinOld8CrossCheckedBeforeNewResolution},
		check{"digest-mismatch-fails-loudly", checkRepinMismatchFailsLoudly},
		check{"old8-lookup-uses-seven-char-tag", checkRepinOld8LookupUsesSevenCharTag},
	)
}

const repinEightHex = "[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f])"

func repinMoveShapeAfterBackup(t *testing.T, root string) {
	text := mustRead(t, rootPath(root, repinScript))
	start := strings.Index(text, `case "$OLD8" in`)
	end := strings.Index(text[start:], "esac\n")
	if start < 0 || end < 0 {
		t.Fatal("OLD8 case block not found, the defect did not land")
	}
	block := text[start : start+end+len("esac\n")]
	plant(t, root, repinScript, block, "")
	backup := `cp -n "$OV" "$OV".bak-"$OLD8"` + "\n"
	plant(t, root, repinScript, backup, backup+block)
}

func TestRepinOld8GuardPlanted(t *testing.T) {
	repin := []string{repinScript, repinHelper}
	runPlanted(t, []planted{
		{
			name:   "shape-check-accepts-any-suffix",
			copy:   repin,
			mutate: repinMut(repinScript, repinEightHex, strings.TrimSuffix(repinEightHex, ")")+"*)"),
			check:  checkRepinOld8ShapeBeforeAnyWrite,
			want:   "images.env was rewritten for a malformed OLD8",
		},
		{
			name:   "shape-check-after-backup",
			copy:   repin,
			mutate: repinMoveShapeAfterBackup,
			check:  checkRepinOld8ShapeBeforeAnyWrite,
			want:   "repin wrote files before rejecting a malformed OLD8",
		},
		{
			name:   "mismatch-refusal-unnamed",
			copy:   repin,
			mutate: repinMut(repinScript, "OLD8 does not name the ACTUALLY-running build", "OLD8 is stale"),
			check:  checkRepinMismatchFailsLoudly,
			want:   "does not say OLD8 does not name the ACTUALLY-running build",
		},
		{
			name:   "shape-check-without-exit",
			copy:   repin,
			mutate: repinMut(repinScript, `(D2801)"; exit 4 ;;`, `(D2801)" ;;`),
			check:  checkRepinOld8ShapeBeforeAnyWrite,
			want:   "images.env was rewritten for a malformed OLD8",
		},
		{
			name:   "shape-check-accepts-any-suffix-minimal-root",
			copy:   repin,
			mutate: repinMut(repinScript, repinEightHex, strings.TrimSuffix(repinEightHex, ")")+"*)"),
			check:  checkRepinOld8RejectedBeforeWriteMinimal,
			want:   "repin did not reject OLD8 bd25cd0a9 by its shape",
		},
		{
			name:   "shape-pattern-takes-nine-characters",
			copy:   repin,
			mutate: repinMut(repinScript, repinEightHex, strings.TrimSuffix(repinEightHex, ")")+"|"+strings.TrimSuffix(repinEightHex, ")")+"?)"),
			check:  checkRepinOld8ExactlyEightHex,
			want:   `OLD8 "bd25cd0a9" (9 chars) was not rejected by the shape check`,
		},
		{
			name:   "cross-check-does-not-stop-the-repin",
			copy:   repin,
			mutate: repinMut(repinScript, "  exit 4\nfi\ndig() {", "fi\ndig() {"),
			check:  checkRepinOld8CrossCheckedBeforeNewResolution,
			want:   "repin resolved a NEW image before the OLD8 cross-check refused",
		},
		{
			name:   "mismatch-exits-zero",
			copy:   repin,
			mutate: repinMut(repinScript, "  exit 4\nfi\ndig() {", "  exit 0\nfi\ndig() {"),
			check:  checkRepinMismatchFailsLoudly,
			want:   "an OLD8/live-digest mismatch exited 0",
		},
		{
			name:   "old8-lookup-uses-eight-char-tag",
			copy:   repin,
			mutate: repinMut(repinScript, "dev-health-go-dho:sha-${OLD8:0:7}", "dev-health-go-dho:sha-${OLD8}"),
			check:  checkRepinOld8LookupUsesSevenCharTag,
			want:   "the OLD8 lookup used a tag the registry does not have",
		},
	})
}
