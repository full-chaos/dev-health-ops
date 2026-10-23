package workflows_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Decision logic of ci/image_promote.sh (docker-images.yml `resolve` job), run under bash with gh and docker shims that
// serve fixture files. Every refusal must leave promoted=false (or hotfix=false) and copy nothing.

const (
	promRepo   = "full-chaos/dev-health-ops"
	promSHA    = "aaaaaaaa11111111aaaaaaaa11111111aaaaaaaa"
	promHead   = "bbbbbbbb22222222bbbbbbbb22222222bbbbbbbb"
	promMerge  = "cccccccc33333333cccccccc33333333cccccccc"
	promBase   = "dddddddd44444444dddddddd44444444dddddddd"
	promTree   = "eeeeeeee55555555eeeeeeee55555555eeeeeeee"
	promOther  = "ffffffff66666666ffffffff66666666ffffffff"
	promPR     = 7
	promPrefix = "ghcr.io/full-chaos/"
)

var promFamilies = []string{"dev-health-go-operator", "dev-health-go-contractcheck", "dev-health-go-migrate", "dev-health-go-dho", "dev-health-go-auth-service", "dev-health-go-auth-migrate", "dev-health-go-api-tools"}

func promKey(s string) string { return strings.NewReplacer("/", "_", ":", "_", "@", "_").Replace(s) }

func promDigest(i int) string { return fmt.Sprintf("sha256:%064x", i+1) }

type promFix struct {
	t   *testing.T
	dir string
}

func (f promFix) put(name string, v any) {
	f.t.Helper()
	var b []byte
	if s, ok := v.(string); ok {
		b = []byte(s)
	} else {
		var err error
		if b, err = json.Marshal(v); err != nil {
			f.t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(f.dir, name+".json"), b, 0o644); err != nil {
		f.t.Fatal(err)
	}
}

func (f promFix) del(name string) {
	f.t.Helper()
	if err := os.Remove(filepath.Join(f.dir, name+".json")); err != nil {
		f.t.Fatal(err)
	}
}

func (f promFix) commit(sha, tree string, parents ...string) {
	ps := []map[string]string{}
	for _, p := range parents {
		ps = append(ps, map[string]string{"sha": p})
	}
	f.put("api_"+promKey("repos/"+promRepo+"/git/commits/"+sha), map[string]any{"tree": map[string]string{"sha": tree}, "parents": ps})
}

func (f promFix) inspect(family string, i int, rev string) {
	labels := map[string]any{"config": map[string]any{"Labels": map[string]string{"org.opencontainers.image.revision": rev}}}
	f.put("inspect_"+promKey(promPrefix+family+":tree-"+promTree), map[string]any{
		"manifest": map[string]string{"digest": promDigest(i)},
		"image":    map[string]any{"linux/amd64": labels, "linux/arm64": labels},
	})
}

func (f promFix) attest(i int, trigger, ref, src string) {
	cert := map[string]string{"githubWorkflowTrigger": trigger, "sourceRepositoryRef": ref, "sourceRepositoryDigest": src}
	f.put("att_"+promKey(promPrefix+promFamilies[i]+"@"+promDigest(i)), []any{
		map[string]any{"verificationResult": map[string]any{"signature": map[string]any{"certificate": cert}}},
	})
}

// newPromFix writes the all-green world: one merged PR whose head, merge-ref source and merge commit share one tree.
func newPromFix(t *testing.T) promFix {
	f := promFix{t: t, dir: t.TempDir()}
	f.commit(promSHA, promTree, promBase)
	f.commit(promHead, promTree, promBase)
	f.commit(promMerge, promTree, promBase, promHead)
	f.put("api_"+promKey("repos/"+promRepo+"/commits/"+promSHA+"/pulls"), []any{
		map[string]any{"number": promPR, "merge_commit_sha": promSHA, "merged_at": "2026-10-23T00:00:00Z", "head": map[string]string{"sha": promHead}},
	})
	for i, fam := range promFamilies {
		f.inspect(fam, i, promHead)
		f.attest(i, "pull_request", fmt.Sprintf("refs/pull/%d/merge", promPR), promMerge)
	}
	return f
}

type promResult struct {
	out     map[string]string
	created []string
	stdout  string
	code    int
}

func runPromote(t *testing.T, f promFix, mode string, env ...string) promResult {
	t.Helper()
	root := rootOf(t)
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, src := range map[string]string{"gh": "promote_gh_shim.sh", "docker": "promote_docker_shim.sh"} {
		body := readFile(t, filepath.Join(root, "internal", "cigates", "workflows", "testdata", src))
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	record, outFile := filepath.Join(dir, "record.txt"), filepath.Join(dir, "output.txt")
	for _, p := range []string{record, outFile} {
		if err := os.WriteFile(p, nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	base := []string{
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "FIXTURES=" + f.dir, "RECORD_FILE=" + record, "GITHUB_OUTPUT=" + outFile,
		"REPO=" + promRepo, "OWNER=full-chaos", "EVENT_NAME=push", "REF=refs/heads/main", "SHA=" + promSHA,
		"PR_NUMBER=" + fmt.Sprint(promPR), "HEAD_REPO=" + promRepo, "HEAD_SHA=" + promHead, "ACTORS=alice alice",
	}
	res := cmdSpec{dir: dir, env: inheritedEnv(nil, append(base, env...)...)}.run("bash", filepath.Join(root, "ci", "image_promote.sh"), mode)
	out := map[string]string{}
	for _, l := range strings.Split(readFile(t, outFile), "\n") {
		if k, v, ok := strings.Cut(l, "="); ok {
			out[k] = v
		}
	}
	var created []string
	for _, l := range strings.Split(readFile(t, record), "\n") {
		if l != "" {
			created = append(created, l)
		}
	}
	return promResult{out: out, created: created, stdout: res.stdout, code: res.code}
}

func wantDeclined(t *testing.T, r promResult, why string) {
	t.Helper()
	if r.code != 0 || r.out["promoted"] != "false" || len(r.created) != 0 {
		t.Fatalf("must decline without copying (exit %d, promoted=%q, created=%v):\n%s", r.code, r.out["promoted"], r.created, r.stdout)
	}
	if !strings.Contains(r.stdout, "::warning::promote:") || !strings.Contains(r.stdout, why) {
		t.Fatalf("decline reason %q missing from output:\n%s", why, r.stdout)
	}
}

func TestPromoteCopiesTreeImagesWhenProvenanceHolds(t *testing.T) {
	r := runPromote(t, newPromFix(t), "promote")
	if r.code != 0 || r.out["promoted"] != "true" || r.out["sha"] != promSHA {
		t.Fatalf("expected promoted=true (exit %d, out %v):\n%s", r.code, r.out, r.stdout)
	}
	if len(r.created) != len(promFamilies) {
		t.Fatalf("expected one copy per family, got %v", r.created)
	}
	for i, fam := range promFamilies {
		want := fmt.Sprintf("-t %s%s:sha-%s %s%s@%s", promPrefix, fam, promSHA[:7], promPrefix, fam, promDigest(i))
		if r.created[i] != want {
			t.Fatalf("copy %d = %q, want %q (by digest, sha-<merge7>)", i, r.created[i], want)
		}
	}
}

func TestPromoteDeclinesEveryUnprovenCase(t *testing.T) {
	cases := []struct {
		name string
		mut  func(f promFix)
		why  string
	}{
		{"one family has no tree image", func(f promFix) { f.del("inspect_" + promKey(promPrefix+promFamilies[3]+":tree-"+promTree)) }, "no tree-"},
		{"tree mismatch: PR head tree differs from the merge tree (out-of-date PR)", func(f promFix) { f.commit(promHead, promOther, promBase) }, "out-of-date PR"},
		{"attestation missing", func(f promFix) { f.del("att_" + promKey(promPrefix+promFamilies[5]+"@"+promDigest(5))) }, "no verifiable build provenance"},
		{"provenance from a push run, not a pull_request run", func(f promFix) {
			f.attest(0, "push", "refs/heads/main", promMerge)
		}, "not a pull_request run on refs/pull/"},
		{"provenance from another PR's merge ref", func(f promFix) {
			f.attest(1, "pull_request", "refs/pull/99/merge", promMerge)
		}, "not a pull_request run on refs/pull/"},
		{"wrong source sha: source commit does not contain the PR head", func(f promFix) { f.commit(promMerge, promTree, promBase, promOther) }, "does not contain PR head"},
		{"wrong source sha: source commit tree differs from the merge tree", func(f promFix) { f.commit(promMerge, promOther, promBase, promHead) }, "provenance source"},
		{"revision label is not the PR head", func(f promFix) { f.inspect(promFamilies[2], 2, promOther) }, "revision label"},
		{"merge commit is the merge of no PR", func(f promFix) {
			f.put("api_"+promKey("repos/"+promRepo+"/commits/"+promSHA+"/pulls"), []any{})
		}, "exactly one merged pull request"},
		{"merge commit tree unreadable", func(f promFix) { f.del("api_" + promKey("repos/"+promRepo+"/git/commits/"+promSHA)) }, "cannot read the tree"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newPromFix(t)
			c.mut(f)
			wantDeclined(t, runPromote(t, f, "promote"), c.why)
		})
	}
	t.Run("a failed copy declines", func(t *testing.T) {
		r := runPromote(t, newPromFix(t), "promote", "CREATE_FAIL=1")
		wantDeclined(t, r, "failed")
	})
}

func TestPromoteIsSilentOutsideAMainPush(t *testing.T) {
	for _, env := range [][]string{{"EVENT_NAME=pull_request"}, {"EVENT_NAME=workflow_dispatch"}, {"REF=refs/heads/other"}} {
		r := runPromote(t, newPromFix(t), "promote", env...)
		if r.code != 0 || r.out["promoted"] != "false" || len(r.created) != 0 || strings.Contains(r.stdout, "::warning::") {
			t.Fatalf("%v must be a quiet no-op (exit %d, out %v, created %v):\n%s", env, r.code, r.out, r.created, r.stdout)
		}
	}
}

func TestHotfixGate(t *testing.T) {
	prPath := "api_" + promKey(fmt.Sprintf("repos/%s/pulls/%d", promRepo, promPR))
	permPath := func(a string) string { return "api_" + promKey("repos/"+promRepo+"/collaborators/"+a+"/permission") }
	setup := func(t *testing.T, labels ...string) promFix {
		f := newPromFix(t)
		ls := []map[string]string{}
		for _, l := range labels {
			ls = append(ls, map[string]string{"name": l})
		}
		f.put(prPath, map[string]any{"labels": ls})
		f.put(permPath("alice"), map[string]string{"permission": "write"})
		f.put(permPath("mallory"), map[string]string{"permission": "read"})
		return f
	}
	pr := func(extra ...string) []string { return append([]string{"EVENT_NAME=pull_request"}, extra...) }

	r := runPromote(t, setup(t, "hotfix-image", "ci:integration"), "gate", pr()...)
	if r.code != 0 || r.out["hotfix"] != "true" || r.out["tree"] != promTree {
		t.Fatalf("label + same repo + write must enable the hotfix push (exit %d, out %v):\n%s", r.code, r.out, r.stdout)
	}
	deny := []struct {
		name string
		f    promFix
		env  []string
		warn string
	}{
		{"no label is a quiet no-op", setup(t, "ci:integration"), pr(), ""},
		{"fork head", setup(t, "hotfix-image"), pr("HEAD_REPO=evil/dev-health-ops"), "fork"},
		{"pusher without write", setup(t, "hotfix-image"), pr("ACTORS=alice mallory"), "write is required"},
		{"re-runner without write", setup(t, "hotfix-image"), pr("ACTORS=mallory alice"), "write is required"},
		{"no actor", setup(t, "hotfix-image"), pr("ACTORS="), "no actor"},
		{"unreadable permission", setup(t, "hotfix-image"), pr("ACTORS=ghost"), "cannot read the permission"},
	}
	for _, c := range deny {
		t.Run(c.name, func(t *testing.T) {
			r := runPromote(t, c.f, "gate", c.env...)
			if r.code != 0 || r.out["hotfix"] != "false" {
				t.Fatalf("must not enable the hotfix push (exit %d, out %v):\n%s", r.code, r.out, r.stdout)
			}
			if c.warn == "" && strings.Contains(r.stdout, "::warning::") || c.warn != "" && !strings.Contains(r.stdout, c.warn) {
				t.Fatalf("warning mismatch (want %q):\n%s", c.warn, r.stdout)
			}
		})
	}
	t.Run("a push event never enables it", func(t *testing.T) {
		r := runPromote(t, setup(t, "hotfix-image"), "gate", "EVENT_NAME=push")
		if r.out["hotfix"] != "false" {
			t.Fatalf("hotfix=%q on a push event", r.out["hotfix"])
		}
	})
}

func TestResolveJobWiring(t *testing.T) {
	doc := loadWf(t, repoRoot(t), "docker-images.yml")
	jobs := jobsOf(doc)
	if jobs["resolve"] == nil {
		t.Fatal("docker-images.yml has no resolve job")
	}
	for _, name := range []string{"go-build", "go-build-dho-arm64", "go-api-tools-build", "go-merge", "go-api-tools-merge", "fan-in-latest"} {
		found := false
		for _, n := range asList(asMap(jobs[name])["needs"]) {
			found = found || asString(n) == "resolve"
		}
		if !found {
			t.Errorf("job %s does not need resolve: it would build/push/tag without the promotion decision", name)
		}
	}
	for _, name := range []string{"go-build", "go-build-dho-arm64", "go-api-tools-build"} {
		if !strings.Contains(asString(asMap(jobs[name])["if"]), "needs.resolve.outputs.promoted != 'true'") {
			t.Errorf("job %s still builds after a promotion", name)
		}
	}
	for _, name := range []string{"go-merge", "go-api-tools-merge"} {
		perms := asMap(asMap(jobs[name])["permissions"])
		if asString(perms["attestations"]) != "write" {
			t.Errorf("job %s cannot attest hotfix provenance", name)
		}
	}
}

func (f promFix) attestMany(i int, certs ...map[string]string) {
	var arr []any
	for _, c := range certs {
		arr = append(arr, map[string]any{"verificationResult": map[string]any{"signature": map[string]any{"certificate": c}}})
	}
	f.put("att_"+promKey(promPrefix+promFamilies[i]+"@"+promDigest(i)), arr)
}

func promCert(src, run string) map[string]string {
	return map[string]string{"githubWorkflowTrigger": "pull_request", "sourceRepositoryRef": fmt.Sprintf("refs/pull/%d/merge", promPR), "sourceRepositoryDigest": src, "runInvocationURI": run}
}

func TestPromoteChecksEveryProvenanceCertificate(t *testing.T) {
	t.Run("two runs of the same reviewed source are accepted", func(t *testing.T) {
		f := newPromFix(t)
		f.attestMany(0, promCert(promMerge, "https://github.com/x/runs/1"), promCert(promMerge, "https://github.com/x/runs/2"))
		if r := runPromote(t, f, "promote"); r.out["promoted"] != "true" {
			t.Fatalf("a re-run of the same source must still promote:\n%s", r.stdout)
		}
	})
	t.Run("a second certificate from another source declines", func(t *testing.T) {
		f := newPromFix(t)
		f.commit(promOther, promOther, promBase, promHead)
		f.attestMany(0, promCert(promMerge, "https://github.com/x/runs/1"), promCert(promOther, "https://github.com/x/runs/2"))
		wantDeclined(t, runPromote(t, f, "promote"), "tree differs from merge tree")
	})
	t.Run("a second certificate from a non-PR run declines", func(t *testing.T) {
		f := newPromFix(t)
		bad := promCert(promMerge, "https://github.com/x/runs/2")
		bad["githubWorkflowTrigger"] = "workflow_dispatch"
		f.attestMany(0, promCert(promMerge, "https://github.com/x/runs/1"), bad)
		wantDeclined(t, runPromote(t, f, "promote"), "not a pull_request run on refs/pull/")
	})
	t.Run("an empty attestation list declines", func(t *testing.T) {
		f := newPromFix(t)
		f.put("att_"+promKey(promPrefix+promFamilies[0]+"@"+promDigest(0)), []any{})
		wantDeclined(t, runPromote(t, f, "promote"), "empty or unreadable")
	})
}

func TestLabeledTriggerIsGatedToHotfixImage(t *testing.T) {
	text := readFile(t, filepath.Join(repoRoot(t), publishWorkflow))
	doc := loadWf(t, repoRoot(t), "docker-images.yml")
	on := asMap(doc["on"])
	types := asList(asMap(on["pull_request"])["types"])
	hasLabeled := false
	for _, x := range types {
		hasLabeled = hasLabeled || asString(x) == "labeled"
	}
	if !hasLabeled {
		t.Error("pull_request types lack labeled: adding the label to an open PR would not build the hotfix images")
	}
	if !strings.Contains(asString(asMap(jobsOf(doc)["changes"])["if"]), "github.event.label.name == 'hotfix-image'") {
		t.Error("changes job does not skip a labeled event of another label: every label would rebuild every image")
	}
	if !strings.Contains(text, "github.event.label.name != 'hotfix-image' && '-other-label'") {
		t.Error("another label's run shares the PR concurrency group and would cancel the real run")
	}
}
