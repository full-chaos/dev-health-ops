package workflows_test

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11788: the dho release asset. auto-release.yml (jobs dho-build and dho-publish) builds dho-linux-arm64 for a new
// tag with the build commit stamped into version.Commit (the deploy pin check refuses "unknown"), then uploads the file,
// its sha256 and a build provenance attestation. dho-release-asset.yml runs the SAME build step on pull requests (and by
// dispatch for a tag) and proves the stamp; it publishes nothing. There is no local reusable workflow: a release made
// with GITHUB_TOKEN does not fire `on: release`, and the image, login and cache guards refuse a local `uses:` workflow.
// The build line is inline in the run text because a dispatch builds an OLD tag, which carries no script of ours. The two
// build step texts are pinned byte-equal here.

const (
	dhoProofWf = ".github/workflows/dho-release-asset.yml"
	dhoAutoWf  = ".github/workflows/auto-release.yml"
	dhoTagRe   = `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+\.[0-9]+)?$`
)

var (
	dhoSHAUses  = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$`)
	dhoWritePer = map[string]string{"contents": "write", "id-token": "write", "attestations": "write"}
	dhoReadPer  = map[string]string{"contents": "read"}
)

func dhoStep(job any, name string) yamlMap {
	for _, s := range stepsOf(job) {
		if strings.HasPrefix(asString(s["name"]), name) {
			return s
		}
	}
	return nil
}

func dhoBuildText(job any) string { return asString(dhoStep(job, "Build dho linux arm64")["run"]) }

// dhoBuildJobDefects holds what a dho build job must be, in either file.
func dhoBuildJobDefects(label string, job yamlMap, tagExpr string) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, label+": "+fmt.Sprintf(f, a...)) }
	if p, _, _ := permsOf(job); !reflect.DeepEqual(p, dhoReadPer) {
		add("permissions %v, want exactly contents: read", p)
	}
	if asString(job["uses"]) != "" {
		add("a job-level uses: (a reusable workflow) is refused by the image, login and cache guards")
	}
	for _, s := range stepsOf(job) {
		if strings.Contains(asString(s["run"]), "${{") {
			add("step %q interpolates an expression into run text; pass it through env", asString(s["name"]))
		}
		if u := asString(s["uses"]); u != "" && !dhoSHAUses.MatchString(u) {
			add("step %q uses %q: not pinned to a 40 hex SHA", asString(s["name"]), u)
		}
		if strings.Contains(asString(s["run"]), "gh release") || strings.HasPrefix(asString(s["uses"]), "actions/attest-") {
			add("step %q publishes from a build job", asString(s["name"]))
		}
	}
	if co := dhoStep(job, "Checkout code"); co == nil || asMap(co["with"])["persist-credentials"] != false {
		add("checkout must set persist-credentials: false")
	}
	tag := dhoStep(job, "Check the tag shape")
	if tag == nil || !strings.Contains(asString(tag["run"]), dhoTagRe) || asString(asMap(tag["env"])["TAG"]) != tagExpr {
		add("no tag shape check (regex, tag through env) before the checkout")
	}
	tree := dhoStep(job, "Check the tree can build dho")
	if tree == nil || !strings.Contains(asString(tree["run"]), "internal/platform/version/version.go") || !strings.Contains(asString(tree["run"]), "cmd/dho/main.go") {
		add("no check that the tree can build dho (go.mod, cmd/dho, version.go)")
	}
	bs := dhoStep(job, "Build dho linux arm64")
	run := asString(bs["run"])
	for _, want := range []string{
		"VP=github.com/full-chaos/dev-health-ops/internal/platform/version",
		"COMMIT=\"$(git rev-parse HEAD)\"",
		"[[ \"${COMMIT}\" =~ ^[0-9a-f]{40}$ ]]",
		"if [ -n \"${TAG}\" ]; then VERSION=\"${TAG}\"; else VERSION=\"pr-${COMMIT:0:12}\"; fi",
		"CGO_ENABLED=0 GOFLAGS=-mod=readonly SOURCE_DATE_EPOCH",
		"-buildvcs=false -trimpath",
		"-ldflags=\"-s -w -buildid= -X ${VP}.Version=${VERSION} -X ${VP}.Commit=${COMMIT} -X ${VP}.BuildTime=${BUILD_TIME}\"",
		"touch -d \"@${SOURCE_DATE_EPOCH}\"",
		"cmp dist/dho-linux-arm64 twin/dho-linux-arm64-again",
		"sha256sum -c dho-linux-arm64.sha256",
		"grep -c -F -e \"-trimpath=true\")\" = 1 ]",
		"grep -a -q -F -e \"${COMMIT}\" dist/dho-linux-arm64",
		"twin/dho-linux-amd64 version | jq -e --arg c \"${COMMIT}\" '.commit == $c and .commit != \"unknown\"'",
	} {
		if !strings.Contains(run, want) {
			add("build line lost %q", want)
		}
	}
	if env := asMap(bs["env"]); len(env) != 1 || asString(env["TAG"]) != tagExpr {
		add("the build step must take the tag only through env TAG = %s", tagExpr)
	}
	return out
}

func dhoDefects(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	proof := loadWf(t, root, "dho-release-asset.yml")
	var events []string
	for e := range asMap(proof["on"]) {
		events = append(events, e)
	}
	sort.Strings(events)
	if !reflect.DeepEqual(events, []string{"pull_request", "workflow_dispatch"}) {
		add("dho-release-asset.yml triggers are %v, want exactly pull_request and workflow_dispatch (no release, push, workflow_call or pull_request_target)", events)
	}
	if top, _, _ := permsOf(proof); !reflect.DeepEqual(top, dhoReadPer) {
		add("dho-release-asset.yml workflow-level permissions %v, want exactly contents: read", top)
	}
	pj := jobsOf(proof)
	if len(pj) != 1 || asMap(pj["build"]) == nil {
		add("dho-release-asset.yml: want exactly the job build and no publish")
		return out
	}
	pbuild := asMap(pj["build"])
	out = append(out, dhoBuildJobDefects("dho-release-asset.yml build", pbuild, "${{ inputs.tag }}")...)

	auto := loadWf(t, root, "auto-release.yml")
	aj := jobsOf(auto)
	abuild, apub := asMap(aj["dho-build"]), asMap(aj["dho-publish"])
	if abuild == nil || apub == nil {
		return append(out, "auto-release.yml: want the jobs dho-build and dho-publish")
	}
	const tagOut = "${{ needs.tag-and-release.outputs.new_tag }}"
	if asString(abuild["needs"]) != "tag-and-release" {
		add("auto-release.yml dho-build must need tag-and-release")
	}
	out = append(out, dhoBuildJobDefects("auto-release.yml dho-build", abuild, tagOut)...)
	if co := dhoStep(abuild, "Checkout code"); co != nil && asString(asMap(co["with"])["ref"]) != "refs/tags/"+tagOut {
		add("auto-release.yml dho-build must check out refs/tags/<new_tag>")
	}
	if a, b := dhoBuildText(abuild), dhoBuildText(pbuild); a == "" || a != b {
		add("the build step texts of auto-release.yml dho-build and dho-release-asset.yml build differ: the proof leg must run the release build line byte for byte")
	}
	if asString(asMap(asMap(aj["tag-and-release"])["outputs"])["new_tag"]) != "${{ steps.version.outputs.new_tag }}" {
		add("auto-release.yml tag-and-release must output new_tag")
	}
	if p, _, _ := permsOf(asMap(aj["tag-and-release"])); !reflect.DeepEqual(p, map[string]string{"contents": "write"}) {
		add("auto-release.yml tag-and-release permissions %v, want exactly contents: write", p)
	}
	if p, _, _ := permsOf(apub); !reflect.DeepEqual(p, dhoWritePer) {
		add("auto-release.yml dho-publish permissions %v, want exactly %v", p, dhoWritePer)
	}
	if !reflect.DeepEqual(asList(apub["needs"]), []any{"tag-and-release", "dho-build"}) || asString(apub["uses"]) != "" {
		add("auto-release.yml dho-publish must need tag-and-release and dho-build and call no workflow")
	}
	for _, s := range stepsOf(apub) {
		if strings.HasPrefix(strings.ToLower(asString(s["uses"])), "actions/checkout@") {
			add("auto-release.yml dho-publish has no checkout (no source, no token on disk)")
		}
		if strings.Contains(asString(s["run"]), "${{") {
			add("auto-release.yml dho-publish step %q interpolates an expression into run text", asString(s["name"]))
		}
		if u := asString(s["uses"]); u != "" && !dhoSHAUses.MatchString(u) {
			add("auto-release.yml dho-publish step %q uses %q: not pinned to a 40 hex SHA", asString(s["name"]), u)
		}
	}
	at := dhoStep(apub, "Attest build provenance")
	if at == nil || !strings.HasPrefix(asString(at["uses"]), "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8") || asString(asMap(at["with"])["subject-path"]) != "dist/dho-linux-arm64" {
		add("dho-publish must attest dist/dho-linux-arm64 with the pinned attest-build-provenance")
	}
	upl := dhoStep(apub, "Upload the release assets")
	up := asString(upl["run"])
	if !strings.Contains(up, `gh release upload "${TAG}" dist/dho-linux-arm64 dist/dho-linux-arm64.sha256`) || strings.Contains(up, "--clobber") || !strings.Contains(up, dhoTagRe) || asString(asMap(upl["env"])["TAG"]) != tagOut {
		add("dho-publish must check the tag shape and upload both files with gh release upload and no --clobber, tag through env")
	}
	if !strings.Contains(asString(dhoStep(apub, "Verify the bytes")["run"]), "sha256sum -c dho-linux-arm64.sha256") {
		add("dho-publish must verify the sha256 before the attestation")
	}
	return out
}

func TestDhoReleaseAssetHoldsOnTheRealTree(t *testing.T) {
	if d := dhoDefects(t, rootOf(t)); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
}

func TestDhoReleaseAssetRefusesEachDefectClass(t *testing.T) {
	root := rootOf(t)
	files := allWorkflowFiles(t, root)
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the Commit stamp is dropped", mutation{dhoAutoWf, "-X ${VP}.Commit=${COMMIT} ", ""}, "build line lost"},
		{"the commit is read from the tag", mutation{dhoProofWf, `COMMIT="$(git rev-parse HEAD)"`, `COMMIT="${TAG}"`}, "build line lost"},
		{"the 40 hex check is dropped", mutation{dhoAutoWf, "[[ \"${COMMIT}\" =~ ^[0-9a-f]{40}$ ]]", "true"}, "build line lost"},
		{"trimpath is dropped", mutation{dhoAutoWf, "-buildvcs=false -trimpath", "-buildvcs=false"}, "build line lost"},
		{"the arm64 stamp byte check is dropped", mutation{dhoAutoWf, "grep -a -q -F", "grep -a -c -F"}, "build line lost"},
		{"the amd64 twin proof is dropped", mutation{dhoAutoWf, "jq -e --arg c", "jq --arg c"}, "build line lost"},
		{"one character drifts in the release copy", mutation{dhoAutoWf, `echo "commit=${COMMIT}"`, `echo "commit=${COMMIT}."`}, "build step texts of auto-release.yml dho-build and dho-release-asset.yml build differ"},
		{"one character drifts in the proof copy", mutation{dhoProofWf, "cat dist/dho-linux-arm64.sha256", "cat dist/dho-linux-arm64.sha25"}, "build step texts of auto-release.yml dho-build and dho-release-asset.yml build differ"},
		{"a release trigger returns", mutation{dhoProofWf, "on:\n  workflow_dispatch:", "on:\n  release:\n    types: [published]\n  workflow_dispatch:"}, "triggers are"},
		{"the proof build gets a write token", mutation{dhoProofWf, "    permissions:\n      contents: read\n    steps:", "    permissions:\n      contents: write\n    steps:"}, "dho-release-asset.yml build: permissions"},
		{"the release build gets a write token", mutation{dhoAutoWf, "    timeout-minutes: 15\n    permissions:\n      contents: read\n", "    timeout-minutes: 15\n    permissions:\n      contents: write\n"}, "dho-build: permissions"},
		{"publish loses id-token", mutation{dhoAutoWf, "      contents: write\n      id-token: write\n", "      contents: write\n"}, "dho-publish permissions"},
		{"publish checks out source", mutation{dhoAutoWf, "    steps:\n      - name: Download the asset files", "    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n      - name: Download the asset files"}, "dho-publish has no checkout"},
		{"clobber returns", mutation{dhoAutoWf, "dist/dho-linux-arm64 dist/dho-linux-arm64.sha256\n", "dist/dho-linux-arm64 dist/dho-linux-arm64.sha256 --clobber\n"}, "no --clobber"},
		{"an action floats", mutation{dhoProofWf, "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0", "actions/setup-go@v7"}, "not pinned to a 40 hex SHA"},
		{"the tag is interpolated into run text", mutation{dhoAutoWf, "          set -euo pipefail\n          cd dist\n", "          set -euo pipefail\n          echo ${{ needs.tag-and-release.outputs.new_tag }}\n          cd dist\n"}, "interpolates an expression"},
		{"the checkout persists credentials", mutation{dhoProofWf, "          fetch-depth: 1\n          persist-credentials: false\n", "          fetch-depth: 1\n"}, "persist-credentials: false"},
		{"a local reusable workflow returns", mutation{dhoAutoWf, "  dho-build:\n    needs: tag-and-release\n    runs-on: ubuntu-latest\n", "  dho-build:\n    needs: tag-and-release\n    uses: ./.github/workflows/dho-release-asset.yml\n    runs-on: ubuntu-latest\n"}, "reusable workflow"},
		{"the release build checks out another ref", mutation{dhoAutoWf, "          ref: refs/tags/${{ needs.tag-and-release.outputs.new_tag }}\n", "          ref: main\n"}, "must check out refs/tags/<new_tag>"},
	} {
		tmp := plantTree(t, root, files, c.m)
		if d := strings.Join(dhoDefects(t, tmp), "\n"); !strings.Contains(d, c.want) {
			t.Errorf("%s: want %q in:\n%s", c.name, c.want, d)
		}
	}
}
