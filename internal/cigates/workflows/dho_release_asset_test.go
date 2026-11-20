package workflows_test

import (
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-11788: the dho release asset. dho-release-asset.yml builds dho-linux-arm64 with the build commit stamped into
// version.Commit (the deploy pin check refuses "unknown"), and its publish job uploads the file, its sha256 and a build
// provenance attestation. The build line is inline in the run text because a dispatch builds an OLD tag, which carries no
// script of ours.

const (
	dhoAssetWf = ".github/workflows/dho-release-asset.yml"
	dhoAutoWf  = ".github/workflows/auto-release.yml"
)

var (
	dhoSHAUses  = regexp.MustCompile(`^[A-Za-z0-9_.-]+/[A-Za-z0-9_./-]+@[0-9a-f]{40}$`)
	dhoWritePer = map[string]string{"contents": "write", "id-token": "write", "attestations": "write"}
)

func dhoAssetStep(job any, name string) yamlMap {
	for _, s := range stepsOf(job) {
		if strings.HasPrefix(asString(s["name"]), name) {
			return s
		}
	}
	return nil
}

func dhoAssetDefects(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	doc := loadWf(t, root, "dho-release-asset.yml")
	on := asMap(doc["on"])
	var events []string
	for e := range on {
		events = append(events, e)
	}
	sort.Strings(events)
	if !reflect.DeepEqual(events, []string{"pull_request", "workflow_call", "workflow_dispatch"}) {
		add("triggers are %v, want exactly pull_request, workflow_call, workflow_dispatch (no release, push or pull_request_target)", events)
	}
	if top, _, _ := permsOf(doc); !reflect.DeepEqual(top, map[string]string{"contents": "read"}) {
		add("workflow-level permissions %v, want exactly contents: read", top)
	}
	jobs := jobsOf(doc)
	build, publish := asMap(jobs["build"]), asMap(jobs["publish"])
	if build == nil || publish == nil || len(jobs) != 2 {
		return append(out, "want exactly the jobs build and publish")
	}

	if p, _, _ := permsOf(build); !reflect.DeepEqual(p, map[string]string{"contents": "read"}) {
		add("build job permissions %v, want exactly contents: read", p)
	}
	for _, s := range append(stepsOf(build), stepsOf(publish)...) {
		if strings.Contains(asString(s["run"]), "${{") {
			add("step %q interpolates an expression into run text; pass it through env", asString(s["name"]))
		}
	}
	if co := dhoAssetStep(build, "Checkout code"); co == nil || asMap(co["with"])["persist-credentials"] != false {
		add("build checkout must set persist-credentials: false")
	}
	tag := dhoAssetStep(build, "Check the tag shape")
	if tag == nil || !strings.Contains(asString(tag["run"]), `^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+\.[0-9]+)?$`) || asString(tag["if"]) != "github.event_name != 'pull_request'" {
		add("build has no tag shape check before the checkout, off pull_request")
	}
	tree := dhoAssetStep(build, "Check the tree can build dho")
	if tree == nil || !strings.Contains(asString(tree["run"]), "internal/platform/version/version.go") || !strings.Contains(asString(tree["run"]), "cmd/dho/main.go") {
		add("build has no check that the tree can build dho (go.mod, cmd/dho, version.go)")
	}
	bs := dhoAssetStep(build, "Build dho linux arm64")
	run := asString(bs["run"])
	for _, want := range []string{
		"VP=github.com/full-chaos/dev-health-ops/internal/platform/version",
		"COMMIT=\"$(git rev-parse HEAD)\"",
		"[[ \"${COMMIT}\" =~ ^[0-9a-f]{40}$ ]]",
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
	if env := asMap(bs["env"]); asString(env["TAG"]) != "${{ inputs.tag }}" || asString(env["EVENT_NAME"]) != "${{ github.event_name }}" {
		add("the build step must take the tag and event only through env")
	}

	if p, _, _ := permsOf(publish); !reflect.DeepEqual(p, dhoWritePer) {
		add("publish job permissions %v, want exactly %v", p, dhoWritePer)
	}
	if asString(publish["if"]) != "github.event_name != 'pull_request'" || asString(publish["needs"]) != "build" {
		add("publish must need build and skip pull_request")
	}
	pubSteps := stepsOf(publish)
	for _, s := range pubSteps {
		if strings.HasPrefix(strings.ToLower(asString(s["uses"])), "actions/checkout@") {
			add("publish has no checkout (no source, no token on disk)")
		}
	}
	at := dhoAssetStep(publish, "Attest build provenance")
	if at == nil || !strings.HasPrefix(asString(at["uses"]), "actions/attest-build-provenance@4d101475d8b20a2381f78447822ac1eab6504dd8") || asString(asMap(at["with"])["subject-path"]) != "dist/dho-linux-arm64" {
		add("publish must attest dist/dho-linux-arm64 with the pinned attest-build-provenance")
	}
	up := asString(dhoAssetStep(publish, "Upload the release assets")["run"])
	if !strings.Contains(up, `gh release upload "${TAG}" dist/dho-linux-arm64 dist/dho-linux-arm64.sha256`) || strings.Contains(up, "--clobber") {
		add("publish must upload both files with gh release upload and no --clobber")
	}
	if !strings.Contains(asString(dhoAssetStep(publish, "Verify the bytes")["run"]), "sha256sum -c dho-linux-arm64.sha256") {
		add("publish must verify the sha256 before the attestation")
	}
	for _, j := range []any{build, publish} {
		for _, s := range stepsOf(j) {
			if u := asString(s["uses"]); u != "" {
				if !dhoSHAUses.MatchString(u) {
					add("step %q uses %q: not pinned to a 40 hex SHA", asString(s["name"]), u)
				}
			}
		}
	}
	return out
}

func dhoAutoDefects(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	jobs := jobsOf(loadWf(t, root, "auto-release.yml"))
	job := asMap(jobs["dho-asset"])
	if job == nil {
		return []string{"auto-release.yml has no dho-asset job"}
	}
	if asString(job["needs"]) != "tag-and-release" || asString(job["uses"]) != "./.github/workflows/dho-release-asset.yml" {
		out = append(out, "dho-asset must need tag-and-release and call ./.github/workflows/dho-release-asset.yml")
	}
	if asString(asMap(job["with"])["tag"]) != "${{ needs.tag-and-release.outputs.new_tag }}" {
		out = append(out, "dho-asset must pass the tag from needs.tag-and-release.outputs.new_tag")
	}
	if p, _, _ := permsOf(job); !reflect.DeepEqual(p, dhoWritePer) {
		out = append(out, fmt.Sprintf("dho-asset permissions %v, want exactly %v", p, dhoWritePer))
	}
	if p, _, _ := permsOf(asMap(jobs["tag-and-release"])); !reflect.DeepEqual(p, map[string]string{"contents": "write"}) {
		out = append(out, fmt.Sprintf("tag-and-release permissions %v, want exactly contents: write", p))
	}
	if asString(asMap(asMap(jobs["tag-and-release"])["outputs"])["new_tag"]) != "${{ steps.version.outputs.new_tag }}" {
		out = append(out, "tag-and-release must output new_tag")
	}
	return out
}

func TestDhoReleaseAssetHoldsOnTheRealTree(t *testing.T) {
	root := rootOf(t)
	if d := append(dhoAssetDefects(t, root), dhoAutoDefects(t, root)...); len(d) > 0 {
		t.Fatalf("%s", strings.Join(d, "\n"))
	}
}

func TestDhoReleaseAssetRefusesEachDefectClass(t *testing.T) {
	root := rootOf(t)
	files := allWorkflowFiles(t, root)
	const stamp = "-X ${VP}.Commit=${COMMIT} "
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"the Commit stamp is dropped", mutation{dhoAssetWf, stamp, ""}, "build line lost"},
		{"the commit is read from an input", mutation{dhoAssetWf, `COMMIT="$(git rev-parse HEAD)"`, `COMMIT="${TAG}"`}, "build line lost"},
		{"the 40 hex check is dropped", mutation{dhoAssetWf, "[[ \"${COMMIT}\" =~ ^[0-9a-f]{40}$ ]]", "true"}, "build line lost"},
		{"trimpath is dropped", mutation{dhoAssetWf, "-buildvcs=false -trimpath", "-buildvcs=false"}, "build line lost"},
		{"the arm64 stamp byte check is dropped", mutation{dhoAssetWf, "grep -a -q -F", "grep -a -c -F"}, "build line lost"},
		{"the amd64 twin proof is dropped", mutation{dhoAssetWf, "jq -e --arg c", "jq --arg c"}, "build line lost"},
		{"a release trigger returns", mutation{dhoAssetWf, "on:\n  workflow_call:", "on:\n  release:\n    types: [published]\n  workflow_call:"}, "triggers are"},
		{"the build job gets a write token", mutation{dhoAssetWf, "    timeout-minutes: 15\n    permissions:\n      contents: read\n", "    timeout-minutes: 15\n    permissions:\n      contents: write\n"}, "build job permissions"},
		{"publish loses id-token", mutation{dhoAssetWf, "      id-token: write\n", ""}, "publish job permissions"},
		{"publish runs on a pull request", mutation{dhoAssetWf, "  publish:\n    if: github.event_name != 'pull_request'\n", "  publish:\n"}, "publish must need build and skip pull_request"},
		{"clobber returns", mutation{dhoAssetWf, "dist/dho-linux-arm64 dist/dho-linux-arm64.sha256\n", "dist/dho-linux-arm64 dist/dho-linux-arm64.sha256 --clobber\n"}, "no --clobber"},
		{"an action floats", mutation{dhoAssetWf, "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0", "actions/setup-go@v7"}, "not pinned to a 40 hex SHA"},
		{"the tag is interpolated into run text", mutation{dhoAssetWf, "[[ \"${TAG}\" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+(-[0-9A-Za-z]+\\.[0-9]+)?$ ]] || { echo \"::error::the tag is not vMAJOR.MINOR.PATCH[-suffix.N]\"; exit 1; }\n          gh release", "echo ${{ inputs.tag }}\n          gh release"}, "interpolates an expression"},
		{"the checkout persists credentials", mutation{dhoAssetWf, "          fetch-depth: 1\n          persist-credentials: false\n", "          fetch-depth: 1\n"}, "persist-credentials: false"},
		{"the call job is gone", mutation{dhoAutoWf, "  dho-asset:\n", "  dho-asset-x:\n"}, "no dho-asset job"},
		{"the call job takes another tag", mutation{dhoAutoWf, "tag: ${{ needs.tag-and-release.outputs.new_tag }}", "tag: ${{ github.ref_name }}"}, "must pass the tag from"},
		{"the call job loses its grant", mutation{dhoAutoWf, "      attestations: write\n    uses:", "    uses:"}, "dho-asset permissions"},
	} {
		tmp := plantTree(t, root, files, c.m)
		if d := strings.Join(append(dhoAssetDefects(t, tmp), dhoAutoDefects(t, tmp)...), "\n"); !strings.Contains(d, c.want) {
			t.Errorf("%s: want %q in:\n%s", c.name, c.want, d)
		}
	}
}
