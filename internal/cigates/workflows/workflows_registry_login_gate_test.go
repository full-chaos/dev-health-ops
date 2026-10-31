package workflows_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRegistryLoginGateHoldsOnTheRealTree(t *testing.T) {
	expectNoViolations(t, registryLoginDefects(t, rootOf(t), jobLoginCensus))
}

const (
	plantLogin = "uses: docker/login-action@dbcb813823bdd20940b903addbd779551569679f # v4.6.0"
	plantWf    = ".github/workflows/test.yml"
)

// plantJob adds one job to test.yml; with no packages grant it inherits the workflow map, contents: read.
func plantJob(packages bool, steps string) mutation {
	perms := ""
	if packages {
		perms = "    permissions:\n      contents: read\n      packages: read\n"
	}
	return mutation{plantWf, "jobs:\n", "jobs:\n  planted:\n    runs-on: ubuntu-latest\n" + perms + "    steps:\n" + steps}
}

func runStep(line string) string { return "      - run: |\n          " + line + "\n" }

func loginActionStep(extra string) string { return "      - " + plantLogin + "\n" + extra }

func registryLoginTree(t *testing.T, root string, extraScripts map[string]string, muts ...mutation) string {
	t.Helper()
	files := append(allWorkflowFiles(t, root), ciScripts(t, root)...)
	tmp := plantTree(t, root, files, muts...)
	for name, body := range extraScripts {
		dst := filepath.Join(tmp, name)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return tmp
}

func TestRegistryLoginGateRefusesEachDefectClass(t *testing.T) {
	root := rootOf(t)
	const ghcr = "        with:\n          registry: ghcr.io\n"
	const noScope = "logs in to a registry without packages scope"
	const direct = "run holds a login registry login"
	const indirect = "run holds a indirect registry login"
	const named = "in a form other than exactly"
	for _, c := range []struct {
		name string
		m    mutation
		want string
	}{
		{"login-action without packages scope", plantJob(false, loginActionStep(ghcr)), noScope},
		{"the login script without packages scope", plantJob(false, runStep(loginScriptRun)), noScope},
		{"docker login on stdin without packages scope", plantJob(false, runStep("echo $T | docker login ghcr.io -u u --password-stdin")), direct},
		{"docker login in a job with packages scope", plantJob(true, runStep("docker login ghcr.io -u u")), direct},
		{"env prefix and sudo before the tool", plantJob(true, runStep("A=b sudo docker login ghcr.io")), direct},
		{"login after a semicolon", plantJob(true, runStep("true; docker login ghcr.io")), direct},
		{"login after &&", plantJob(true, runStep("true && podman login ghcr.io")), direct},
		{"helm registry login", plantJob(true, runStep("helm registry login ghcr.io")), direct},
		{"oras login", plantJob(true, runStep("oras login ghcr.io")), direct},
		{"crane auth login", plantJob(true, runStep("crane auth login ghcr.io")), direct},
		{"skopeo login", plantJob(true, runStep("skopeo login ghcr.io")), direct},
		{"bash -c around the login script", plantJob(true, runStep("bash -c 'bash ci/lib/ghcr_login.sh'")), indirect},
		{"eval of the login script", plantJob(true, runStep("eval \"bash ci/lib/ghcr_login.sh\"")), indirect},
		{"eval of a docker login", plantJob(true, runStep("eval 'docker login ghcr.io'")), indirect},
		{"source of the login script", plantJob(true, runStep("source ci/lib/ghcr_login.sh")), indirect},
		{"dot of the login script", plantJob(true, runStep(". ci/lib/ghcr_login.sh")), indirect},
		{"login script run by path", plantJob(true, runStep("./ci/lib/ghcr_login.sh")), named},
		{"login script run with a prefix", plantJob(true, runStep("set -e\n          bash ci/lib/ghcr_login.sh")), named},
		{"login-action unpinned", plantJob(true, "      - uses: docker/login-action@v4\n"+ghcr), "docker/login-action is not pinned to a full commit sha"},
		{"login-action registry omitted", plantJob(true, loginActionStep("")), "registry is \"\", want ghcr.io"},
		{"login-action on another registry", plantJob(true, loginActionStep("        with:\n          registry: docker.io\n")), "registry is \"docker.io\", want ghcr.io"},
		{"a login step the census does not list", plantJob(true, loginActionStep(ghcr)), "planted: login steps (action 1, script 0) are not in the login census"},
	} {
		t.Run(c.name, func(t *testing.T) {
			tmp := registryLoginTree(t, root, nil, c.m)
			expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), c.want)
		})
	}
	scriptCases := []struct {
		name, file, body, want string
	}{
		{"a new script with a login", "ci/extra_login.sh", "#!/usr/bin/env bash\ndocker login ghcr.io -u u\n", "ci scripts with a registry login are [ci/extra_login.sh ci/lib/ghcr_login.sh]"},
		{"a new script with a piped login", "ci/extra_login.sh", "printf x | docker login ghcr.io --password-stdin\n", "ci scripts with a registry login are"},
		{"a new script with an eval login", "ci/extra_login.sh", "eval \"docker login ghcr.io\"\n", "an indirect registry login"},
		{"a new script sourcing the login script", "ci/extra_login.sh", "source ci/lib/ghcr_login.sh\n", "an indirect registry login"},
	}
	for _, c := range scriptCases {
		t.Run(c.name, func(t *testing.T) {
			tmp := registryLoginTree(t, root, map[string]string{c.file: c.body})
			expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), c.want)
		})
	}
	t.Run("the login script loses its login", func(t *testing.T) {
		tmp := registryLoginTree(t, root, nil, mutation{loginScript, "| docker login ghcr.io", "| docker logout ghcr.io"})
		expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), "ci scripts with a registry login are []")
	})
	t.Run("a census row for a job that is gone", func(t *testing.T) {
		census := map[jobKey]loginCount{{"go.yml", "no-such-job"}: {1, 0}}
		for k, v := range jobLoginCensus {
			census[k] = v
		}
		expectViolation(t, registryLoginDefects(t, registryLoginTree(t, root, nil), census), "go.yml job no-such-job: stale login census row, no such job")
	})
	t.Run("a census row whose job lost its login", func(t *testing.T) {
		census := map[jobKey]loginCount{{"go.yml", "go-queue-gate"}: {1, 0}}
		for k, v := range jobLoginCensus {
			census[k] = v
		}
		expectViolation(t, registryLoginDefects(t, registryLoginTree(t, root, nil), census), "go.yml job go-queue-gate: stale login census row, the job has no login step")
	})
}

func TestRegistryLoginGateAcceptsTheHarmlessForms(t *testing.T) {
	root := rootOf(t)
	for _, c := range []struct{ name, steps string }{
		{"echo of a login", runStep("echo docker login ghcr.io")},
		{"printf of a login", runStep("printf 'helm registry login\\n'")},
		{"a comment naming a login", runStep("# docker login ghcr.io\n          echo ok")},
		{"docker logout", runStep("docker logout ghcr.io")},
		{"docker pull and build", runStep("docker pull alpine && docker build .")},
		{"the login test suite", runStep("bash ci/ghcr_login_test.sh")},
		{"the exact script line in a packages job", runStep(loginScriptRun)},
	} {
		t.Run(c.name, func(t *testing.T) {
			packages := strings.Contains(c.name, "packages job")
			steps := c.steps
			if packages {
				steps = loginActionStep("        with:\n          registry: ghcr.io\n") + steps
			}
			tmp := registryLoginTree(t, root, nil, plantJob(packages, steps))
			census := map[jobKey]loginCount{}
			for k, v := range jobLoginCensus {
				census[k] = v
			}
			if packages {
				census[jobKey{"test.yml", "planted"}] = loginCount{1, 1}
			}
			expectNoViolations(t, registryLoginDefects(t, tmp, census))
		})
	}
	t.Run("a login in a test script", func(t *testing.T) {
		tmp := registryLoginTree(t, root, map[string]string{"ci/extra_test.sh": "docker login ghcr.io\n"})
		expectNoViolations(t, registryLoginDefects(t, tmp, jobLoginCensus))
	})
}

var commandPositionLogins = []string{
	"if docker login ghcr.io -u x; then echo ok; fi",
	"if true; then docker login ghcr.io -u x; fi",
	"if false; then echo no; else docker login ghcr.io -u x; fi",
	"if false; then echo no; elif docker login ghcr.io -u x; then echo ok; fi",
	"until docker login ghcr.io -u x; do break; done",
	"while docker login ghcr.io -u x; do break; done",
	"! docker login ghcr.io -u x",
	"(docker login ghcr.io -u x)",
	"{ docker login ghcr.io -u x; }",
	"echo \"$(docker login ghcr.io -u x)\"",
	"echo `docker login ghcr.io -u x`",
	"echo $(echo $(docker login ghcr.io -u x))",
	"echo $(docker login ghcr.io -u x",
	"time docker login ghcr.io -u x",
	"env -i docker login ghcr.io -u x",
	"env -u HOME docker login ghcr.io -u x",
	"sudo -u runner docker login ghcr.io -u x",
	"command -p docker login ghcr.io -u x",
	"exec -a argv0 docker login ghcr.io -u x",
	"nice -n 5 docker login ghcr.io -u x",
	"nohup docker login ghcr.io -u x",
	"bash -c 'docker login x'",
	"eval \"docker login x\"",
	"timeout 60 docker login ghcr.io -u x",
	"xargs docker login ghcr.io -u x",
	"stdbuf -oL docker login ghcr.io -u x",
	"setsid docker login ghcr.io -u x",
	"timeout 60 bash -c 'docker login ghcr.io'",
	"xargs sh -c 'docker login ghcr.io'",
	"nohup bash -c 'bash ci/lib/ghcr_login.sh'",
	"env -S 'docker login ghcr.io'",
	"env -S'docker login ghcr.io'",
	"env --split-string='docker login ghcr.io'",
}

var commandPositionCalls = []string{
	"sh ci/lib/ghcr_login.sh",
	"bash -x ci/lib/ghcr_login.sh",
	"./ci/lib/ghcr_login.sh",
	"ci/lib/ghcr_login.sh",
	"if true; then bash ci/lib/ghcr_login.sh; fi",
	"sudo -u runner bash ci/lib/ghcr_login.sh",
	"echo $(bash ci/lib/ghcr_login.sh)",
	"bash \"ci/lib/ghcr_login.sh\"",
	"source ci/lib/ghcr_login.sh",
	"eval \"bash ci/lib/ghcr_login.sh\"",
	"timeout 60 bash ci/lib/ghcr_login.sh",
	"xargs bash ci/lib/ghcr_login.sh",
	"stdbuf -oL bash ci/lib/ghcr_login.sh",
	"setsid bash ci/lib/ghcr_login.sh",
}

var harmlessLines = []string{
	"echo 'text; docker login ghcr.io'",
	"echo \"text && docker login ghcr.io\"",
	"echo ci/lib/ghcr_login.sh",
	"echo \"ci/lib/ghcr_login.sh\"",
	"echo docker login",
	"echo \"docker login\"",
	"env -S 'echo docker login'",
	"echo \"${HOME} { not a group }\"",
}

func TestRegistryLoginGateRefusesLoginsAtCommandPosition(t *testing.T) {
	root := rootOf(t)
	for _, line := range commandPositionLogins {
		t.Run("workflow "+line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, nil, plantJob(true, runStep(line)))
			expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), "run holds a")
		})
		t.Run("script "+line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, map[string]string{"ci/extra_login.sh": line + "\n"})
			expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), "ci/extra_login.sh")
		})
	}
	for _, line := range commandPositionCalls {
		t.Run("workflow call "+line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, nil, plantJob(true, runStep(line)))
			d := registryLoginDefects(t, tmp, jobLoginCensus)
			expectViolation(t, d, "test.yml job planted step 1: run ")
		})
	}
}

func TestRegistryLoginGateAcceptsQuotedAndEchoedForms(t *testing.T) {
	root := rootOf(t)
	for _, line := range harmlessLines {
		t.Run("workflow "+line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, nil, plantJob(false, runStep(line)))
			expectNoViolations(t, registryLoginDefects(t, tmp, jobLoginCensus))
		})
		t.Run("script "+line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, map[string]string{"ci/extra.sh": line + "\n"})
			expectNoViolations(t, registryLoginDefects(t, tmp, jobLoginCensus))
		})
	}
}

func TestRegistryLoginGateRefusesOtherCiScriptsThatCallTheLoginScript(t *testing.T) {
	root := rootOf(t)
	for _, line := range []string{
		"bash ci/lib/ghcr_login.sh",
		"source ci/lib/ghcr_login.sh",
		"./ci/lib/ghcr_login.sh",
		"timeout 60 bash ci/lib/ghcr_login.sh",
		"if true; then sh ci/lib/ghcr_login.sh; fi",
	} {
		t.Run(line, func(t *testing.T) {
			tmp := registryLoginTree(t, root, map[string]string{"ci/extra_login.sh": line + "\n"})
			expectViolation(t, registryLoginDefects(t, tmp, jobLoginCensus), "ci/extra_login.sh")
		})
	}
}
