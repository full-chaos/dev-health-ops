package bigboycompose_test

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-7838: the bigboy cut recreated the worker plane from an override that was an untracked host file and
// named no service for the heavy group, so queues investment, metrics, reports and workgraph had no worker on
// bigboy from 2026-09-20/21 (workgraph.build jobs piled up, no issue->PR link written after 09-20). These tests
// DERIVE the queue set from the base compose file (ops/compose.yml, every service whose command selects
// `--queues=`) and require every queue to be served by a service of the tracked bigboy override, and every
// such service to be in the cut's `up -d` list. An empty derived set fails: a parse that finds nothing must
// never read as "all queues are served".

func opsRoot(t *testing.T) string {
	t.Helper()
	return filepath.Join(toolsDir(t), "..", "..")
}

// queueServices returns service name -> queues selected by its `--queues=` command argument.
func queueServices(t *testing.T, composePath string) map[string][]string {
	t.Helper()
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc struct {
		Services map[string]struct {
			Command any `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	out := map[string][]string{}
	for name, service := range doc.Services {
		var arguments []string
		switch command := service.Command.(type) {
		case string: // compose accepts a shell-form string command
			arguments = strings.Fields(command)
		case []any:
			for _, item := range command {
				if text, ok := item.(string); ok {
					arguments = append(arguments, text)
				}
			}
		}
		for _, argument := range arguments {
			if value, ok := strings.CutPrefix(argument, "--queues="); ok {
				out[name] = strings.Split(value, ",")
			}
		}
	}
	return out
}

func union(sets map[string][]string) []string {
	seen := map[string]bool{}
	for _, queues := range sets {
		for _, queue := range queues {
			seen[queue] = true
		}
	}
	out := make([]string, 0, len(seen))
	for queue := range seen {
		out = append(out, queue)
	}
	sort.Strings(out)
	return out
}

// cutUpList returns the service names of bigboy-cut.sh's worker-plane `up -d --no-deps --no-build` line.
func cutUpList(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(toolsDir(t), "bigboy-cut.sh"))
	if err != nil {
		t.Fatalf("read bigboy-cut.sh: %v", err)
	}
	pattern := regexp.MustCompile(`up -d --no-deps --no-build ((?:go-[a-z-]+ ?)+)`)
	var found []string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue // a comment line naming a service starts nothing
		}
		if cut, _, ok := strings.Cut(line, " #"); ok {
			line = cut // a trailing comment is not part of the command
		}
		for _, match := range pattern.FindAllStringSubmatch(line, -1) {
			if strings.Contains(match[1], "go-worker") {
				found = append(found, strings.Fields(match[1])...)
			}
		}
	}
	return found
}

func TestEveryQueueOfTheBaseComposeHasAWorkerOnBigboy(t *testing.T) {
	base := queueServices(t, filepath.Join(opsRoot(t), "compose.yml"))
	baseQueues := union(base)
	if len(baseQueues) == 0 {
		t.Fatalf("no `--queues=` service found in ops/compose.yml: the derivation is empty, so it proves nothing")
	}
	override := queueServices(t, filepath.Join(toolsDir(t), "compose.bigboy.workers.yml"))
	served := map[string]bool{}
	for _, queue := range union(override) {
		served[queue] = true
	}
	var missing []string
	for _, queue := range baseQueues {
		if !served[queue] {
			missing = append(missing, queue)
		}
	}
	if len(missing) != 0 {
		t.Fatalf("queues of ops/compose.yml with no worker service in ci/bigboy/compose.bigboy.workers.yml: %v (base queues %v)", missing, baseQueues)
	}
}

func TestEveryQueueServingBigboyServiceIsInTheCutUpList(t *testing.T) {
	override := queueServices(t, filepath.Join(toolsDir(t), "compose.bigboy.workers.yml"))
	if len(override) == 0 {
		t.Fatalf("no `--queues=` service in ci/bigboy/compose.bigboy.workers.yml: the derivation is empty")
	}
	upList := cutUpList(t)
	if len(upList) == 0 {
		t.Fatalf("no worker `up -d --no-deps --no-build` line found in bigboy-cut.sh")
	}
	inList := map[string]bool{}
	for _, name := range upList {
		inList[name] = true
	}
	var missing []string
	for name := range override {
		if !inList[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) != 0 {
		t.Fatalf("queue-serving services of the bigboy override that bigboy-cut.sh never starts: %v (up list %v)", missing, upList)
	}
}

// bigboyCutLines returns the non-comment lines of bigboy-cut.sh.
func bigboyCutLines(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(toolsDir(t), "bigboy-cut.sh"))
	if err != nil {
		t.Fatalf("read bigboy-cut.sh: %v", err)
	}
	var lines []string
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "#") {
			lines = append(lines, line)
		}
	}
	return lines
}

// The test reads ci/bigboy/compose.bigboy.workers.yml, so the cut must run THAT file: the COMPOSE_FILE chain and
// the redacted-config `-f` list name it through $HERE, and no entry still names the untracked host copy
// (compose/compose.bigboy.workers.yml) that this change replaces.
func TestTheCutRunsTheTrackedWorkersOverlay(t *testing.T) {
	const tracked = "$HERE/compose.bigboy.workers.yml"
	const hostCopy = "compose/compose.bigboy.workers.yml"
	var composeFile, redactedConfig string
	for _, line := range bigboyCutLines(t) {
		switch {
		case strings.HasPrefix(strings.TrimSpace(line), "export COMPOSE_FILE="):
			composeFile = line
		case strings.Contains(line, "compose-config-redacted.sh") && strings.Contains(line, " -f "):
			redactedConfig = line
		}
		if strings.Contains(line, hostCopy) {
			t.Errorf("bigboy-cut.sh still names the untracked host copy %s: %s", hostCopy, line)
		}
	}
	if composeFile == "" || redactedConfig == "" {
		t.Fatalf("bigboy-cut.sh has no COMPOSE_FILE export (%v) or no redacted-config -f line (%v)", composeFile != "", redactedConfig != "")
	}
	for name, line := range map[string]string{"COMPOSE_FILE": composeFile, "redacted-config -f list": redactedConfig} {
		if !strings.Contains(line, tracked) {
			t.Errorf("%s does not name %s: %s", name, tracked, line)
		}
	}
}

// Every override service runs the CI operator digest and never a local build: the cut recreates the plane from
// CI digests (Trap #420), and one service on a local tag would be the 09-20 local build again.
func TestEveryBigboyWorkerServiceRunsTheCIOperatorImage(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(toolsDir(t), "compose.bigboy.workers.yml"))
	if err != nil {
		t.Fatalf("read the override: %v", err)
	}
	var doc struct {
		Services map[string]map[string]yaml.Node `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse the override: %v", err)
	}
	if len(doc.Services) == 0 {
		t.Fatalf("no service in the override: the check proves nothing")
	}
	for name, service := range doc.Services {
		image, ok := service["image"]
		if !ok || image.Value != "${BIGBOY_OPERATOR_IMAGE:?set BIGBOY_OPERATOR_IMAGE}" {
			t.Errorf("service %s image = %q, want the required ${BIGBOY_OPERATOR_IMAGE}", name, image.Value)
		}
		if build, ok := service["build"]; !ok || build.Tag != "!reset" {
			t.Errorf("service %s does not reset the base build (`build: !reset null`): a local build would replace the CI image", name)
		}
	}
}

// commandArguments returns the command of a service as a list of arguments (list or shell-form string).
func commandArguments(command any) []string {
	switch typed := command.(type) {
	case string:
		return strings.Fields(typed)
	case []any:
		var out []string
		for _, item := range typed {
			if text, ok := item.(string); ok {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}

func serviceByWorkerGroup(t *testing.T, composePath, group string) []string {
	t.Helper()
	raw, err := os.ReadFile(composePath)
	if err != nil {
		t.Fatalf("read %s: %v", composePath, err)
	}
	var doc struct {
		Services map[string]struct {
			Command any `yaml:"command"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", composePath, err)
	}
	for _, service := range doc.Services {
		arguments := commandArguments(service.Command)
		for _, argument := range arguments {
			if argument == "--worker-group="+group {
				return arguments
			}
		}
	}
	return nil
}

func flagName(argument string) string {
	name, _, _ := strings.Cut(argument, "=")
	return name
}

// The bigboy heavy worker is the BASE heavy command (CHAOS-7976, vet of a8772a0ce4): the five identity flags must be
// identical to the base, and every other flag name of the base command (the DB mode, role, pooler, schema and log
// flags) must be present, so the override cannot copy the omission the other bigboy workers show. An empty
// derivation fails.
func TestTheBigboyHeavyWorkerKeepsTheBaseHeavyCommand(t *testing.T) {
	base := serviceByWorkerGroup(t, filepath.Join(opsRoot(t), "compose.yml"), "heavy")
	if len(base) == 0 {
		t.Fatalf("no base service with --worker-group=heavy in ops/compose.yml: the derivation is empty")
	}
	override := serviceByWorkerGroup(t, filepath.Join(toolsDir(t), "compose.bigboy.workers.yml"), "heavy")
	if len(override) == 0 {
		t.Fatalf("no heavy worker in ci/bigboy/compose.bigboy.workers.yml")
	}
	overrideByName := map[string]string{}
	for _, argument := range override {
		overrideByName[flagName(argument)] = argument
	}
	identity := map[string]bool{"--queues": true, "--queue-concurrency": true, "--worker-group": true, "--shutdown-timeout": true, "--http-addr": true}
	for _, argument := range base {
		if !strings.HasPrefix(argument, "--") {
			continue
		}
		got, present := overrideByName[flagName(argument)]
		if !present {
			t.Errorf("the bigboy heavy worker drops the base flag %s", flagName(argument))
			continue
		}
		if identity[flagName(argument)] && got != argument {
			t.Errorf("the bigboy heavy worker has %q, the base has %q", got, argument)
		}
	}
}
