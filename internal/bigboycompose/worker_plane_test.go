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
	for _, match := range pattern.FindAllStringSubmatch(string(raw), -1) {
		if strings.Contains(match[1], "go-worker") {
			found = append(found, strings.Fields(match[1])...)
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
