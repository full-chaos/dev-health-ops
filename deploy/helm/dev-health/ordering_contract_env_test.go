package devhealth_test

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

var orderingContractValue = regexp.MustCompile(`(?m)- name: OPERATIONAL_ORDERING_CONTRACT\n\s+value: "([^"]*)"`)

// Every go-worker Deployment carries OPERATIONAL_ORDERING_CONTRACT = "2", whether
// the chart value is its default or is left empty: the template's fallback is
// contract 2 (contract 1 is unsupported and every Go reader refuses a value other
// than 2). The migrate Job carries it too (CHAOS-7421).
func TestEveryGoWorkerAndTheMigrateJobCarryOrderingContract2(t *testing.T) {
	for name, sets := range map[string][]string{
		"chart default":      {"--set", "goWorkers.enabled=true"},
		"value left empty":   {"--set", "goWorkers.enabled=true", "--set", "goWorkers.operationalOrderingContract="},
		"value explicitly 2": {"--set", "goWorkers.enabled=true", "--set", "goWorkers.operationalOrderingContract=2"},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := exec.Command("helm", append([]string{"template", "ordering-test", "."}, sets...)...).CombinedOutput()
			if err != nil {
				t.Fatalf("render failed: %v\n%s", err, output)
			}
			workers := 0
			for _, document := range strings.Split(string(output), "\n---") {
				if !strings.Contains(document, "\nkind: Deployment\n") || !strings.Contains(document, "-dev-health-go-") {
					continue
				}
				if !strings.Contains(document, "OPERATIONAL_ORDERING_CONTRACT") {
					continue
				}
				workers++
				matches := orderingContractValue.FindAllStringSubmatch(document, -1)
				if len(matches) == 0 {
					t.Fatalf("a go-worker Deployment names the variable without a literal value:\n%s", document)
				}
				for _, match := range matches {
					if match[1] != "2" {
						t.Fatalf("a go-worker Deployment renders OPERATIONAL_ORDERING_CONTRACT=%q, want \"2\"", match[1])
					}
				}
			}
			if workers < 4 {
				t.Fatalf("found %d go-worker Deployments with the variable, want at least the four queue groups", workers)
			}
			// The migrate Job: its OWN value must be "2" (the Job's template falls back to "2"
			// too, so an empty chart value cannot leave it refused by dho).
			job := ""
			for _, document := range strings.Split(string(output), "\n---") {
				if strings.Contains(document, "\nkind: Job\n") && strings.Contains(document, "OPERATIONAL_ORDERING_CONTRACT") && strings.Contains(document, "migrate") {
					job = document
				}
			}
			if job == "" {
				t.Fatal("no migrate Job carrying the variable in the render")
			}
			matches := orderingContractValue.FindAllStringSubmatch(job, -1)
			if len(matches) != 1 || matches[0][1] != "2" {
				t.Fatalf("the migrate Job renders %v, want exactly one value \"2\"", matches)
			}
		})
	}
}

// The Python api and metrics-api carry OPERATIONAL_ORDERING_CONTRACT = "2" too: Python reads an unset
// value as contract 1 (r1 of CHAOS-7421 executed a stale read after a tombstone against a contract-2 table
// with it unset). Red when the api Deployment lacks it.
func TestPythonApiDeploymentsCarryOrderingContract2(t *testing.T) {
	for name, sets := range map[string][]string{
		"chart default":    {"--set", "metricsApi.enabled=true"},
		"value left empty": {"--set", "metricsApi.enabled=true", "--set", "goWorkers.operationalOrderingContract="},
	} {
		t.Run(name, func(t *testing.T) {
			output, err := exec.Command("helm", append([]string{"template", "pyapi-test", "."}, sets...)...).CombinedOutput()
			if err != nil {
				t.Fatalf("render failed: %v\n%s", err, output)
			}
			for _, deployment := range []string{"pyapi-test-dev-health-api", "pyapi-test-dev-health-metrics-api"} {
				document := deploymentOf(t, string(output), deployment)
				matches := orderingContractValue.FindAllStringSubmatch(document, -1)
				if len(matches) != 1 || matches[0][1] != "2" {
					t.Fatalf("%s renders %v, want exactly one OPERATIONAL_ORDERING_CONTRACT \"2\":\n%s", deployment, matches, document)
				}
			}
		})
	}
}
