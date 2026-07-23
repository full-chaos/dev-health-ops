package providersync

import (
	"encoding/json"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

type workItemContractVariant struct {
	SyncPRs             bool  `json:"sync_prs"`
	IncludeIssues       *bool `json:"include_issues"`
	IncludePullRequests *bool `json:"include_pull_requests"`
	HasFetchComments    bool  `json:"has_fetch_comments"`
	HasFetchMilestones  bool  `json:"has_fetch_milestones"`
}

type workItemContractEntry struct {
	Variants []workItemContractVariant `json:"variants"`
}

func TestWorkItemCompatibilityContractComesFromLivePythonAdapter(t *testing.T) {
	python := pythonExecutable(t)
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	adapterSource := filepath.Join(
		packageDir, "..", "..", "src", "dev_health_ops", "processors", "dataset_adapters.py",
	)
	oracleScript := filepath.Join(packageDir, "testdata", "python_work_item_contract_oracle.py")
	output, err := exec.Command(python, oracleScript, adapterSource).CombinedOutput()
	if err != nil {
		t.Fatalf("execute Python work-item oracle: %v: %s", err, output)
	}
	var contract map[string]map[string]workItemContractEntry
	if err := json.Unmarshal(output, &contract); err != nil {
		t.Fatalf("decode Python work-item oracle: %v: %s", err, output)
	}
	for _, provider := range []string{"github", "gitlab"} {
		for _, dataset := range []string{
			"work-items",
			"work-item-labels",
			"work-item-projects",
			"work-item-history",
			"work-item-comments",
		} {
			entry, ok := contract[provider][dataset]
			if !ok || len(entry.Variants) != 2 {
				t.Fatalf("%s/%s entry=%+v", provider, dataset, entry)
			}
			for index, variant := range entry.Variants {
				if variant.HasFetchComments || variant.HasFetchMilestones {
					t.Fatalf("%s/%s unexpectedly partitions full work-item job: %+v", provider, dataset, variant)
				}
				if provider == "github" {
					if variant.IncludeIssues == nil || !*variant.IncludeIssues ||
						variant.IncludePullRequests == nil ||
						*variant.IncludePullRequests != (index == 1) {
						t.Fatalf("%s/%s variant=%+v", provider, dataset, variant)
					}
				} else if variant.IncludeIssues != nil || variant.IncludePullRequests != nil {
					t.Fatalf("%s/%s GitLab flags drifted: %+v", provider, dataset, variant)
				}
			}
		}
	}
}
