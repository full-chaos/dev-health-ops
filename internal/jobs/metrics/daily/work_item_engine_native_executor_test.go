package daily

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemengine"
)

type nilableTypeNormalizer struct{}

func (*nilableTypeNormalizer) NormalizeType(_, typeRaw string, _ []string) string { return typeRaw }

type nilableClassifier struct{}

func (*nilableClassifier) Classify(workitemengine.InvestmentArtifact) (workitemengine.InvestmentClassification, error) {
	return workitemengine.InvestmentClassification{}, nil
}

// TestWorkItemEngineExecutorsRefuseAMissingConnectionOrEngine: each of the two
// families fails closed at construction, so the worker reports it refused and
// fails its start. A typed nil engine (a nil *StatusMapping in an interface)
// must be refused too: a plain nil check would accept it and the family would
// panic on its first item.
func TestWorkItemEngineExecutorsRefuseAMissingConnectionOrEngine(t *testing.T) {
	conn := &workItemSendFailingConn{}
	var noNormalizer *nilableTypeNormalizer
	var noClassifier *nilableClassifier

	if _, err := NewWorkItemIssueTypeExecutor(nil, &nilableTypeNormalizer{}); !errors.Is(err, errWorkItemIssueTypeUnavailable) {
		t.Errorf("issue type, nil connection: err = %v", err)
	}
	if _, err := NewWorkItemIssueTypeExecutor(conn, nil); !errors.Is(err, errWorkItemIssueTypeUnavailable) {
		t.Errorf("issue type, nil engine: err = %v", err)
	}
	if _, err := NewWorkItemIssueTypeExecutor(conn, noNormalizer); !errors.Is(err, errWorkItemIssueTypeUnavailable) {
		t.Errorf("issue type, typed nil engine: err = %v", err)
	}
	if executor, err := NewWorkItemIssueTypeExecutor(conn, &nilableTypeNormalizer{}); err != nil || executor == nil {
		t.Errorf("issue type, connection and engine: executor = %v, err = %v", executor, err)
	}

	if _, err := NewWorkItemInvestmentExecutor(nil, &nilableClassifier{}); !errors.Is(err, errWorkItemInvestmentUnavailable) {
		t.Errorf("investment, nil connection: err = %v", err)
	}
	if _, err := NewWorkItemInvestmentExecutor(conn, nil); !errors.Is(err, errWorkItemInvestmentUnavailable) {
		t.Errorf("investment, nil engine: err = %v", err)
	}
	if _, err := NewWorkItemInvestmentExecutor(conn, noClassifier); !errors.Is(err, errWorkItemInvestmentUnavailable) {
		t.Errorf("investment, typed nil engine: err = %v", err)
	}
	if executor, err := NewWorkItemInvestmentExecutor(conn, &nilableClassifier{}); err != nil || executor == nil {
		t.Errorf("investment, connection and engine: executor = %v, err = %v", executor, err)
	}

	// A run with no organization or day is a state error, not a write.
	issueType, _ := NewWorkItemIssueTypeExecutor(conn, &nilableTypeNormalizer{})
	investment, _ := NewWorkItemInvestmentExecutor(conn, &nilableClassifier{})
	for name, executor := range map[string]NativeFamilyExecutor{
		WorkItemIssueTypeFamilyName: issueType, WorkItemInvestmentFamilyName: investment,
	} {
		if _, err := executor.ComputeFamily(context.Background(), Run{}, Partition{ID: "p"}); !errors.Is(err, ErrInvalidState) {
			t.Errorf("%s with an empty run: err = %v, want ErrInvalidState", name, err)
		}
		if _, err := executor.ComputeFamily(context.Background(),
			Run{OrganizationID: "org", TargetDay: time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)},
			Partition{ID: "p", RepoIDs: []RepositoryID{"not-a-uuid"}}); !errors.Is(err, ErrInvalidState) {
			t.Errorf("%s with a repository id that is no UUID: err = %v, want ErrInvalidState", name, err)
		}
	}
}
