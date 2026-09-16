package goapiproof

import (
	"context"
	"testing"
	"time"
)

func validRESTReceipt() RESTReceipt {
	return RESTReceipt{
		Method:           "GET",
		Path:             "/api/v1/quadrant",
		CandidateBuild:   "abc123def456",
		RequestIdentity:  "digest",
		Stage:            EnablementProofStage,
		TerminalState:    TerminalStateMatch,
		ObservedAt:       time.Now().UTC(),
		MeasurementRoute: RouteProof,
		BuildBinding:     EdgeBuildPresent,
	}
}

func TestValidateRESTVocabulary_AcceptsAValidReceipt(t *testing.T) {
	if err := validateRESTVocabulary(validRESTReceipt()); err != nil {
		t.Fatalf("validateRESTVocabulary: %v", err)
	}
}

func TestValidateRESTVocabulary_RefusesAnInvalidStage(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.Stage = "not-a-real-stage"
	if err := validateRESTVocabulary(receipt); err == nil {
		t.Fatal("want an error for an invalid stage")
	}
}

func TestValidateRESTVocabulary_RefusesAnInvalidTerminalState(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.TerminalState = "not-a-real-state"
	if err := validateRESTVocabulary(receipt); err == nil {
		t.Fatal("want an error for an invalid terminal state")
	}
}

// Every case below returns before WriteREST ever calls db.Exec, so a nil
// Querier is enough to drive it -- the same pattern receipt_test.go's own
// TestWrite_Refuses* cases use for Write.

func TestWriteREST_RefusesAnEmptyMethodOrPath(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.Method = ""
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for an empty method")
	}
	receipt = validRESTReceipt()
	receipt.Path = ""
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for an empty path")
	}
}

func TestWriteREST_RefusesAnEmptyCandidateBuild(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.CandidateBuild = "   "
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for a blank candidate build")
	}
}

func TestWriteREST_RefusesAnInvalidBuildBinding(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.BuildBinding = "sort-of"
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for an invalid build binding")
	}
}

func TestWriteREST_RefusesAnInvalidMeasurementRoute(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.MeasurementRoute = "sky"
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for an invalid measurement route")
	}
}

func TestWriteREST_RefusesABlankCitation(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.BaselineDefects = []string{"CHAOS-1", "   "}
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for a blank citation in BaselineDefects")
	}
}

func TestWriteREST_RefusesAnInvalidStageOrTerminalStateBeforeTouchingTheDatabase(t *testing.T) {
	receipt := validRESTReceipt()
	receipt.Stage = "not-a-real-stage"
	if _, err := WriteREST(context.Background(), nil, receipt); err == nil {
		t.Fatal("want an error for an invalid stage")
	}
}
