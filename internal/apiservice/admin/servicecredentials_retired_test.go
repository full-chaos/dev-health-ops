package admin

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestGenerateServiceTokenRefusesWorkerOperator(t *testing.T) {
	token, err := generateServiceToken(ServiceWorkerOperator)
	var refusal *ServiceCredentialError
	if !errors.As(err, &refusal) || token != "" {
		t.Fatalf("token %q err %v: want a refusal and no token", token, err)
	}
	for _, service := range []string{ServiceACR, ServiceAuthSessionIssuer} {
		if _, err := generateServiceToken(service); err != nil {
			t.Errorf("%s: %v", service, err)
		}
	}
}

func TestServiceMintRetiredIsWorkerOperatorOnly(t *testing.T) {
	for service, want := range map[string]bool{ServiceWorkerOperator: true, ServiceACR: false, ServiceAuthSessionIssuer: false} {
		if got := ServiceMintRetired(service); got != want {
			t.Errorf("ServiceMintRetired(%q) = %v, want %v", service, got, want)
		}
	}
	found := false
	for _, name := range ServiceNames() {
		found = found || name == ServiceWorkerOperator
	}
	if !found {
		t.Error("worker-operator left ServiceNames: list and revoke still need the name")
	}
}

// A zero Operator has no pool: a mint that reached for a transaction would panic.
func TestWorkerOperatorMintOpensNoTransaction(t *testing.T) {
	spec := ServiceCredentialSpec{Service: ServiceWorkerOperator, Scopes: []string{"workers:read"}}
	var refusal *ServiceCredentialError
	if _, err := (Operator{}).IssueServiceCredential(context.Background(), spec); !errors.As(err, &refusal) {
		t.Fatalf("issue: %v", err)
	}
	if _, err := (Operator{}).RotateServiceCredential(context.Background(), uuid.New(), 0, spec); !errors.As(err, &refusal) {
		t.Fatalf("rotate: %v", err)
	}
}
