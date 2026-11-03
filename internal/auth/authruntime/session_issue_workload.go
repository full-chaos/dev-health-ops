package authruntime

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// sessionIssueWorkload is the session issue route's workload-token path: built
// only when AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT is set (nil otherwise, so the
// route refuses every workload token), over the signer's published key set.
func sessionIssueWorkload(cfg authconfig.Config, signer authapi.AccessSigner) (*authapi.SessionIssueWorkload, error) {
	if cfg.SessionIssueWorkloadSubject == "" {
		return nil, nil
	}
	keys, ok := signer.(authapi.KeySetSource)
	if !ok {
		return nil, fmt.Errorf("authruntime: %s needs a signer that reads the key set", authconfig.EnvSessionIssueWorkloadSubject)
	}
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{
		IssuerURL: cfg.TokenIssuerURL, Keys: keys, Subject: cfg.SessionIssueWorkloadSubject,
	})
	if err != nil {
		return nil, fmt.Errorf("authruntime: %s: %w", authconfig.EnvSessionIssueWorkloadSubject, err)
	}
	return workload, nil
}
