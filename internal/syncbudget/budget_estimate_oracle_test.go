package syncbudget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
)

// Live-Python differential oracle for the in-process budget estimator
// (CHAOS-6243). testdata/budget_estimate_oracle.py generates every case,
// runs the REAL production functions (estimate_provider_budget over a real
// SyncTaskContext, credential_fingerprint, _resolve_env_credentials,
// _credential_mapping over ciphertext it encrypted itself) and this test
// runs the Go port on the same input bytes.
//
// Gated like every live oracle: it runs only through
// `ci/check_go.sh live-python-oracles`, which forces -count=1 (the Python
// sources are outside the Go test cache key) and checks the proof file this
// test writes, so a skipped run fails the gate instead of reading as a pass.

const (
	livePythonOraclesEnv     = "DEV_HEALTH_LIVE_PYTHON_ORACLES"
	livePythonOracleProofDir = "DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR"
	budgetOracleProofFile    = "sync-budget-estimate"
	oracleIntegrationID      = "00000000-0000-4000-8000-000000000002"
)

type oracleEstimateInput struct {
	Provider       string            `json:"provider"`
	DatasetKey     string            `json:"dataset_key"`
	CredentialID   *string           `json:"credential_id"`
	Credentials    string            `json:"credentials"`
	ProcessorFlags string            `json:"processor_flags"`
	WindowStart    *string           `json:"window_start"`
	WindowEnd      *string           `json:"window_end"`
	DatasetOptions string            `json:"dataset_options"`
	Env            map[string]string `json:"env"`
}

type oracleOutput struct {
	SettingsEncryptionKey string `json:"settings_encryption_key"`
	Estimate              []struct {
		Input  oracleEstimateInput `json:"input"`
		Python json.RawMessage     `json:"python"`
	} `json:"estimate"`
	Fingerprint []struct {
		Input struct {
			Credentials  string  `json:"credentials"`
			CredentialID *string `json:"credential_id"`
		} `json:"input"`
		Python struct {
			Fingerprint string `json:"fingerprint"`
			Error       string `json:"error"`
		} `json:"python"`
	} `json:"fingerprint"`
	EnvCredentials []struct {
		Input struct {
			Provider string            `json:"provider"`
			Env      map[string]string `json:"env"`
		} `json:"input"`
		Python json.RawMessage `json:"python"`
	} `json:"env_credentials"`
	CredentialMapping []struct {
		Input struct {
			Ciphertext *string `json:"ciphertext"`
			Config     string  `json:"config"`
		} `json:"input"`
		Python struct {
			Repr  *string `json:"repr"`
			Error string  `json:"error"`
		} `json:"python"`
	} `json:"credential_mapping"`
	PagerDutyHydration []struct {
		Input struct {
			Descriptor string            `json:"descriptor"`
			Env        map[string]string `json:"env"`
		} `json:"input"`
		Python string `json:"python"`
	} `json:"pagerduty_hydration"`
}

// declaredDivergence names the inputs where the Go port is known and
// decided to differ. Each one must STILL differ (checked below), so a later
// change that closes or moves it is noticed instead of passing quietly.
func declaredDivergence(_ oracleEstimateInput, python json.RawMessage) string {
	var result struct {
		Estimates []struct {
			EstimatedUnits json.Number `json:"estimated_units"`
		} `json:"estimates"`
	}
	if json.Unmarshal(python, &result) == nil {
		for _, estimate := range result.Estimates {
			if _, err := strconv.ParseInt(estimate.EstimatedUnits.String(), 10, 64); err != nil {
				// An estimate past int64 (a huge PagerDuty enrichment_cap):
				// Python computes it, Go has no int for it and fails the
				// unit. The old bridge client failed to decode it too.
				return "estimate-past-int64"
			}
		}
	}
	return ""
}

func TestBudgetEstimatorMatchesLivePython(t *testing.T) {
	if os.Getenv(livePythonOraclesEnv) != "1" {
		t.Skip("live Python oracles run only through ci/check_go.sh live-python-oracles")
	}
	proofDir := os.Getenv(livePythonOracleProofDir)
	if proofDir == "" {
		t.Fatal("live Python oracle opt-in requires a proof directory from ci/check_go.sh")
	}
	_, currentFile, _, _ := runtime.Caller(0)
	packageDir := filepath.Dir(currentFile)
	root := filepath.Dir(filepath.Dir(packageDir))
	python := pyoracle.Resolve(t, root)
	command := exec.Command(python, filepath.Join(packageDir, "testdata", "budget_estimate_oracle.py"))
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	var stderr bytes.Buffer
	command.Stderr = &stderr
	raw, err := command.Output()
	if err != nil {
		t.Fatal(pyoracle.RunError(python, err, stderr.Bytes()))
	}
	var output oracleOutput
	if err := json.Unmarshal(raw, &output); err != nil {
		t.Fatalf("decode oracle output: %v", err)
	}
	if len(output.Estimate) < 8000 || len(output.Fingerprint) == 0 ||
		len(output.EnvCredentials) == 0 || len(output.CredentialMapping) == 0 {
		t.Fatalf("oracle produced too few cases: estimate=%d fingerprint=%d env=%d mapping=%d",
			len(output.Estimate), len(output.Fingerprint), len(output.EnvCredentials), len(output.CredentialMapping))
	}

	t.Run("estimate", func(t *testing.T) {
		nonEmpty := map[string]int{}
		divergences := map[string]int{}
		failures := 0
		for index, oracleCase := range output.Estimate {
			want := canonicalPython(t, oracleCase.Python)
			got := goEstimateResult(t, oracleCase.Input)
			if name := declaredDivergence(oracleCase.Input, oracleCase.Python); name != "" {
				if got == want {
					t.Errorf("case %d: declared divergence %q no longer diverges: %s", index, name, got)
				}
				divergences[name]++
				continue
			}
			if got != want {
				failures++
				if failures <= 20 {
					t.Errorf("case %d %+v\n  python %s\n  go     %s", index, oracleCase.Input, want, got)
				}
				continue
			}
			if strings.Contains(want, `"estimated_units"`) {
				nonEmpty[strings.ToLower(oracleCase.Input.Provider)]++
			}
		}
		if failures > 0 {
			t.Fatalf("%d of %d estimate cases diverge", failures, len(output.Estimate))
		}
		for _, provider := range []string{"github", "gitlab", "jira", "linear", "pagerduty", "launchdarkly"} {
			if nonEmpty[provider] == 0 {
				t.Errorf("no case produced an estimate for %s: the comparison would pass on empty output", provider)
			}
		}
		for _, name := range []string{"estimate-past-int64"} {
			if divergences[name] == 0 {
				t.Errorf("declared divergence %q has no case", name)
			}
		}
		t.Logf("estimate cases: %d, with estimates per provider: %v, declared divergences: %v",
			len(output.Estimate), nonEmpty, divergences)
	})

	t.Run("fingerprint", func(t *testing.T) {
		for index, oracleCase := range output.Fingerprint {
			credentials, err := decodeJSON([]byte(oracleCase.Input.Credentials))
			if err != nil {
				t.Fatalf("case %d: decode: %v", index, err)
			}
			got, goErr := RunAuthFingerprint(credentials, oracleCase.Input.CredentialID, oracleIntegrationID)
			if (oracleCase.Python.Error != "") != (goErr != nil) || got != oracleCase.Python.Fingerprint {
				t.Errorf("case %d %s: python %q (%s), go %q", index, oracleCase.Input.Credentials,
					oracleCase.Python.Fingerprint, oracleCase.Python.Error, got)
			}
		}
	})

	t.Run("env_credentials", func(t *testing.T) {
		for index, oracleCase := range output.EnvCredentials {
			env := oracleCase.Input.Env
			loader := Loader{Getenv: func(name string) string { return env[name] }}
			wantValue, err := decodeJSON(oracleCase.Python)
			if err != nil {
				t.Fatalf("case %d: decode: %v", index, err)
			}
			if got, want := pyRepr(loader.environmentCredentials(oracleCase.Input.Provider)), pyRepr(wantValue); got != want {
				t.Errorf("case %d %s: python %s, go %s", index, oracleCase.Input.Provider, want, got)
			}
		}
	})

	t.Run("credential_mapping", func(t *testing.T) {
		decryptor, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(output.SettingsEncryptionKey), "")
		if err != nil {
			t.Fatal(err)
		}
		loader := Loader{Decryptor: decryptor}
		for index, oracleCase := range output.CredentialMapping {
			config := oracleCase.Input.Config
			mapping, err := loader.credentialMapping(oracleCase.Input.Ciphertext, &config)
			switch {
			case oracleCase.Python.Error != "" && err == nil:
				t.Errorf("case %d: python raised %s, go returned %s", index, oracleCase.Python.Error, pyRepr(mapping))
			case oracleCase.Python.Error == "" && err != nil:
				t.Errorf("case %d: python returned %s, go failed: %v", index, *oracleCase.Python.Repr, err)
			case err == nil && pyRepr(mapping) != *oracleCase.Python.Repr:
				t.Errorf("case %d: python %s, go %s", index, *oracleCase.Python.Repr, pyRepr(mapping))
			}
		}
	})

	t.Run("pagerduty_hydration", func(t *testing.T) {
		if len(output.PagerDutyHydration) == 0 {
			t.Fatal("no PagerDuty hydration cases")
		}
		for index, oracleCase := range output.PagerDutyHydration {
			descriptor, err := decodeJSON([]byte(oracleCase.Input.Descriptor))
			if err != nil {
				t.Fatalf("case %d: decode: %v", index, err)
			}
			env := oracleCase.Input.Env
			// No hydrator and no doer: every case here is decided before
			// either would run, so reaching one is itself a divergence.
			loader := Loader{Getenv: func(name string) string { return env[name] }}
			goErr := loader.hydratePagerDutyOnce(context.Background(), "org", "credential", descriptor.(*object))
			if errors.Is(goErr, ErrLoaderUnavailable) {
				t.Errorf("case %d %s: Go reached a hydrator Python never called", index, oracleCase.Input.Descriptor)
				continue
			}
			if (oracleCase.Python == "ok") != (goErr == nil) {
				t.Errorf("case %d %s env=%v: python %s, go %v", index, oracleCase.Input.Descriptor, env, oracleCase.Python, goErr)
			}
		}
	})

	if !t.Failed() {
		if err := os.WriteFile(filepath.Join(proofDir, budgetOracleProofFile), []byte("executed\n"), 0o600); err != nil {
			t.Fatalf("write live Python budget oracle proof: %v", err)
		}
	}
}

// canonicalPython re-encodes the Python result with sorted keys and literal
// numbers, so the comparison is the whole record and exact.
func canonicalPython(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode python result: %v", err)
	}
	if typed, ok := value.(map[string]any); ok {
		if _, isError := typed["error"]; isError {
			return `{"error":true}`
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// goEstimateResult runs the Go port on one case and renders it the way
// canonicalPython renders the Python result (BudgetEstimate.to_dict()).
func goEstimateResult(t *testing.T, input oracleEstimateInput) string {
	t.Helper()
	context, err := oracleContext(input)
	var estimates []Estimate
	if err == nil {
		estimates, err = EstimateProviderBudget(context)
	}
	if err != nil {
		return `{"error":true}`
	}
	rendered := make([]any, len(estimates))
	for index, estimate := range estimates {
		rendered[index] = map[string]any{
			"bucket": map[string]any{
				"provider": estimate.Bucket.Provider, "org_id": estimate.Bucket.OrgID,
				"host": estimate.Bucket.Host, "credential_fingerprint": estimate.Bucket.CredentialFingerprint,
				"dimension": estimate.Bucket.Dimension,
			},
			"estimated_units": json.Number(strconv.Itoa(estimate.EstimatedUnits)),
			"confidence":      estimate.Confidence,
			"route_family":    estimate.RouteFamily,
			"notes":           estimate.Notes,
		}
	}
	encoded, err := json.Marshal(map[string]any{"estimates": rendered})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

// oracleContext builds the Context the loader would, from the case's JSON
// text, with the same dict() and bool() normalisation.
func oracleContext(input oracleEstimateInput) (Context, error) {
	credentials, err := decodeJSON([]byte(input.Credentials))
	if err != nil {
		return Context{}, err
	}
	flagsValue, err := decodeJSON([]byte(input.ProcessorFlags))
	if err != nil {
		return Context{}, err
	}
	processorFlags, err := processorFlags(flagsValue)
	if err != nil {
		return Context{}, err
	}
	optionsValue, err := decodeJSON([]byte(input.DatasetOptions))
	if err != nil {
		return Context{}, err
	}
	options, err := dictOrEmpty(optionsValue)
	if err != nil {
		return Context{}, err
	}
	parse := func(value *string) (*time.Time, error) {
		if value == nil {
			return nil, nil
		}
		parsed, err := time.Parse(time.RFC3339, *value)
		if err != nil {
			return nil, fmt.Errorf("parse window %q: %w", *value, err)
		}
		return &parsed, nil
	}
	start, err := parse(input.WindowStart)
	if err != nil {
		return Context{}, err
	}
	end, err := parse(input.WindowEnd)
	if err != nil {
		return Context{}, err
	}
	env := input.Env
	return Context{
		Provider: input.Provider, DatasetKey: input.DatasetKey,
		OrgID: "00000000-0000-4000-8000-000000000001", IntegrationID: oracleIntegrationID,
		CredentialID: input.CredentialID, Credentials: credentials, ProcessorFlags: processorFlags,
		WindowStart: start, WindowEnd: end, DatasetOptions: options,
		Getenv: func(name string) string { return env[name] },
	}, nil
}
