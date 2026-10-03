package syncbudget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
)

// Frozen-Python differential oracle for the in-process budget estimator
// (CHAOS-6243). testdata/budget_estimate_oracle.py generates every case,
// runs the REAL production functions (estimate_provider_budget over a real
// SyncTaskContext, credential_fingerprint, _resolve_env_credentials,
// _credential_mapping over ciphertext it encrypted itself) and this test
// runs the Go port on the same input bytes.
//
// The answers are the script executed once on the pinned build and frozen in
// testdata/golden; no Python runs.

const oracleIntegrationID = "00000000-0000-4000-8000-000000000002"

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
			Config     *string `json:"config"`
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
	PagerDutyExchange []struct {
		Input struct {
			Descriptor string `json:"descriptor"`
			Status     int    `json:"status"`
			Body       string `json:"body"`
			Label      string `json:"label"`
		} `json:"input"`
		Python struct {
			Outcome  string `json:"outcome"`
			Requests []struct {
				Method string      `json:"method"`
				URL    string      `json:"url"`
				Form   [][2]string `json:"form"`
			} `json:"requests"`
		} `json:"python"`
	} `json:"pagerduty_exchange"`
	JSONValues []struct {
		Input  string `json:"input"`
		Python struct {
			Error      string `json:"error"`
			Repr       string `json:"repr"`
			Dumps      string `json:"dumps"`
			Dict       string `json:"dict"`
			DictError  string `json:"dict_error"`
			Flags      string `json:"flags"`
			FlagsError string `json:"flags_error"`
		} `json:"python"`
	} `json:"json_values"`
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

func TestBudgetEstimatorMatchesFrozenPython(t *testing.T) {
	script, err := os.ReadFile(filepath.Join("testdata", "budget_estimate_oracle.py"))
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(frozenPython(t, "budget-estimate.golden.json",
		programoracle.Script("budget estimate oracle", path.Join("internal", "syncbudget", "testdata", "budget_estimate_oracle.py"), string(script), nil))[0])
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
			if len(oracleCase.Input.Env) == 0 {
				// An empty environment is a nil Getenv: Python reads an
				// os.environ the script cleared.
				if nilEnv := goEstimateResultWithGetenv(t, oracleCase.Input, true); nilEnv != got {
					t.Errorf("case %d: nil Getenv answers %s, empty environment %s", index, nilEnv, got)
				}
			}
			if name := declaredDivergence(oracleCase.Input, oracleCase.Python); name != "" {
				if got == want {
					t.Errorf("case %d: declared divergence %q no longer diverges: %s", index, name, got)
				}
				// The decided behaviour is a refusal, not a wrong number.
				if got != `{"error":true}` {
					t.Errorf("case %d: declared divergence %q must be Go's refusal, got %s", index, name, got)
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
			if len(env) == 0 {
				// An empty environment is a Loader with no Getenv.
				if got, want := pyRepr(Loader{}.environmentCredentials(oracleCase.Input.Provider)), pyRepr(wantValue); got != want {
					t.Errorf("case %d %s: python %s, go without Getenv %s", index, oracleCase.Input.Provider, want, got)
				}
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
			mapping, err := loader.credentialMapping(oracleCase.Input.Ciphertext, oracleCase.Input.Config)
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

	t.Run("pagerduty_exchange", func(t *testing.T) {
		if len(output.PagerDutyExchange) == 0 {
			t.Fatal("no PagerDuty exchange cases")
		}
		declared := map[string]int{}
		for index, oracleCase := range output.PagerDutyExchange {
			descriptor, err := decodeJSON([]byte(oracleCase.Input.Descriptor))
			if err != nil {
				t.Fatalf("case %d: decode: %v", index, err)
			}
			doer := &recordedExchangeDoer{status: oracleCase.Input.Status, body: oracleCase.Input.Body}
			loader := Loader{PagerDutyDoer: fakehttp.Client(doer)}
			goErr := loader.hydratePagerDutyOnce(context.Background(), "org", "credential", descriptor.(*object))
			label := fmt.Sprintf("case %d %s status=%d body=%s", index, oracleCase.Input.Descriptor, oracleCase.Input.Status, oracleCase.Input.Body)
			same := (oracleCase.Python.Outcome == "ok") == (goErr == nil) && len(doer.requests) == len(oracleCase.Python.Requests)
			if same {
				for position, want := range oracleCase.Python.Requests {
					got := doer.requests[position]
					if got.method != want.Method || got.url != want.URL || fmt.Sprint(got.form) != fmt.Sprint(want.Form) {
						same = false
					}
				}
			}
			if class := oracleCase.Input.Label; class != "" {
				// A declared divergence (CHAOS-8388): the recorded Python answer
				// is frozen in the golden, Go's own answer is pinned here, and
				// either one moving turns this red.
				declared[class]++
				if same {
					t.Errorf("%s: declared divergence %q no longer diverges", label, class)
				}
				if got, want := goExchangeSummary(goErr, doer), declaredExchangeGo[class]; got != want {
					t.Errorf("%s: declared divergence %q pins go %q, got %q", label, class, want, got)
				}
				continue
			}
			if !same {
				t.Errorf("%s: python %s %v, go %v %+v", label, oracleCase.Python.Outcome, oracleCase.Python.Requests, goErr, doer.requests)
			}
		}
		for class := range declaredExchangeGo {
			if declared[class] == 0 {
				t.Errorf("declared divergence %q has no case", class)
			}
		}
	})

	t.Run("json_values", func(t *testing.T) {
		if len(output.JSONValues) < 5000 {
			t.Fatalf("oracle produced too few JSON cases: %d", len(output.JSONValues))
		}
		accepted := 0
		nanSeen := false
		for index, oracleCase := range output.JSONValues {
			value, err := decodeJSON([]byte(oracleCase.Input))
			if (oracleCase.Python.Error != "") != (err != nil) {
				t.Errorf("case %d %q: python error %q, go %v", index, oracleCase.Input, oracleCase.Python.Error, err)
				continue
			}
			if err != nil {
				continue
			}
			accepted++
			declaredDict := false
			if oracleCase.Input == nanKeyDivergenceInput {
				// Declared divergence (CHAOS-8387): json.loads hands Python one
				// shared NaN object, so dict() keeps one NaN key; Go keeps
				// each. The recorded Python answer is in the golden, Go's own
				// answer is pinned here.
				entries, _ := dictEntries(value)
				parts := make([]string, len(entries))
				for position, entry := range entries {
					parts[position] = pyRepr(entry.key) + ": " + pyRepr(entry.value)
				}
				got := "{" + strings.Join(parts, ", ") + "}"
				if got == oracleCase.Python.Dict {
					t.Errorf("declared divergence %q no longer diverges: %s", oracleCase.Input, got)
				}
				if got != nanKeyDivergenceGo || oracleCase.Python.Dict != nanKeyDivergencePython {
					t.Errorf("declared divergence %q pins python %s / go %s, got python %s / go %s",
						oracleCase.Input, nanKeyDivergencePython, nanKeyDivergenceGo, oracleCase.Python.Dict, got)
				}
				nanSeen = true
				declaredDict = true
			}
			if got := pyRepr(value); got != oracleCase.Python.Repr {
				t.Errorf("case %d %q: repr python %s, go %s", index, oracleCase.Input, oracleCase.Python.Repr, got)
			}
			if got := string(dumpsSortedCompact(value)); got != oracleCase.Python.Dumps {
				t.Errorf("case %d %q: dumps python %s, go %s", index, oracleCase.Input, oracleCase.Python.Dumps, got)
			}
			entries, dictErr := dictEntries(value)
			if declaredDict {
				// pinned above
			} else if (oracleCase.Python.DictError != "") != (dictErr != nil) {
				t.Errorf("case %d %q: dict() python error %q, go %v", index, oracleCase.Input, oracleCase.Python.DictError, dictErr)
			} else if dictErr == nil {
				parts := make([]string, len(entries))
				for position, entry := range entries {
					parts[position] = pyRepr(entry.key) + ": " + pyRepr(entry.value)
				}
				if got := "{" + strings.Join(parts, ", ") + "}"; got != oracleCase.Python.Dict {
					t.Errorf("case %d %q: dict python %s, go %s", index, oracleCase.Input, oracleCase.Python.Dict, got)
				}
			}
			flags, flagsErr := processorFlags(value)
			if (oracleCase.Python.FlagsError != "") != (flagsErr != nil) {
				t.Errorf("case %d %q: flags python error %q, go %v", index, oracleCase.Input, oracleCase.Python.FlagsError, flagsErr)
			} else if flagsErr == nil {
				names := make([]string, 0, len(flags))
				for name := range flags {
					names = append(names, name)
				}
				sort.Strings(names)
				parts := make([]string, len(names))
				for position, name := range names {
					parts[position] = "(" + reprString(name) + ", " + pyRepr(flags[name]) + ")"
				}
				if got := "[" + strings.Join(parts, ", ") + "]"; got != oracleCase.Python.Flags {
					t.Errorf("case %d %q: flags python %s, go %s", index, oracleCase.Input, oracleCase.Python.Flags, got)
				}
			}
		}
		if !nanSeen {
			t.Error("declared divergence of the repeated NaN dict key has no case")
		}
		if accepted == 0 {
			t.Error("no JSON text was accepted: the comparison would pass on errors alone")
		}
		t.Logf("json cases: %d, accepted: %d", len(output.JSONValues), accepted)
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
	return goEstimateResultWithGetenv(t, input, false)
}

// goEstimateResultWithGetenv is goEstimateResult; noGetenv leaves the
// Getenv hook unset.
func goEstimateResultWithGetenv(t *testing.T, input oracleEstimateInput, noGetenv bool) string {
	t.Helper()
	context, err := oracleContext(input)
	if noGetenv {
		context.Getenv = nil
	}
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

// recordedExchangeDoer answers a PagerDuty token exchange with a fixed status
// and body and records each request it receives: the Go half of the fake
// endpoint the Python oracle stood where identity.pagerduty.com does.
type recordedExchangeDoer struct {
	status   int
	body     string
	requests []recordedExchange
}

type recordedExchange struct {
	method, url string
	form        [][2]string
}

func (d *recordedExchangeDoer) Do(request *http.Request) (*http.Response, error) {
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	values, err := url.ParseQuery(string(raw))
	if err != nil {
		return nil, err
	}
	var form [][2]string
	for name, list := range values {
		for _, value := range list {
			form = append(form, [2]string{name, value})
		}
	}
	sort.Slice(form, func(i, j int) bool {
		if form[i][0] != form[j][0] {
			return form[i][0] < form[j][0]
		}
		return form[i][1] < form[j][1]
	})
	d.requests = append(d.requests, recordedExchange{method: request.Method, url: request.URL.String(), form: form})
	return &http.Response{
		StatusCode: d.status, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(d.body)), Request: request,
	}, nil
}

// declaredExchangeGo pins Go's answer for each declared divergence of the
// PagerDuty client-credentials exchange: scope-missing and redirect-301 are
// the two classes where Go is looser than the recorded Python answer (Go
// must refuse a token response without the required read scopes and a
// non-2xx status; a Go fix, not a recording, flips them); the others are
// malformed input Python tolerates and Go refuses or defaults.
var declaredExchangeGo = map[string]string{
	"scope-missing":    "ok requests=1 region=us",
	"redirect-301":     "ok requests=1 region=us",
	"token-not-string": "fail requests=1 region=us",
	"expires-not-int":  "fail requests=1 region=us",
	"region-empty":     "ok requests=1 region=us",
	"credential-empty": "fail requests=0 region=-",
}

func goExchangeSummary(err error, doer *recordedExchangeDoer) string {
	outcome := "ok"
	if err != nil {
		outcome = "fail"
	}
	region := "-"
	if len(doer.requests) > 0 {
		for _, pair := range doer.requests[0].form {
			if pair[0] == "region" {
				region = pair[1]
			}
		}
	}
	return fmt.Sprintf("%s requests=%d region=%s", outcome, len(doer.requests), region)
}

const (
	nanKeyDivergenceInput  = "[[NaN, 1], [NaN, 2]]"
	nanKeyDivergencePython = "{nan: 2}"
	nanKeyDivergenceGo     = "{nan: 1, nan: 2}"
)
