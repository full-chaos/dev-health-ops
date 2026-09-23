// Package syncbudget is the in-process port of the Python sync budget
// estimator: estimate_provider_budget (src/dev_health_ops/sync/budget.py)
// and the six per-provider estimator classes it dispatches to
// (src/dev_health_ops/providers/{github,gitlab,jira,linear,pagerduty,
// launchdarkly}/budget.py). The Go sync dispatcher used to reach them over
// the /api/internal/worker-sync/dispatch-budget-estimate bridge; it now
// calls Loader.EstimateUnits in-process (CHAOS-6243).
//
// The Python estimators stay in the tree as the reference: the live-Python
// differential oracle in internal/providersync
// (sync_budget_estimate_oracle_test.go) runs both on the same inputs.
package syncbudget

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errEstimateOutOfRange is an estimate past int64. Python computes it exactly;
// the Go caller has no integer type for it, so the unit fails its estimate.
var errEstimateOutOfRange = errors.New("budget estimate exceeds int64")

// Dimension values are BudgetDimension (sync/budget_types.py).
const (
	DimensionRESTCore           = "rest_core"
	DimensionGraphQLCost        = "graphql_cost"
	DimensionContentsBlob       = "contents_blob"
	DimensionSearch             = "search"
	DimensionSecondaryAbuseRisk = "secondary_abuse_risk"
)

const (
	confidenceHigh   = "high"
	confidenceMedium = "medium"
	confidenceLow    = "low"
)

// Bucket is BudgetBucketKey.
type Bucket struct {
	Provider              string
	OrgID                 string
	Host                  string
	CredentialFingerprint string
	Dimension             string
}

// Estimate is BudgetEstimate. Notes is never nil.
type Estimate struct {
	Bucket         Bucket
	EstimatedUnits int
	Confidence     string
	RouteFamily    string
	Notes          []string
}

// Context holds the SyncTaskContext fields the estimators read.
type Context struct {
	Provider      string
	DatasetKey    string
	OrgID         string
	IntegrationID string
	// CredentialID is nil where Python's context.credential_id is None
	// (environment credentials).
	CredentialID *string
	// Credentials is the decrypted credential mapping, as decodeJSON builds
	// it (a *object for a JSON object). Anything else is Python's
	// "not a Mapping".
	Credentials    any
	ProcessorFlags map[string]bool
	WindowStart    *time.Time
	WindowEnd      *time.Time
	DatasetOptions *object
	// Getenv is os.getenv for the Jira estimator's four environment reads.
	Getenv func(string) string
}

// EstimateProviderBudget is estimate_provider_budget: the provider's
// estimates, or none for a provider without an estimator. An error is an
// exception Python's estimator raises (only urlparse can raise here).
func EstimateProviderBudget(context Context) ([]Estimate, error) {
	switch pythonparity.Lower(context.Provider) {
	case "github":
		return estimateGitHub(context)
	case "gitlab":
		return estimateGitLab(context)
	case "jira":
		return estimateJira(context)
	case "linear":
		return estimateLinear(context)
	case "pagerduty":
		return estimatePagerDuty(context)
	case "launchdarkly":
		return estimateLaunchDarkly(context)
	}
	return []Estimate{}, nil
}

func newEstimate(bucket Bucket, units int, confidence, routeFamily string, notes ...string) Estimate {
	if notes == nil {
		notes = []string{}
	}
	return Estimate{Bucket: bucket, EstimatedUnits: units, Confidence: confidence, RouteFamily: routeFamily, Notes: notes}
}

type bucketFactory func(dimension string) Bucket

func bucketsFor(provider string, context Context, host, fingerprint string) bucketFactory {
	return func(dimension string) Bucket {
		return Bucket{Provider: provider, OrgID: context.OrgID, Host: host, CredentialFingerprint: fingerprint, Dimension: dimension}
	}
}

// windowSpanDays is budget_types.window_span_days: whole days between the
// window bounds (timedelta.days floors), at least 1, and 1 when a bound is
// missing.
func windowSpanDays(context Context) int {
	if context.WindowStart == nil || context.WindowEnd == nil {
		return 1
	}
	delta := context.WindowEnd.Sub(*context.WindowStart)
	days := int(delta / (24 * time.Hour))
	if delta < 0 && delta%(24*time.Hour) != 0 {
		days--
	}
	return max(1, days)
}

// scaledUnits is the github/gitlab/jira _scaled_units.
func scaledUnits(fixedFloor, spanDays int) int {
	return max(fixedFloor, fixedFloor*max(1, spanDays))
}

// sha256Hex is hashlib.sha256(text.encode("utf-8")).hexdigest(): the
// strict UTF-8 encode raises on a lone surrogate, which a JSON \uD800
// escape can put in a Python str (see hasSurrogate).
func sha256Hex(text string) (string, error) {
	if hasSurrogate(text) {
		return "", errUnicodeEncode
	}
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:]), nil
}

// fingerprintOf is each estimator's _credential_fingerprint over its safe
// scope.
func fingerprintOf(scope *object) string {
	sum := sha256.Sum256(dumpsSortedCompact(scope))
	return hex.EncodeToString(sum[:])
}

func fallbackScope(context Context) *object {
	scope := newObject()
	credentialID := "env"
	if context.CredentialID != nil && *context.CredentialID != "" {
		credentialID = *context.CredentialID
	}
	scope.set("credential_id", credentialID)
	scope.set("integration_id", context.IntegrationID)
	return scope
}

// credentialMapping is isinstance(credentials, Mapping).
func credentialMapping(context Context) (*object, bool) {
	mapping, ok := context.Credentials.(*object)
	return mapping, ok && mapping != nil
}

// hostFrom resolves `urlparse(base_url).hostname or default` where base_url
// is str(first truthy of the keys) when one is truthy, else fallback.
func hostFrom(context Context, fallback, defaultHost string, keys ...string) (string, error) {
	baseURL := fallback
	if mapping, ok := credentialMapping(context); ok {
		values := make([]any, len(keys))
		for index, key := range keys {
			values[index] = get(mapping, key)
		}
		if raw := firstTruthy(values...); truthy(raw) {
			baseURL = pyStr(raw)
		}
	}
	host, ok, err := urlHostname(baseURL)
	if err != nil {
		return "", err
	}
	if !ok {
		return defaultHost, nil
	}
	return host, nil
}

// copyPresent copies each key whose value is not None.
func copyPresent(scope, mapping *object, keys ...string) {
	for _, key := range keys {
		if value := get(mapping, key); value != nil {
			scope.set(key, value)
		}
	}
}

// baseURLOf is `credentials.get("base_url") or credentials.get("baseUrl")`.
func baseURLOf(mapping *object) any {
	return firstTruthy(get(mapping, "base_url"), get(mapping, "baseUrl"))
}

func flagEnabled(context Context, name string, defaultValue bool) bool {
	value, ok := context.ProcessorFlags[name]
	if !ok {
		return defaultValue
	}
	return value
}
