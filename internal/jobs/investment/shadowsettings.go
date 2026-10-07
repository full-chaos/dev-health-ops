package investment

// shadowsettings.go holds the switches of the investment shadow phase
// (CHAOS-8869). Everything is OFF by default: with no INVESTMENT_SHADOW_PROVIDER
// no client is built and no request is possible.

import (
	"fmt"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
)

// Names of the environment variables of the shadow phase.
const (
	EnvShadowProvider      = "INVESTMENT_SHADOW_PROVIDER"
	EnvShadowOrgIDs        = "INVESTMENT_SHADOW_ORG_IDS"
	EnvShadowSamplePercent = "INVESTMENT_SHADOW_SAMPLE_PERCENT"
	EnvShadowConcurrency   = "INVESTMENT_SHADOW_CONCURRENCY"
	EnvShadowMaxSeconds    = "INVESTMENT_SHADOW_MAX_SECONDS"
	EnvShadowMaxUSDPerRun  = "INVESTMENT_SHADOW_MAX_USD_PER_RUN"
)

const (
	// DefaultShadowBudget is the time budget of one shadow phase.
	DefaultShadowBudget = 60 * time.Second
	// MaxShadowBudget is the hard clamp of the budget. The phase runs inside
	// the materialize request, before its completion fence: every second of it
	// delays the jobs that wait for the request, and a cancel inside it spends
	// one of the request's 9 claims. No configuration can lift this bound.
	MaxShadowBudget = 120 * time.Second

	defaultShadowConcurrency   = 4
	defaultShadowSamplePercent = 100
	// defaultShadowMaxNanoUSD is USD 0.50 for one run.
	defaultShadowMaxNanoUSD = 500_000_000
	// maxShadowMaxNanoUSD bounds the configured cap (USD 100 for one run), so a
	// typing mistake cannot remove the cap.
	maxShadowMaxNanoUSD = 100_000_000_000

	shadowAllOrgs = "*"
)

// ShadowSettings is the decoded configuration of the shadow phase.
type ShadowSettings struct {
	// Provider is "" (off) or "typesafe".
	Provider string
	// AllOrgs is the "*" entry of the org list.
	AllOrgs bool
	// OrgIDs is the org allow-list. Empty with AllOrgs false means no org: a
	// provider with no org list is OFF (it fails closed).
	OrgIDs map[string]struct{}
	// SamplePercent is 0..100: the share of work units asked.
	SamplePercent int
	// Concurrency is the number of parallel shadow requests, 1..maxLLMConcurrency.
	Concurrency int
	// Budget is the time budget of the phase, at most MaxShadowBudget.
	Budget time.Duration
	// MaxNanoUSD is the spend cap of one phase in 1e-9 USD.
	MaxNanoUSD int64
}

// ShadowSettingsFromEnv decodes the settings through lookup (production passes
// secrets.GetenvNamed). An unset provider is the zero value: off. A value that
// cannot be used is an error that names the variable and never its value; the
// caller then keeps the phase off, it does not fail the served run.
func ShadowSettingsFromEnv(lookup func(string) string) (ShadowSettings, error) {
	provider := strings.ToLower(strings.TrimSpace(lookup(EnvShadowProvider)))
	if provider == "" {
		return ShadowSettings{}, nil
	}
	if provider != string(categorize.ProviderKindTypeSafe) {
		return ShadowSettings{}, shadowSettingError(EnvShadowProvider)
	}
	settings := ShadowSettings{
		Provider: provider, OrgIDs: map[string]struct{}{},
		SamplePercent: defaultShadowSamplePercent, Concurrency: defaultShadowConcurrency,
		Budget: DefaultShadowBudget, MaxNanoUSD: defaultShadowMaxNanoUSD,
	}
	for _, entry := range strings.Split(lookup(EnvShadowOrgIDs), ",") {
		switch entry = strings.TrimSpace(entry); entry {
		case "":
		case shadowAllOrgs:
			settings.AllOrgs = true
		default:
			settings.OrgIDs[entry] = struct{}{}
		}
	}
	if raw := strings.TrimSpace(lookup(EnvShadowSamplePercent)); raw != "" {
		percent, err := strconv.Atoi(raw)
		if err != nil || percent < 0 || percent > 100 {
			return ShadowSettings{}, shadowSettingError(EnvShadowSamplePercent)
		}
		settings.SamplePercent = percent
	}
	if raw := strings.TrimSpace(lookup(EnvShadowConcurrency)); raw != "" {
		concurrency, err := strconv.Atoi(raw)
		if err != nil || concurrency < 1 {
			return ShadowSettings{}, shadowSettingError(EnvShadowConcurrency)
		}
		settings.Concurrency = concurrency
	}
	if raw := strings.TrimSpace(lookup(EnvShadowMaxSeconds)); raw != "" {
		seconds, err := strconv.Atoi(raw)
		if err != nil || seconds < 1 {
			return ShadowSettings{}, shadowSettingError(EnvShadowMaxSeconds)
		}
		// Compared before the multiplication, so a huge value cannot overflow
		// into a small budget.
		if seconds > int(MaxShadowBudget/time.Second) {
			seconds = int(MaxShadowBudget / time.Second)
		}
		settings.Budget = time.Duration(seconds) * time.Second
	}
	if raw := strings.TrimSpace(lookup(EnvShadowMaxUSDPerRun)); raw != "" {
		usd, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(usd) || math.IsInf(usd, 0) || usd < 0 {
			return ShadowSettings{}, shadowSettingError(EnvShadowMaxUSDPerRun)
		}
		nano := math.Round(usd * 1e9)
		if nano > maxShadowMaxNanoUSD {
			nano = maxShadowMaxNanoUSD
		}
		settings.MaxNanoUSD = int64(nano)
	}
	return settings.clamped(), nil
}

func shadowSettingError(name string) error {
	return fmt.Errorf("investment shadow phase: %s holds a value that cannot be used", name)
}

// clamped applies the bounds that hold whatever built the settings: the
// environment, or a caller that filled the struct.
func (settings ShadowSettings) clamped() ShadowSettings {
	if settings.Budget <= 0 {
		settings.Budget = DefaultShadowBudget
	}
	if settings.Budget > MaxShadowBudget {
		settings.Budget = MaxShadowBudget
	}
	if settings.Concurrency < 1 {
		settings.Concurrency = 1
	}
	if settings.Concurrency > maxLLMConcurrency {
		settings.Concurrency = maxLLMConcurrency
	}
	if settings.SamplePercent < 0 {
		settings.SamplePercent = 0
	}
	if settings.SamplePercent > 100 {
		settings.SamplePercent = 100
	}
	if settings.MaxNanoUSD < 0 {
		settings.MaxNanoUSD = 0
	}
	if settings.MaxNanoUSD > maxShadowMaxNanoUSD {
		settings.MaxNanoUSD = maxShadowMaxNanoUSD
	}
	return settings
}

// orgAllowed reports whether the org is in the allow-list. An empty org is
// never allowed: "*" means every organization, not a run with none.
func (settings ShadowSettings) orgAllowed(orgID string) bool {
	if orgID == "" {
		return false
	}
	if settings.AllOrgs {
		return true
	}
	_, ok := settings.OrgIDs[orgID]
	return ok
}

// inSample reports whether the work unit is in the sample. It depends on the
// unit id only, so a unit is always in or always out, on every run and every
// architecture.
func (settings ShadowSettings) inSample(workUnitID string) bool {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(workUnitID))
	return int(hash.Sum64()%100) < settings.SamplePercent
}

// EnabledFor reports whether the phase runs for the org at all: a client is
// built only when it does.
func (settings ShadowSettings) EnabledFor(orgID string) bool {
	return settings.Provider != "" && settings.orgAllowed(orgID)
}

// unitSelected is the whole switch for one work unit.
func (settings ShadowSettings) unitSelected(orgID, workUnitID string) bool {
	return settings.Provider != "" && settings.orgAllowed(orgID) && settings.inSample(workUnitID)
}
