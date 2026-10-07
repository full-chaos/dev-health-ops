package decisioneval

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// Environment variable names of the entry points. No value is ever printed.
const (
	EnvRun             = "DECISIONEVAL_RUN"          // 1 = the runner test may run (dry run or live)
	EnvLive            = "DECISIONEVAL_LIVE"         // 1 = allow real requests (also needs a cap > 0)
	EnvDryRun          = "DECISIONEVAL_DRY_RUN"      // 1 = render requests, send nothing
	EnvOut             = "DECISIONEVAL_OUT"          // ledger + raw directory (outside the repo)
	EnvFixtures        = "DECISIONEVAL_FIXTURES"     // fixtures JSONL
	EnvArms            = "DECISIONEVAL_ARMS"         // comma list of incumbent,incumbent+defs,jev,decisions
	EnvConcurrency     = "DECISIONEVAL_CONCURRENCY"  // in-flight classifications
	EnvMaxFixtures     = "DECISIONEVAL_MAX_FIXTURES" // first N fixtures of the file
	EnvCapTypeSafe     = "DECISIONEVAL_CAP_TYPESAFE_USD"
	EnvCapOpenAI       = "DECISIONEVAL_CAP_OPENAI_USD"
	EnvSet             = "DECISIONEVAL_SET"    // smoke | development | heldout | ...
	EnvRepeat          = "DECISIONEVAL_REPEAT" // repeat index (N2 noise floor uses 1)
	EnvRunID           = "DECISIONEVAL_RUN_ID"
	EnvRubric          = "DECISIONEVAL_RUBRIC" // rubric JSON (default: embedded decision-support-v1)
	EnvRates           = "DECISIONEVAL_RATES"  // rate table JSON (default: embedded rates-2026-10-06)
	EnvMap             = "DECISIONEVAL_MAP"    // weight map name (default: primary)
	EnvRetryOrphans    = "DECISIONEVAL_RETRY_ORPHANS"
	EnvIncumbentModel  = "DECISIONEVAL_INCUMBENT_MODEL"
	EnvIncumbentMaxOut = "DECISIONEVAL_INCUMBENT_MAX_OUTPUT_TOKENS"
	EnvJevModel        = "DECISIONEVAL_JEV_MODEL"
	EnvDecisionsModel  = "DECISIONEVAL_DECISIONS_MODEL"
	EnvJevAccept       = "DECISIONEVAL_JEV_ACCEPT_MODELS"
	EnvDecisionsAccept = "DECISIONEVAL_DECISIONS_ACCEPT_MODELS"
	EnvCustomEndpoints = "DECISIONEVAL_ALLOW_CUSTOM_ENDPOINTS"
	EnvGold            = "DECISIONEVAL_GOLD"  // scorer
	EnvScore           = "DECISIONEVAL_SCORE" // 1 = the scorer test may run
	EnvReportDir       = "DECISIONEVAL_REPORT_DIR"
	EnvResamples       = "DECISIONEVAL_RESAMPLES"

	// Credentials (names only; values are read once into secrets.Hidden).
	EnvJevEndpoint = "TYPESAFE_JEN_ENDPOINT"
	EnvJevToken    = "TYPESAFE_JEN_TOKEN"
	EnvOpenAIKey   = "OPENAI_API_KEY"
	EnvOpenAIBase  = "OPENAI_BASE_URL"
)

const defaultOpenAIBase = "https://api.openai.com/v1"

func isOne(v string) bool { return v == "1" || strings.EqualFold(v, "true") }

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func envFloat(getenv func(string) string, name string) (float64, error) {
	v := strings.TrimSpace(getenv(name))
	if v == "" {
		return 0, nil
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		return 0, fmt.Errorf("%s: %q is not a non-negative number", name, v)
	}
	return f, nil
}

func envInt(getenv func(string) string, name string, def int) (int, error) {
	v := strings.TrimSpace(getenv(name))
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s: %q is not a non-negative integer", name, v)
	}
	return n, nil
}

// RunConfigFromEnv builds a RunConfig from the environment. Credentials go
// only to their documented hosts: a custom endpoint needs
// DECISIONEVAL_ALLOW_CUSTOM_ENDPOINTS=1.
func RunConfigFromEnv(getenv func(string) string) (RunConfig, error) {
	var cfg RunConfig
	var err error
	if cfg.Rubric, err = LoadRubric(getenv(EnvRubric)); err != nil {
		return cfg, err
	}
	if cfg.Rates, err = LoadRates(getenv(EnvRates)); err != nil {
		return cfg, err
	}
	cfg.OutDir = getenv(EnvOut)
	cfg.FixturesPath = getenv(EnvFixtures)
	cfg.Arms = splitList(getenv(EnvArms))
	if len(cfg.Arms) == 0 {
		cfg.Arms = []string{ArmIncumbent, ArmJev, ArmDecisions}
	}
	if cfg.Concurrency, err = envInt(getenv, EnvConcurrency, 2); err != nil {
		return cfg, err
	}
	if cfg.MaxFixtures, err = envInt(getenv, EnvMaxFixtures, 0); err != nil {
		return cfg, err
	}
	if cfg.Repeat, err = envInt(getenv, EnvRepeat, 0); err != nil {
		return cfg, err
	}
	cfg.DryRun, cfg.Live = isOne(getenv(EnvDryRun)), isOne(getenv(EnvLive))
	cfg.RetryOrphans = isOne(getenv(EnvRetryOrphans))
	cfg.Set, cfg.RunID, cfg.MapName = getenv(EnvSet), getenv(EnvRunID), getenv(EnvMap)
	capTS, err := envFloat(getenv, EnvCapTypeSafe)
	if err != nil {
		return cfg, err
	}
	capOA, err := envFloat(getenv, EnvCapOpenAI)
	if err != nil {
		return cfg, err
	}
	cfg.Caps = map[string]float64{ProviderTypeSafe: capTS, ProviderOpenAI: capOA}

	custom := isOne(getenv(EnvCustomEndpoints))
	jevEndpoint := strings.TrimSpace(getenv(EnvJevEndpoint))
	if jevEndpoint == "" {
		jevEndpoint = JevEndpoint
	}
	if jevEndpoint != JevEndpoint && !custom {
		return cfg, fmt.Errorf("%s must be %s (the token goes only to that host); set %s=1 to override", EnvJevEndpoint, JevEndpoint, EnvCustomEndpoints)
	}
	base := strings.TrimRight(strings.TrimSpace(getenv(EnvOpenAIBase)), "/")
	if base == "" {
		base = defaultOpenAIBase
	}
	if base != defaultOpenAIBase && !custom {
		return cfg, fmt.Errorf("%s must be %s (the key goes only to that host); set %s=1 to override", EnvOpenAIBase, defaultOpenAIBase, EnvCustomEndpoints)
	}
	cfg.Jev = NewJevBackend(jevEndpoint, secrets.NewHidden(getenv(EnvJevToken)), getenv(EnvJevModel))
	cfg.Jev.AcceptedModels = splitList(getenv(EnvJevAccept))
	cfg.Decisions = NewDecisionsBackend(base+"/decisions", secrets.NewHidden(getenv(EnvOpenAIKey)), getenv(EnvDecisionsModel))
	cfg.Decisions.AcceptedModels = splitList(getenv(EnvDecisionsAccept))
	maxOut, err := envInt(getenv, EnvIncumbentMaxOut, 2048)
	if err != nil {
		return cfg, err
	}
	cfg.Incumbent = IncumbentConfig{BaseURL: base, APIKey: secrets.NewHidden(getenv(EnvOpenAIKey)), Model: getenv(EnvIncumbentModel), MaxOutputTokens: maxOut, Rubric: cfg.Rubric}
	cfg.Timeout, cfg.IncumbentTimeout = 30*time.Second, 60*time.Second
	return cfg, nil
}

// ScoreConfigFromEnv builds a ScoreConfig from the environment.
func ScoreConfigFromEnv(getenv func(string) string) (ScoreConfig, error) {
	var cfg ScoreConfig
	var err error
	if cfg.Rubric, err = LoadRubric(getenv(EnvRubric)); err != nil {
		return cfg, err
	}
	cfg.OutDir, cfg.FixturesPath, cfg.GoldPath = getenv(EnvOut), getenv(EnvFixtures), getenv(EnvGold)
	cfg.ReportDir, cfg.MapName = getenv(EnvReportDir), getenv(EnvMap)
	cfg.Arms = splitList(getenv(EnvArms))
	if cfg.Resamples, err = envInt(getenv, EnvResamples, DefaultResamples); err != nil {
		return cfg, err
	}
	for name, v := range map[string]string{EnvOut: cfg.OutDir, EnvFixtures: cfg.FixturesPath, EnvGold: cfg.GoldPath, EnvReportDir: cfg.ReportDir} {
		if v == "" {
			return cfg, fmt.Errorf("%s is not set", name)
		}
	}
	return cfg, nil
}

var _ = os.Getenv
