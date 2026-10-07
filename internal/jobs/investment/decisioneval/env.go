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
	EnvLevelRule       = "DECISIONEVAL_LEVEL_RULE" // median | conditional-median:0.5 | conditional-median:0.67
	EnvCustomEndpoints = "DECISIONEVAL_ALLOW_CUSTOM_ENDPOINTS"
	EnvGold            = "DECISIONEVAL_GOLD"     // scorer
	EnvGoldSet         = "DECISIONEVAL_GOLD_SET" // set of the gold rows when the file has no `set` field
	EnvScore           = "DECISIONEVAL_SCORE"    // 1 = the scorer test may run
	EnvReportDir       = "DECISIONEVAL_REPORT_DIR"
	EnvResamples       = "DECISIONEVAL_RESAMPLES"
	EnvPersisted       = "DECISIONEVAL_PERSISTED"   // persisted incumbent rows (eval-incumbent-persisted.jsonl) for N1
	EnvFull            = "DECISIONEVAL_FULL"        // 1 = the full-run scorer test may run
	EnvDecide          = "DECISIONEVAL_DECIDE"      // 1 = the decision test may run
	EnvDevIDs          = "DECISIONEVAL_DEV_IDS"     // development-set ids (or the eval-development.jsonl file)
	EnvHeldoutIDs      = "DECISIONEVAL_HELDOUT_IDS" // held-out ids (or the eval-heldout.jsonl file)
	EnvSampleSize      = "DECISIONEVAL_SAMPLE_SIZE" // disagreement sample size (0 = no draw)
	EnvSampleGold      = "DECISIONEVAL_SAMPLE_GOLD" // labels of the drawn sample
	EnvTwinOut         = "DECISIONEVAL_TWIN_OUT"    // held-out run ledger dir (gate K)
	EnvTwinFixtures    = "DECISIONEVAL_TWIN_FIXTURES"
	EnvTwinGold        = "DECISIONEVAL_TWIN_GOLD"
	EnvCandidates      = "DECISIONEVAL_CANDIDATES" // jev,decisions
	EnvT2              = "DECISIONEVAL_T2"         // 1 = the throughput report test may run (set DECISIONEVAL_SET=throughput for the batch run)
	EnvT2Set           = "DECISIONEVAL_T2_SET"
	EnvT2Batch         = "DECISIONEVAL_T2_BATCH" // expected batch size (default 320; 0 skips the check)

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
	cfg.LevelRule = getenv(EnvLevelRule)
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
	cfg.Decisions = NewDecisionsBackend(base+"/decisions", secrets.NewHidden(getenv(EnvOpenAIKey)), getenv(EnvDecisionsModel))
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
	cfg.LevelRule, cfg.IncumbentPersistedPath = getenv(EnvLevelRule), getenv(EnvPersisted)
	cfg.GoldSet = getenv(EnvGoldSet)
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

// FullConfigFromEnv builds a FullConfig from the environment.
func FullConfigFromEnv(getenv func(string) string) (FullConfig, error) {
	var cfg FullConfig
	var err error
	if cfg.Rubric, err = LoadRubric(getenv(EnvRubric)); err != nil {
		return cfg, err
	}
	cfg.OutDir, cfg.FixturesPath, cfg.ReportDir = getenv(EnvOut), getenv(EnvFixtures), getenv(EnvReportDir)
	cfg.DevelopmentIDsPath, cfg.HeldoutIDsPath = getenv(EnvDevIDs), getenv(EnvHeldoutIDs)
	cfg.MapName, cfg.LevelRule, cfg.Arms = getenv(EnvMap), getenv(EnvLevelRule), splitList(getenv(EnvArms))
	if cfg.Resamples, err = envInt(getenv, EnvResamples, DefaultResamples); err != nil {
		return cfg, err
	}
	if cfg.SampleSize, err = envInt(getenv, EnvSampleSize, 0); err != nil {
		return cfg, err
	}
	for name, v := range map[string]string{EnvOut: cfg.OutDir, EnvFixtures: cfg.FixturesPath, EnvReportDir: cfg.ReportDir, EnvDevIDs: cfg.DevelopmentIDsPath} {
		if v == "" {
			return cfg, fmt.Errorf("%s is not set", name)
		}
	}
	return cfg, nil
}

// DecideConfigFromEnv builds a DecideConfig from the environment.
func DecideConfigFromEnv(getenv func(string) string) (DecideConfig, error) {
	var cfg DecideConfig
	var err error
	if cfg.Rubric, err = LoadRubric(getenv(EnvRubric)); err != nil {
		return cfg, err
	}
	cfg.FullOutDir, cfg.FullFixturesPath, cfg.ReportDir = getenv(EnvOut), getenv(EnvFixtures), getenv(EnvReportDir)
	cfg.DevelopmentIDsPath, cfg.HeldoutIDsPath, cfg.SampleGoldPath = getenv(EnvDevIDs), getenv(EnvHeldoutIDs), getenv(EnvSampleGold)
	cfg.TwinOutDir, cfg.TwinFixturesPath, cfg.TwinGoldPath = getenv(EnvTwinOut), getenv(EnvTwinFixtures), getenv(EnvTwinGold)
	cfg.TwinGoldSet = getenv(EnvGoldSet)
	cfg.MapName, cfg.LevelRule, cfg.Candidates = getenv(EnvMap), getenv(EnvLevelRule), splitList(getenv(EnvCandidates))
	if cfg.Resamples, err = envInt(getenv, EnvResamples, DefaultResamples); err != nil {
		return cfg, err
	}
	for name, v := range map[string]string{EnvOut: cfg.FullOutDir, EnvFixtures: cfg.FullFixturesPath, EnvReportDir: cfg.ReportDir,
		EnvDevIDs: cfg.DevelopmentIDsPath, EnvHeldoutIDs: cfg.HeldoutIDsPath, EnvSampleGold: cfg.SampleGoldPath} {
		if v == "" {
			return cfg, fmt.Errorf("%s is not set", name)
		}
	}
	return cfg, nil
}

// ThroughputConfigFromEnv builds a ThroughputConfig from the environment.
func ThroughputConfigFromEnv(getenv func(string) string) (ThroughputConfig, error) {
	cfg := ThroughputConfig{OutDir: getenv(EnvOut), ReportDir: getenv(EnvReportDir), Set: getenv(EnvT2Set)}
	var err error
	if cfg.ExpectBatch, err = envInt(getenv, EnvT2Batch, DefaultThroughputBatch); err != nil {
		return cfg, err
	}
	for name, v := range map[string]string{EnvOut: cfg.OutDir, EnvReportDir: cfg.ReportDir} {
		if v == "" {
			return cfg, fmt.Errorf("%s is not set", name)
		}
	}
	return cfg, nil
}
