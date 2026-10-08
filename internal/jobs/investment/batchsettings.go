package investment

import (
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// The two settings of provider batch mode. Minimum items and the poll
// interval have no env setting (25 and 30 s); the scope may still name them.
const (
	envLLMBatchMode    = "INVESTMENT_LLM_BATCH_MODE"
	envLLMBatchTimeout = "INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS"

	// maxLLMBatchSeconds is the provider's completion window: a wait longer
	// than that cannot end in a finished batch.
	maxLLMBatchSeconds = 24 * 60 * 60
)

type batchConfig struct {
	mode     string
	minItems int
	poll     time.Duration
	timeout  time.Duration
}

// resolveBatchSettings is materialize.py's resolve_llm_batch_* over the
// request scope: a scope key wins over the env setting, which wins over the
// default. A scope value that cannot be used refuses the request; an env
// mode that cannot be used too (Python raised); an env timeout that cannot be
// used falls back to the default with a warning (Python fell back).
func resolveBatchSettings(scope materializeScope, logger *slog.Logger) (batchConfig, error) {
	config := batchConfig{
		minItems: defaultLLMBatchMinItems,
		poll:     defaultLLMBatchPollInterval,
		timeout:  defaultLLMBatchTimeout,
	}

	raw := os.Getenv(envLLMBatchMode)
	if scope.LLMBatchMode != nil {
		raw = *scope.LLMBatchMode
	}
	mode := strings.ReplaceAll(strings.ToLower(strings.TrimSpace(raw)), "-", "_")
	switch mode {
	case "":
		mode = LLMBatchModeSync
	case LLMBatchModeSync, LLMBatchModeAuto, LLMBatchModeProvider:
	default:
		return batchConfig{}, fmt.Errorf("llm_batch_mode must be one of: sync, auto, provider_batch; got %q", raw)
	}
	config.mode = mode

	if scope.LLMBatchTimeoutSeconds != nil {
		timeout, ok := batchSeconds(*scope.LLMBatchTimeoutSeconds)
		if !ok {
			return batchConfig{}, fmt.Errorf("llm_batch_timeout_seconds must be > 0 and <= %d", maxLLMBatchSeconds)
		}
		config.timeout = timeout
	} else if value := strings.TrimSpace(os.Getenv(envLLMBatchTimeout)); value != "" {
		seconds, err := strconv.ParseFloat(value, 64)
		timeout, ok := batchSeconds(seconds)
		if err != nil || !ok {
			logger.Warn("investment llm batch timeout setting cannot be used; using the default",
				"setting", envLLMBatchTimeout, "default_seconds", int(defaultLLMBatchTimeout/time.Second))
		} else {
			config.timeout = timeout
		}
	}

	if scope.LLMBatchMinItems != nil {
		if *scope.LLMBatchMinItems < 1 {
			return batchConfig{}, fmt.Errorf("llm_batch_min_items must be >= 1")
		}
		config.minItems = *scope.LLMBatchMinItems
	}
	if scope.LLMBatchPollIntervalSeconds != nil {
		poll, ok := batchSeconds(*scope.LLMBatchPollIntervalSeconds)
		if !ok {
			return batchConfig{}, fmt.Errorf("llm_batch_poll_interval_seconds must be > 0 and <= %d", maxLLMBatchSeconds)
		}
		config.poll = poll
	}
	return config, nil
}

func batchSeconds(seconds float64) (time.Duration, bool) {
	if math.IsNaN(seconds) || seconds <= 0 || seconds > maxLLMBatchSeconds {
		return 0, false
	}
	return time.Duration(seconds * float64(time.Second)), true
}
