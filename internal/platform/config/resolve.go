package config

import (
	"fmt"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// layeredLookup returns a lookup that resolves flag overrides before the
// process environment. Every existing resolution helper takes a
// secrets.LookupEnv, so layering here gives the whole configuration surface
// flag > env > default precedence without each site learning about flags.
func layeredLookup(overrides map[string]string, environment secrets.LookupEnv) secrets.LookupEnv {
	if len(overrides) == 0 {
		return environment
	}
	return func(key string) (string, bool) {
		if value, present := overrides[key]; present {
			return value, true
		}
		return environment(key)
	}
}

// validateOverrides rejects an override for a setting that has no flag.
//
// Secrets are environment-only by design: a DSN or token passed as a process
// argument is readable through `ps`, `docker inspect`, and the `docker compose
// config` output this ticket asks operators to read. The flag layer never
// produces such an override, so reaching this error means a caller constructed
// a Spec by hand and the guard keeps that from quietly becoming a supported
// path.
func validateOverrides(overrides map[string]string) error {
	names := make([]string, 0, len(overrides))
	for key := range overrides {
		names = append(names, key)
	}
	slices.Sort(names)
	for _, key := range names {
		option, declared := optionByEnv[key]
		if !declared {
			return fmt.Errorf("%s is not a declared configuration setting", key)
		}
		if option.Secret {
			return fmt.Errorf(
				"%s is a credential and must be supplied through the environment, not a flag",
				key,
			)
		}
	}
	return nil
}

// settingConfigured reports whether an operator supplied a value for a setting
// through either surface, as opposed to the package default applying.
func settingConfigured(lookup secrets.LookupEnv, key string) bool {
	value, present := lookup(key)
	return present && strings.TrimSpace(value) != ""
}

// envOnlySettings names the settings this process took from the environment
// even though a canonical flag exists.
//
// It is deliberately computed against the raw environment rather than the
// layered lookup: the question is which surface the operator actually used, and
// the layered lookup exists precisely to erase that difference everywhere else.
func envOnlySettings(spec Spec, environment secrets.LookupEnv) []string {
	names := make([]string, 0, 8)
	for _, option := range optionRegistry {
		if option.Secret || option.Env == "" || option.Flag == "" {
			continue
		}
		if !option.AppliesTo(spec.Service, spec.RequireQueues) {
			continue
		}
		if _, overridden := spec.Overrides[option.Env]; overridden {
			continue
		}
		if settingConfigured(environment, option.Env) {
			names = append(names, option.Flag)
		}
	}
	slices.Sort(names)
	return slices.Compact(names)
}
