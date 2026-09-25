package adminops

import (
	"context"
	"errors"
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// `admin llm-settings get|set|delete` port the Python verbs over one
// organization's BYO LLM settings. The organization is --org, else ORG_ID.
// Named difference: Python guesses the first organization of the database when
// neither is given, and would then write credentials to it; dho refuses.

func llmSettingsGroup() cli.Command {
	return cli.Command{
		Name: "llm-settings", Summary: "organization BYO LLM settings", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "get", Summary: "show the organization's BYO LLM settings", Kind: cli.Verb, Run: runLLMGet},
			{Name: "set", Summary: "set the organization's BYO LLM settings", Kind: cli.Verb, Run: runLLMSet},
			{Name: "delete", Summary: "delete the organization's BYO LLM settings", Kind: cli.Verb, Run: runLLMDelete},
		},
	}
}

// orgFrom is the organization the verb acts on: --org as typed, else ORG_ID. An
// empty value is no organization (Python's root parser refuses it with exit 2).
func orgFrom(env cli.Env, typed optString) (string, bool) {
	value := typed.value
	if !typed.set {
		value, _ = env.Lookup("ORG_ID")
	}
	if value == "" {
		fmt.Fprintln(env.Stderr, "argument error: an organization is required: pass --org or set ORG_ID")
		return "", false
	}
	return value, true
}

// llmSettingsConfig builds the decryptor and the redactor of an error text.
func llmSettingsConfig(env cli.Env) (admin.LLMSettingsConfig, func(error) error, bool) {
	decryptor, values, err := settingsDecryptor(env)
	if err != nil {
		fmt.Fprintf(env.Stderr, "configuration error: %v\n", err)
		return admin.LLMSettingsConfig{}, nil, false
	}
	database := redactor(env)
	redact := func(err error) error {
		return database(fmt.Errorf("%s", secrets.RedactValues(err.Error(), values...)))
	}
	return admin.LLMSettingsConfig{Decryptor: decryptor}, redact, true
}

// finishLLM prints a refusal as Python does ("Error: ..." on stdout, exit 1); a
// failure Python reports by a fixed text prints that text, and the cause goes to
// stderr with credentials removed. Any other failure is a structured error.
//
// secretValues are the credentials the operator typed on this command line (the
// API key, the credentials of the base URL): a refusal's text can echo part of
// what was typed (an unparsable port), so they are removed from it. Python
// printed such text as it was; this is a named difference.
func finishLLM(env cli.Env, redact func(error) error, err error, secretValues ...string) int {
	var refusal *admin.OperatorError
	if errors.As(err, &refusal) {
		fmt.Fprintf(env.Stdout, "Error: %s\n", secrets.RedactValues(refusal.Message, secretValues...))
		return cli.ExitFailure
	}
	var fixed interface{ FixedText() string }
	if errors.As(err, &fixed) {
		fmt.Fprintf(env.Stdout, "Error: %s\n", fixed.FixedText())
		if cause := errors.Unwrap(err); cause != nil {
			fmt.Fprintf(env.Stderr, "cause: %s\n", secrets.RedactValues(redact(cause).Error(), secretValues...))
		}
		return cli.ExitFailure
	}
	return writeError(env.Stderr, "admin_failed", secrets.RedactValues(redact(err).Error(), secretValues...))
}

func printDocument(env cli.Env, document any) int {
	text, err := pythonDumps(document)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, text)
	return cli.ExitOK
}

func runLLMGet(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin llm-settings get")
	var org optString
	flags.Var(&org, "org", "organization id (default: ORG_ID)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	orgID, ok := orgFrom(env, org)
	if !ok {
		return cli.ExitUsage
	}
	config, redact, ok := llmSettingsConfig(env)
	if !ok {
		return cli.ExitFailure
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	document, err := op.GetLLMSettings(ctx, orgID, config)
	if err != nil {
		return finishLLM(env, redact, err)
	}
	return printDocument(env, document)
}

func runLLMSet(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin llm-settings set")
	var org, provider, model, apiKey, baseURL optString
	flags.Var(&org, "org", "organization id (default: ORG_ID)")
	flags.Var(&provider, "provider", "BYO LLM provider (required)")
	flags.Var(&model, "model", "BYO LLM model")
	flags.Var(&apiKey, "api-key", "BYO LLM API key (encrypted at rest)")
	flags.Var(&baseURL, "base-url", "base URL")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !provider.set {
		fmt.Fprintln(env.Stderr, "argument error: --provider is required")
		return cli.ExitUsage
	}
	orgID, ok := orgFrom(env, org)
	if !ok {
		return cli.ExitUsage
	}
	config, redact, ok := llmSettingsConfig(env)
	if !ok {
		return cli.ExitFailure
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	document, err := op.SetLLMSettings(ctx, orgID, admin.LLMSettingsInput{Provider: provider.value,
		Model: model.ptr(), APIKey: apiKey.ptr(), BaseURL: baseURL.ptr()}, config)
	if err != nil {
		typed := secrets.CredentialComponents(baseURL.value)
		if apiKey.value != "" {
			typed = append(typed, apiKey.value)
		}
		return finishLLM(env, redact, err, typed...)
	}
	return printDocument(env, document)
}

func runLLMDelete(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin llm-settings delete")
	var org optString
	flags.Var(&org, "org", "organization id (default: ORG_ID)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	orgID, ok := orgFrom(env, org)
	if !ok {
		return cli.ExitUsage
	}
	config, redact, ok := llmSettingsConfig(env)
	if !ok {
		return cli.ExitFailure
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	if err := op.DeleteLLMSettings(ctx, orgID, config); err != nil {
		return finishLLM(env, redact, err)
	}
	return printDocument(env, map[string]any{"deleted": true})
}
