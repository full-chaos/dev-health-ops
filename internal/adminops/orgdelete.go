package adminops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// runOrgsDelete is `admin orgs delete --org-id ID [--dry-run]`: the organization's
// Postgres rows and ClickHouse rows are counted and, unless --dry-run, deleted;
// the plan prints as JSON (sorted keys, two-space indent, as Python's
// json.dumps(..., indent=2, sort_keys=True)). A failure prints "Error: ..." on
// stdout and exits 1.
func runOrgsDelete(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin orgs delete")
	var orgID, analyticsDB optString
	flags.Var(&orgID, "org-id", "organization id (required)")
	flags.Var(&analyticsDB, "analytics-db", "ClickHouse URI (default: CLICKHOUSE_URI or CLICKHOUSE_URI_FILE)")
	dryRun := flags.Bool("dry-run", false, "return the deletion plan without deleting data")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set {
		fmt.Fprintln(env.Stderr, "argument error: --org-id is required")
		return cli.ExitUsage
	}
	deleteConfig, redact, ok := deleteConfigFrom(env, analyticsDB.value)
	if !ok {
		return cli.ExitFailure
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	plan, err := op.DeleteOrg(ctx, orgID.value, *dryRun, deleteConfig)
	if err != nil {
		// Python prints "Error: <text>" for every failure of the deletion, a
		// refusal or not; the text has every credential read above removed.
		message := redact(err).Error()
		var refusal *admin.OperatorError
		if errors.As(err, &refusal) {
			message = refusal.Message
		}
		fmt.Fprintf(env.Stdout, "Error: %s\n", message)
		return cli.ExitFailure
	}
	text, err := pythonDumps(plan)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return cli.ExitFailure
	}
	fmt.Fprintln(env.Stdout, text)
	return cli.ExitOK
}

// deleteConfigFrom reads what the deletion needs beyond the database, the way
// the API service reads it: CLICKHOUSE_URI (the flag overrides it), the settings
// encryption key and salt, and the PagerDuty OAuth client identity. The returned
// redactor removes every credential value read here from an error's text.
func deleteConfigFrom(env cli.Env, analyticsDB string) (admin.DeleteOrgConfig, func(error) error, bool) {
	var deleteConfig admin.DeleteOrgConfig
	var secretValues []string
	fail := func(err error) (admin.DeleteOrgConfig, func(error) error, bool) {
		fmt.Fprintf(env.Stderr, "configuration error: %v\n", err)
		return deleteConfig, nil, false
	}
	if analyticsDB != "" {
		deleteConfig.ClickHouseDSN = analyticsDB
		secretValues = append(secretValues, analyticsDB)
	} else if dsn, configured, err := config.ResolveDSN(env.Lookup, "CLICKHOUSE_URI", config.ClickHouseSpec); err != nil {
		return fail(err)
	} else if configured {
		deleteConfig.ClickHouseDSN = dsn.Reveal()
		secretValues = append(secretValues, dsn.Reveal())
	}
	decryptor, salted, err := settingsDecryptor(env)
	if err != nil {
		return fail(err)
	}
	read := func(name string) (secrets.Value, error) {
		value, _, err := secrets.Resolve(name, env.Lookup)
		return value, err
	}
	clientID, err := read("PAGER_DUTY_CLIENT_ID")
	if err != nil {
		return fail(err)
	}
	clientSecret, err := read("PAGER_DUTY_SECRET")
	if err != nil {
		return fail(err)
	}
	redirect, _ := env.Lookup("PAGER_DUTY_REDIRECT_URI")
	secretValues = append(secretValues, salted...)
	for _, value := range []secrets.Value{clientID, clientSecret} {
		if value.Configured() {
			secretValues = append(secretValues, value.Reveal())
		}
	}
	deleteConfig.Decryptor = decryptor
	deleteConfig.PagerDuty = providerfoundation.PagerDutyRevokeConfig{ClientID: clientID.Reveal(), ClientSecret: clientSecret.Reveal(), RedirectURI: redirect}
	database := redactor(env)
	redact := func(err error) error {
		return database(fmt.Errorf("%s", secrets.RedactValues(err.Error(), secretValues...)))
	}
	return deleteConfig, redact, true
}

// settingsDecryptor builds the settings decryptor the way the API service does:
// none when SETTINGS_ENCRYPTION_KEY is not configured. It also returns the key
// and salt values, so an error text can have them removed.
func settingsDecryptor(env cli.Env) (providerfoundation.FernetDecryptor, []string, error) {
	var decryptor providerfoundation.FernetDecryptor
	key, _, err := secrets.Resolve("SETTINGS_ENCRYPTION_KEY", env.Lookup)
	if err != nil {
		return decryptor, nil, err
	}
	salt, _, err := secrets.Resolve("SETTINGS_ENCRYPTION_SALT", env.Lookup)
	if err != nil {
		return decryptor, nil, err
	}
	var values []string
	for _, value := range []secrets.Value{key, salt} {
		if value.Configured() {
			values = append(values, value.Reveal())
		}
	}
	if key.Configured() {
		built, err := providerfoundation.NewFernetDecryptor(key, salt.Reveal())
		if err != nil {
			return decryptor, values, fmt.Errorf("settings decryptor: %w", err)
		}
		decryptor = built
	}
	return decryptor, values, nil
}

// pythonDumps is json.dumps(value, indent=2, sort_keys=True): two-space indent,
// keys sorted, every non-ASCII character escaped (ensure_ascii).
func pythonDumps(value any) (string, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return "", err
	}
	var ascii strings.Builder
	for _, r := range strings.TrimSuffix(out.String(), "\n") {
		switch {
		case r < 0x80:
			ascii.WriteRune(r)
		case r >= 0x10000:
			high, low := utf16.EncodeRune(r)
			fmt.Fprintf(&ascii, "\\u%04x\\u%04x", high, low)
		default:
			fmt.Fprintf(&ascii, "\\u%04x", r)
		}
	}
	return ascii.String(), nil
}
