package adminops

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/admincli"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// `admin bundles create|list|assign-plan|assign-org` port the Python verbs over
// feature bundles: named sets of feature keys, their assignment to billing
// plans, and per-organization feature overrides.
//
// Named difference: where Python prints an SQLAlchemy exception (a duplicate
// key, a missing organization, a malformed id: its text carries the statement and
// its parameters), dho prints "Error: " and the database's own message; the exit
// code, 1, is the same.

func bundlesGroup() cli.Command {
	return cli.Command{
		Name: "bundles", Summary: "feature bundle management", Kind: cli.Group,
		Children: []cli.Command{
			{Name: "create", Summary: "create a feature bundle", Kind: cli.Verb, Run: runBundlesCreate},
			{Name: "list", Summary: "list feature bundles with their features and plans", Kind: cli.Verb, Run: runBundlesList},
			{Name: "assign-plan", Summary: "assign a feature bundle to a billing plan", Kind: cli.Verb, Run: runBundlesAssignPlan},
			{Name: "assign-org", Summary: "grant an organization a feature override", Kind: cli.Verb, Run: runBundlesAssignOrg},
		},
	}
}

// sortedFeatureKeys is sorted(known): every key of the standard registry.
func sortedFeatureKeys() []string {
	keys := make([]string, 0, len(admincli.StandardFeatures))
	for _, feature := range admincli.StandardFeatures {
		keys = append(keys, feature.Key)
	}
	sort.Strings(keys)
	return keys
}

// validateBundleFeatureKeys is validate_bundle_feature_keys: the first unknown
// key is named, with every valid key as Python's list repr.
func validateBundleFeatureKeys(features []string) error {
	known := map[string]bool{}
	for _, key := range sortedFeatureKeys() {
		known[key] = true
	}
	for _, key := range features {
		if !known[key] {
			reprs := make([]string, 0, len(known))
			for _, valid := range sortedFeatureKeys() {
				reprs = append(reprs, pythonparity.StrRepr(valid))
			}
			return fmt.Errorf("Unknown feature key %s. Valid keys are: [%s]", pythonparity.StrRepr(key), strings.Join(reprs, ", "))
		}
	}
	return nil
}

// databaseFailure prints the way Python prints any exception of a bundle verb
// ("Error: <text>", stdout, exit 1); the text is the database's, with the
// connection details removed.
func databaseFailure(env cli.Env, err error) int {
	fmt.Fprintf(env.Stdout, "Error: %s\n", redactor(env)(err).Error())
	return cli.ExitFailure
}

func runBundlesCreate(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin bundles create")
	var key, name, features, description optString
	flags.Var(&key, "key", "unique bundle key (required)")
	flags.Var(&name, "name", "bundle display name (required)")
	flags.Var(&features, "features", "comma-separated feature keys (required)")
	flags.Var(&description, "description", "bundle description")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !key.set || !name.set || !features.set {
		fmt.Fprintln(env.Stderr, "argument error: --key, --name and --features are required")
		return cli.ExitUsage
	}
	var keys []string
	for _, part := range strings.Split(features.value, ",") {
		if stripped := pythonparity.Strip(part); stripped != "" {
			keys = append(keys, stripped)
		}
	}
	if err := validateBundleFeatureKeys(keys); err != nil {
		fmt.Fprintf(env.Stdout, "Error: %s\n", err)
		return cli.ExitFailure
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	list := make([]pyjson.Value, len(keys))
	for index, item := range keys {
		list[index] = item
	}
	document, err := pyjson.Dumps(list)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not encode the features")
		return cli.ExitFailure
	}
	if _, err := op.Pool.Exec(ctx, `INSERT INTO feature_bundles (id, key, name, description, features, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, now(), now())`, uuid.New(), key.value, name.value, description.ptr(), document); err != nil {
		return databaseFailure(env, err)
	}
	fmt.Fprintf(env.Stdout, "Created bundle: %s (%d features)\n", key.value, len(keys))
	return cli.ExitOK
}

func runBundlesList(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin bundles list")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	rows, err := op.Pool.Query(ctx, `SELECT id, key, name, features::text FROM feature_bundles ORDER BY key`)
	if err != nil {
		return databaseFailure(env, err)
	}
	type bundle struct {
		id           uuid.UUID
		key, name    string
		featuresJSON string
	}
	var bundles []bundle
	for rows.Next() {
		var b bundle
		if err := rows.Scan(&b.id, &b.key, &b.name, &b.featuresJSON); err != nil {
			rows.Close()
			return databaseFailure(env, err)
		}
		bundles = append(bundles, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return databaseFailure(env, err)
	}
	if len(bundles) == 0 {
		fmt.Fprintln(env.Stdout, "No feature bundles found.")
		return cli.ExitOK
	}
	for _, b := range bundles {
		planRows, err := op.Pool.Query(ctx, `SELECT billing_plans.key FROM billing_plans
JOIN plan_feature_bundles ON plan_feature_bundles.plan_id = billing_plans.id
WHERE plan_feature_bundles.bundle_id = $1`, b.id)
		if err != nil {
			return databaseFailure(env, err)
		}
		plans, err := pgx.CollectRows(planRows, pgx.RowTo[string])
		if err != nil {
			return databaseFailure(env, err)
		}
		featureText, plansText := "none", "none"
		if names := bundleFeatureNames(b.featuresJSON); len(names) > 0 {
			featureText = strings.Join(names, ", ")
		}
		if len(plans) > 0 {
			plansText = strings.Join(plans, ", ")
		}
		fmt.Fprintf(env.Stdout, "%s (%s)\n", b.key, b.name)
		fmt.Fprintf(env.Stdout, "  Features: %s\n", featureText)
		fmt.Fprintf(env.Stdout, "  Plans:    %s\n", plansText)
	}
	return cli.ExitOK
}

// bundleFeatureNames is list(bundle.features or []): a JSON list's items, a JSON
// object's keys (the column takes either), nothing for anything else.
func bundleFeatureNames(document string) []string {
	value, err := pyjson.DecodeString(document)
	if err != nil {
		return nil
	}
	var names []string
	switch typed := value.(type) {
	case []pyjson.Value:
		for _, item := range typed {
			names = append(names, pyjson.Str(item))
		}
	case *pyjson.Object:
		names = typed.Keys()
	}
	return names
}

func lookupID(ctx context.Context, op interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, sql, key string) (uuid.UUID, bool, error) {
	var id uuid.UUID
	err := op.QueryRow(ctx, sql, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	return id, err == nil, err
}

func runBundlesAssignPlan(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin bundles assign-plan")
	var bundleKey, planKey optString
	flags.Var(&bundleKey, "bundle-key", "feature bundle key (required)")
	flags.Var(&planKey, "plan-key", "billing plan key (required)")
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !bundleKey.set || !planKey.set {
		fmt.Fprintln(env.Stderr, "argument error: --bundle-key and --plan-key are required")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	bundleID, found, err := lookupID(ctx, op.Pool, `SELECT id FROM feature_bundles WHERE key = $1`, bundleKey.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	if !found {
		fmt.Fprintf(env.Stdout, "Error: Bundle '%s' not found\n", bundleKey.value)
		return cli.ExitFailure
	}
	planID, found, err := lookupID(ctx, op.Pool, `SELECT id FROM billing_plans WHERE key = $1`, planKey.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	if !found {
		fmt.Fprintf(env.Stdout, "Error: Plan '%s' not found\n", planKey.value)
		return cli.ExitFailure
	}
	if _, err := op.Pool.Exec(ctx, `INSERT INTO plan_feature_bundles (id, plan_id, bundle_id) VALUES ($1, $2, $3)`, uuid.New(), planID, bundleID); err != nil {
		return databaseFailure(env, err)
	}
	fmt.Fprintf(env.Stdout, "Assigned bundle '%s' to plan '%s'\n", bundleKey.value, planKey.value)
	return cli.ExitOK
}

// isoUTC is datetime.isoformat() of an aware UTC time.
func isoUTC(t time.Time) string {
	t = t.UTC()
	text := t.Format("2006-01-02T15:04:05")
	if micro := t.Nanosecond() / 1000; micro != 0 {
		text += fmt.Sprintf(".%06d", micro)
	}
	return text + "+00:00"
}

func runBundlesAssignOrg(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho admin bundles assign-org")
	var orgID, featureKey, reason optString
	var expiresDays *big.Int
	flags.Var(&orgID, "org-id", "organization id, a UUID (required)")
	flags.Var(&featureKey, "feature-key", "feature flag key (required)")
	flags.Var(&reason, "reason", "why the override exists")
	flags.Func("expires-days", "days until the override expires", func(text string) error {
		days, err := parsePyInt(text)
		if err != nil {
			return err
		}
		expiresDays = days
		return nil
	})
	if code, ok := parseArgs(flags, env); !ok {
		return code
	}
	if !orgID.set || !featureKey.set {
		fmt.Fprintln(env.Stderr, "argument error: --org-id and --feature-key are required")
		return cli.ExitUsage
	}
	op, closePool, code := operator(ctx, env)
	defer closePool()
	if code != 0 {
		return code
	}
	featureID, found, err := lookupID(ctx, op.Pool, `SELECT id FROM feature_flags WHERE key = $1`, featureKey.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	if !found {
		fmt.Fprintf(env.Stdout, "Error: Feature flag '%s' not found\n", featureKey.value)
		return cli.ExitFailure
	}
	var expires *time.Time
	if expiresDays != nil && expiresDays.Sign() != 0 {
		// datetime.now(utc) + timedelta(days=N): Python's own limits (a day
		// count beyond a C int is refused before its magnitude is checked).
		if !expiresDays.IsInt64() || expiresDays.Int64() > math.MaxInt32 || expiresDays.Int64() < math.MinInt32 {
			fmt.Fprintln(env.Stdout, "Error: Python int too large to convert to C int")
			return cli.ExitFailure
		}
		if days := expiresDays.Int64(); days > 999999999 || days < -999999999 {
			fmt.Fprintf(env.Stdout, "Error: days=%d; must have magnitude <= 999999999\n", days)
			return cli.ExitFailure
		}
		moment := time.Now().UTC().AddDate(0, 0, int(expiresDays.Int64()))
		if moment.Year() < 1 || moment.Year() > 9999 {
			fmt.Fprintln(env.Stdout, "Error: date value out of range")
			return cli.ExitFailure
		}
		expires = &moment
	}
	org, err := parseDriverUUID(orgID.value)
	if err != nil {
		return databaseFailure(env, err)
	}
	if _, err := op.Pool.Exec(ctx, `INSERT INTO org_feature_overrides (id, org_id, feature_id, is_enabled, expires_at, config, reason, created_at, updated_at)
VALUES ($1, $2, $3, true, $4, '{}', $5, now(), now())`, uuid.New(), org, featureID, expires, reason.ptr()); err != nil {
		return databaseFailure(env, err)
	}
	fmt.Fprintf(env.Stdout, "Assigned feature '%s' override to org '%s'\n", featureKey.value, orgID.value)
	if expires != nil {
		fmt.Fprintf(env.Stdout, "  Expires: %s\n", isoUTC(*expires))
	}
	return cli.ExitOK
}

// parsePyInt is int(text) as argparse's type=int calls it (pythonparity.ParseInt:
// Unicode decimal digits and whitespace, a sign, single underscores between
// digits); Python's integers are unbounded, so is the result.
func parsePyInt(text string) (*big.Int, error) {
	value, err := pythonparity.ParseInt(text)
	if err != nil {
		return nil, errors.New("not an integer")
	}
	return value, nil
}

// parseDriverUUID is how the Python verb's database driver reads a UUID
// argument: 32 to 36 characters, hyphens dropped, exactly 32 hexadecimal digits
// (so braces and a "urn:uuid:" prefix, which uuid.Parse accepts, are refused).
func parseDriverUUID(text string) (uuid.UUID, error) {
	length := len([]rune(text))
	if length < 32 || length > 36 {
		return uuid.Nil, fmt.Errorf("invalid UUID %s: length must be between 32..36 characters, got %d", pythonparity.StrRepr(text), length)
	}
	digits := strings.ReplaceAll(text, "-", "")
	if len(digits) != 32 {
		return uuid.Nil, fmt.Errorf("invalid UUID %s: decoding error", pythonparity.StrRepr(text))
	}
	parsed, err := uuid.Parse(digits)
	if err != nil {
		return uuid.Nil, fmt.Errorf("invalid UUID %s: decoding error", pythonparity.StrRepr(text))
	}
	return parsed, nil
}
