package pgmigrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// `downgrade` is `dev-hops migrate postgres downgrade TARGET` (Alembic's
// command.downgrade, nothing else: Python adds no dry run, confirmation or
// production refusal, and dho adds none). The Go migrator carries down SQL only for
// the chain revisions after the baseline (sql/down/<revision>_<slug>.sql, one per
// sql/ file, TestDownChainCoversEveryChainRevision): the ported range is the chain
// revisions 0139 and later, reverted down to the baseline application head (0138).
// A step outside it (the River cutover 0066, anything at or below the baseline) has
// no down SQL, so dho refuses BEFORE it resolves a DSN or touches a database when the
// target itself is outside the range, and refuses before the first step when the
// recorded state would need a step outside it. Faithful to Python otherwise: each
// walk runs in ONE transaction (Alembic's env.py wraps the whole walk in
// context.begin_transaction(): a failing step rolls back every earlier step), and explicit targets leave the other branch
// (0066) where it is.
//
// Divergences from Python, named here and in the change's RISK-NOTES:
//   - a relative target (-N) on a database that also records 0066 makes Python revert
//     0066 first (and, for N>1, walk further down the other branch): dho refuses it
//     naming 0066, the first revision it cannot revert;
//   - base, head(s), +N, rev@-N, branch@base and partial revision ids are refused as
//     unsupported targets; 0066 and revisions below the baseline are refused as below
//     the baseline floor (Python runs them);
//   - Python numbers its failures exit 1; a refusal here is exit 3 (before anything is
//     read or written) or exit 1 (a state refusal after the database was read).

// DownFile is the down SQL of one chain revision.
type DownFile struct {
	Revision string
	Name     string
	SQL      string
}

// LoadDownChain returns the down SQL, ordered by revision.
func LoadDownChain() ([]DownFile, error) {
	entries, err := fs.ReadDir(chainFiles, "sql/down")
	if err != nil {
		return nil, err
	}
	var out []DownFile
	for _, entry := range entries {
		if entry.IsDir() || path.Ext(entry.Name()) != ".sql" {
			continue
		}
		match := chainName.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("sql/down/%s is not named <revision>_<slug>.sql", entry.Name())
		}
		data, err := chainFiles.ReadFile("sql/down/" + entry.Name())
		if err != nil {
			return nil, err
		}
		out = append(out, DownFile{Revision: match[1], Name: entry.Name(), SQL: string(data)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Revision < out[j].Revision })
	return out, nil
}

// DowngradeRefusal is a downgrade dho will not run. Code is the error code printed;
// PreDatabase refusals are decided from the target alone (exit 3, nothing connected).
type DowngradeRefusal struct {
	Code        string
	Detail      string
	PreDatabase bool
}

func (r DowngradeRefusal) Error() string { return r.Code + ": " + r.Detail }

// DowngradeTarget is a parsed TARGET: a relative step count or one revision id.
type DowngradeTarget struct {
	Steps    int
	Revision string
}

var (
	relativeTarget = regexp.MustCompile(`^-(\d+)$`)
	revisionTarget = regexp.MustCompile(`^[0-9]{4,}$`)
	// negativeNumber: argparse takes "-1" as a positional (no option looks like a
	// number), and so does this verb.
	negativeNumber = regexp.MustCompile(`^-\d+$|^-\d*\.\d+$`)
	bareWord       = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

// ParseDowngradeTarget decides from the text alone: -N (N >= 1) and a revision id of the
// Alembic graph are supported; everything else Alembic accepts (base, head, heads, +N,
// rev@-N, branch@base, a partial id) is refused as unsupported, and a word that names no
// revision is an unknown revision.
func ParseDowngradeTarget(arg string, graph map[string]bool) (DowngradeTarget, *DowngradeRefusal) {
	if match := relativeTarget.FindStringSubmatch(arg); match != nil {
		steps, err := strconv.Atoi(match[1])
		if err != nil || steps < 1 {
			return DowngradeTarget{}, &DowngradeRefusal{Code: "unsupported_target", PreDatabase: true,
				Detail: fmt.Sprintf("relative target %q is not a step count of 1 or more", arg)}
		}
		return DowngradeTarget{Steps: steps}, nil
	}
	if revisionTarget.MatchString(arg) {
		if !graph[arg] {
			return DowngradeTarget{}, &DowngradeRefusal{Code: "unknown_revision", PreDatabase: true,
				Detail: fmt.Sprintf("revision %q is not a revision of this release", arg)}
		}
		return DowngradeTarget{Revision: arg}, nil
	}
	if bareWord.MatchString(arg) && arg != "base" && arg != "head" && arg != "heads" {
		return DowngradeTarget{}, &DowngradeRefusal{Code: "unknown_revision", PreDatabase: true,
			Detail: fmt.Sprintf("%q is not a revision of this release", arg)}
	}
	return DowngradeTarget{}, &DowngradeRefusal{Code: "unsupported_target", PreDatabase: true,
		Detail: fmt.Sprintf("target %q is not supported: dho reverts to a revision id (%s) or -N; base, head, heads, +N, rev@-N, branch@base and partial ids are not ported (use `dev-hops migrate postgres downgrade` from the Python image)", arg, "0138-0145")}
}

// DownRange is the ported range: the floor is the baseline application head (the
// revision the first chain revision continues, which dho reverts TO but never below),
// and Covered are the chain revisions with down SQL, in order.
type DownRange struct {
	Floor   string
	Covered []string
}

// NewDownRange is the range of this build: every chain revision that has a down file.
func NewDownRange(baseline Baseline, chain []ChainFile, down []DownFile) DownRange {
	has := map[string]bool{}
	for _, file := range down {
		has[file.Revision] = true
	}
	out := DownRange{Floor: applicationHead(baseline)}
	for _, file := range chain {
		if !has[file.Revision] {
			break
		}
		out.Covered = append(out.Covered, file.Revision)
	}
	return out
}

func (r DownRange) contains(revision string) bool {
	if revision == r.Floor {
		return true
	}
	for _, covered := range r.Covered {
		if covered == revision {
			return true
		}
	}
	return false
}

// CheckTarget is the refusal that needs no database: an explicit target outside the
// ported range (the floor up to the last covered revision).
func (r DownRange) CheckTarget(target DowngradeTarget) *DowngradeRefusal {
	if target.Revision == "" || r.contains(target.Revision) {
		return nil
	}
	first := r.Floor
	last := r.Floor
	if len(r.Covered) > 0 {
		first, last = r.Covered[0], r.Covered[len(r.Covered)-1]
	}
	return &DowngradeRefusal{Code: "below_baseline_floor", PreDatabase: true,
		Detail: fmt.Sprintf("target %s is outside the ported downgrade range %s-%s (revert to %s at the lowest): its steps have no down SQL in dho; nothing was read or changed. Run `dev-hops migrate postgres downgrade` from the Python image",
			target.Revision, first, last, r.Floor)}
}

// DownStep is one revision to revert and the revision that replaces it in alembic_version.
type DownStep struct{ Revision, Previous string }

// PlanDowngrade decides what to revert from the recorded alembic_version rows.
// known is every revision this build knows (recorded rows outside it are ahead_of_build).
func PlanDowngrade(recorded []string, target DowngradeTarget, r DownRange, known map[string]bool) ([]DownStep, *DowngradeRefusal) {
	if len(recorded) == 0 {
		return nil, &DowngradeRefusal{Code: "no_revision_recorded", Detail: "alembic_version records no revision: there is nothing to revert"}
	}
	if unknown := unknownRevisions(recorded, known); len(unknown) > 0 {
		return nil, &DowngradeRefusal{Code: "ahead_of_build", Detail: AheadOfBuildError{Recorded: recorded, Unknown: unknown}.Error()}
	}
	order := append([]string{r.Floor}, r.Covered...)
	index := map[string]int{}
	for position, revision := range order {
		index[revision] = position
	}
	at := -1
	var outside []string
	for _, revision := range recorded {
		if position, ok := index[revision]; ok {
			if position > at {
				at = position
			}
		} else {
			outside = append(outside, revision)
		}
	}
	sort.Strings(outside)
	if target.Revision == "" && len(outside) > 0 {
		// Alembic walks every recorded head for a relative step: the first revision it
		// reverts is one of these, and it has no down SQL here.
		return nil, belowFloor(outside[0], recorded)
	}
	if at < 0 {
		return nil, belowFloor(outside[0], recorded)
	}
	stop := 0
	if target.Revision != "" {
		stop = index[target.Revision]
		if stop > at {
			return nil, &DowngradeRefusal{Code: "target_not_below_current",
				Detail: fmt.Sprintf("target %s is above the recorded revision %s: a downgrade goes down only. Nothing was changed", target.Revision, order[at])}
		}
	} else {
		stop = at - target.Steps
		if stop < 0 {
			// The first step past the floor is the baseline revision itself.
			return nil, belowFloor(r.Floor, recorded)
		}
	}
	var steps []DownStep
	for position := at; position > stop; position-- {
		steps = append(steps, DownStep{Revision: order[position], Previous: order[position-1]})
	}
	return steps, nil
}

func belowFloor(revision string, recorded []string) *DowngradeRefusal {
	return &DowngradeRefusal{Code: "below_baseline_floor",
		Detail: fmt.Sprintf("reverting would need a step for revision %s (alembic_version holds %v), which has no down SQL in dho: nothing was changed. Name a revision id in the ported range (an explicit id leaves the other branch alone), or run `dev-hops migrate postgres downgrade` from the Python image", revision, recorded)}
}

// DowngradeResult is what one downgrade did.
type DowngradeResult struct {
	Action   string   `json:"action"`
	Recorded []string `json:"recorded"`
	Reverted []string `json:"reverted,omitempty"`
}

// Downgrade reverts the planned steps in ONE transaction, with the alembic_version
// updates that record them: Alembic's env.py runs the whole walk inside one
// context.begin_transaction() and PostgreSQL DDL is transactional, so a step that
// fails rolls back every step before it and the database is exactly as it was.
// logger gets one line per step (Python logs one INFO line per migration).
func Downgrade(ctx context.Context, conn *pgx.Conn, baseline Baseline, chain []ChainFile, down []DownFile, target DowngradeTarget, logger *slog.Logger, reconnect func(context.Context) (*pgx.Conn, error)) (DowngradeResult, error) {
	known, err := embeddedKnown(baseline, chain)
	if err != nil {
		return DowngradeResult{}, err
	}
	r := NewDownRange(baseline, chain, down)
	sqlOf := map[string]string{}
	for _, file := range down {
		sqlOf[file.Revision] = file.SQL
	}
	recorded, err := Recorded(ctx, conn)
	if err != nil {
		return DowngradeResult{}, err
	}
	steps, refusal := PlanDowngrade(recorded, target, r, known)
	if refusal != nil {
		return DowngradeResult{Recorded: recorded}, *refusal
	}
	result := DowngradeResult{Action: "at_target", Recorded: recorded}
	if len(steps) == 0 {
		return result, nil
	}
	err = inTransaction(ctx, conn, func(tx pgx.Tx) error {
		for _, step := range steps {
			logger.Info("migrate downgrade step", "revision", step.Revision, "to", step.Previous)
			tag, err := tx.Exec(ctx, "UPDATE alembic_version SET version_num = $1 WHERE version_num = $2", step.Previous, step.Revision)
			if err != nil {
				return fmt.Errorf("%s: record the revision: %w", step.Revision, err)
			}
			if tag.RowsAffected() != 1 {
				return fmt.Errorf("%s: alembic_version no longer holds it: the database changed while the downgrade ran", step.Revision)
			}
			if _, err := tx.Exec(ctx, sqlOf[step.Revision]); err != nil {
				return fmt.Errorf("down %s: %w", step.Revision, err)
			}
		}
		return nil
	})
	if err != nil {
		result, err := verifyFailedDowngrade(ctx, reconnect, recorded, err)
		logger.Info("migrate downgrade outcome", "outcome", result.Action, "recorded", result.Recorded)
		return result, err
	}
	for _, step := range steps {
		result.Reverted = append(result.Reverted, step.Revision)
	}
	result.Action = "downgraded"
	if result.Recorded, err = Recorded(ctx, conn); err != nil {
		return result, err
	}
	logger.Info("migrate downgrade outcome", "outcome", result.Action, "recorded", result.Recorded)
	return result, nil
}

// verifyFailedDowngrade reports the outcome of a downgrade whose transaction returned
// an error using ONLY state read back from the database on a fresh connection: a
// COMMIT whose acknowledgement was lost may have committed, so the error alone proves
// nothing about what the database holds. When the read-back itself fails the outcome
// is reported as unknown, never as rolled back.
func verifyFailedDowngrade(ctx context.Context, reconnect func(context.Context) (*pgx.Conn, error), before []string, cause error) (DowngradeResult, error) {
	observed, readErr := readRecordedFresh(ctx, reconnect)
	switch {
	case readErr != nil:
		return DowngradeResult{Action: "outcome_unknown", Recorded: before},
			fmt.Errorf("%w (outcome unknown: alembic_version could not be re-read: %v; it held %v before the downgrade: run `dho migrate postgres current` before anything else)", cause, readErr, before)
	case sameSet(observed, before):
		return DowngradeResult{Action: "unchanged", Recorded: observed},
			fmt.Errorf("%w (verified by re-read: alembic_version still holds %v)", cause, observed)
	default:
		return DowngradeResult{Action: "changed", Recorded: observed},
			fmt.Errorf("%w (verified by re-read: the database CHANGED although the command failed: alembic_version held %v and now holds %v)", cause, before, observed)
	}
}

func readRecordedFresh(ctx context.Context, reconnect func(context.Context) (*pgx.Conn, error)) ([]string, error) {
	if reconnect == nil {
		return nil, errors.New("no connection to read it back on")
	}
	conn, err := reconnect(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close(context.Background())
	return Recorded(ctx, conn)
}

// downgrade is `dho migrate postgres downgrade TARGET`.
func downgrade(ctx context.Context, resolve ResolveDSN, env cli.Env) int {
	var targets []string
	for _, arg := range env.Args {
		switch {
		case arg == "-h" || arg == "--help":
			fmt.Fprintln(env.Stderr, "Usage of dho migrate postgres downgrade TARGET: TARGET is a revision id 0138-0145 (revert everything above it) or -N (revert N steps); environment as `migrate postgres current`")
			return cli.ExitOK
		case strings.HasPrefix(arg, "-") && arg != "-" && !negativeNumber.MatchString(arg):
			fmt.Fprintf(env.Stderr, "argument error: unknown flag %s\n", arg)
			return cli.ExitUsage
		default:
			targets = append(targets, arg)
		}
	}
	if len(targets) != 1 {
		fmt.Fprintln(env.Stderr, "argument error: exactly one target revision is required (e.g. -1 or a revision)")
		return cli.ExitUsage
	}
	baseline, err := LoadBaseline()
	if err != nil {
		return writeError(env.Stderr, "baseline_unavailable", err.Error())
	}
	chain, err := LoadChain()
	if err != nil {
		return writeError(env.Stderr, "chain_unavailable", err.Error())
	}
	down, err := LoadDownChain()
	if err != nil {
		return writeError(env.Stderr, "down_chain_unavailable", err.Error())
	}
	entries, err := LoadHistory()
	if err != nil {
		return writeError(env.Stderr, "history_unavailable", err.Error())
	}
	graph := KnownRevisions(entries, baseline, chain)
	target, refusal := ParseDowngradeTarget(targets[0], graph)
	if refusal == nil {
		refusal = NewDownRange(baseline, chain, down).CheckTarget(target)
	}
	if refusal != nil {
		return writeRefusal(env, refusal)
	}
	dsn, _, ok := resolve(env.Lookup, env.Stderr)
	if !ok {
		return cli.ExitFailure
	}
	boundary := pgstorage.Boundary(dsn.Reveal())
	conn, err := pgx.Connect(ctx, dsn.Reveal())
	if err != nil {
		return writeError(env.Stderr, "postgres_unavailable", boundary.Redact(err).Error())
	}
	defer conn.Close(context.Background())
	result, err := Downgrade(ctx, conn, baseline, chain, down, target, logging.NewJSON(env.Stderr, slog.LevelInfo),
		func(ctx context.Context) (*pgx.Conn, error) { return pgx.Connect(ctx, dsn.Reveal()) })
	if err != nil {
		var state DowngradeRefusal
		if errors.As(err, &state) {
			return writeRefusal(env, &state)
		}
		return writeError(env.Stderr, "downgrade_failed", boundary.Redact(err).Error())
	}
	return writeResult(env.Stdout, env.Stderr, result)
}

// writeRefusal prints the refusal: exit 3 when it was decided before the database was
// read, exit 1 (Python's failure code) when the database state refused it.
func writeRefusal(env cli.Env, refusal *DowngradeRefusal) int {
	code := writeError(env.Stderr, refusal.Code, refusal.Detail)
	if refusal.PreDatabase {
		return cli.ExitRefused
	}
	return code
}
