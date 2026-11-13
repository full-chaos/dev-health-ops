// Command shadow-venue drives go-api's shadow decision point on the bigboy compose venue and judges each row from
// go-api's log (internal/shadowvenue). It sends to a loopback origin only.
//
//	shadow-venue plan  -principals <tsv> [-rows <tsv>] [-profiles <json>] [-only-rows 1,22] [-k 3] [-allow-external a,b]
//	                   [-destructive -throwaway-pool T1,T2,T3 -throwaway-ids <tsv>] [-throwaway-org T] [-states <tsv, 0600>]
//	shadow-venue drive -base http://127.0.0.1:8093 -principals <tsv> -out <slots.tsv> [plan flags] [-gap 2.5s] [-token-file name=path ...]
//	                   [-bodykeys-key <0600 key> -bodykeys-out <new side file>]
//	shadow-venue mint  -base http://127.0.0.1:8093 -principals <tsv> -names a,b -out-dir <0700 dir> [-egress <txt> -run-id <id>]
//	shadow-venue bodykey -out <new 0600 key file>
//	shadow-venue bodydiff -a <slots> -b <slots> [-keys-a <side> -keys-b <side> [-paths]]
//	shadow-venue judge -slots <slots.tsv> -log <go-api log> -out <table.tsv> [-k 3] [-grace 2.2s] [-rulings <tsv>]
//	                   [-rows <tsv>] [-stub-log <jsonl>] [-outbound-out <tsv>] [-egress-before <txt> -egress-between <txt>]
//	                   [-membership-before <tsv> -membership-after <tsv>]
//	shadow-venue judge-enforced ... [-stub-log <jsonl> -yardstick-outbound <tsv>] [-egress-before -egress-between -egress-after]
//	                       [-volatile-keys <frozen 0600> -keys-yardstick <side> -keys-enforced <side>]
//	                   [-log <go-api log>] [-membership-before <tsv> -membership-after <tsv>]
//
// Exit: 0 every row passes (judge) or the step finished; 1 a row fails; 2 usage or input; 3 the run is void.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"time"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/shadowvenue"
)

const (
	exitOK    = 0
	exitFail  = 1
	exitUsage = 2
)

func main() {
	logging.InstallDefault(logging.NewJSON(os.Stderr, slog.LevelInfo))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: shadow-venue plan|drive|mint|judge|judge-enforced|bodykey|bodydiff [flags]")
		return exitUsage
	}
	switch args[0] {
	case "plan", "drive":
		return planOrDrive(ctx, args[0], args[1:], stdout, stderr)
	case "judge":
		return judge(args[1:], stdout, stderr)
	case "judge-enforced":
		return judgeEnforced(args[1:], stdout, stderr)
	case "mint":
		return mint(ctx, args[1:], stdout, stderr)
	case "bodykey":
		return bodykey(args[1:], stderr)
	case "bodydiff":
		return bodydiff(args[1:], stdout, stderr)
	}
	fmt.Fprintf(stderr, "shadow-venue: unknown step %q\n", args[0])
	return exitUsage
}

// tokenFiles is the repeatable -token-file name=path flag.
type tokenFiles map[string]string

func (t tokenFiles) String() string { return fmt.Sprint(map[string]string(t)) }

func (t tokenFiles) Set(value string) error {
	name, path, ok := strings.Cut(value, "=")
	if !ok || name == "" || path == "" {
		return errors.New("want name=path")
	}
	if _, dup := t[name]; dup {
		return fmt.Errorf("%s twice", name)
	}
	t[name] = path
	return nil
}

func planOrDrive(ctx context.Context, step string, args []string, stdout, stderr io.Writer) (code int) {
	fs := flag.NewFlagSet(step, flag.ContinueOnError)
	fs.SetOutput(stderr)
	principals := fs.String("principals", "", "venue principals file (kind name email uuid org role password_file)")
	rowsPath := fs.String("rows", "", "row table; empty = the embedded one")
	profiles := fs.String("profiles", "", "endpoint-profiles.go-api.json of the venue go-api image; empty = the embedded one")
	throwaway := fs.String("throwaway-org", "T", "row-table org label of the destructive rows: the throwaway pool")
	pool := fs.String("throwaway-pool", "", "comma-separated principals-file org labels, one per k: destructive slot k acts in the k-th (with -destructive)")
	idsPath := fs.String("throwaway-ids", "", "frozen throwaway ids file (kind label uuid): no other id may be sent by a destructive slot (with -destructive)")
	only := fs.String("only-rows", "", "comma-separated row ids the run sends; the controls always run")
	statesPath := fs.String("states", "", "owner-only (0600) file of the minted single-use states, one per slot of a row whose body names {state}")
	k := fs.Int("k", 3, "requests per cell")
	allow := fs.String("allow-external", "", "comma-separated outside services a request may reach (stripe, pagerduty, github, mail)")
	base := fs.String("base", "", "venue go-api origin: loopback, or go-api's venue-int IP named by -egress (drive)")
	egressPath := fs.String("egress", "", "egress record read just before this drive (drive; needed for a non-loopback -base)")
	runID := fs.String("run-id", "", "this drive's run id; the -egress record must name it as run_id (drive)")
	out := fs.String("out", "", "slots file to create, 0600 (drive)")
	gap := fs.Duration("gap", 2500*time.Millisecond, "wait after each answer (drive)")
	keyPath := fs.String("bodykeys-key", "", "owner-only key file for the per-leaf HMACs of the answers; with -bodykeys-out (drive)")
	keysOut := fs.String("bodykeys-out", "", "new side file of per-leaf HMACs (path and 12 hex per leaf, no value) (drive)")
	loginOnly := fs.Bool("login-only", false, "sign each principal of the plan in and stop: no row request is sent, no slots file is written (drive)")
	destructive := fs.Bool("destructive", false, "send the destructive rows (last, throwaway org only); without it they are gaps")
	files := tokenFiles{}
	fs.Var(files, "token-file", "name=path: use the minted user access token of principal name (an owner-only file) instead of signing it in; repeatable (drive)")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	frozen, err := readFrozen(*idsPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	states, err := readStates(*statesPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	plan, venue, err := makePlan(*principals, *rowsPath, *profiles, shadowvenue.PlanOptions{Throwaway: *throwaway, Pool: list(*pool),
		Frozen: frozen, States: states, OnlyRows: list(*only), AllowExternal: list(*allow), K: *k, Destructive: *destructive})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if step == "plan" {
		for _, s := range plan.Slots {
			note := s.Note
			if note == "" {
				note = "-"
			}
			fmt.Fprintf(stdout, "%d\t%s\t%s\t%s\tk=%d\t%s %s\tcross_org=%v\t%s\n", s.Seq, s.Row, s.Caller, s.Principal, s.K, s.Method, s.Path, s.CrossOrg, note)
		}
		return exitOK
	}
	var record shadowvenue.EgressRead
	if reads := readEgress(*egressPath); len(reads) == 1 {
		record = reads[0]
	}
	if _, err := shadowvenue.CheckVenueBase(*base, record, *runID); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if *out == "" && !*loginOnly {
		fmt.Fprintln(stderr, "drive: -out is required")
		return exitUsage
	}
	if *loginOnly {
		return loginPrincipals(ctx, &shadowvenue.Driver{Base: *base, Egress: record, RunID: *runID, Client: newClient()}, plan, venue, stdout, stderr)
	}
	if (*keyPath == "") != (*keysOut == "") {
		fmt.Fprintln(stderr, "drive: -bodykeys-key and -bodykeys-out go together")
		return exitUsage
	}
	var bodyKey []byte
	if *keyPath != "" {
		var err error
		if bodyKey, err = shadowvenue.ReadBodyKey(*keyPath); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "drive: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	driver := &shadowvenue.Driver{Base: *base, AllowExternal: list(*allow), Egress: record, RunID: *runID, Client: newClient(), Gap: *gap, Frozen: frozen}
	if *keysOut != "" {
		side, err := shadowvenue.CreateKeysFile(*keysOut)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return finish(file, exitUsage, stderr)
		}
		defer func() { code = finish(side, code, stderr) }()
		driver.BodyKey, driver.KeysOut = bodyKey, side
	}
	tokens := map[string]string{}
	inPlan := map[string]bool{}
	for _, s := range plan.Slots {
		inPlan[s.Principal] = true
	}
	for name, path := range files {
		if !inPlan[name] {
			fmt.Fprintf(stderr, "-token-file %s: no slot of the plan uses that principal\n", name)
			return finish(file, exitUsage, stderr)
		}
		token, err := shadowvenue.ReadTokenFile(path)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return finish(file, exitUsage, stderr)
		}
		tokens[name] = token
	}
	for _, s := range plan.Slots {
		if s.Gap() || tokens[s.Principal] != "" {
			continue
		}
		p, _ := venue.Principal(s.Principal)
		token, err := driver.Login(ctx, p, venue.Orgs[p.Org])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return finish(file, exitFail, stderr)
		}
		tokens[s.Principal] = token
	}
	fmt.Fprintf(stdout, "drive: %d slots, gap %s, start %s\n", len(plan.Slots), *gap, time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(stdout, "drive: schedule sha12 %s\n", shadowvenue.ScheduleSHA12(plan.Slots))
	if err := driver.Drive(ctx, plan, tokens, file); err != nil {
		fmt.Fprintln(stderr, err)
		return finish(file, exitFail, stderr)
	}
	if err := file.Sync(); err != nil {
		fmt.Fprintln(stderr, err)
		return finish(file, exitFail, stderr)
	}
	fmt.Fprintf(stdout, "drive: done %s, slots in %s\n", time.Now().UTC().Format(time.RFC3339), *out)
	return finish(file, exitOK, stderr)
}

// loginPrincipals signs every principal of the plan in once and sends nothing else: it shows what the drive's own
// sign-ins change.
func loginPrincipals(ctx context.Context, driver *shadowvenue.Driver, plan shadowvenue.Plan, venue shadowvenue.Venue, stdout, stderr io.Writer) int {
	done := map[string]bool{}
	for _, s := range plan.Slots {
		if s.Gap() || done[s.Principal] {
			continue
		}
		done[s.Principal] = true
		p, _ := venue.Principal(s.Principal)
		if _, err := driver.Login(ctx, p, venue.Orgs[p.Org]); err != nil {
			fmt.Fprintln(stderr, err)
			return exitFail
		}
	}
	fmt.Fprintf(stdout, "login-only: %d sign-ins, no row request\n", len(done))
	return exitOK
}

// newClient sends on the no-proxy outbound transport and never follows a redirect: a bearer reaches only the venue
// origin.
func newClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: httpguard.Transport(), CheckRedirect: noRedirect}
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// list splits a comma-separated flag value; "" is no item.
func list(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

// readFrozen reads the frozen throwaway ids file; "" gives none.
func readFrozen(path string) (shadowvenue.ThrowawayIDs, error) {
	if path == "" {
		return shadowvenue.ThrowawayIDs{}, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return shadowvenue.ThrowawayIDs{}, fmt.Errorf("-throwaway-ids: %w", err)
	}
	return shadowvenue.ParseThrowawayIDs(raw)
}

// readStates reads the owner-only states file; "" gives none. No state value reaches an error.
func readStates(path string) (shadowvenue.States, error) {
	if path == "" {
		return shadowvenue.States{}, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		return shadowvenue.States{}, fmt.Errorf("-states: %w", errors.Unwrap(err))
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return shadowvenue.States{}, fmt.Errorf("-states %s: want an owner-only (0600) regular file", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return shadowvenue.States{}, fmt.Errorf("-states %s: unreadable", path)
	}
	return shadowvenue.ParseStates(raw)
}

// readEgress reads the egress records named by paths, in order; "" is a read not given. An unreadable file is a
// record with its error: the judge voids on it.
func readEgress(paths ...string) []shadowvenue.EgressRead {
	var out []shadowvenue.EgressRead
	for _, path := range paths {
		if path == "" {
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			out = append(out, shadowvenue.EgressRead{Name: path, Err: errors.New("unreadable")})
			continue
		}
		out = append(out, shadowvenue.ParseEgress(path, raw))
	}
	return out
}

// readSeedLog reads the stub's seed-mode log; "" is a log not given. An unreadable or malformed one is kept as its
// error: the judge voids on it.
func readSeedLog(path string) shadowvenue.SeedLog {
	if path == "" {
		return shadowvenue.SeedLog{}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return shadowvenue.SeedLog{Given: true, Err: errors.New("unreadable")}
	}
	hits, err := shadowvenue.ParseStubLog(raw)
	return shadowvenue.SeedLog{Given: true, Hits: hits, Err: err}
}

// readRows reads a row table; "" is the embedded one.
func readRows(path string) ([]shadowvenue.Row, error) {
	raw := shadowvenue.DefaultRows
	if path != "" {
		var err error
		if raw, err = os.ReadFile(path); err != nil {
			return nil, fmt.Errorf("-rows: %w", err)
		}
	}
	return shadowvenue.ParseRows(raw)
}

// readOutbound is the stub evidence of slots: the stub log at path ("" = none given) and the row table's ext column.
func readOutbound(path, rowsPath string, slots []shadowvenue.Slot) (shadowvenue.Outbound, error) {
	rows, err := readRows(rowsPath)
	if err != nil {
		return shadowvenue.Outbound{}, err
	}
	if err := shadowvenue.CheckRowKeys(slots, rows); err != nil {
		return shadowvenue.Outbound{}, err
	}
	ob := shadowvenue.Outbound{Rows: rows}
	if path == "" {
		return ob, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return shadowvenue.Outbound{}, fmt.Errorf("-stub-log: %w", err)
	}
	if ob.Hits, err = shadowvenue.ParseStubLog(raw); err != nil {
		return shadowvenue.Outbound{}, err
	}
	ob.Logged = true
	return ob, nil
}

func makePlan(principalsPath, rowsPath, profilesPath string, options shadowvenue.PlanOptions) (shadowvenue.Plan, shadowvenue.Venue, error) {
	rawPrincipals, err := os.ReadFile(principalsPath)
	if err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, fmt.Errorf("-principals: %w", err)
	}
	venue, err := shadowvenue.ParsePrincipals(rawPrincipals)
	if err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, err
	}
	rows, err := readRows(rowsPath)
	if err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, err
	}
	profiles := authv1.GoAPIEndpointProfiles
	if profilesPath != "" {
		if profiles, err = os.ReadFile(profilesPath); err != nil {
			return shadowvenue.Plan{}, shadowvenue.Venue{}, fmt.Errorf("-profiles: %w", err)
		}
	}
	if err := shadowvenue.CheckRows(rows, profiles); err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, err
	}
	plan, err := shadowvenue.MakePlan(rows, venue, options)
	return plan, venue, err
}

func judge(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("judge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	slotsPath := fs.String("slots", "", "slots file drive wrote")
	logPath := fs.String("log", "", "go-api JSON log of the run (docker logs --since <drive start>)")
	rulingsPath := fs.String("rulings", "", "mismatch tuples with a passing pm verdict")
	verdictsOut := fs.String("verdicts-out", "", "per-slot verdicts file to create, 0600 (the yardstick of a later enforced run)")
	noOrgBefore := fs.String("noorg-before", "", "go-api /metrics scrape taken before the drive (with -noorg-after: the enforcement no_org counter evidence)")
	noOrgAfter := fs.String("noorg-after", "", "go-api /metrics scrape taken after the drive")
	out := fs.String("out", "", "per-row table to create, 0600")
	k := fs.Int("k", 3, "requests per cell")
	grace := fs.Duration("grace", 2200*time.Millisecond, "time after an answer in which its result line or outside call may land")
	rowsPath := fs.String("rows", "", "row table of the drive (its ext column); empty = the embedded one")
	stubLog := fs.String("stub-log", "", "venue stub JSON-lines log over the run (required when an external row was sent)")
	outboundOut := fs.String("outbound-out", "", "per-slot outside-call counts file to create, 0600 (the yardstick of a later enforced run)")
	egressBefore := fs.String("egress-before", "", "egress record read before the yardstick run")
	egressBetween := fs.String("egress-between", "", "egress record read after the yardstick run, before the enforced run")
	seedLog := fs.String("seed-log", "", "the stub's seed-mode log (required when an external row was sent; an empty file is a log)")
	membershipBefore := fs.String("membership-before", "", "membership snapshot taken at most 5 minutes before the drive (required when a superuser-O slot ran)")
	membershipAfter := fs.String("membership-after", "", "membership snapshot taken right after the drive")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if (*noOrgBefore == "") != (*noOrgAfter == "") {
		fmt.Fprintln(stderr, "judge: -noorg-before and -noorg-after go together")
		return exitUsage
	}
	rawSlots, err := os.ReadFile(*slotsPath)
	if err != nil {
		fmt.Fprintf(stderr, "-slots: %v\n", err)
		return exitUsage
	}
	slots, err := shadowvenue.ParseSlots(rawSlots)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	logFile, err := os.Open(*logPath)
	if err != nil {
		fmt.Fprintf(stderr, "-log: %v\n", err)
		return exitUsage
	}
	defer logFile.Close()
	ev, err := shadowvenue.ParseLog(logFile)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	var rulings []shadowvenue.Ruling
	if *rulingsPath != "" {
		raw, err := os.ReadFile(*rulingsPath)
		if err != nil {
			fmt.Fprintf(stderr, "-rulings: %v\n", err)
			return exitUsage
		}
		if rulings, err = shadowvenue.ParseRulings(raw); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	if *out == "" {
		fmt.Fprintln(stderr, "judge: -out is required")
		return exitUsage
	}
	var noOrg shadowvenue.NoOrgEvidence
	if *noOrgBefore != "" {
		before, err := os.ReadFile(*noOrgBefore)
		if err != nil {
			fmt.Fprintf(stderr, "-noorg-before: %v\n", err)
			return exitUsage
		}
		after, err := os.ReadFile(*noOrgAfter)
		if err != nil {
			fmt.Fprintf(stderr, "-noorg-after: %v\n", err)
			return exitUsage
		}
		delta, err := shadowvenue.ParseUnjudgedDelta(before, after)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		noOrg = delta
	}
	outbound, err := readOutbound(*stubLog, *rowsPath, slots)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	outbound.Egress = readEgress(*egressBefore, *egressBetween)
	outbound.Seed = readSeedLog(*seedLog)
	membership, err := readMembership(*membershipBefore, *membershipAfter)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	report := shadowvenue.Judge(slots, ev, rulings, shadowvenue.JudgeOptions{K: *k, Grace: *grace, NoOrg: noOrg, Outbound: outbound, Strict: true, Membership: membership})
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "judge: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	if err := report.WriteTable(file); err != nil {
		fmt.Fprintln(stderr, err)
		return finish(file, exitUsage, stderr)
	}
	if *verdictsOut != "" {
		vf, err := os.OpenFile(*verdictsOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			fmt.Fprintf(stderr, "judge: -verdicts-out %s: %v\n", *verdictsOut, errors.Unwrap(err))
			return finish(file, exitUsage, stderr)
		}
		werr := report.WriteVerdicts(vf)
		if cerr := vf.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			fmt.Fprintln(stderr, werr)
			return finish(file, exitFail, stderr)
		}
	}
	if *outboundOut != "" {
		of, err := os.OpenFile(*outboundOut, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			fmt.Fprintf(stderr, "judge: -outbound-out %s: %v\n", *outboundOut, errors.Unwrap(err))
			return finish(file, exitUsage, stderr)
		}
		werr := report.WriteOutbound(of)
		if cerr := of.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			fmt.Fprintln(stderr, werr)
			return finish(file, exitFail, stderr)
		}
	}
	for _, reason := range report.Reasons {
		fmt.Fprintln(stdout, reason)
	}
	passed := 0
	for _, rv := range report.Rows {
		if rv.Pass && !strings.HasPrefix(rv.Row, "C") {
			passed++
		}
	}
	counts := report.VerdictCounts()
	verdicts := make([]string, 0, len(counts))
	for verdict := range counts {
		verdicts = append(verdicts, verdict)
	}
	slices.Sort(verdicts)
	for i, verdict := range verdicts {
		verdicts[i] = fmt.Sprintf("%s %d", verdict, counts[verdict])
	}
	fmt.Fprintf(stdout, "judge: slots %s\n", strings.Join(verdicts, ", "))
	fmt.Fprintf(stdout, "judge: schedule sha12 %s\n", shadowvenue.ScheduleSHA12(slots))
	fmt.Fprintf(stdout, "judge: rows %d, rows passed %d, void %v, exit %d, table %s\n", len(report.Rows), passed, report.Void, report.Exit(), *out)
	return finish(file, report.Exit(), stderr)
}

// finish closes an output file and keeps code, unless the close fails on a step that passed: a lost write fails it.
func finish(file *os.File, code int, stderr io.Writer) int {
	if err := file.Close(); err != nil {
		fmt.Fprintf(stderr, "close %s: %v\n", file.Name(), err)
		if code == exitOK {
			return exitFail
		}
	}
	return code
}

// judgeEnforced judges a run with enforcement on against the verdicts file of the paired run with it off.
func judgeEnforced(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("judge-enforced", flag.ContinueOnError)
	fs.SetOutput(stderr)
	slotsPath := fs.String("slots", "", "slots file the enforced drive wrote")
	yardPath := fs.String("yardstick", "", "verdicts file of the paired run with enforcement off")
	yardDigest := fs.String("yardstick-digest", "", "file with the data digest taken before the yardstick run")
	yardDigestAfter := fs.String("yardstick-digest-after", "", "file with the data digest taken after the yardstick run")
	digestBefore := fs.String("digest-before", "", "file with the data digest taken before this run")
	digestAfter := fs.String("digest-after", "", "file with the data digest taken after this run")
	metricsBefore := fs.String("metrics-before", "", "go-api /metrics scrape before the drive")
	metricsAfter := fs.String("metrics-after", "", "go-api /metrics scrape after the drive")
	profiles := fs.String("profiles", "", "endpoint-profiles.go-api.json of the venue image; empty = the embedded one")
	out := fs.String("out", "", "per-row table to create, 0600")
	k := fs.Int("k", 3, "requests per cell")
	grace := fs.Duration("grace", 2200*time.Millisecond, "time after an answer in which its outside call may land")
	rowsPath := fs.String("rows", "", "row table of the drive (its ext column); empty = the embedded one")
	stubLog := fs.String("stub-log", "", "venue stub JSON-lines log over this run (required when an external row was sent)")
	yardOutbound := fs.String("yardstick-outbound", "", "outbound file of the yardstick run (judge -outbound-out; required when an external row was sent)")
	egressBefore := fs.String("egress-before", "", "egress record read before the yardstick run")
	egressBetween := fs.String("egress-between", "", "egress record read between the two runs")
	egressAfter := fs.String("egress-after", "", "egress record read after this run")
	seedLog := fs.String("seed-log", "", "the stub's seed-mode log (required when an external row was sent; an empty file is a log)")
	logPath := fs.String("log", "", "go-api JSON log of this run: the proof that an equal 5xx cell is past the guard")
	attributedPath := fs.String("attributed-requests", "", "file of the requests of the run that are no slot (method, route, source file:line, optional request id; tab-separated): the drive's own sign-in; any other request in the go-api log is unknown traffic")
	processPath := fs.String("process-facts", "", "file naming the go-api process of each metrics scrape (before and after: scrape time, container id, container start), tab-separated with a header: the proof that the two scrapes are one process that started before the drive")
	volatilePath := fs.String("volatile-keys", "", "owner-only frozen file of the volatile leaf paths per row (bodydiff -paths of legacy-vs-legacy pairs); with -keys-yardstick and -keys-enforced")
	keysYard := fs.String("keys-yardstick", "", "side file of the yardstick drive (the leaves of its answers by slot seq)")
	keysEnf := fs.String("keys-enforced", "", "side file of this run's drive")
	membershipBefore := fs.String("membership-before", "", "membership snapshot taken at most 5 minutes before the drive (required when a superuser-O slot ran)")
	membershipAfter := fs.String("membership-after", "", "membership snapshot taken right after the drive")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	read := func(name, path string) ([]byte, bool) {
		if path == "" {
			fmt.Fprintf(stderr, "judge-enforced: -%s is required\n", name)
			return nil, false
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(stderr, "-%s: %v\n", name, err)
			return nil, false
		}
		return raw, true
	}
	files := map[string][]byte{}
	for name, path := range map[string]string{"slots": *slotsPath, "yardstick": *yardPath, "yardstick-digest": *yardDigest, "yardstick-digest-after": *yardDigestAfter, "digest-before": *digestBefore,
		"digest-after": *digestAfter, "metrics-before": *metricsBefore, "metrics-after": *metricsAfter} {
		raw, ok := read(name, path)
		if !ok {
			return exitUsage
		}
		files[name] = raw
	}
	slots, err := shadowvenue.ParseSlots(files["slots"])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	yard, err := shadowvenue.ParseYardstick(files["yardstick"])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	profileBytes := authv1.GoAPIEndpointProfiles
	if *profiles != "" {
		if profileBytes, err = os.ReadFile(*profiles); err != nil {
			fmt.Fprintf(stderr, "-profiles: %v\n", err)
			return exitUsage
		}
	}
	namespaces, err := shadowvenue.RowNamespaces(profileBytes)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if *out == "" {
		fmt.Fprintln(stderr, "judge-enforced: -out is required")
		return exitUsage
	}
	outbound, err := readOutbound(*stubLog, *rowsPath, slots)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	outbound.Egress = readEgress(*egressBefore, *egressBetween, *egressAfter)
	outbound.Seed = readSeedLog(*seedLog)
	var yardOut []shadowvenue.OutboundSlot
	if *yardOutbound != "" {
		raw, err := os.ReadFile(*yardOutbound)
		if err != nil {
			fmt.Fprintf(stderr, "-yardstick-outbound: %v\n", err)
			return exitUsage
		}
		if yardOut, err = shadowvenue.ParseOutbound(raw); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	var evidence *shadowvenue.Evidence
	if *logPath != "" {
		logFile, err := os.Open(*logPath)
		if err != nil {
			fmt.Fprintf(stderr, "-log: %v\n", err)
			return exitUsage
		}
		defer logFile.Close()
		ev, err := shadowvenue.ParseLog(logFile)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		evidence = &ev
	}
	membership, err := readMembership(*membershipBefore, *membershipAfter)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	leafParity, err := readLeafParity(*volatilePath, *keysYard, *keysEnf)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if leafParity != nil {
		fmt.Fprintf(stdout, "judge-enforced: volatile keys sha12 %s, paths per row: %s\n", leafParity.Volatile.SHA12, leafParity.Volatile.PathsPerRow())
	}
	var attributed []shadowvenue.AttributedRequest
	if *attributedPath != "" {
		raw, err := os.ReadFile(*attributedPath)
		if err != nil {
			fmt.Fprintf(stderr, "-attributed-requests: %v\n", err)
			return exitUsage
		}
		if attributed, err = shadowvenue.ParseAttributed(raw); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	var process *shadowvenue.ProcessFacts
	if *processPath != "" {
		raw, err := os.ReadFile(*processPath)
		if err != nil {
			fmt.Fprintf(stderr, "-process-facts: %v\n", err)
			return exitUsage
		}
		pf, err := shadowvenue.ParseProcessFacts(raw)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		process = &pf
	}
	report := shadowvenue.JudgeEnforced(shadowvenue.EnforcedInput{J1: true, Attributed: attributed, Process: process, Strict: true, Membership: membership, Leaves: leafParity, Log: evidence, Slots: slots, Yard: yard, K: *k, Namespaces: namespaces,
		Grace: *grace, Outbound: outbound, YardOutbound: yardOut, YardOutboundGiven: *yardOutbound != "",
		YardDigest: strings.TrimSpace(string(files["yardstick-digest"])), YardDigestAfter: strings.TrimSpace(string(files["yardstick-digest-after"])), DigestBefore: strings.TrimSpace(string(files["digest-before"])),
		DigestAfter: strings.TrimSpace(string(files["digest-after"])), MetricsBefore: files["metrics-before"], MetricsAfter: files["metrics-after"]})
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "judge-enforced: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	if err := report.WriteTable(file); err != nil {
		fmt.Fprintln(stderr, err)
		return finish(file, exitUsage, stderr)
	}
	for _, reason := range report.Reasons {
		fmt.Fprintln(stdout, reason)
	}
	for _, note := range report.Notes {
		fmt.Fprintln(stdout, note)
	}
	counts := report.VerdictCounts()
	verdicts := make([]string, 0, len(counts))
	for verdict := range counts {
		verdicts = append(verdicts, verdict)
	}
	slices.Sort(verdicts)
	for i, verdict := range verdicts {
		verdicts[i] = fmt.Sprintf("%s %d", verdict, counts[verdict])
	}
	fmt.Fprintf(stdout, "judge-enforced: slots %s\n", strings.Join(verdicts, ", "))
	fmt.Fprintf(stdout, "judge-enforced: schedule sha12 %s\n", shadowvenue.ScheduleSHA12(slots))
	if line, ok := unjudgedRecordLine(files["metrics-before"], files["metrics-after"]); ok {
		fmt.Fprintln(stdout, line)
	}
	fmt.Fprintf(stdout, "judge-enforced: void %v, exit %d, table %s\n", report.Void, report.Exit(), *out)
	return finish(file, report.Exit(), stderr)
}

// unjudgedRecordLine is the record line of the unjudged counter: the org_required count the verdicts rest on, and
// every other reason summed; false when the scrapes give no counter delta.
func unjudgedRecordLine(before, after []byte) (string, bool) {
	sums, err := shadowvenue.UnjudgedTotals(before, after)
	if err != nil {
		return "", false
	}
	return fmt.Sprintf("judge-enforced: unjudged org_required %d, other %d", sums.OrgRequired, sums.Other), true
}

// readMembership reads the two membership snapshots; "" is a snapshot not given (the judge voids when a superuser-O
// slot ran without it).
// readLeafParity reads the frozen volatile file and the two side files; "" for all three gives none, any other mix is an
// error. The frozen file must be an owner-only regular file; its sha12 goes to the record line.
func readLeafParity(volatile, yard, enforced string) (*shadowvenue.LeafParity, error) {
	if volatile == "" && yard == "" && enforced == "" {
		return nil, nil
	}
	if volatile == "" || yard == "" || enforced == "" {
		return nil, errors.New("judge-enforced: -volatile-keys, -keys-yardstick and -keys-enforced go together")
	}
	info, err := os.Stat(volatile)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("-volatile-keys %s: want an owner-only (0600) regular file", volatile)
	}
	raw, err := os.ReadFile(volatile)
	if err != nil {
		return nil, fmt.Errorf("-volatile-keys: %v", errors.Unwrap(err))
	}
	frozen, err := shadowvenue.ParseFrozenVolatile(raw)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	frozen.SHA12 = hex.EncodeToString(sum[:])[:12]
	out := &shadowvenue.LeafParity{Volatile: frozen}
	for i, path := range []string{yard, enforced} {
		side, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("side file %s: %v", path, errors.Unwrap(err))
		}
		leaves, err := shadowvenue.ParseKeysFile(side)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			out.Yard = leaves
		} else {
			out.Enforced = leaves
		}
	}
	return out, nil
}

func readMembership(before, after string) (shadowvenue.Membership, error) {
	var out shadowvenue.Membership
	for flag, target := range map[string]struct {
		path string
		into **shadowvenue.MembershipSnapshot
	}{"membership-before": {before, &out.Before}, "membership-after": {after, &out.After}} {
		if target.path == "" {
			continue
		}
		raw, err := os.ReadFile(target.path)
		if err != nil {
			return out, fmt.Errorf("-%s: %w", flag, errors.Unwrap(err))
		}
		snap, err := shadowvenue.ParseMembershipSnapshot(raw)
		if err != nil {
			return out, fmt.Errorf("-%s: %w", flag, err)
		}
		*target.into = &snap
	}
	return out, nil
}

// mint signs the named principals in and writes one owner-only token file each; it prints a line per token with no token.
func mint(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("mint", flag.ContinueOnError)
	fs.SetOutput(stderr)
	principals := fs.String("principals", "", "venue principals file (kind name email uuid org role password_file)")
	names := fs.String("names", "", "comma-separated principal names to sign in, each with an org in the file")
	outDir := fs.String("out-dir", "", "existing owner-only (0700) directory for the <name>.token files")
	base := fs.String("base", "", "venue go-api origin: loopback, or go-api's venue-int IP named by -egress")
	egressPath := fs.String("egress", "", "egress record read just before this step (needed for a non-loopback -base)")
	runID := fs.String("run-id", "", "this step's run id; the -egress record must name it as run_id")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	raw, err := os.ReadFile(*principals)
	if err != nil {
		fmt.Fprintf(stderr, "-principals: %v\n", err)
		return exitUsage
	}
	venue, err := shadowvenue.ParsePrincipals(raw)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	var record shadowvenue.EgressRead
	if reads := readEgress(*egressPath); len(reads) == 1 {
		record = reads[0]
	}
	if _, err := shadowvenue.CheckVenueBase(*base, record, *runID); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	driver := &shadowvenue.Driver{Base: *base, Egress: record, RunID: *runID, Client: newClient()}
	results, err := shadowvenue.Mint(ctx, driver, venue, list(*names), *outDir)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitFail
	}
	for _, r := range results {
		fmt.Fprintln(stdout, shadowvenue.MintLine(r))
	}
	return exitOK
}

// bodydiff lists the cells whose normalized body differs between two slots files (a legacy-versus-legacy pair
// calibrates the pinned volatile set); it prints no body. Exit 1 when any cell differs.
func bodydiff(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bodydiff", flag.ContinueOnError)
	fs.SetOutput(stderr)
	a := fs.String("a", "", "first slots file")
	b := fs.String("b", "", "second slots file")
	keysA := fs.String("keys-a", "", "side file of the first drive (with -keys-b: name the differing leaf paths)")
	keysB := fs.String("keys-b", "", "side file of the second drive")
	pathsOnly := fs.Bool("paths", false, "print only the differing leaf paths as row<TAB>path lines, sorted and unique, for the frozen volatile file (needs -keys-a and -keys-b); the summary goes to stderr")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if (*keysA == "") != (*keysB == "") {
		fmt.Fprintln(stderr, "bodydiff: -keys-a and -keys-b go together")
		return exitUsage
	}
	if *pathsOnly && *keysA == "" {
		fmt.Fprintln(stderr, "bodydiff: -paths needs -keys-a and -keys-b")
		return exitUsage
	}
	var sets [2][]shadowvenue.Slot
	for i, path := range []string{*a, *b} {
		raw, err := os.ReadFile(path)
		if err != nil {
			fmt.Fprintf(stderr, "bodydiff: %v\n", errors.Unwrap(err))
			return exitUsage
		}
		if sets[i], err = shadowvenue.ParseSlots(raw); err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
	}
	if *pathsOnly {
		return bodydiffPaths(sets, *keysA, *keysB, stdout, stderr)
	}
	diffs, err := shadowvenue.BodyDiff(sets[0], sets[1])
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	for _, d := range diffs {
		fmt.Fprintln(stdout, d)
	}
	fmt.Fprintf(stdout, "bodydiff: %d cells differ\n", len(diffs))
	leafDiffs := 0
	if *keysA != "" {
		lines, err := leafDiff(sets, *keysA, *keysB)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitUsage
		}
		for _, l := range lines {
			fmt.Fprintln(stdout, l)
		}
		leafDiffs = len(lines)
		fmt.Fprintf(stdout, "bodydiff: %d cells with differing leaves\n", leafDiffs)
	}
	if len(diffs) > 0 || leafDiffs > 0 {
		return exitFail
	}
	return exitOK
}

// bodykey writes a new random per-window key for the per-leaf HMACs; it prints no key.
func bodykey(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("bodykey", flag.ContinueOnError)
	fs.SetOutput(stderr)
	out := fs.String("out", "", "new key file, 0600")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	if *out == "" {
		fmt.Fprintln(stderr, "bodykey: -out is required")
		return exitUsage
	}
	if err := shadowvenue.NewBodyKey(*out); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	return exitOK
}

// leafDiff lists, per cell of the pair, the leaf paths whose HMACs differ or that one side lacks: paths only.
// bodydiffPaths prints the differing leaf paths as row<TAB>path lines on stdout and the count on stderr.
func bodydiffPaths(sets [2][]shadowvenue.Slot, keysA, keysB string, stdout, stderr io.Writer) int {
	lines, err := leafPathLines(sets, keysA, keysB)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	for _, l := range lines {
		fmt.Fprintln(stdout, l)
	}
	fmt.Fprintf(stderr, "bodydiff: %d differing leaf paths\n", len(lines))
	if len(lines) > 0 {
		return exitFail
	}
	return exitOK
}

// leafPathLines reads the two side files and gives the differing leaf paths of the cells as row<TAB>path lines.
func leafPathLines(sets [2][]shadowvenue.Slot, pathA, pathB string) ([]string, error) {
	var leaves [2]map[int][]shadowvenue.Leaf
	for i, path := range []string{pathA, pathB} {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("bodydiff: %v", errors.Unwrap(err))
		}
		if leaves[i], err = shadowvenue.ParseKeysFile(raw); err != nil {
			return nil, err
		}
	}
	return shadowvenue.DiffPathLines(sets[0], sets[1], leaves[0], leaves[1]), nil
}

func leafDiff(sets [2][]shadowvenue.Slot, pathA, pathB string) ([]string, error) {
	var leaves [2]map[int][]shadowvenue.Leaf
	for i, path := range []string{pathA, pathB} {
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("bodydiff: %v", errors.Unwrap(err))
		}
		if leaves[i], err = shadowvenue.ParseKeysFile(raw); err != nil {
			return nil, err
		}
	}
	type cell struct {
		row, caller string
		k           int
	}
	seqB := map[cell]shadowvenue.Slot{}
	for _, s := range sets[1] {
		seqB[cell{s.Row, s.Caller, s.K}] = s
	}
	var lines []string
	for _, sa := range sets[0] {
		sb, ok := seqB[cell{sa.Row, sa.Caller, sa.K}]
		if !ok || sa.Gap() {
			continue
		}
		if paths := shadowvenue.KeyDiff(leaves[0][sa.Seq], leaves[1][sb.Seq]); len(paths) > 0 {
			lines = append(lines, fmt.Sprintf("row %s\t%s\tk=%d\t%s\tleaves: %s", sa.Row, sa.Caller, sa.K, sa.Key(), strings.Join(paths, ",")))
		}
	}
	slices.Sort(lines)
	return lines, nil
}
