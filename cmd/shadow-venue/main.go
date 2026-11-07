// Command shadow-venue drives go-api's shadow decision point on the bigboy compose venue and judges each row from
// go-api's log (internal/shadowvenue). It sends to a loopback origin only.
//
//	shadow-venue plan  -principals <tsv> [-rows <tsv>] [-profiles <json>] [-only-rows 1,22] [-k 3] [-allow-external a,b]
//	                   [-destructive -throwaway-pool T1,T2,T3 -throwaway-ids <tsv>] [-throwaway-org T] [-states <tsv, 0600>]
//	shadow-venue drive -base http://127.0.0.1:8093 -principals <tsv> -out <slots.tsv> [plan flags] [-gap 2.5s]
//	shadow-venue judge -slots <slots.tsv> -log <go-api log> -out <table.tsv> [-k 3] [-grace 2.2s] [-rulings <tsv>]
//	                   [-rows <tsv>] [-stub-log <jsonl>] [-outbound-out <tsv>] [-egress-before <txt> -egress-between <txt>]
//	shadow-venue judge-enforced ... [-stub-log <jsonl> -yardstick-outbound <tsv>] [-egress-before -egress-between -egress-after]
//
// Exit: 0 every row passes (judge) or the step finished; 1 a row fails; 2 usage or input; 3 the run is void.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
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
		fmt.Fprintln(stderr, "usage: shadow-venue plan|drive|judge [flags]")
		return exitUsage
	}
	switch args[0] {
	case "plan", "drive":
		return planOrDrive(ctx, args[0], args[1:], stdout, stderr)
	case "judge":
		return judge(args[1:], stdout, stderr)
	case "judge-enforced":
		return judgeEnforced(args[1:], stdout, stderr)
	}
	fmt.Fprintf(stderr, "shadow-venue: unknown step %q\n", args[0])
	return exitUsage
}

func planOrDrive(ctx context.Context, step string, args []string, stdout, stderr io.Writer) int {
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
	base := fs.String("base", "", "venue go-api origin, loopback only (drive)")
	out := fs.String("out", "", "slots file to create, 0600 (drive)")
	gap := fs.Duration("gap", 2500*time.Millisecond, "wait after each answer (drive)")
	loginOnly := fs.Bool("login-only", false, "sign each principal of the plan in and stop: no row request is sent, no slots file is written (drive)")
	destructive := fs.Bool("destructive", false, "send the destructive rows (last, throwaway org only); without it they are gaps")
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
	origin, err := shadowvenue.CheckBase(*base)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
	}
	if *out == "" && !*loginOnly {
		fmt.Fprintln(stderr, "drive: -out is required")
		return exitUsage
	}
	if *loginOnly {
		return loginPrincipals(ctx, origin, plan, venue, stdout, stderr)
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "drive: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	driver := &shadowvenue.Driver{Base: origin, Client: newClient(), Gap: *gap, Frozen: frozen}
	tokens := map[string]string{}
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
func loginPrincipals(ctx context.Context, origin *url.URL, plan shadowvenue.Plan, venue shadowvenue.Venue, stdout, stderr io.Writer) int {
	driver := &shadowvenue.Driver{Base: origin, Client: newClient()}
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
	ob := shadowvenue.Outbound{Ext: shadowvenue.RowExt(rows)}
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
	report := shadowvenue.Judge(slots, ev, rulings, shadowvenue.JudgeOptions{K: *k, Grace: *grace, NoOrg: noOrg, Outbound: outbound})
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
	report := shadowvenue.JudgeEnforced(shadowvenue.EnforcedInput{Slots: slots, Yard: yard, K: *k, Namespaces: namespaces,
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
	fmt.Fprintf(stdout, "judge-enforced: void %v, exit %d, table %s\n", report.Void, report.Exit(), *out)
	return finish(file, report.Exit(), stderr)
}
