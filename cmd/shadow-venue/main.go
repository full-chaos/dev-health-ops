// Command shadow-venue drives go-api's shadow decision point on the bigboy compose venue and judges each row from
// go-api's log (internal/shadowvenue). It sends to a loopback origin only.
//
//	shadow-venue plan  -principals <tsv> [-rows <tsv>] [-profiles <json>] [-throwaway-org C] [-k 3] [-allow-external a,b] [-destructive]
//	shadow-venue drive -base http://127.0.0.1:8093 -principals <tsv> -out <slots.tsv> [plan flags] [-gap 2.5s]
//	shadow-venue judge -slots <slots.tsv> -log <go-api log> -out <table.tsv> [-k 3] [-grace 2.2s] [-rulings <tsv>]
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
	"os"
	"os/signal"
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
	throwaway := fs.String("throwaway-org", "C", "org label of the throwaway org, the only org destructive rows act in")
	k := fs.Int("k", 3, "requests per cell")
	allow := fs.String("allow-external", "", "comma-separated outside services a request may reach (stripe, pagerduty, github, mail)")
	base := fs.String("base", "", "venue go-api origin, loopback only (drive)")
	out := fs.String("out", "", "slots file to create, 0600 (drive)")
	gap := fs.Duration("gap", 2500*time.Millisecond, "wait after each answer (drive)")
	destructive := fs.Bool("destructive", false, "send the destructive rows (last, throwaway org only); without it they are gaps")
	if err := fs.Parse(args); err != nil {
		return exitUsage
	}
	plan, venue, err := makePlan(*principals, *rowsPath, *profiles, *throwaway, *allow, *k, *destructive)
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
	if *out == "" {
		fmt.Fprintln(stderr, "drive: -out is required")
		return exitUsage
	}
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "drive: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	defer file.Close()
	driver := &shadowvenue.Driver{Base: origin, Client: newClient(), Gap: *gap}
	tokens := map[string]string{}
	for _, s := range plan.Slots {
		if s.Gap() || tokens[s.Principal] != "" {
			continue
		}
		p, _ := venue.Principal(s.Principal)
		token, err := driver.Login(ctx, p, venue.Orgs[p.Org])
		if err != nil {
			fmt.Fprintln(stderr, err)
			return exitFail
		}
		tokens[s.Principal] = token
	}
	fmt.Fprintf(stdout, "drive: %d slots, gap %s, start %s\n", len(plan.Slots), *gap, time.Now().UTC().Format(time.RFC3339))
	if err := driver.Drive(ctx, plan, tokens, file); err != nil {
		fmt.Fprintln(stderr, err)
		return exitFail
	}
	if err := file.Sync(); err != nil {
		fmt.Fprintln(stderr, err)
		return exitFail
	}
	fmt.Fprintf(stdout, "drive: done %s, slots in %s\n", time.Now().UTC().Format(time.RFC3339), *out)
	return exitOK
}

// newClient sends on the no-proxy outbound transport and never follows a redirect: a bearer reaches only the venue
// origin.
func newClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second, Transport: httpguard.Transport(), CheckRedirect: noRedirect}
}

func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

func makePlan(principalsPath, rowsPath, profilesPath, throwaway, allow string, k int, destructive bool) (shadowvenue.Plan, shadowvenue.Venue, error) {
	rawPrincipals, err := os.ReadFile(principalsPath)
	if err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, fmt.Errorf("-principals: %w", err)
	}
	venue, err := shadowvenue.ParsePrincipals(rawPrincipals)
	if err != nil {
		return shadowvenue.Plan{}, shadowvenue.Venue{}, err
	}
	rawRows := shadowvenue.DefaultRows
	if rowsPath != "" {
		if rawRows, err = os.ReadFile(rowsPath); err != nil {
			return shadowvenue.Plan{}, shadowvenue.Venue{}, fmt.Errorf("-rows: %w", err)
		}
	}
	rows, err := shadowvenue.ParseRows(rawRows)
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
	var external []string
	if allow != "" {
		external = strings.Split(allow, ",")
	}
	plan, err := shadowvenue.MakePlan(rows, venue, shadowvenue.PlanOptions{Throwaway: throwaway, AllowExternal: external, K: k, Destructive: destructive})
	return plan, venue, err
}

func judge(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("judge", flag.ContinueOnError)
	fs.SetOutput(stderr)
	slotsPath := fs.String("slots", "", "slots file drive wrote")
	logPath := fs.String("log", "", "go-api JSON log of the run (docker logs --since <drive start>)")
	rulingsPath := fs.String("rulings", "", "mismatch tuples with a passing pm verdict")
	out := fs.String("out", "", "per-row table to create, 0600")
	k := fs.Int("k", 3, "requests per cell")
	grace := fs.Duration("grace", 2200*time.Millisecond, "time after an answer in which its result line may land")
	if err := fs.Parse(args); err != nil {
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
	report := shadowvenue.Judge(slots, ev, rulings, shadowvenue.JudgeOptions{K: *k, Grace: *grace})
	file, err := os.OpenFile(*out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		fmt.Fprintf(stderr, "judge: -out %s: %v\n", *out, errors.Unwrap(err))
		return exitUsage
	}
	defer file.Close()
	if err := report.WriteTable(file); err != nil {
		fmt.Fprintln(stderr, err)
		return exitUsage
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
	fmt.Fprintf(stdout, "judge: rows %d, rows passed %d, void %v, exit %d, table %s\n", len(report.Rows), passed, report.Void, report.Exit(), *out)
	return report.Exit()
}
