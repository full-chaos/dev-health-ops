package policy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/auth/policycatalog"
)

// Closed label value sets of the shadow counters.
var (
	shadowNamespaces = []string{"admin", "billing", "integrations", "license", "members", "settings", "teams"}
	shadowCallers    = []string{"member", "admin", "superuser"}
	shadowClasses    = []string{"legacy_bug", "new_bug", "unresolved", "intentional"}
	shadowResults    = []string{"match", "mismatch", "error", "dropped", "skipped", "impersonation_refused"}
)

const (
	classUnresolved = "unresolved"

	resultMatch    = "match"
	resultMismatch = "mismatch"
	resultError    = "error"
	resultDropped  = "dropped"
	resultSkipped  = "skipped"
	// resultImpersonationRefused: a caller with a live go-api impersonation row, which enforcement refuses unasked.
	resultImpersonationRefused = "impersonation_refused"

	shadowDefaultInFlight = 32
	shadowDefaultTimeout  = 2 * time.Second
	shadowPlatformKey     = "platform"
	shadowClassHeader     = "legacy\tpdp\treason\tnamespace\tclass\tnote"
)

// ShadowRow is one go-api endpoint-profile row that names a catalog action.
type ShadowRow struct {
	Action       string
	Namespace    string
	ResourceType string
	HighRisk     bool
}

func (row ShadowRow) valid() bool {
	return row.Action != "" && slices.Contains(shadowNamespaces, row.Namespace) &&
		(row.ResourceType == policycatalog.ScopeOrganization || row.ResourceType == policycatalog.ScopePlatform)
}

func (row ShadowRow) matchesCatalog() bool {
	action, err := policycatalog.Default().Action(row.Action)
	return err == nil && row == shadowRowOf(action)
}

func shadowRowOf(action policycatalog.Action) ShadowRow {
	return ShadowRow{Action: action.ID, Namespace: strings.SplitN(action.ID, ".", 2)[0], ResourceType: action.ResourceKind,
		HighRisk: action.RiskClass == policycatalog.RiskHigh}
}

// ParseShadowRows reads the protected rows of service that name a catalog action, keyed by the
// route template "METHOD /route" the mux reports as Request.Pattern.
func ParseShadowRows(raw []byte, service string) (map[string]ShadowRow, error) {
	var document struct {
		Rows []struct {
			Service        string `json:"service"`
			Method         string `json:"method"`
			Route          string `json:"route"`
			Action         string `json:"action"`
			Classification string `json:"classification"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("shadow rows: %w", err)
	}
	rows := map[string]ShadowRow{}
	for _, entry := range document.Rows {
		if entry.Service != service || entry.Action == "" || entry.Classification != "protected" {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("shadow rows: a row has no method or route")
		}
		key := entry.Method + " " + entry.Route
		if _, seen := rows[key]; seen {
			return nil, fmt.Errorf("shadow rows: %s has two rows", key)
		}
		action, err := policycatalog.Default().Action(entry.Action)
		if err != nil {
			return nil, fmt.Errorf("shadow rows: %s: %w", key, err)
		}
		row := shadowRowOf(action)
		if !row.valid() {
			return nil, fmt.Errorf("shadow rows: %s: action %s is outside the shadow's namespaces or resource kinds", key, action.ID)
		}
		rows[key] = row
	}
	if len(rows) == 0 {
		return nil, errors.New("shadow rows: no row names an action")
	}
	return rows, nil
}

// ShadowClassRule is one row of the mismatch class table. "*" in Reason or Namespace matches any.
type ShadowClassRule struct {
	Legacy, PDP, Reason, Namespace, Class string
}

// LoadShadowClasses parses a class table: a header line, then one row per line of six tab-separated
// fields (legacy, pdp, reason, namespace, class, note). The first row that matches wins; a pair no row
// matches is "unresolved".
func LoadShadowClasses(raw []byte) ([]ShadowClassRule, error) {
	wire := map[string]bool{}
	for _, reason := range pep.WireReasons() {
		wire[string(reason)] = true
	}
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var rules []ShadowClassRule
	for line := 1; scanner.Scan(); line++ {
		text := scanner.Text()
		if line == 1 {
			if text != shadowClassHeader {
				return nil, errors.New("shadow classes: the first line is not the header")
			}
			continue
		}
		if strings.TrimSpace(text) == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 6 {
			return nil, fmt.Errorf("shadow classes: line %d has %d fields, want 6", line, len(fields))
		}
		rule := ShadowClassRule{Legacy: fields[0], PDP: fields[1], Reason: fields[2], Namespace: fields[3], Class: fields[4]}
		switch {
		case rule.Legacy != "allow" && rule.Legacy != "deny", rule.PDP != "allow" && rule.PDP != "deny":
			return nil, fmt.Errorf("shadow classes: line %d: legacy and pdp are allow or deny", line)
		case rule.Reason != "*" && !wire[rule.Reason]:
			return nil, fmt.Errorf("shadow classes: line %d: reason is not a PDP reason code", line)
		case rule.Namespace != "*" && !slices.Contains(shadowNamespaces, rule.Namespace):
			return nil, fmt.Errorf("shadow classes: line %d: unknown namespace", line)
		case !slices.Contains(shadowClasses, rule.Class):
			return nil, fmt.Errorf("shadow classes: line %d: class is not in the closed set", line)
		case strings.TrimSpace(fields[5]) == "":
			return nil, fmt.Errorf("shadow classes: line %d: a rule needs its note", line)
		}
		rules = append(rules, rule)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("shadow classes: %w", err)
	}
	if len(raw) == 0 {
		return nil, errors.New("shadow classes: empty table")
	}
	return rules, nil
}

// ShadowConfig builds a Shadow.
type ShadowConfig struct {
	// Decider asks the policy decision point; a *pep.Client in production.
	Decider pep.Decider
	// Rows is the action map, keyed by "METHOD /route".
	Rows map[string]ShadowRow
	// Classes is a class table; nil reads the embedded one.
	Classes []byte
	// HighRisk lets high-risk rows ask the PDP, which writes an audit row per decision.
	HighRisk bool
	// SamplePercent is the share of covered requests that ask, 1 to 100; zero is 100.
	SamplePercent int
	// MaxInFlight bounds the calls in flight; a call over it is dropped. Zero is 32.
	MaxInFlight int
	// Timeout bounds one call. Zero is 2s.
	Timeout time.Duration
	Logger  *slog.Logger
}

// Shadow asks the policy decision point beside the legacy guard and compares its answer with
// the guard's. It never changes a response: Observe has no result, runs the call in its own
// goroutine, and swallows every failure.
type Shadow struct {
	decider  pep.Decider
	rows     map[string]ShadowRow
	classes  []ShadowClassRule
	highRisk bool
	percent  uint64
	timeout  time.Duration
	slots    chan struct{}
	logger   *slog.Logger

	seen    atomic.Uint64
	logGate [5]atomic.Int64 // one per class, then the error line
	wg      sync.WaitGroup
}

// NewShadow checks cfg.
func NewShadow(cfg ShadowConfig) (*Shadow, error) {
	if cfg.Decider == nil {
		return nil, errors.New("shadow: no decider")
	}
	if len(cfg.Rows) == 0 {
		return nil, errors.New("shadow: no rows")
	}
	for key, row := range cfg.Rows {
		if !row.valid() {
			return nil, fmt.Errorf("shadow: row %q is outside the closed namespaces or resource kinds", key)
		}
		if !row.matchesCatalog() {
			return nil, fmt.Errorf("shadow: row %q differs from its catalog entry", key)
		}
	}
	if cfg.SamplePercent < 0 || cfg.SamplePercent > 100 {
		return nil, errors.New("shadow: the sample percent is 1 to 100")
	}
	table := cfg.Classes
	if table == nil {
		table = shadowClassTable
	}
	classes, err := LoadShadowClasses(table)
	if err != nil {
		return nil, err
	}
	s := &Shadow{decider: cfg.Decider, rows: cfg.Rows, classes: classes, highRisk: cfg.HighRisk, percent: uint64(cfg.SamplePercent),
		timeout: cfg.Timeout, logger: cfg.Logger}
	if s.percent == 0 {
		s.percent = 100
	}
	if s.timeout <= 0 {
		s.timeout = shadowDefaultTimeout
	}
	inFlight := cfg.MaxInFlight
	if inFlight <= 0 {
		inFlight = shadowDefaultInFlight
	}
	s.slots = make(chan struct{}, inFlight)
	if s.logger == nil {
		s.logger = slog.Default()
	}
	return s, nil
}

// Callers are the closed kinds of the counter's caller label.
const (
	ShadowCallerMember    = "member"
	ShadowCallerAdmin     = "admin"
	ShadowCallerSuperuser = "superuser"
)

// ShadowObservation is what a service tells the shadow about one request its legacy check has
// already decided. It holds values only, and no go-api type: any service can fill it.
type ShadowObservation struct {
	// Pattern is the route template "METHOD /route" that keys the row.
	Pattern string
	// Bearer is the end user's access token, the PDP's principal reference. Never logged.
	Bearer string
	// OrgID is the caller's effective organization; empty when it has none.
	OrgID string
	// Caller is one of the ShadowCaller kinds.
	Caller string
	// UserAccessToken is true when Bearer is a user access token the PDP can resolve.
	UserAccessToken bool
	// Impersonating is true while a live go-api impersonation row is in force for the caller's own bearer. The PDP
	// would judge that bearer as the admin, not the delegation, so such a call is never asked (stop route aside).
	Impersonating bool
	// LegacyAllowed is the legacy check's answer.
	LegacyAllowed bool
}

// shadowCall is everything the goroutine needs: values only, no request.
type shadowCall struct {
	ctx           context.Context
	request       pep.Request
	row           ShadowRow
	pattern       string
	caller        string
	legacyAllowed bool
}

// Observe records how the PDP would have decided a request the legacy check has already decided.
// A service calls it once, after its decision and before it answers; it returns at once and has no
// result. A nil Shadow is off.
func (s *Shadow) Observe(ctx context.Context, o ShadowObservation) {
	if s == nil {
		return
	}
	defer func() { _ = recover() }()
	row, ok := s.rows[o.Pattern]
	if !ok {
		return
	}
	if s.percent < 100 && s.seen.Add(1)%100 >= s.percent {
		return
	}
	if o.Impersonating && o.Pattern != stopImpersonationPattern {
		countShadowResult(ctx, resultImpersonationRefused)
		return
	}
	if row.HighRisk && !s.highRisk {
		countShadowResult(ctx, resultSkipped)
		return
	}
	org := strings.ToLower(o.OrgID)
	if o.Bearer == "" || !o.UserAccessToken || (row.ResourceType == policycatalog.ScopeOrganization && org == "") {
		countShadowResult(ctx, resultSkipped)
		return
	}
	request := pep.Request{PrincipalReference: o.Bearer, Action: row.Action, ResourceType: row.ResourceType, ResourceKey: shadowPlatformKey}
	if row.ResourceType == policycatalog.ScopeOrganization {
		request.ResourceKey, request.OrganizationID = org, org
	}
	select {
	case s.slots <- struct{}{}:
	default:
		countShadowResult(ctx, resultDropped)
		return
	}
	caller := o.Caller
	if !slices.Contains(shadowCallers, caller) {
		caller = ShadowCallerMember
	}
	call := shadowCall{ctx: context.WithoutCancel(ctx), request: request, row: row, pattern: o.Pattern, caller: caller, legacyAllowed: o.LegacyAllowed}
	s.wg.Add(1)
	go s.run(call)
}

func (s *Shadow) run(call shadowCall) {
	defer s.wg.Done()
	defer func() { <-s.slots }()
	defer func() {
		if recover() != nil {
			countShadowResult(call.ctx, resultError)
		}
	}()
	ctx, cancel := context.WithTimeout(call.ctx, s.timeout)
	defer cancel()
	decision := s.decider.Decide(ctx, call.request)
	if decision.Local() {
		countShadowResult(call.ctx, resultError)
		if s.logAllowed(len(shadowClasses)) {
			s.logger.WarnContext(call.ctx, "api shadow pdp: no verdict", slog.String("namespace", call.row.Namespace),
				slog.String("caller", call.caller), slog.String("reason", string(decision.Reason())))
		}
		return
	}
	if decision.Allowed() == call.legacyAllowed {
		countShadowResult(call.ctx, resultMatch)
		return
	}
	class := s.classify(call.legacyAllowed, decision, call.row.Namespace)
	countShadowResult(call.ctx, resultMismatch)
	countShadowMismatch(call.ctx, call.row.Namespace, call.caller, class)
	if s.logAllowed(slices.Index(shadowClasses, class)) {
		s.logger.WarnContext(call.ctx, "api shadow pdp: mismatch with the legacy guard",
			slog.String("namespace", call.row.Namespace), slog.String("caller", call.caller), slog.String("class", class),
			slog.String("action", call.row.Action), slog.String("route", call.pattern),
			slog.String("legacy", verdictWord(call.legacyAllowed)), slog.String("shadow", verdictWord(decision.Allowed())),
			slog.String("reason_code", string(decision.Reason())), slog.String("decision_id", decision.DecisionID()))
	}
}

func verdictWord(allowed bool) string {
	if allowed {
		return "allow"
	}
	return "deny"
}

func (s *Shadow) classify(legacyAllowed bool, decision pep.Decision, namespace string) string {
	legacy, shadow, reason := verdictWord(legacyAllowed), verdictWord(decision.Allowed()), string(decision.Reason())
	for _, rule := range s.classes {
		if rule.Legacy == legacy && rule.PDP == shadow && (rule.Reason == "*" || rule.Reason == reason) &&
			(rule.Namespace == "*" || rule.Namespace == namespace) {
			return rule.Class
		}
	}
	return classUnresolved
}

// logAllowed lets one log line per second through for each gate: a class or the error line.
func (s *Shadow) logAllowed(gate int) bool {
	if gate < 0 || gate >= len(s.logGate) {
		return false
	}
	now := time.Now().UnixNano()
	last := s.logGate[gate].Load()
	return now-last >= int64(time.Second) && s.logGate[gate].CompareAndSwap(last, now)
}
