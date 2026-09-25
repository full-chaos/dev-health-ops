package pushcli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Exit codes the network verbs add (output.py): 3 transport or API error after the
// retries, or stream_unavailable; 4 poll timeout.
const (
	exitTransport   = 3
	exitPollTimeout = 4
)

// Polling defaults and the floor of --poll-interval (poll.py, cli.py).
const (
	defaultPollInterval = 5.0
	defaultPollTimeout  = 300.0
	minPollInterval     = 0.5
	// absoluteMaxBodyBytes is limits.py's ABSOLUTE_MAX_BODY_BYTES: no server-reported
	// limit makes the payload read larger than this.
	absoluteMaxBodyBytes = 100_000_000
)

// terminalStatuses is poll.py's TERMINAL_STATUSES.
var terminalStatuses = map[string]bool{"completed": true, "partial": true, "failed": true}

// lookup is the value of an environment variable, "" when unset.
func lookup(env cli.Env, key string) string {
	if env.Lookup == nil {
		return ""
	}
	value, _ := env.Lookup(key)
	return value
}

// connectionFlags are the flags of both network verbs.
type connectionFlags struct {
	apiURL, token, org *string
	poll, asJSON       *bool
	interval, timeout  *pollFloat
}

// pollFloat is an argparse float option: a Python float() that is finite and
// positive, and (for the interval) not below the floor.
type pollFloat struct {
	value   float64
	floor   float64
	name    string
	changed bool
}

func (p *pollFloat) String() string { return strconv.FormatFloat(p.value, 'g', -1, 64) }

func (p *pollFloat) Set(text string) error {
	parsed, ok := pythonparity.ParseFloat(text)
	if !ok {
		return fmt.Errorf("invalid float value: %s", pythonparity.StrRepr(text))
	}
	if math.IsInf(parsed, 0) || math.IsNaN(parsed) || parsed <= 0 {
		return fmt.Errorf("must be a finite positive number, got %s", pythonparity.StrRepr(text))
	}
	if p.floor > 0 && parsed < p.floor {
		return fmt.Errorf("--%s must be >= %s, got %s", p.name, strconv.FormatFloat(p.floor, 'g', -1, 64), pythonparity.StrRepr(text))
	}
	p.value, p.changed = parsed, true
	return nil
}

func addConnectionFlags(flags *flag.FlagSet) *connectionFlags {
	c := &connectionFlags{
		apiURL:   flags.String("api-url", "", "FullChaos API base URL (env FULLCHAOS_API_URL)"),
		token:    flags.String("token", "", "ingest token (env FULLCHAOS_INGEST_TOKEN, deprecated alias FULLCHAOS_API_TOKEN)"),
		org:      flags.String("org", "", "organization ID (env FULLCHAOS_ORG_ID)"),
		poll:     flags.Bool("poll", false, "poll GET /batches/{id} until a terminal status"),
		asJSON:   flags.Bool("json", false, "emit machine-readable JSON to stdout"),
		interval: &pollFloat{value: defaultPollInterval, floor: minPollInterval, name: "poll-interval"},
		timeout:  &pollFloat{value: defaultPollTimeout, name: "poll-timeout"},
	}
	flags.Var(c.interval, "poll-interval", "seconds between polls (minimum 0.5)")
	flags.Var(c.timeout, "poll-timeout", "give up polling after this many seconds")
	return c
}

// resolveConfig is _resolve_client_config: flag, then environment; the API URL
// loses its trailing slashes. ok false means the usage message was printed.
func resolveConfig(env cli.Env, c *connectionFlags) (clientConfig, bool) {
	apiURL := *c.apiURL
	if apiURL == "" {
		apiURL = lookup(env, "FULLCHAOS_API_URL")
	}
	apiURL = strings.TrimRight(apiURL, "/")
	token := *c.token
	if token == "" {
		token = lookup(env, "FULLCHAOS_INGEST_TOKEN")
	}
	if token == "" {
		if legacy := lookup(env, "FULLCHAOS_API_TOKEN"); legacy != "" {
			slog.Warn("FULLCHAOS_API_TOKEN is a deprecated alias; set FULLCHAOS_INGEST_TOKEN instead")
			token = legacy
		}
	}
	org := *c.org
	if org == "" {
		org = lookup(env, "FULLCHAOS_ORG_ID")
	}
	var missing []string
	for _, item := range [][2]string{{"--api-url / FULLCHAOS_API_URL", apiURL}, {"--token / FULLCHAOS_INGEST_TOKEN", token}, {"--org / FULLCHAOS_ORG_ID", org}} {
		if item[1] == "" {
			missing = append(missing, item[0])
		}
	}
	if len(missing) > 0 {
		fmt.Fprintf(env.Stderr, "error: missing required: %s\n", strings.Join(missing, ", "))
		return clientConfig{}, false
	}
	return clientConfig{apiURL: apiURL, token: token, orgID: org}, true
}

// batchLimits is BatchLimits.
type batchLimits struct {
	maxRecords *big.Int
	maxBytes   int64
	// recordsText and bytesText are how the limits print: a limit the server
	// sent as JSON true is the Python bool, which prints "True".
	recordsText, bytesText string
}

func defaultLimits() batchLimits {
	return batchLimits{maxRecords: big.NewInt(maxRecordsDefault), maxBytes: maxBodyBytesDefault, recordsText: strconv.Itoa(maxRecordsDefault), bytesText: strconv.Itoa(maxBodyBytesDefault)}
}

// pythonPositiveInt is isinstance(value, int) and value > 0 (a bool is an int).
func pythonPositiveInt(value pyjson.Value) (*big.Int, bool) {
	switch typed := value.(type) {
	case pyjson.Int:
		if typed.Int != nil && typed.Sign() > 0 {
			return typed.Int, true
		}
	case bool:
		if typed {
			return big.NewInt(1), true
		}
	}
	return nil, false
}

// limitsFromSchema is limits_from_schema_response: the server's limits field by
// field, the defaults for anything missing or malformed, the body limit never
// above the absolute ceiling.
func limitsFromSchema(document *pyjson.Object) batchLimits {
	limits := defaultLimits()
	if document == nil {
		return limits
	}
	raw, _ := document.Get("limits")
	object, ok := raw.(*pyjson.Object)
	if !ok {
		return limits
	}
	if records, present := object.Get("maxRecordsPerBatch"); present {
		if value, valid := pythonPositiveInt(records); valid {
			limits.maxRecords, limits.recordsText = value, pyjson.Str(records)
		}
	}
	if bytesValue, present := object.Get("maxBodyBytes"); present {
		if value, valid := pythonPositiveInt(bytesValue); valid {
			if value.Cmp(big.NewInt(absoluteMaxBodyBytes)) > 0 {
				limits.maxBytes, limits.bytesText = absoluteMaxBodyBytes, strconv.Itoa(absoluteMaxBodyBytes)
			} else {
				limits.maxBytes, limits.bytesText = value.Int64(), pyjson.Str(bytesValue)
			}
		}
	}
	return limits
}

// checkEnvelopeShape is check_envelope_shape: parse, schema version, batch size,
// body size, then unknown record kinds; not the per-record field validation.
func checkEnvelopeShape(raw []byte, limits batchLimits) (idempotencyKey string, errs []pyjson.Value) {
	envelope, err := parseEnvelope(raw)
	if err != nil {
		var failure *parseFailure
		if errors.As(err, &failure) {
			if len(failure.errors) > 0 {
				return "", failure.errors
			}
			return "", []pyjson.Value{errorItem(-1, nil, "invalid_envelope", failure.message, nil)}
		}
		return "", []pyjson.Value{errorItem(-1, nil, "invalid_envelope", err.Error(), nil)}
	}
	if envelope.SchemaVersion != schemaVersion {
		return "", []pyjson.Value{errorItem(-1, nil, "unsupported_schema_version",
			"Unsupported schemaVersion: "+pythonparity.StrRepr(envelope.SchemaVersion), "schemaVersion")}
	}
	if big.NewInt(int64(len(envelope.Records))).Cmp(limits.maxRecords) > 0 {
		return "", []pyjson.Value{errorItem(-1, nil, "batch_too_large",
			fmt.Sprintf("Batch has %d records; max is %s", len(envelope.Records), limits.recordsText), "records")}
	}
	if int64(len(raw)) > limits.maxBytes {
		return "", []pyjson.Value{errorItem(-1, nil, "payload_too_large",
			fmt.Sprintf("Payload is %d bytes; max is %s", len(raw), limits.bytesText), nil)}
	}
	known := map[string]bool{}
	for _, kind := range recordKinds() {
		known[kind] = true
	}
	var unknown []pyjson.Value
	for index, record := range envelope.Records {
		if !known[record.Kind] {
			unknown = append(unknown, errorItem(index, record.Kind, "unknown_record_kind",
				fmt.Sprintf("Unknown record kind at index %d: %s", index, pythonparity.StrRepr(record.Kind)),
				fmt.Sprintf("records[%d].kind", index)))
		}
	}
	if len(unknown) > 0 {
		return "", unknown
	}
	return envelope.IdempotencyKey, nil
}

// emitJSONValue is emit_json for any decoded value.
func emitJSONValue(env cli.Env, value pyjson.Value) {
	text, err := dumps(value, true, false)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return
	}
	fmt.Fprintln(env.Stdout, text)
}

// pythonInt is isinstance(value, int) for a decoded value, with its number.
func pythonInt(value pyjson.Value) (*big.Int, bool) {
	switch typed := value.(type) {
	case pyjson.Int:
		return typed.Int, true
	case bool:
		if typed {
			return big.NewInt(1), true
		}
		return big.NewInt(0), true
	}
	return nil, false
}

// serverRejectionTable is emit_rejection_table over a value the server sent: a
// list of objects prints as validate's table does; any other shape is where the
// Python code raises, after the lines it had already printed.
func serverRejectionTable(env cli.Env, errs pyjson.Value) error {
	if !pyjson.Truthy(errs) {
		return nil
	}
	var list []pyjson.Value
	switch typed := errs.(type) {
	case []pyjson.Value:
		list = typed
	case string:
		fmt.Fprintf(env.Stdout, "%d error(s):\n", pyjson.Len(typed))
		return crash("errors is a string, not a list of objects")
	case *pyjson.Object:
		fmt.Fprintf(env.Stdout, "%d error(s):\n", typed.Len())
		return crash("errors is an object, not a list of objects")
	default:
		return crash("errors is not a list")
	}
	fmt.Fprintf(env.Stdout, "%d error(s):\n", len(list))
	for _, entry := range list {
		item, ok := entry.(*pyjson.Object)
		if !ok {
			return crash("an entry of errors is not an object")
		}
		get := func(key string) pyjson.Value { value, _ := item.Get(key); return value }
		location := "envelope"
		if index, isInt := pythonInt(get("index")); isInt && index.Sign() >= 0 {
			location = "records[" + pyjson.Str(get("index")) + "]"
		}
		if path := get("path"); pyjson.Truthy(path) {
			location += " (" + pyjson.Str(path) + ")"
		}
		kindPart := ""
		if kind := get("kind"); pyjson.Truthy(kind) {
			kindPart = " kind=" + pyjson.Str(kind)
		}
		fmt.Fprintf(env.Stdout, "  - %s%s [%s] %s\n", location, kindPart, pyjson.Str(get("code")), pyjson.Str(get("message")))
	}
	return nil
}

// emitResult is _emit_result.
func emitResult(env cli.Env, asJSON bool, body pyjson.Value) error {
	if asJSON {
		emitJSONValue(env, body)
		return nil
	}
	object, ok := body.(*pyjson.Object)
	if !ok {
		return crash("the server's answer is not a JSON object")
	}
	get := func(key string) pyjson.Value { value, _ := object.Get(key); return value }
	ingestionID := get("ingestionId")
	if !pyjson.Truthy(ingestionID) {
		ingestionID = get("ingestion_id")
	}
	fmt.Fprintf(env.Stdout, "ingestion_id: %s\n", pyjson.Str(ingestionID))
	fmt.Fprintf(env.Stdout, "status: %s\n", pyjson.Str(get("status")))
	for _, field := range [][2]string{{"itemsReceived", "items_received"}, {"itemsAccepted", "items_accepted"}, {"itemsRejected", "items_rejected"}} {
		if _, present := object.Get(field[0]); present {
			fmt.Fprintf(env.Stdout, "%s: %s\n", field[1], pyjson.Str(get(field[0])))
		}
	}
	return serverRejectionTable(env, get("errors"))
}

// rejectionReport is the {valid, itemsAccepted, itemsRejected, errors} object of a
// batch refused before it is sent.
func rejectionReport(errs []pyjson.Value) *pyjson.Object {
	report := pyjson.NewObject()
	report.Set("valid", false)
	report.Set("itemsAccepted", pyjson.IntOf(0))
	report.Set("itemsRejected", pyjson.IntOf(int64(len(errs))))
	report.Set("errors", errs)
	return report
}

func emitRejections(env cli.Env, asJSON bool, errs []pyjson.Value) {
	if asJSON {
		emitJSONValue(env, rejectionReport(errs))
		return
	}
	rejectionTable(env, errs)
}

// emitError maps a request failure to the verb's output and exit code.
func emitError(env cli.Env, asJSON bool, err error) int {
	var api *apiError
	var transient *transientError
	switch {
	case errors.As(err, &api):
		if asJSON {
			inner := pyjson.NewObject()
			inner.Set("code", api.code)
			inner.Set("message", api.message)
			inner.Set("errors", api.errs)
			outer := pyjson.NewObject()
			outer.Set("error", inner)
			emitJSONValue(env, outer)
		} else {
			fmt.Fprintf(env.Stderr, "error: HTTP %d %s: %s\n", api.status, api.code, api.message)
			_ = serverRejectionTable(env, api.errs)
		}
		return exitTransport
	case errors.As(err, &transient):
		return emitTransportError(env, asJSON, transient.message)
	}
	fmt.Fprintf(env.Stderr, "error: unexpected server response: %v\n", err)
	return exitDataFailure
}

func emitTransportError(env cli.Env, asJSON bool, message string) int {
	if asJSON {
		inner := pyjson.NewObject()
		inner.Set("code", "transport_error")
		inner.Set("message", message)
		outer := pyjson.NewObject()
		outer.Set("error", inner)
		emitJSONValue(env, outer)
	} else {
		fmt.Fprintf(env.Stderr, "error: transport_error: %s\n", message)
	}
	return exitTransport
}

// withHint is {**body, "hint": hint}.
func withHint(body pyjson.Value, hint string) pyjson.Value {
	object, ok := body.(*pyjson.Object)
	if !ok {
		return body
	}
	out := pyjson.NewObject()
	for _, key := range object.Keys() {
		value, _ := object.Get(key)
		out.Set(key, value)
	}
	out.Set("hint", hint)
	return out
}

// emitHinted is _emit_stream_unavailable and _emit_poll_timeout: the last status,
// and the hint (with --json inside the object, otherwise on stderr).
func emitHinted(env cli.Env, asJSON bool, body pyjson.Value, hint string, code int) int {
	if asJSON {
		emitJSONValue(env, withHint(body, hint))
		return code
	}
	if err := emitResult(env, false, body); err != nil {
		return emitError(env, false, err)
	}
	fmt.Fprintln(env.Stderr, hint)
	return code
}

// exitForTerminal is _exit_for_terminal_status: 0 only for completed with no
// rejected item (a missing, null or otherwise falsy count is none).
func exitForTerminal(final pyjson.Value) int {
	object, _ := final.(*pyjson.Object)
	status, _ := object.Get("status")
	rejected, present := object.Get("itemsRejected")
	if !present {
		rejected, _ = object.Get("items_rejected")
	}
	if status == "completed" && !pyjson.Truthy(rejected) {
		return exitOK
	}
	return exitDataFailure
}

// pollOutcome is how poll_until_terminal ended.
type pollOutcome int

const (
	pollTerminal pollOutcome = iota
	pollStreamUnavailable
	pollTimedOut
)

// statusOf is body.get("status") of a status answer; the answer must be an
// object, and a status that is a list or an object is unhashable where Python
// tests set membership.
func statusOf(body pyjson.Value) (pyjson.Value, error) {
	object, ok := body.(*pyjson.Object)
	if !ok {
		return nil, crash("the server's status answer is not a JSON object")
	}
	status, _ := object.Get("status")
	switch status.(type) {
	case []pyjson.Value, *pyjson.Object:
		return nil, crash("the server's status is not a plain value")
	}
	return status, nil
}

// pollUntilTerminal is poll_until_terminal: GET the batch until a terminal
// status, stream_unavailable, or the timeout; the last body comes back with
// how it ended.
func (c *ingestClient) pollUntilTerminal(ctx context.Context, config clientConfig, ingestionID string, interval, timeout float64) (pyjson.Value, pollOutcome, error) {
	start := time.Now()
	now := func() float64 { return time.Since(start).Seconds() }
	deadline := now() + timeout
	body, err := c.getBatchStatus(ctx, config, ingestionID)
	if err != nil {
		return nil, 0, err
	}
	for {
		status, err := statusOf(body)
		if err != nil {
			return nil, 0, err
		}
		if text, isText := status.(string); isText {
			if terminalStatuses[text] {
				return body, pollTerminal, nil
			}
			if text == "stream_unavailable" {
				return body, pollStreamUnavailable, nil
			}
		}
		if now() >= deadline {
			return body, pollTimedOut, nil
		}
		remaining := deadline - now()
		c.sleep(math.Min(interval, math.Max(remaining, 0)))
		body, err = c.getBatchStatus(ctx, config, ingestionID)
		if err != nil {
			return nil, 0, err
		}
	}
}

// finishPoll prints how a poll ended and returns the exit code.
func finishPoll(env cli.Env, asJSON bool, body pyjson.Value, outcome pollOutcome) int {
	switch outcome {
	case pollStreamUnavailable:
		return emitHinted(env, asJSON, body, "re-run `push batch` (same idempotency key re-enqueues)", exitTransport)
	case pollTimedOut:
		return emitHinted(env, asJSON, body, "poll timed out; re-run `push status <ingestion_id> --poll` rather than resubmitting", exitPollTimeout)
	}
	if err := emitResult(env, asJSON, body); err != nil {
		return emitError(env, asJSON, err)
	}
	return exitForTerminal(body)
}

func runBatch(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho push batch")
	conn := addConnectionFlags(flags)
	skipLimits := flags.Bool("skip-limits-check", false, "skip the GET /schemas limits pre-flight; use the client defaults only")
	positional, code, ok := parseArgs(flags, env, env.Args)
	if !ok {
		return code
	}
	if len(positional) != 1 {
		return usageError(env, "exactly one payload path (or -) is required")
	}
	config, ok := resolveConfig(env, conn)
	if !ok {
		return exitUsage
	}
	asJSON := *conn.asJSON
	client := newIngestClient()

	limits := defaultLimits()
	if !*skipLimits {
		limits = limitsFromSchema(client.schemaDocument(ctx, config.apiURL))
	}
	raw, tooLarge, err := readPayload(env, positional[0], limits.maxBytes)
	if err != nil {
		fmt.Fprintf(env.Stderr, "error: cannot read payload: %s\n", osErrorText(err))
		return exitUsage
	}
	if tooLarge {
		emitRejections(env, asJSON, []pyjson.Value{errorItem(-1, nil, "payload_too_large", fmt.Sprintf("payload exceeds %s bytes", limits.bytesText), nil)})
		return exitDataFailure
	}
	idempotencyKey, shapeErrors := checkEnvelopeShape(raw, limits)
	if shapeErrors != nil {
		emitRejections(env, asJSON, shapeErrors)
		return exitDataFailure
	}

	status, body, err := client.postBatch(ctx, config, raw, idempotencyKey)
	if err != nil {
		return emitError(env, asJSON, err)
	}
	if !*conn.poll {
		if err := emitResult(env, asJSON, body); err != nil {
			return emitError(env, asJSON, err)
		}
		return exitOK
	}

	object, isObject := body.(*pyjson.Object)
	if !isObject {
		return emitError(env, asJSON, crash("the server's answer is not a JSON object"))
	}
	idValue, _ := object.Get("ingestionId")
	if !pyjson.Truthy(idValue) {
		idValue, _ = object.Get("ingestion_id")
	}
	ingestionID, isText := idValue.(string)
	if !isText {
		return emitTransportError(env, asJSON, "server response is missing ingestionId: "+pyjson.Repr(body))
	}
	// A 200 (replay) already carries the full status: no further GET when it is
	// terminal.
	if status == 200 {
		current, err := statusOf(body)
		if err != nil {
			return emitError(env, asJSON, err)
		}
		if text, isText := current.(string); isText && terminalStatuses[text] {
			return finishPoll(env, asJSON, body, pollTerminal)
		}
	}
	final, outcome, err := client.pollUntilTerminal(ctx, config, ingestionID, conn.interval.value, conn.timeout.value)
	if err != nil {
		return emitError(env, asJSON, err)
	}
	return finishPoll(env, asJSON, final, outcome)
}

func runStatus(ctx context.Context, env cli.Env) int {
	flags := newFlags(env, "dho push status")
	conn := addConnectionFlags(flags)
	positional, code, ok := parseArgs(flags, env, env.Args)
	if !ok {
		return code
	}
	if len(positional) != 1 {
		return usageError(env, "exactly one ingestion_id is required")
	}
	config, ok := resolveConfig(env, conn)
	if !ok {
		return exitUsage
	}
	asJSON := *conn.asJSON
	client := newIngestClient()
	if !*conn.poll {
		body, err := client.getBatchStatus(ctx, config, positional[0])
		if err != nil {
			return emitError(env, asJSON, err)
		}
		if err := emitResult(env, asJSON, body); err != nil {
			return emitError(env, asJSON, err)
		}
		return exitOK
	}
	final, outcome, err := client.pollUntilTerminal(ctx, config, positional[0], conn.interval.value, conn.timeout.value)
	if err != nil {
		return emitError(env, asJSON, err)
	}
	return finishPoll(env, asJSON, final, outcome)
}
