package pushcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errorItem is one entry of a validation report: the dict validate.py builds
// (index, kind, code, message, path), path None as nil.
func errorItem(index int, kind pyjson.Value, code, message string, path pyjson.Value) *pyjson.Object {
	item := pyjson.NewObject()
	item.Set("index", pyjson.IntOf(int64(index)))
	item.Set("kind", kind)
	item.Set("code", code)
	item.Set("message", message)
	item.Set("path", path)
	return item
}

// outcome is ValidationOutcome.
type outcome struct {
	valid                        bool
	itemsAccepted, itemsRejected int
	errors                       []pyjson.Value
}

// parseFailure is PayloadParseError: a failure with no per-record structure.
type parseFailure struct {
	message string
	errors  []pyjson.Value
}

func (f *parseFailure) Error() string { return f.message }

// locPart is str(part) of one element of a pydantic error location.
func locPart(part pyjson.Value) string {
	switch typed := part.(type) {
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case pyjson.Int:
		return typed.String()
	}
	return pyjson.Str(part)
}

// pathOf is ".".join(str(part) for part in loc), None when empty.
func pathOf(loc []pyjson.Value) pyjson.Value {
	parts := make([]string, len(loc))
	for index, part := range loc {
		parts[index] = locPart(part)
	}
	if joined := strings.Join(parts, "."); joined != "" {
		return joined
	}
	return nil
}

// pythonModeMessage is the message pydantic gives for the same failure when the
// envelope is validated from Python objects (model_validate(json.loads(...)),
// what the CLI does) rather than from JSON text (what the API's parser and the
// shared validator model): the type errors of a dict, a model and a list read
// differently.
func pythonModeMessage(item recordvalidation.PydanticError) string {
	switch item.Type {
	case "model_type":
		class, _ := item.Ctx.Get("class_name")
		return "Input should be a valid dictionary or instance of " + pyjson.Str(class)
	case "dict_type":
		return "Input should be a valid dictionary"
	case "list_type":
		return "Input should be a valid list"
	}
	return item.Msg
}

// parseEnvelope is parse_envelope: json.loads, then BatchEnvelope validation.
func parseEnvelope(raw []byte) (*recordvalidation.ValidEnvelope, error) {
	if _, err := pyjson.Decode(raw); err != nil {
		var syntax *pyjson.SyntaxError
		if errors.As(err, &syntax) {
			text, _ := pyjson.DecodeBody(raw)
			return nil, &parseFailure{message: "Malformed JSON: " + syntax.Text(text)}
		}
		return nil, &parseFailure{message: "Malformed JSON: " + err.Error()}
	}
	// json.loads(bytes) drops a UTF-8 byte order mark; the validator reads the text.
	text, _ := pyjson.DecodeBody(raw)
	valid, errs, typeErr := recordvalidation.ValidateEnvelopeJSON([]byte(text))
	if typeErr != nil {
		return nil, &parseFailure{message: "Malformed batch envelope"}
	}
	if len(errs) > 0 {
		items := make([]pyjson.Value, len(errs))
		for index, item := range errs {
			items[index] = errorItem(-1, nil, "invalid_envelope", pythonModeMessage(item), pathOf(item.Loc))
		}
		return nil, &parseFailure{message: "Malformed batch envelope", errors: items}
	}
	return valid, nil
}

// validatePayload is validate_payload: envelope parse, schema version, batch
// size, then every record's own validator.
func validatePayload(raw []byte) (outcome, error) {
	envelope, err := parseEnvelope(raw)
	if err != nil {
		return outcome{}, err
	}
	total := len(envelope.Records)
	if envelope.SchemaVersion != schemaVersion {
		return outcome{itemsRejected: total, errors: []pyjson.Value{errorItem(-1, nil, "unsupported_schema_version",
			"Unsupported schemaVersion: "+pythonparity.StrRepr(envelope.SchemaVersion), "schemaVersion")}}, nil
	}
	if total > maxRecordsDefault {
		return outcome{itemsRejected: total, errors: []pyjson.Value{errorItem(-1, nil, "batch_too_large",
			fmt.Sprintf("Batch has %d records; max is %d", total, maxRecordsDefault), "records")}}, nil
	}
	inputs := make([]recordvalidation.RecordInput, total)
	for index, record := range envelope.Records {
		inputs[index] = recordvalidation.RecordInput{Kind: record.Kind, Payload: record.Payload}
	}
	items := recordvalidation.ValidateRecords(inputs)
	rejected := map[int]bool{}
	out := make([]pyjson.Value, len(items))
	for index, item := range items {
		rejected[item.Index] = true
		var path pyjson.Value
		if item.Path != "" {
			path = item.Path
		}
		out[index] = errorItem(item.Index, item.Kind, item.Code, item.Message, path)
	}
	return outcome{valid: len(items) == 0, itemsAccepted: total - len(rejected), itemsRejected: len(rejected), errors: out}, nil
}

// errnoText is os.strerror for the errors a payload path can raise.
var errnoText = map[syscall.Errno]string{
	syscall.ENOENT: "No such file or directory", syscall.EACCES: "Permission denied", syscall.EISDIR: "Is a directory",
	syscall.ENOTDIR: "Not a directory", syscall.ELOOP: "Too many levels of symbolic links", syscall.ENAMETOOLONG: "File name too long",
	syscall.EMFILE: "Too many open files", syscall.EIO: "Input/output error", syscall.EPERM: "Operation not permitted",
}

// osErrorText is str(OSError) of the exception open()/read() raises.
func osErrorText(err error) string {
	var pathErr *fs.PathError
	var errno syscall.Errno
	if errors.As(err, &pathErr) && errors.As(pathErr.Err, &errno) {
		text, known := errnoText[errno]
		if !known {
			text = strings.ToUpper(errno.Error()[:1]) + errno.Error()[1:]
		}
		return fmt.Sprintf("[Errno %d] %s: %s", int(errno), text, pythonparity.StrRepr(pathErr.Path))
	}
	return err.Error()
}

// readPayload reads at most limit+1 bytes from the file, or from stdin for "-",
// so an unbounded source is never buffered whole; more than limit bytes is
// tooLarge.
func readPayload(env cli.Env, arg string, limit int64) (data []byte, tooLarge bool, err error) {
	var source io.Reader = env.Stdin
	if arg != "-" {
		file, openErr := os.Open(arg)
		if openErr != nil {
			return nil, false, openErr
		}
		defer file.Close()
		source = file
	}
	data, err = io.ReadAll(io.LimitReader(source, limit+1))
	if err != nil {
		return nil, false, err
	}
	return data, int64(len(data)) > limit, nil
}

// rejectionTable is emit_rejection_table.
func rejectionTable(env cli.Env, errs []pyjson.Value) {
	if len(errs) == 0 {
		return
	}
	fmt.Fprintf(env.Stdout, "%d error(s):\n", len(errs))
	for _, entry := range errs {
		item := entry.(*pyjson.Object)
		get := func(key string) pyjson.Value { value, _ := item.Get(key); return value }
		location := "envelope"
		if index, ok := get("index").(pyjson.Int); ok && index.Sign() >= 0 {
			location = fmt.Sprintf("records[%s]", index.String())
		}
		if path, ok := get("path").(string); ok && path != "" {
			location += " (" + path + ")"
		}
		kindPart := ""
		if kind, ok := get("kind").(string); ok && kind != "" {
			kindPart = " kind=" + kind
		}
		fmt.Fprintf(env.Stdout, "  - %s%s [%s] %s\n", location, kindPart, pyjson.Str(get("code")), pyjson.Str(get("message")))
	}
}

// emitJSON is emit_json: one sorted-keys object on a line.
func emitJSON(env cli.Env, value *pyjson.Object) int {
	text, err := dumps(value, true, false)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the result")
		return exitDataFailure
	}
	fmt.Fprintln(env.Stdout, text)
	return exitOK
}

func failureReport(errs []pyjson.Value) *pyjson.Object {
	report := pyjson.NewObject()
	report.Set("valid", false)
	report.Set("itemsAccepted", pyjson.IntOf(0))
	report.Set("itemsRejected", pyjson.IntOf(0))
	report.Set("errors", errs)
	return report
}

func runValidate(_ context.Context, env cli.Env) int {
	flags := newFlags(env, "dho push validate")
	schema := flags.String("schema", schemaVersion, "schema version to validate against")
	asJSON := flags.Bool("json", false, "emit machine-readable JSON to stdout")
	positional, code, ok := parseArgs(flags, env, env.Args)
	if !ok {
		return code
	}
	if len(positional) != 1 {
		return usageError(env, "exactly one payload path (or -) is required")
	}
	if *schema != schemaVersion {
		return usageError(env, "--schema must be %s", schemaVersion)
	}
	raw, tooLarge, err := readPayload(env, positional[0], maxBodyBytesDefault)
	if err != nil {
		fmt.Fprintf(env.Stderr, "error: cannot read payload: %s\n", osErrorText(err))
		return exitUsage
	}
	if tooLarge {
		errs := []pyjson.Value{errorItem(-1, nil, "payload_too_large", fmt.Sprintf("payload exceeds %d bytes", maxBodyBytesDefault), nil)}
		if *asJSON {
			emitJSON(env, failureReport(errs))
		} else {
			rejectionTable(env, errs)
		}
		return exitDataFailure
	}
	result, err := validatePayload(raw)
	var failure *parseFailure
	if errors.As(err, &failure) {
		errs := failure.errors
		if len(errs) == 0 {
			errs = []pyjson.Value{errorItem(-1, nil, "invalid_envelope", failure.message, nil)}
		}
		if *asJSON {
			emitJSON(env, failureReport(errs))
		} else {
			rejectionTable(env, errs)
		}
		return exitDataFailure
	}
	if err != nil {
		fmt.Fprintf(env.Stderr, "error: %v\n", err)
		return exitDataFailure
	}
	switch {
	case *asJSON:
		report := pyjson.NewObject()
		report.Set("valid", result.valid)
		report.Set("itemsAccepted", pyjson.IntOf(int64(result.itemsAccepted)))
		report.Set("itemsRejected", pyjson.IntOf(int64(result.itemsRejected)))
		report.Set("errors", result.errors)
		emitJSON(env, report)
	case result.valid:
		fmt.Fprintf(env.Stdout, "valid: %d record(s) accepted\n", result.itemsAccepted)
	default:
		rejectionTable(env, result.errors)
	}
	if result.valid {
		return exitOK
	}
	return exitDataFailure
}
