package pushcli

import (
	"context"
	"embed"
	"flag"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/recordvalidation"
	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// examples are the packaged canonical example payloads, one per record kind,
// copies of api/external_ingest/examples/*.json (TestExamplesAreThePythonFiles
// holds them to the Python files while those exist).
//
//go:embed examples/*.json
var examples embed.FS

// exampleFor is load_example: the decoded example payload of a kind.
func exampleFor(kind string) (*pyjson.Object, error) {
	raw, err := examples.ReadFile("examples/" + kind + ".json")
	if err != nil {
		return nil, err
	}
	value, err := pyjson.Decode(raw)
	if err != nil {
		return nil, err
	}
	object, _ := value.(*pyjson.Object)
	if object == nil {
		return nil, fmt.Errorf("example of %s is not an object", kind)
	}
	return object, nil
}

func field(payload *pyjson.Object, name string) string {
	value, _ := payload.Get(name)
	return pyjson.Str(value)
}

// correlationExternalID is _derive_correlation_external_id: the wrapper's
// externalId, the per-record correlation id of rejection diagnostics.
func correlationExternalID(kind string, payload *pyjson.Object) (string, error) {
	base := kind[:strings.LastIndex(kind, ".")]
	switch base {
	case "repository":
		return field(payload, "externalId"), nil
	case "identity":
		return field(payload, "canonicalId"), nil
	case "team":
		return field(payload, "id"), nil
	case "work_item":
		return field(payload, "externalKey"), nil
	case "work_item_transition":
		return field(payload, "externalKey") + ":" + field(payload, "occurredAt"), nil
	case "work_item_dependency":
		return field(payload, "sourceExternalKey") + "->" + field(payload, "targetExternalKey"), nil
	case "pull_request":
		return field(payload, "repositoryExternalId") + "#" + field(payload, "number"), nil
	case "review":
		return field(payload, "repositoryExternalId") + "#" + field(payload, "pullRequestNumber") + ":review:" + field(payload, "reviewId"), nil
	case "commit":
		return field(payload, "repositoryExternalId") + "@" + field(payload, "hash"), nil
	case "operational_service", "operational_incident", "operational_alert", "incident_timeline_event", "incident_note",
		"incident_responder", "escalation_policy", "on_call_schedule", "on_call_assignment", "operational_team",
		"operational_user", "service_repository_mapping":
		return field(payload, "externalId"), nil
	}
	return "", fmt.Errorf("no correlation-id rule for kind: %s", kind)
}

func sampleRecord(kind string) (*pyjson.Object, error) {
	payload, err := exampleFor(kind)
	if err != nil {
		return nil, err
	}
	externalID, err := correlationExternalID(kind, payload)
	if err != nil {
		return nil, err
	}
	record := pyjson.NewObject()
	record.Set("kind", kind)
	record.Set("externalId", externalID)
	record.Set("payload", payload)
	return record, nil
}

// wrapBatchEnvelope is _wrap_batch_envelope: a one-source, one-window batch
// around the records. producerVersion is this binary's version (Python: the
// installed dev-health-ops distribution's, "0.0.0-dev" when it is not installed).
func wrapBatchEnvelope(records []pyjson.Value, idempotencyKey string) *pyjson.Object {
	source := pyjson.NewObject()
	source.Set("type", "customer_push")
	source.Set("system", "github")
	source.Set("instance", "acme/api")
	source.Set("producer", "dev-hops-cli")
	source.Set("producerVersion", version.Current("dho").Version)
	window := pyjson.NewObject()
	window.Set("startedAt", "2026-06-20T00:00:00Z")
	window.Set("endedAt", "2026-06-26T00:00:00Z")
	envelope := pyjson.NewObject()
	envelope.Set("schemaVersion", schemaVersion)
	envelope.Set("idempotencyKey", idempotencyKey)
	envelope.Set("source", source)
	envelope.Set("window", window)
	envelope.Set("records", records)
	return envelope
}

// kindArgument is _kind_type: the versioned kind, or the bare one (".v1" added).
func kindArgument(value string) (string, bool) {
	kinds := recordvalidation.RecordKinds()
	for _, kind := range kinds {
		if kind == value {
			return value, true
		}
	}
	for _, kind := range kinds {
		if kind == value+".v1" {
			return kind, true
		}
	}
	return "", false
}

func runSample(_ context.Context, env cli.Env) int {
	flags := newFlags(env, "dho push sample")
	kind := flags.String("kind", "", "record kind, bare or versioned (for example pull_request or pull_request.v1)")
	all := flags.Bool("all", false, "print a combined batch envelope with one record of every kind")
	positional, code, ok := parseArgs(flags, env, env.Args)
	if !ok {
		return code
	}
	kindGiven := false
	flags.Visit(func(f *flag.Flag) { kindGiven = kindGiven || f.Name == "kind" })
	switch {
	case len(positional) != 0:
		return usageError(env, "unexpected argument %q", positional[0])
	case kindGiven && *all:
		return usageError(env, "argument --all: not allowed with argument --kind")
	case !kindGiven && !*all:
		return usageError(env, "one of the arguments --kind --all is required")
	}
	var envelope *pyjson.Object
	if *all {
		var records []pyjson.Value
		for _, name := range recordvalidation.RecordKinds() {
			record, err := sampleRecord(name)
			if err != nil {
				fmt.Fprintf(env.Stderr, "error: %v\n", err)
				return exitDataFailure
			}
			records = append(records, record)
		}
		envelope = wrapBatchEnvelope(records, "sample-full-batch")
	} else {
		resolved, valid := kindArgument(*kind)
		if !valid {
			return usageError(env, "invalid choice: %s (choose from %s)", pythonparity.StrRepr(*kind), strings.Join(recordvalidation.RecordKinds(), ", "))
		}
		record, err := sampleRecord(resolved)
		if err != nil {
			fmt.Fprintf(env.Stderr, "error: %v\n", err)
			return exitDataFailure
		}
		envelope = wrapBatchEnvelope([]pyjson.Value{record}, "sample-"+resolved)
	}
	text, err := dumps(envelope, true, true)
	if err != nil {
		fmt.Fprintln(env.Stderr, "could not write the sample")
		return exitDataFailure
	}
	fmt.Fprintln(env.Stdout, text)
	return exitOK
}

// recordKinds is every record kind, sorted (schema_registry.iter_record_kinds).
func recordKinds() []string { return recordvalidation.RecordKinds() }
