package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// An error of a coordinator job can carry a URL, an id, a response body: its text must not reach the job's span
// (CHAOS-7896). The planted error holds secret-shaped markers in its text and in the text of an error it wraps; none of
// them may appear in the status description, an attribute or an event of the finished span.

const (
	plantedMarker = "the planted detail of ticket 7896"
	plantedUser   = "hunter2"
	plantedHost   = "internal-host.example.test"
)

type plantedFailure struct{ message string }

func (failure *plantedFailure) Error() string { return failure.message }

func plantedError() error {
	inner := &plantedFailure{message: "response body: {\"token\":\"" + plantedMarker + "\"}"}
	return fmt.Errorf("GET https://x:%s@%s/orgs/42/repos?access_token=%s: %w", plantedUser, plantedHost, plantedMarker, inner)
}

func TestFinishCoordinatorSpanRecordsNoErrorText(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("span-error-text-test").Start(context.Background(), "coordinator")
	finishCoordinatorSpan(span, plantedError())

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("exported %d spans, want 1", len(spans))
	}
	got := spans[0]
	seen := []string{got.Status.Description}
	collect := func(attributes []attribute.KeyValue) {
		for _, kv := range attributes {
			seen = append(seen, string(kv.Key), kv.Value.Emit())
		}
	}
	collect(got.Attributes)
	for _, event := range got.Events {
		seen = append(seen, event.Name)
		collect(event.Attributes)
	}
	for _, text := range seen {
		for _, marker := range []string{plantedMarker, plantedUser, plantedHost, "access_token", "orgs/42"} {
			if strings.Contains(text, marker) {
				t.Fatalf("the finished span carries error text (%q holds %q)", text, marker)
			}
		}
	}
	if got.Status.Code != codes.Error || got.Status.Description != "coordinator job failed" {
		t.Fatalf("status = %v %q, want Error and the fixed description", got.Status.Code, got.Status.Description)
	}
	var typed []string
	for _, event := range got.Events {
		for _, kv := range event.Attributes {
			if kv.Key == "exception.type" {
				typed = append(typed, kv.Value.AsString())
			}
		}
	}
	if len(typed) != 1 || typed[0] != "*syncdispatchruntime.plantedFailure" {
		t.Fatalf("exception.type events = %q, want exactly the Go type name of the innermost error", typed)
	}
}

func TestFinishCoordinatorSpanOnSuccessHasNoError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	_, span := provider.Tracer("t").Start(context.Background(), "coordinator")
	finishCoordinatorSpan(span, nil)
	got := exporter.GetSpans()[0]
	if got.Status.Code != codes.Ok || len(got.Events) != 0 {
		t.Fatalf("a successful span: status %v, %d events", got.Status.Code, len(got.Events))
	}
}

func TestErrorTypeNameIsTheInnermostGoTypeNeverAMessage(t *testing.T) {
	for name, err := range map[string]error{
		"a plain error":   errors.New(plantedMarker),
		"a wrapped error": fmt.Errorf("outer %s: %w", plantedMarker, &plantedFailure{message: plantedMarker}),
		"a joined error":  errors.Join(errors.New(plantedMarker), errors.New("x")),
		"a deep chain":    fmt.Errorf("a: %w", fmt.Errorf("b: %w", &plantedFailure{message: plantedMarker})),
	} {
		t.Run(name, func(t *testing.T) {
			if got := errorTypeName(err); strings.Contains(got, plantedMarker) || got == "" {
				t.Fatalf("errorTypeName = %q", got)
			}
		})
	}
	if got := errorTypeName(fmt.Errorf("w: %w", &plantedFailure{message: "m"})); got != "*syncdispatchruntime.plantedFailure" {
		t.Fatalf("errorTypeName of a wrapped pointer error = %q", got)
	}
}

// CHAOS-7896 r1: what a coordinator Work hands back to River. River's otelriver middleware copies err.Error() into the parent
// span and the job queue stores it: the text is fixed, the cause stays reachable.
func coordinatorWork(t *testing.T, fn func() error) (err error, spans tracetest.SpanStubs) {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("coordinator-work-test").Start(context.Background(), "coordinator")
	func() {
		defer finishCoordinatorWork(span, &err)
		err = fn()
	}()
	return err, exporter.GetSpans()
}

func TestACoordinatorWorkReturnsAFixedTextAndKeepsItsCause(t *testing.T) {
	failure := plantedError()
	returned, spans := coordinatorWork(t, func() error { return failure })
	for _, marker := range []string{plantedMarker, plantedUser, plantedHost, "access_token", "orgs/42"} {
		if strings.Contains(returned.Error(), marker) {
			t.Fatalf("the error handed to River carries %q: %s", marker, returned)
		}
	}
	if !strings.HasPrefix(returned.Error(), "coordinator job failed: ") {
		t.Fatalf("returned text = %q", returned)
	}
	var planted *plantedFailure
	if !errors.Is(returned, failure) || !errors.As(returned, &planted) {
		t.Fatal("the cause is no longer reachable through errors.Is / errors.As")
	}
	var cancel *rivertype.JobCancelError
	cancelled, _ := coordinatorWork(t, func() error { return river.JobCancel(failure) })
	if !errors.As(cancelled, &cancel) {
		t.Fatal("River's cancel classification no longer reaches the JobCancelError")
	}
	var snooze *river.JobSnoozeError
	snoozed, _ := coordinatorWork(t, func() error { return river.JobSnooze(0) })
	if !errors.As(snoozed, &snooze) {
		t.Fatal("River's snooze classification no longer reaches the JobSnoozeError")
	}
	if len(spans) != 1 || spans[0].Status.Code != codes.Error {
		t.Fatalf("spans = %v", spans)
	}
	if nilErr, _ := coordinatorWork(t, func() error { return nil }); nilErr != nil {
		t.Fatalf("a successful Work returned %v", nilErr)
	}
}

func TestAPanickingCoordinatorWorkIsAFailedSpanAndStillPanics(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	_, span := provider.Tracer("coordinator-panic-test").Start(context.Background(), "coordinator")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		var err error
		defer finishCoordinatorWork(span, &err)
		panic(plantedMarker)
	}()
	if recovered != plantedMarker {
		t.Fatalf("the panic did not continue to propagate: %v", recovered)
	}
	spans := exporter.GetSpans()
	if len(spans) != 1 || spans[0].Status.Code != codes.Error || spans[0].Status.Description != "coordinator job failed" {
		t.Fatalf("a panicking coordinator span = %+v, want Error / coordinator job failed", spans)
	}
	seen := []string{spans[0].Name, spans[0].Status.Description}
	for _, kv := range spans[0].Attributes {
		seen = append(seen, string(kv.Key), kv.Value.Emit())
	}
	for _, event := range spans[0].Events {
		seen = append(seen, event.Name)
		for _, kv := range event.Attributes {
			seen = append(seen, string(kv.Key), kv.Value.Emit())
		}
	}
	for _, text := range seen {
		if strings.Contains(text, plantedMarker) {
			t.Fatalf("the panic value reached the span: %q", text)
		}
	}
}

type cyclicError struct{}

func (cyclic *cyclicError) Error() string { return "cyclic" }
func (cyclic *cyclicError) Unwrap() error { return cyclic }

func TestACyclicUnwrapChainDoesNotStallTheCoordinatorFinalizer(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = coordinatorWork(t, func() error { return &cyclicError{} })
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the finalizer is stuck on a self-unwrapping error")
	}
}

// CHAOS-7896 r1, through the real River middleware: its river.work span takes its status description from err.Error().
func TestTheRiverWorkSpanOfACoordinatorFailureCarriesNoErrorText(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	middleware := otelriver.NewMiddleware(&otelriver.MiddlewareConfig{TracerProvider: provider})
	err := middleware.Work(context.Background(), &rivertype.JobRow{Kind: "coordinator_test"}, func(ctx context.Context) (err error) {
		_, span := provider.Tracer("coordinator-river-test").Start(ctx, "coordinator")
		defer finishCoordinatorWork(span, &err)
		return plantedError()
	})
	if err == nil {
		t.Fatal("no error")
	}
	var river string
	for _, span := range exporter.GetSpans() {
		if span.Name == "river.work" {
			river = span.Status.Description
		}
	}
	if river == "" || !strings.HasPrefix(river, "coordinator job failed: ") {
		t.Fatalf("river.work status description = %q, want the fixed coordinator text", river)
	}
	for _, marker := range []string{plantedMarker, plantedUser, plantedHost, "access_token", "orgs/42"} {
		if strings.Contains(river, marker) {
			t.Fatalf("the river.work span carries %q: %s", marker, river)
		}
	}
}

// CHAOS-7896 r1 (vet): the set of coordinator Works is DERIVED from the source, not listed: every worker type that a Register*
// function of worker.go adds to River (`&xWorker{`) must have a Work method whose body defers finishCoordinatorWork(span, &err)
// with a named error result, and no Work may call finishCoordinatorSpan itself (that would hand River the raw error again).
func registeredCoordinatorWorkers(t *testing.T) (registered map[string]bool, works map[string]*ast.FuncDecl) {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "worker.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	registered = map[string]bool{}
	works = map[string]*ast.FuncDecl{}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Recv != nil && fn.Name.Name == "Work" {
			if star, ok := fn.Recv.List[0].Type.(*ast.StarExpr); ok {
				if ident, ok := star.X.(*ast.Ident); ok {
					works[ident.Name] = fn
				}
			}
		}
		if fn.Recv == nil && strings.HasPrefix(fn.Name.Name, "Register") && fn.Body != nil {
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if unary, ok := node.(*ast.UnaryExpr); ok && unary.Op == token.AND {
					if lit, ok := unary.X.(*ast.CompositeLit); ok {
						if ident, ok := lit.Type.(*ast.Ident); ok && strings.HasSuffix(ident.Name, "Worker") {
							registered[ident.Name] = true
						}
					}
				}
				return true
			})
		}
	}
	return registered, works
}

// the innermost type of a chain deeper than one wrapper is what the span and River text name
func TestTheInnermostTypeOfADeepChainIsNamed(t *testing.T) {
	err := fmt.Errorf("a: %w", fmt.Errorf("b: %w", fmt.Errorf("c: %w", &plantedFailure{message: plantedMarker})))
	if got := errorTypeName(err); got != "*syncdispatchruntime.plantedFailure" {
		t.Fatalf("errorTypeName = %q", got)
	}
}
