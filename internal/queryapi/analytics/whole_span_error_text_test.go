package analytics

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// CHAOS-7936 (vet): the WHOLE recorded span of each failure recorder (status description, every span attribute, every event name
// and attribute) carries no error text, not only the one event the other tests read: a SetStatus(err.Error()) or a span-level
// attribute added after the event is found here.
func TestTheWholeSpanOfEachFailureRecorderCarriesNoErrorText(t *testing.T) {
	planted := fmt.Errorf("dial https://svc:planted-userinfo@ch.example.test/?x=planted-query: %w", errors.New("planted-cause"))
	for name, record := range map[string]func(context.Context){
		"degraded": func(ctx context.Context) { defaultRecordDegradation(ctx, "flowMatrix", planted) },
		"coverage": func(ctx context.Context) {
			defaultRecordInvestmentCoverageFailure(ctx, "org-1", MeasureCount, true, coverageStageQuery, "", planted)
		},
	} {
		recorder := tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		ctx, span := provider.Tracer("whole-span-test").Start(context.Background(), "resolve")
		_ = captureSlog(t)
		record(ctx)
		span.End()
		ended := recorder.Ended()
		if len(ended) != 1 {
			t.Fatalf("%s: %d spans", name, len(ended))
		}
		texts := []string{ended[0].Name(), ended[0].Status().Description}
		for _, attribute := range ended[0].Attributes() {
			texts = append(texts, string(attribute.Key), attribute.Value.Emit())
		}
		for _, event := range ended[0].Events() {
			texts = append(texts, event.Name)
			for _, attribute := range event.Attributes {
				texts = append(texts, string(attribute.Key), attribute.Value.Emit())
			}
		}
		for _, text := range texts {
			if strings.Contains(text, "planted") || strings.Contains(text, "ch.example.test") {
				t.Errorf("%s: the span carries error text: %q", name, text)
			}
		}
		_ = provider.Shutdown(context.Background())
	}
}
