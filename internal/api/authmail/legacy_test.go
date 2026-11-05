package authmail

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/signedtoken"
)

func installLegacyLinkCounter(t *testing.T) *sdkmetric.ManualReader {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	counter, err := NewLegacyLinkCounter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"), "svc-a")
	if err != nil {
		t.Fatal(err)
	}
	SetLegacyLinkAccepted(counter)
	t.Cleanup(func() { SetLegacyLinkAccepted(nil) })
	return reader
}

// legacyLinkPoints is every series of the counter: "service|purpose" -> value.
func legacyLinkPoints(t *testing.T, reader *sdkmetric.ManualReader) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != LegacyLinkAcceptedMetric || !ok {
				continue
			}
			for _, point := range sum.DataPoints {
				if point.Attributes.Len() != 2 {
					t.Fatalf("label set has %d labels, want service and purpose only", point.Attributes.Len())
				}
				service, _ := point.Attributes.Value(attribute.Key("service"))
				purpose, _ := point.Attributes.Value(attribute.Key("purpose"))
				out[service.AsString()+"|"+purpose.AsString()] += point.Value
			}
		}
	}
	return out
}

func legacyTotal(points map[string]int64) int64 {
	var total int64
	for _, v := range points {
		total += v
	}
	return total
}

func TestLegacyLinkCounterPreRegistersEveryPurposeAtZero(t *testing.T) {
	reader := installLegacyLinkCounter(t)
	points := legacyLinkPoints(t, reader)
	for _, purpose := range []string{PurposeInvite, PurposeEmailVerification, PurposePasswordReset} {
		if value, present := points["svc-a|"+purpose]; !present || value != 0 {
			t.Fatalf("series %s before any accept = %v (present %t), want 0", purpose, value, present)
		}
	}
	if len(points) != 3 {
		t.Fatalf("series %v, want the three purposes only", points)
	}
}

func TestLegacyLinkCounterIsNilSafe(t *testing.T) {
	var counter *LegacyLinkCounter
	counter.Accepted(context.Background(), PurposeInvite)
}

func TestValidCountsOnlyALinkTheLegacyKeyAloneSigns(t *testing.T) {
	const legacy, link, previous = "access-key", "link-key", "link-old"
	cases := []struct {
		name     string
		config   Config
		signedBy string
		valid    bool
		counted  bool
	}{
		{"link secret signs: fresh link", Config{TokenSecret: legacy, LinkSecret: link}, link, true, false},
		{"link secret signs: previous link secret", Config{TokenSecret: legacy, LinkSecret: link, PreviousLinkSecret: previous}, previous, true, false},
		{"link secret signs: pre-roll link of the legacy key", Config{TokenSecret: legacy, LinkSecret: link}, legacy, true, true},
		{"legacy key removed: pre-roll link refused", Config{LinkSecret: link}, legacy, false, false},
		{"no link secret: the legacy key signs", Config{TokenSecret: legacy}, legacy, true, false},
		{"previous equals the legacy key", Config{TokenSecret: legacy, LinkSecret: link, PreviousLinkSecret: legacy}, legacy, true, false},
		{"foreign key refused", Config{TokenSecret: legacy, LinkSecret: link, PreviousLinkSecret: previous}, "other-key", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reader := installLegacyLinkCounter(t)
			token, _ := signedtoken.Build(uuid.New(), c.signedBy)
			ok, err := c.config.Valid(context.Background(), PurposePasswordReset, token)
			if err != nil || ok != c.valid {
				t.Fatalf("Valid = %v, %v, want %v", ok, err, c.valid)
			}
			want := int64(0)
			if c.counted {
				want = 1
			}
			points := legacyLinkPoints(t, reader)
			if points["svc-a|"+PurposePasswordReset] != want || legacyTotal(points) != want {
				t.Fatalf("counter %v, want %s = %d and no other count", points, PurposePasswordReset, want)
			}
		})
	}
}

// Valid answers exactly what ValidAny over AcceptSecrets answered before it.
func TestValidAgreesWithValidAnyOverAcceptSecrets(t *testing.T) {
	installLegacyLinkCounter(t)
	configs := []Config{
		{}, {TokenSecret: "a"}, {LinkSecret: "l"}, {TokenSecret: "a", LinkSecret: "l"},
		{TokenSecret: "a", LinkSecret: "l", PreviousLinkSecret: "p"}, {LinkSecret: "l", PreviousLinkSecret: "p"},
		{TokenSecret: "a", LinkSecret: "a"}, {TokenSecret: "a", LinkSecret: "l", PreviousLinkSecret: "a"},
	}
	id := uuid.New()
	var tokens []string
	for _, secret := range []string{"a", "l", "p", "x", "", signedtoken.DevFallbackSecret} {
		token, _ := signedtoken.Build(id, secret)
		tokens = append(tokens, token)
	}
	tokens = append(tokens, "", "no-dot", "zz.zz", id.String()+".é", id.String()+".")
	for _, config := range configs {
		for _, token := range tokens {
			wantOK, wantErr := signedtoken.ValidAny(token, config.AcceptSecrets())
			gotOK, gotErr := config.Valid(context.Background(), PurposeInvite, token)
			if gotOK != wantOK || !errors.Is(gotErr, wantErr) {
				t.Fatalf("config %+v token %q: Valid = %v, %v; ValidAny = %v, %v", config, token, gotOK, gotErr, wantOK, wantErr)
			}
		}
	}
}
