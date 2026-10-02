package otlpmetrics

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
)

// metricCollector is a real OTLP/gRPC MetricsService receiver: the points a
// test compares crossed the wire as production points cross to the host
// collector.
type metricCollector struct {
	colmetricpb.UnimplementedMetricsServiceServer
	mu        sync.Mutex
	resources []*metricpb.ResourceMetrics
}

func (c *metricCollector) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resources = append(c.resources, req.GetResourceMetrics()...)
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

func startCollector(t *testing.T) (*metricCollector, string) {
	t.Helper()
	collector := &metricCollector{}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return collector, lis.Addr().String()
}

// textSource is a FragmentSource that serves fixed Prometheus text as one
// source each, in name order.
type textSource map[string]string

func (s textSource) EachMetricsFragment(skip map[string]bool, fn func(string, []byte, error)) {
	names := make([]string, 0, len(s))
	for name := range s {
		if !skip[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		fn(name, []byte(s[name]), nil)
	}
}

// push runs one export of source through a real Pipeline to the collector and
// returns what the collector received.
func push(t *testing.T, source FragmentSource, skip map[string]bool) ([]*metricpb.ResourceMetrics, *Stats) {
	t.Helper()
	collector, addr := startCollector(t)
	pipeline, err := NewPipeline(context.Background(), Options{
		Enabled: true, Endpoint: addr, Interval: time.Hour,
		ServiceName: "dev-health-go-test", Environment: "test", InstanceID: "pod-1",
	}, source, skip, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewPipeline: %v", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(pipeline.Reader), sdkmetric.WithResource(pipeline.Resource))
	if err := provider.ForceFlush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	_ = provider.Shutdown(context.Background())
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return append([]*metricpb.ResourceMetrics(nil), collector.resources...), pipeline.Stats
}

// ---- the oracle: a small independent reader of the exposition text ----

type series struct {
	labels string // canonical k=v,k=v with le removed
	le     string
	value  float64
}

type family struct {
	typ    string
	series map[string][]series // sample name -> series
}

func (f *family) empty() bool {
	for _, samples := range f.series {
		if len(samples) > 0 {
			return false
		}
	}
	return true
}

// fragmentsOf splits one scraped /metrics text back into the per-source
// fragments it was concatenated from: a family declared a second time starts a
// new fragment, as the registry writes each source separately.
func fragmentsOf(text string) textSource {
	out := textSource{}
	seen := map[string]bool{}
	index := 0
	var current strings.Builder
	flush := func() {
		if current.Len() > 0 {
			out[fmt.Sprintf("source%02d", index)] = current.String()
			index++
			current.Reset()
			seen = map[string]bool{}
		}
	}
	for _, line := range strings.SplitAfter(text, "\n") {
		if strings.HasPrefix(line, "# HELP ") {
			name := strings.Fields(line)[2]
			if seen[name] {
				flush()
			}
			seen[name] = true
		}
		current.WriteString(line)
	}
	flush()
	return out
}

func parseExposition(t *testing.T, text string) map[string]*family {
	t.Helper()
	families := map[string]*family{}
	types := map[string]string{}
	scanner := bufio.NewScanner(strings.NewReader(text))
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, "# HELP") {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			parts := strings.Fields(line)
			if existing, ok := families[parts[2]]; ok {
				if existing.typ != parts[3] {
					t.Fatalf("oracle: family %s is declared as %s and as %s", parts[2], existing.typ, parts[3])
				}
				continue // a family two sources both write: one family, both sources' series
			}
			types[parts[2]] = parts[3]
			families[parts[2]] = &family{typ: parts[3], series: map[string][]series{}}
			continue
		}
		name, labels, value := splitSample(t, line)
		owner := name
		if _, ok := families[owner]; !ok {
			for _, suffix := range []string{"_bucket", "_sum", "_count"} {
				if base := strings.TrimSuffix(name, suffix); base != name {
					if f, ok := families[base]; ok && f.typ == "histogram" {
						owner = base
					}
				}
			}
		}
		f, ok := families[owner]
		if !ok {
			t.Fatalf("oracle: sample %q has no TYPE line", line)
		}
		le := labels["le"]
		delete(labels, "le")
		f.series[name] = append(f.series[name], series{labels: canonical(labels), le: le, value: value})
	}
	return families
}

func splitSample(t *testing.T, line string) (string, map[string]string, float64) {
	t.Helper()
	labels := map[string]string{}
	name := line
	rest := ""
	if open := strings.IndexByte(line, '{'); open >= 0 {
		name = line[:open]
		i := open + 1
		for i < len(line) && line[i] != '}' {
			eq := strings.IndexByte(line[i:], '=')
			key := line[i : i+eq]
			i += eq + 2 // skip ="
			var value strings.Builder
			for ; i < len(line); i++ {
				if line[i] == '\\' && i+1 < len(line) {
					i++
					switch line[i] {
					case 'n':
						value.WriteByte('\n')
					default:
						value.WriteByte(line[i])
					}
					continue
				}
				if line[i] == '"' {
					break
				}
				value.WriteByte(line[i])
			}
			labels[key] = value.String()
			i++ // closing quote
			if i < len(line) && line[i] == ',' {
				i++
			}
		}
		rest = strings.TrimSpace(line[i+1:])
	} else {
		fields := strings.Fields(line)
		name, rest = fields[0], strings.Join(fields[1:], " ")
	}
	valueText := strings.Fields(rest)[0]
	value, err := strconv.ParseFloat(strings.Replace(valueText, "Inf", "Inf", 1), 64)
	if err != nil {
		t.Fatalf("oracle: sample %q: %v", line, err)
	}
	return name, labels, value
}

func canonical(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, key := range keys {
		parts[i] = key + "=" + labels[key]
	}
	return strings.Join(parts, ",")
}

func attrsCanonical(attrs []*commonpb.KeyValue) string {
	labels := map[string]string{}
	for _, kv := range attrs {
		labels[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	return canonical(labels)
}

func numberValue(p *metricpb.NumberDataPoint) float64 {
	if _, ok := p.GetValue().(*metricpb.NumberDataPoint_AsInt); ok {
		return float64(p.GetAsInt())
	}
	return p.GetAsDouble()
}

type got struct {
	typ     string
	numbers map[string]float64 // labels -> value
	hists   map[string]*metricpb.HistogramDataPoint
}

func collect(resources []*metricpb.ResourceMetrics) map[string]*got {
	out := map[string]*got{}
	for _, resource := range resources {
		for _, scope := range resource.GetScopeMetrics() {
			for _, metric := range scope.GetMetrics() {
				g := out[metric.GetName()]
				if g == nil {
					g = &got{numbers: map[string]float64{}, hists: map[string]*metricpb.HistogramDataPoint{}}
					out[metric.GetName()] = g
				}
				switch data := metric.GetData().(type) {
				case *metricpb.Metric_Sum:
					g.typ = "counter"
					if !data.Sum.GetIsMonotonic() || data.Sum.GetAggregationTemporality() != metricpb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
						g.typ = "sum-not-monotonic-cumulative"
					}
					for _, p := range data.Sum.GetDataPoints() {
						g.numbers[attrsCanonical(p.GetAttributes())] = numberValue(p)
					}
				case *metricpb.Metric_Gauge:
					g.typ = "gauge"
					for _, p := range data.Gauge.GetDataPoints() {
						g.numbers[attrsCanonical(p.GetAttributes())] = numberValue(p)
					}
				case *metricpb.Metric_Histogram:
					g.typ = "histogram"
					for _, p := range data.Histogram.GetDataPoints() {
						g.hists[attrsCanonical(p.GetAttributes())] = p
					}
				}
			}
		}
	}
	return out
}

// volatileFamilies are compared for presence and label set only: their value is
// the time the process has been up, which moves between the scrape and the push.
var volatileFamilies = map[string]bool{"dev_health_runtime_uptime_seconds": true}

func floatsEqual(a, b float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	return a == b || math.Abs(a-b) <= 1e-9*math.Max(math.Abs(a), math.Abs(b))
}

// compare fails the test for every family the OTLP side lost, renamed, retyped,
// or changed, and returns how many families and series it compared. Zero is a
// failure in the callers: a comparison of nothing proves nothing.
func compare(t *testing.T, label string, want map[string]*family, resources []*metricpb.ResourceMetrics) (families, seriesCount int) {
	t.Helper()
	received := collect(resources)
	names := make([]string, 0, len(want))
	for name := range want {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := want[name]
		if f.empty() {
			continue // declared with no sample: there is no point to carry
		}
		g := received[name]
		if g == nil {
			t.Errorf("%s: family %s (%s) was dropped or renamed: not in the OTLP points", label, name, f.typ)
			continue
		}
		families++
		switch f.typ {
		case "counter", "gauge":
			if g.typ != f.typ {
				t.Errorf("%s: family %s is %s in the scrape and %s over OTLP", label, name, f.typ, g.typ)
			}
			samples := f.series[name]
			if len(samples) != len(g.numbers) {
				t.Errorf("%s: family %s has %d series in the scrape and %d over OTLP", label, name, len(samples), len(g.numbers))
			}
			for _, s := range samples {
				seriesCount++
				value, ok := g.numbers[s.labels]
				if !ok {
					t.Errorf("%s: %s{%s} missing over OTLP", label, name, s.labels)
				} else if !volatileFamilies[name] && !floatsEqual(value, s.value) {
					t.Errorf("%s: %s{%s} = %v over OTLP, %v in the scrape", label, name, s.labels, value, s.value)
				}
			}
		case "histogram":
			if g.typ != "histogram" {
				t.Errorf("%s: family %s is a histogram in the scrape and %s over OTLP", label, name, g.typ)
				continue
			}
			counts := map[string]float64{}
			for _, s := range f.series[name+"_count"] {
				counts[s.labels] = s.value
			}
			sums := map[string]float64{}
			for _, s := range f.series[name+"_sum"] {
				sums[s.labels] = s.value
			}
			buckets := map[string][]series{}
			for _, s := range f.series[name+"_bucket"] {
				buckets[s.labels] = append(buckets[s.labels], s)
			}
			if len(buckets) != len(g.hists) {
				t.Errorf("%s: histogram %s has %d series in the scrape and %d over OTLP", label, name, len(buckets), len(g.hists))
			}
			for labels, bs := range buckets {
				seriesCount++
				point, ok := g.hists[labels]
				if !ok {
					t.Errorf("%s: histogram %s{%s} missing over OTLP", label, name, labels)
					continue
				}
				if float64(point.GetCount()) != counts[labels] || !floatsEqual(point.GetSum(), sums[labels]) {
					t.Errorf("%s: histogram %s{%s} count/sum = %d/%v over OTLP, %v/%v in the scrape", label, name, labels, point.GetCount(), point.GetSum(), counts[labels], sums[labels])
				}
				sort.Slice(bs, func(i, j int) bool { return leValue(bs[i].le) < leValue(bs[j].le) })
				var bounds []float64
				var perBucket []uint64
				previous := 0.0
				for _, b := range bs {
					if math.IsInf(leValue(b.le), 1) {
						perBucket = append(perBucket, uint64(b.value-previous))
						continue
					}
					bounds = append(bounds, leValue(b.le))
					perBucket = append(perBucket, uint64(b.value-previous))
					previous = b.value
				}
				if fmt.Sprint(point.GetExplicitBounds()) != fmt.Sprint(bounds) {
					t.Errorf("%s: histogram %s{%s} bounds = %v over OTLP, %v in the scrape", label, name, labels, point.GetExplicitBounds(), bounds)
				}
				if fmt.Sprint(point.GetBucketCounts()) != fmt.Sprint(perBucket) {
					t.Errorf("%s: histogram %s{%s} bucket counts = %v over OTLP, %v in the scrape", label, name, labels, point.GetBucketCounts(), perBucket)
				}
			}
		default:
			t.Errorf("%s: family %s has type %q the oracle does not model", label, name, f.typ)
		}
	}
	for name := range received {
		if _, ok := want[name]; !ok && !strings.HasPrefix(name, "dev_health_otlp_metrics_") {
			t.Errorf("%s: OTLP carries %s, which the scrape does not have", label, name)
		}
	}
	return families, seriesCount
}

func leValue(le string) float64 {
	if le == "+Inf" {
		return math.Inf(1)
	}
	value, _ := strconv.ParseFloat(le, 64)
	return value
}
