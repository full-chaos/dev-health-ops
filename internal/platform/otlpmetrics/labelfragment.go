package otlpmetrics

import (
	"sort"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"go.opentelemetry.io/otel/attribute"
)

const gaugeCollisionHelp = " Samples of this family that collide after label dropping report the maximum."

// FilterFamilies applies the label table to health-registry fragment families, in place, and returns them. The View of the
// policy cannot reach these: a metric producer hands finished data to the exporter. A family with a row keeps only the label
// names of its row; a family with no row keeps its samples and loses every label. Each dropped pair counts in
// DroppedMetricName (the label name only when it is echoable, "*" for an unlisted family). Samples that become identical
// merge: counter and histogram sum, gauge takes the maximum, summary sums count and sum and loses its quantiles; each merge
// counts once as (family, "*").
func (p *LabelPolicy) FilterFamilies(families map[string]*dto.MetricFamily) {
	for name, family := range families {
		p.filterFamily(name, family)
	}
}

func (p *LabelPolicy) filterFamily(name string, family *dto.MetricFamily) {
	allowed, listed := p.rows[name]
	if !listed {
		p.warnOnce(name)
	}
	var collided bool
	groups := make(map[string]int, len(family.Metric))
	kept := make([]*dto.Metric, 0, len(family.Metric))
	for _, metric := range family.Metric {
		labels := metric.Label[:0:0]
		for _, pair := range metric.Label {
			if listed {
				if _, ok := allowed[attribute.Key(pair.GetName())]; ok {
					labels = append(labels, pair)
					continue
				}
				p.count(name, pair.GetName())
				continue
			}
			p.count(name, allLabels)
		}
		metric.Label = labels
		signature := labelSignature(labels)
		index, seen := groups[signature]
		if !seen {
			groups[signature] = len(kept)
			kept = append(kept, metric)
			continue
		}
		if mergeMetric(family.GetType(), kept[index], metric) {
			collided = true
			p.count(name, allLabels)
		}
	}
	family.Metric = kept
	if collided && family.GetType() == dto.MetricType_GAUGE && !strings.HasSuffix(family.GetHelp(), gaugeCollisionHelp) {
		help := family.GetHelp() + gaugeCollisionHelp
		family.Help = &help
	}
}

func labelSignature(labels []*dto.LabelPair) string {
	pairs := make([]string, 0, len(labels))
	for _, pair := range labels {
		pairs = append(pairs, pair.GetName()+"\xff"+pair.GetValue())
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "\xfe")
}

// mergeMetric folds extra into into, by the rule of the family type, and reports whether it merged. A histogram whose bucket
// bounds differ cannot be summed exactly: extra is dropped whole (never half-merged) and the merge still counts.
func mergeMetric(kind dto.MetricType, into, extra *dto.Metric) bool {
	switch kind {
	case dto.MetricType_COUNTER:
		if into.Counter != nil && extra.Counter != nil {
			sum := into.Counter.GetValue() + extra.Counter.GetValue()
			into.Counter.Value = &sum
		}
	case dto.MetricType_GAUGE:
		if into.Gauge != nil && extra.Gauge != nil && extra.Gauge.GetValue() > into.Gauge.GetValue() {
			into.Gauge.Value = extra.Gauge.Value
		}
	case dto.MetricType_UNTYPED:
		if into.Untyped != nil && extra.Untyped != nil && extra.Untyped.GetValue() > into.Untyped.GetValue() {
			into.Untyped.Value = extra.Untyped.Value
		}
	case dto.MetricType_SUMMARY:
		if into.Summary != nil && extra.Summary != nil {
			count := into.Summary.GetSampleCount() + extra.Summary.GetSampleCount()
			sum := into.Summary.GetSampleSum() + extra.Summary.GetSampleSum()
			into.Summary.SampleCount, into.Summary.SampleSum, into.Summary.Quantile = &count, &sum, nil
		}
	case dto.MetricType_HISTOGRAM:
		if into.Histogram != nil && extra.Histogram != nil {
			mergeHistogram(into.Histogram, extra.Histogram)
		}
	}
	return true
}

func mergeHistogram(into, extra *dto.Histogram) {
	if len(into.Bucket) != len(extra.Bucket) {
		return
	}
	for i := range into.Bucket {
		if into.Bucket[i].GetUpperBound() != extra.Bucket[i].GetUpperBound() {
			return
		}
	}
	count := into.GetSampleCount() + extra.GetSampleCount()
	sum := into.GetSampleSum() + extra.GetSampleSum()
	into.SampleCount, into.SampleSum = &count, &sum
	for i := range into.Bucket {
		cumulative := into.Bucket[i].GetCumulativeCount() + extra.Bucket[i].GetCumulativeCount()
		into.Bucket[i].CumulativeCount = &cumulative
	}
}
