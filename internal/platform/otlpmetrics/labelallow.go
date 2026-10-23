package otlpmetrics

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// labels.tsv pins, per instrument, the label keys that may leave the process. One row per instrument:
// `instrument<TAB>comma-separated sorted keys<TAB>reason`. An unknown key on a listed instrument is dropped (the SDK
// aggregates the dimension away) and counted; an unlisted instrument keeps its datapoints with every label dropped.
//
//go:embed labels.tsv
var embeddedLabels []byte

const (
	// MaxLabelKeys is the key budget of a first-party instrument (emitscan R2 uses the same number). A row above it must say
	// "third-party:" in its reason: a library's instrument this repository cannot change.
	MaxLabelKeys = 6
	// DroppedMetricName is the counter of dropped label keys: labels instrument and label, names only, never a value.
	DroppedMetricName = "dev_health_metric_labels_dropped_total"
	// maxDropSeries bounds the (instrument, label) pairs the drop counter keeps; further pairs count under "other".
	maxDropSeries = 256
	otherName     = "other"
	// allLabels is the label the counter shows when an unlisted instrument loses every label.
	allLabels = "*"
)

// A key name is echoed into the drop counter only in this shape (acp condition); an instrument name may be longer.
var (
	echoablePattern   = regexp.MustCompile(`^[a-z0-9_.]{1,64}$`)
	instrumentPattern = regexp.MustCompile(`^[a-z0-9_.]{1,128}$`)
)

type dropKey struct{ instrument, label string }

// LabelPolicy is the per-instrument label allowlist as an sdkmetric View.
type LabelPolicy struct {
	rows   map[string]map[attribute.Key]struct{}
	logger *slog.Logger

	warnMu sync.Mutex
	warned map[string]struct{}

	dropMu sync.Mutex
	drops  map[dropKey]*atomic.Int64
}

// DefaultLabelPolicy reads the embedded labels.tsv.
func DefaultLabelPolicy(logger *slog.Logger) (*LabelPolicy, error) {
	return NewLabelPolicy(embeddedLabels, logger)
}

// NewLabelPolicy parses a labels table.
func NewLabelPolicy(table []byte, logger *slog.Logger) (*LabelPolicy, error) {
	if logger == nil {
		logger = slog.Default()
	}
	rows, err := parseLabels(table)
	if err != nil {
		return nil, err
	}
	policy := &LabelPolicy{rows: map[string]map[attribute.Key]struct{}{}, logger: logger, warned: map[string]struct{}{}, drops: map[dropKey]*atomic.Int64{}}
	for instrument, keys := range rows {
		allowed := make(map[attribute.Key]struct{}, len(keys))
		for _, key := range keys {
			allowed[attribute.Key(key)] = struct{}{}
		}
		policy.rows[instrument] = allowed
	}
	return policy, nil
}

// LabelRow is one parsed row of labels.tsv.
type LabelRow struct {
	Instrument string
	Keys       []string
	Reason     string
}

// ParseLabelRows returns the rows of a labels table in file order.
func ParseLabelRows(table []byte) ([]LabelRow, error) {
	var rows []LabelRow
	scanner := bufio.NewScanner(bytes.NewReader(table))
	line := 0
	seen := map[string]bool{}
	for scanner.Scan() {
		line++
		text := scanner.Text()
		if line == 1 && strings.HasPrefix(text, "instrument\t") {
			continue
		}
		if strings.TrimSpace(text) == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Split(text, "\t")
		if len(fields) != 3 {
			return nil, fmt.Errorf("labels.tsv line %d: want instrument<TAB>keys<TAB>reason", line)
		}
		row := LabelRow{Instrument: fields[0], Reason: fields[2]}
		if !instrumentPattern.MatchString(row.Instrument) {
			return nil, fmt.Errorf("labels.tsv line %d: instrument name is not [a-z0-9_.]{1,128}", line)
		}
		if seen[row.Instrument] {
			return nil, fmt.Errorf("labels.tsv line %d: instrument %s listed twice", line, row.Instrument)
		}
		seen[row.Instrument] = true
		if fields[1] != "" {
			row.Keys = strings.Split(fields[1], ",")
		}
		if !sort.StringsAreSorted(row.Keys) {
			return nil, fmt.Errorf("labels.tsv line %d: keys of %s are not sorted", line, row.Instrument)
		}
		for i, key := range row.Keys {
			if !echoablePattern.MatchString(key) {
				return nil, fmt.Errorf("labels.tsv line %d: a key of %s is not [a-z0-9_.]{1,64}", line, row.Instrument)
			}
			if i > 0 && row.Keys[i-1] == key {
				return nil, fmt.Errorf("labels.tsv line %d: %s lists a key twice", line, row.Instrument)
			}
		}
		if len(row.Keys) > MaxLabelKeys && !strings.HasPrefix(row.Reason, "third-party:") {
			return nil, fmt.Errorf("labels.tsv line %d: %s has %d keys, over the budget of %d", line, row.Instrument, len(row.Keys), MaxLabelKeys)
		}
		rows = append(rows, row)
	}
	return rows, scanner.Err()
}

func parseLabels(table []byte) (map[string][]string, error) {
	parsed, err := ParseLabelRows(table)
	if err != nil {
		return nil, err
	}
	rows := make(map[string][]string, len(parsed))
	for _, row := range parsed {
		rows[row.Instrument] = row.Keys
	}
	return rows, nil
}

// Listed reports whether the table has a row for the instrument.
func (p *LabelPolicy) Listed(instrument string) bool {
	_, ok := p.rows[instrument]
	return ok
}

// View is the sdkmetric View to install on the provider (sdkmetric.WithView). It matches every instrument, so it is the
// only view: two matching views would make two streams.
func (p *LabelPolicy) View() sdkmetric.View {
	return func(instrument sdkmetric.Instrument) (sdkmetric.Stream, bool) {
		stream := sdkmetric.Stream{Name: instrument.Name, Description: instrument.Description, Unit: instrument.Unit}
		allowed, listed := p.rows[instrument.Name]
		if !listed {
			p.warnOnce(instrument.Name)
			stream.AttributeFilter = func(attribute.KeyValue) bool {
				p.count(instrument.Name, allLabels)
				return false
			}
			return stream, true
		}
		stream.AttributeFilter = func(kv attribute.KeyValue) bool {
			if _, ok := allowed[kv.Key]; ok {
				return true
			}
			p.count(instrument.Name, string(kv.Key))
			return false
		}
		return stream, true
	}
}

func (p *LabelPolicy) warnOnce(instrument string) {
	p.warnMu.Lock()
	_, seen := p.warned[instrument]
	p.warned[instrument] = struct{}{}
	p.warnMu.Unlock()
	if !seen {
		p.logger.Warn("metric instrument has no row in labels.tsv: every label is dropped", "instrument", safeName(instrument))
	}
}

func safeName(name string) string {
	if instrumentPattern.MatchString(name) {
		return name
	}
	return otherName
}

func (p *LabelPolicy) count(instrument, label string) {
	key := dropKey{safeName(instrument), label}
	if label != allLabels && !echoablePattern.MatchString(label) {
		key.label = otherName
	}
	p.dropMu.Lock()
	counter, ok := p.drops[key]
	if !ok {
		if len(p.drops) >= maxDropSeries {
			key = dropKey{otherName, otherName}
			counter = p.drops[key]
		}
		if counter == nil {
			counter = &atomic.Int64{}
			p.drops[key] = counter
		}
	}
	p.dropMu.Unlock()
	counter.Add(1)
}

// Dropped is a snapshot of the drop counts by instrument and label name.
func (p *LabelPolicy) Dropped() map[[2]string]int64 {
	p.dropMu.Lock()
	defer p.dropMu.Unlock()
	out := make(map[[2]string]int64, len(p.drops))
	for key, counter := range p.drops {
		out[[2]string{key.instrument, key.label}] = counter.Load()
	}
	return out
}

// Register exposes the drop counts as DroppedMetricName on the meter's provider: an observable counter, so recording a drop
// never calls back into the SDK from inside a filter.
func (p *LabelPolicy) Register(meter metric.Meter) error {
	_, err := meter.Int64ObservableCounter(DroppedMetricName,
		metric.WithDescription("Label keys the per-instrument allowlist dropped, by instrument and label name (never a value). Counted once per reader."),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			for key, count := range p.Dropped() {
				observer.Observe(count, metric.WithAttributes(attribute.String("instrument", key[0]), attribute.String("label", key[1])))
			}
			return nil
		}))
	return err
}
