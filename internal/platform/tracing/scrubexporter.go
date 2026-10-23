package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// A recorded span's attributes, events, links and status are immutable once set, and a SpanProcessor gets a read-only
// snapshot at OnEnd, so a processor cannot scrub them. The scrub is the exporter in front of the real one: every span
// leaves the process through scrubExporter, which passes each name, attribute, event, link, status description,
// resource and scope attribute through the logging package's redactor (logging.RedactKeyedText), the same code the
// log handler runs. Spans seen by an in-process observer (endCounter) are raw; they never leave the process.
type scrubExporter struct{ sdktrace.SpanExporter }

func (e scrubExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	scrubbed := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, span := range spans {
		scrubbed[i] = scrubSpan(span)
	}
	return e.SpanExporter.ExportSpans(ctx, scrubbed)
}

// hidden replaces a value that is never exported.
const hidden = "[REDACTED]"

// safeKeys are the attribute keys whose names the protected-key rule must not hide: bounded-vocabulary keys this
// repository's own spans carry. The values still go through the text redactor. Exact keys only: a prefix would admit
// whatever an instrumentation library adds under it.
var safeKeys = map[string]bool{
	"outcome": true, "reason": true, "reason_code": true, "denial_reason": true, "phase": true, "provider": true,
	"operation": true, "op": true, "route": true, "family": true, "listener": true, "resolver": true,
	"event_type": true, "verdict": true, "error.class": true, "error.type": true, "error.code": true, "error_class": true,
	"db.system": true, "http.request.method": true, "http.response.status_code": true, "http.route": true,
	"dev_health.job.kind": true,
}

// neverSafe are keys that can carry a credential, a query or free text whatever their name looks like; none of them
// is ever in safeKeys (the package init fails otherwise).
var neverSafe = []string{
	"url.full", "url.query", "url.path", "http.url", "http.target", "db.statement", "db.query.text",
	"exception.message", "exception.stacktrace", "user_agent.original", "enduser.id",
}

func init() {
	for _, key := range neverSafe {
		if safeKeys[key] {
			panic("tracing: " + key + " must never be a safe attribute key")
		}
	}
}

// exceptionTextKeys carry the text of an error (identity-only rule: logging.ErrorSpanAttributes); the type stays.
var exceptionTextKeys = map[string]bool{"exception.message": true, "exception.stacktrace": true}

// scrubValue returns the value to export for one attribute; changed is false when kv can go out as it is.
func scrubValue(kv attribute.KeyValue, exceptionEvent bool) (out attribute.KeyValue, changed bool) {
	key := string(kv.Key)
	if exceptionEvent && exceptionTextKeys[key] {
		return attribute.String(key, hidden), true
	}
	if !safeKeys[key] && logging.KeyProtected(key) {
		return attribute.String(key, hidden), true
	}
	switch kv.Value.Type() {
	case attribute.STRING:
		text := kv.Value.AsString()
		if next := logging.RedactKeyedText(safeName(key), text); next != text {
			return attribute.String(key, next), true
		}
	case attribute.STRINGSLICE:
		values := kv.Value.AsStringSlice()
		var next []string
		for i, text := range values {
			if redacted := logging.RedactKeyedText(safeName(key), text); redacted != text {
				if next == nil {
					next = append([]string(nil), values...)
				}
				next[i] = redacted
			}
		}
		if next != nil {
			return attribute.StringSlice(key, next), true
		}
	}
	return kv, false
}

// safeName is the key the text redactor sees: a safe key is not hidden by the protected-key rule.
func safeName(key string) string {
	if safeKeys[key] {
		return "value"
	}
	return key
}

func scrubAttributes(attrs []attribute.KeyValue, exceptionEvent bool) ([]attribute.KeyValue, bool) {
	var out []attribute.KeyValue
	for i, kv := range attrs {
		next, changed := scrubValue(kv, exceptionEvent)
		if changed && out == nil {
			out = append([]attribute.KeyValue(nil), attrs...)
		}
		if out != nil {
			out[i] = next
		}
	}
	if out == nil {
		return attrs, false
	}
	return out, true
}

// scrubSet is scrubAttributes over an attribute set: it walks the set and builds a slice only when a value changes.
func scrubSet(set *attribute.Set) ([]attribute.KeyValue, bool) {
	if set == nil || set.Len() == 0 {
		return nil, false
	}
	var out []attribute.KeyValue
	iter := set.Iter()
	for index := 0; iter.Next(); index++ {
		next, changed := scrubValue(iter.Attribute(), false)
		if changed && out == nil {
			out = set.ToSlice()
		}
		if out != nil {
			out[index] = next
		}
	}
	return out, out != nil
}

func scrubText(name, text string) (string, bool) {
	next := logging.RedactKeyedText(name, text)
	return next, next != text
}

// scrubbedSpan is a read-only span whose exported parts are scrubbed. The embedded span supplies everything else.
type scrubbedSpan struct {
	sdktrace.ReadOnlySpan
	name     string
	attrs    []attribute.KeyValue
	events   []sdktrace.Event
	links    []sdktrace.Link
	status   sdktrace.Status
	resource *resource.Resource
	scope    instrumentation.Scope
}

func (s scrubbedSpan) Name() string                                { return s.name }
func (s scrubbedSpan) Attributes() []attribute.KeyValue            { return s.attrs }
func (s scrubbedSpan) Events() []sdktrace.Event                    { return s.events }
func (s scrubbedSpan) Links() []sdktrace.Link                      { return s.links }
func (s scrubbedSpan) Status() sdktrace.Status                     { return s.status }
func (s scrubbedSpan) Resource() *resource.Resource                { return s.resource }
func (s scrubbedSpan) InstrumentationScope() instrumentation.Scope { return s.scope }

// scrubSpan returns span itself when nothing in it needs scrubbing (no allocation on that path).
func scrubSpan(span sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	changed := false

	name, c := scrubText("span.name", span.Name())
	changed = changed || c
	attrs, c := scrubAttributes(span.Attributes(), false)
	changed = changed || c

	events := span.Events()
	copiedEvents := false
	for i, event := range events {
		eventName, nameChanged := scrubText("event.name", event.Name)
		eventAttrs, attrsChanged := scrubAttributes(event.Attributes, event.Name == "exception")
		if !nameChanged && !attrsChanged {
			continue
		}
		if !copiedEvents {
			events = append([]sdktrace.Event(nil), events...)
			copiedEvents = true
		}
		events[i].Name, events[i].Attributes = eventName, eventAttrs
		changed = true
	}

	links := span.Links()
	copiedLinks := false
	for i, link := range links {
		linkAttrs, linkChanged := scrubAttributes(link.Attributes, false)
		if !linkChanged {
			continue
		}
		if !copiedLinks {
			links = append([]sdktrace.Link(nil), links...)
			copiedLinks = true
		}
		links[i].Attributes = linkAttrs
		changed = true
	}

	status := span.Status()
	if description, c := scrubText("status.description", status.Description); c {
		status.Description = description
		changed = true
	}

	res := span.Resource()
	if res != nil {
		if resAttrs, c := scrubSet(res.Set()); c {
			res = resource.NewWithAttributes(res.SchemaURL(), resAttrs...)
			changed = true
		}
	}

	scope := span.InstrumentationScope()
	if scopeName, c := scrubText("scope.name", scope.Name); c {
		scope.Name = scopeName
		changed = true
	}
	if scopeVersion, c := scrubText("scope.version", scope.Version); c {
		scope.Version = scopeVersion
		changed = true
	}
	if scopeSchema, c := scrubText("scope.schema_url", scope.SchemaURL); c {
		scope.SchemaURL = scopeSchema
		changed = true
	}
	if scopeAttrs, c := scrubSet(&scope.Attributes); c {
		scope.Attributes = attribute.NewSet(scopeAttrs...)
		changed = true
	}

	if !changed {
		return span
	}
	return scrubbedSpan{ReadOnlySpan: span, name: name, attrs: attrs, events: events, links: links, status: status, resource: res, scope: scope}
}
