package tracetransform // import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/tracetransform"

import (
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

// SpansOld transforms a slice of OpenTelemetry spans into a slice of OTLP
// ResourceSpans.
func SpansOld(sdl []tracesdk.ReadOnlySpan) []*tracepb.ResourceSpans {
	if len(sdl) == 0 {
		return nil
	}

	rsm := make(map[attribute.Distinct]*tracepb.ResourceSpans)

	type key struct {
		r  attribute.Distinct
		is instrumentation.Scope
	}
	ssm := make(map[key]*tracepb.ScopeSpans)

	var resources int
	for _, sd := range sdl {
		if sd == nil {
			continue
		}

		rKey := sd.Resource().Equivalent()
		k := key{
			r:  rKey,
			is: sd.InstrumentationScope(),
		}
		scopeSpan, iOk := ssm[k]
		if !iOk {
			// Either the resource or instrumentation scope were unknown.
			scopeSpan = &tracepb.ScopeSpans{
				Scope:     InstrumentationScopeOld(sd.InstrumentationScope()),
				Spans:     []*tracepb.Span{},
				SchemaUrl: sd.InstrumentationScope().SchemaURL,
			}
		}
		scopeSpan.Spans = append(scopeSpan.Spans, spanOld(sd))
		ssm[k] = scopeSpan

		rs, rOk := rsm[rKey]
		if !rOk {
			resources++
			// The resource was unknown.
			rs = &tracepb.ResourceSpans{
				Resource:   ResourceOld(sd.Resource()),
				ScopeSpans: []*tracepb.ScopeSpans{scopeSpan},
				SchemaUrl:  sd.Resource().SchemaURL(),
			}
			rsm[rKey] = rs
			continue
		}

		// The resource has been seen before. Check if the instrumentation
		// library lookup was unknown because if so we need to add it to the
		// ResourceSpans. Otherwise, the instrumentation library has already
		// been seen and the append we did above will be included it in the
		// ScopeSpans reference.
		if !iOk {
			rs.ScopeSpans = append(rs.ScopeSpans, scopeSpan)
		}
	}

	// Transform the categorized map into a slice
	rss := make([]*tracepb.ResourceSpans, 0, resources)
	for _, rs := range rsm {
		rss = append(rss, rs)
	}
	return rss
}

// span transforms a Span into an OTLP span.
func spanOld(sd tracesdk.ReadOnlySpan) *tracepb.Span {
	if sd == nil {
		return nil
	}

	tid := sd.SpanContext().TraceID()
	sid := sd.SpanContext().SpanID()

	s := &tracepb.Span{
		TraceId:                tid[:],
		SpanId:                 sid[:],
		TraceState:             sd.SpanContext().TraceState().String(),
		Status:                 status(sd.Status().Code, sd.Status().Description),
		StartTimeUnixNano:      uint64(max(0, sd.StartTime().UnixNano())), // nolint:gosec // Overflow checked.
		EndTimeUnixNano:        uint64(max(0, sd.EndTime().UnixNano())),   // nolint:gosec // Overflow checked.
		Links:                  linksOld(sd.Links()),
		Kind:                   spanKind(sd.SpanKind()),
		Name:                   sd.Name(),
		Attributes:             KeyValuesOld(sd.Attributes()),
		Events:                 spanEventsOld(sd.Events()),
		DroppedAttributesCount: clampUint32(sd.DroppedAttributes()),
		DroppedEventsCount:     clampUint32(sd.DroppedEvents()),
		DroppedLinksCount:      clampUint32(sd.DroppedLinks()),
	}

	if psid := sd.Parent().SpanID(); psid.IsValid() {
		s.ParentSpanId = psid[:]
	}
	s.Flags = buildSpanFlagsWith(sd.SpanContext().TraceFlags(), sd.Parent())

	return s
}

// links transforms span Links to OTLP span links.
func linksOld(links []tracesdk.Link) []*tracepb.Span_Link {
	if len(links) == 0 {
		return nil
	}

	sl := make([]*tracepb.Span_Link, 0, len(links))
	for _, otLink := range links {
		// This redefinition is necessary to prevent otLink.*ID[:] copies
		// being reused -- in short we need a new otLink per iteration.

		tid := otLink.SpanContext.TraceID()
		sid := otLink.SpanContext.SpanID()

		flags := buildSpanFlagsWith(otLink.SpanContext.TraceFlags(), otLink.SpanContext)

		sl = append(sl, &tracepb.Span_Link{
			TraceId:                tid[:],
			SpanId:                 sid[:],
			Attributes:             KeyValuesOld(otLink.Attributes),
			DroppedAttributesCount: clampUint32(otLink.DroppedAttributeCount),
			Flags:                  flags,
		})
	}
	return sl
}

// spanEvents transforms span Events to an OTLP span events.
func spanEventsOld(es []tracesdk.Event) []*tracepb.Span_Event {
	if len(es) == 0 {
		return nil
	}

	events := make([]*tracepb.Span_Event, len(es))
	// Transform message events
	for i := range es {
		events[i] = &tracepb.Span_Event{
			Name:                   es[i].Name,
			TimeUnixNano:           uint64(max(0, es[i].Time.UnixNano())), // nolint:gosec // Overflow checked.
			Attributes:             KeyValuesOld(es[i].Attributes),
			DroppedAttributesCount: clampUint32(es[i].DroppedAttributeCount),
		}
	}
	return events
}

// KeyValuesOld transforms a slice of attribute KeyValuesOld into OTLP key-values.
func KeyValuesOld(attrs []attribute.KeyValue) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}

	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, KeyValueOld(kv))
	}
	return out
}

// Iterator transforms an attribute iterator into OTLP key-values.
func IteratorOld(iter attribute.Iterator) []*commonpb.KeyValue {
	l := iter.Len()
	if l == 0 {
		return nil
	}

	out := make([]*commonpb.KeyValue, 0, l)
	for iter.Next() {
		out = append(out, KeyValueOld(iter.Attribute()))
	}
	return out
}

// ResourceAttributes transforms a Resource OTLP key-values.
func ResourceAttributesOld(res *resource.Resource) []*commonpb.KeyValue {
	return IteratorOld(res.Iter())
}

// KeyValue transforms an attribute KeyValue into an OTLP key-value.
func KeyValueOld(kv attribute.KeyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: string(kv.Key), Value: ValueOld(kv.Value)}
}

// Value transforms an attribute Value into an OTLP AnyValue.
func ValueOld(v attribute.Value) *commonpb.AnyValue {
	av := new(commonpb.AnyValue)
	switch v.Type() {
	case attribute.BOOL:
		av.Value = &commonpb.AnyValue_BoolValue{
			BoolValue: v.AsBool(),
		}
	case attribute.BOOLSLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: boolSliceValues(v.AsBoolSlice()),
			},
		}
	case attribute.INT64:
		av.Value = &commonpb.AnyValue_IntValue{
			IntValue: v.AsInt64(),
		}
	case attribute.INT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: int64SliceValues(v.AsInt64Slice()),
			},
		}
	case attribute.FLOAT64:
		av.Value = &commonpb.AnyValue_DoubleValue{
			DoubleValue: v.AsFloat64(),
		}
	case attribute.FLOAT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: float64SliceValues(v.AsFloat64Slice()),
			},
		}
	case attribute.STRING:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: v.AsString(),
		}
	case attribute.BYTESLICE:
		av.Value = &commonpb.AnyValue_BytesValue{
			BytesValue: v.AsByteSlice(),
		}
	case attribute.SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: valuesOld(v.AsSlice()),
			},
		}
	case attribute.STRINGSLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: stringSliceValues(v.AsStringSlice()),
			},
		}
	case attribute.EMPTY:
	default:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: "INVALID",
		}
	}
	return av
}

func valuesOld(vals []attribute.Value) []*commonpb.AnyValue {
	converted := make([]*commonpb.AnyValue, len(vals))
	for i, v := range vals {
		converted[i] = ValueOld(v)
	}
	return converted
}

// Resource transforms a Resource into an OTLP Resource.
func ResourceOld(r *resource.Resource) *resourcepb.Resource {
	if r == nil {
		return nil
	}
	return &resourcepb.Resource{Attributes: ResourceAttributesOld(r)}
}

func InstrumentationScopeOld(il instrumentation.Scope) *commonpb.InstrumentationScope {
	if il == (instrumentation.Scope{}) {
		return nil
	}
	return &commonpb.InstrumentationScope{
		Name:       il.Name,
		Version:    il.Version,
		Attributes: IteratorOld(il.Attributes.Iter()),
	}
}
