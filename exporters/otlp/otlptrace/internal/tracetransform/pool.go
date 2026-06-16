package tracetransform // import "go.opentelemetry.io/otel/exporters/otlp/otlptrace/internal/tracetransform"

import (
	"sync"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
)

var kvPool = sync.Pool{
	New: func() any { return new(commonpb.KeyValue) },
}

var avPool = sync.Pool{
	New: func() any { return new(commonpb.AnyValue) },
}

var avStringPool = sync.Pool{
	New: func() any { return new(commonpb.AnyValue_StringValue) },
}

var avBoolPool = sync.Pool{
	New: func() any { return new(commonpb.AnyValue_BoolValue) },
}

var avIntPool = sync.Pool{
	New: func() any { return new(commonpb.AnyValue_IntValue) },
}

var avFloatPool = sync.Pool{
	New: func() any { return new(commonpb.AnyValue_DoubleValue) },
}

func ResetPools(rss []*tracepb.ResourceSpans) {
	for _, rs := range rss {
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				for _, kv := range s.GetAttributes() {
					av := kv.GetValue()
					releaseAnyValue(av)
					kv.Reset()
					kvPool.Put(kv)
				}
				for _, ev := range s.GetEvents() {
					for _, kv := range ev.GetAttributes() {
						av := kv.GetValue()
						releaseAnyValue(av)
						kv.Reset()
						kvPool.Put(kv)
					}
				}
			}
		}
	}
}

func releaseAnyValue(av *commonpb.AnyValue) {
	switch w := av.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		w.StringValue = ""
		avStringPool.Put(w)
	case *commonpb.AnyValue_IntValue:
		avIntPool.Put(w)
	case *commonpb.AnyValue_BoolValue:
		avBoolPool.Put(w)
	case *commonpb.AnyValue_DoubleValue:
		avFloatPool.Put(w)
		// non-primitive types weren't pooled, don't release
	}
	av.Reset()
	avPool.Put(av)
}

func SpansPooled(sdl []tracesdk.ReadOnlySpan) []*tracepb.ResourceSpans {
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
		scopeSpan.Spans = append(scopeSpan.Spans, spanPooled(sd))
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
func spanPooled(sd tracesdk.ReadOnlySpan) *tracepb.Span {
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
		Attributes:             KeyValuesPooled(sd.Attributes()),
		Events:                 spanEventsPooled(sd.Events()),
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

// spanEvents transforms span Events to an OTLP span events.
func spanEventsPooled(es []tracesdk.Event) []*tracepb.Span_Event {
	if len(es) == 0 {
		return nil
	}

	events := make([]*tracepb.Span_Event, len(es))
	// Transform message events
	for i := range es {
		events[i] = &tracepb.Span_Event{
			Name:                   es[i].Name,
			TimeUnixNano:           uint64(max(0, es[i].Time.UnixNano())), // nolint:gosec // Overflow checked.
			Attributes:             KeyValuesPooled(es[i].Attributes),
			DroppedAttributesCount: clampUint32(es[i].DroppedAttributeCount),
		}
	}
	return events
}

func KeyValuesPooled(attrs []attribute.KeyValue) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}

	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, KeyValuePooled(kv))
	}
	return out
}

func KeyValuePooled(kv attribute.KeyValue) *commonpb.KeyValue {
	pb := kvPool.Get().(*commonpb.KeyValue)
	pb.Key = string(kv.Key)
	pb.Value = ValuePooled(kv.Value)
	return pb
}

// ValuePooled transforms an attribute Value into an OTLP AnyValue.
func ValuePooled(v attribute.Value) *commonpb.AnyValue {
	av := avPool.Get().(*commonpb.AnyValue)
	switch v.Type() {
	case attribute.BOOL:
		w := avBoolPool.Get().(*commonpb.AnyValue_BoolValue)
		w.BoolValue = v.AsBool()
		av.Value = w
	case attribute.BOOLSLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: boolSliceValues(v.AsBoolSlice()),
			},
		}
	case attribute.INT64:
		w := avIntPool.Get().(*commonpb.AnyValue_IntValue)
		w.IntValue = v.AsInt64()
		av.Value = w
	case attribute.INT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: int64SliceValues(v.AsInt64Slice()),
			},
		}
	case attribute.FLOAT64:
		w := avFloatPool.Get().(*commonpb.AnyValue_DoubleValue)
		w.DoubleValue = v.AsFloat64()
		av.Value = w
	case attribute.FLOAT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: float64SliceValues(v.AsFloat64Slice()),
			},
		}
	case attribute.STRING:
		w := avStringPool.Get().(*commonpb.AnyValue_StringValue)
		w.StringValue = v.AsString()
		av.Value = w
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
