package obs

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TraceHeader asks for a request to be traced, whatever the sampler would have said.
const TraceHeader = "X-Bourse-Trace"

// Tracing sends the service's spans to an OpenTelemetry collector, Jaeger here, on localhost:4318
// unless OTEL_EXPORTER_OTLP_ENDPOINT says otherwise. It keeps a fraction of the traces that start
// here, every trace whose caller kept it, and every request that asks with TraceHeader. Call the
// function it returns before exiting: it sends what's still buffered.
func Tracing(ctx context.Context, service string, fraction float64) (func(context.Context) error, error) {
	exp, err := otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", service))),
		sdktrace.WithSampler(sdktrace.ParentBased(asked{sdktrace.TraceIDRatioBased(fraction)})),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return tp.Shutdown, nil
}

type askedKey struct{}

// asked keeps every trace that a request asked for, and leaves the others to its sampler.
type asked struct{ sdktrace.Sampler }

func (a asked) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if p.ParentContext.Value(askedKey{}) != nil {
		return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
	}
	return a.Sampler.ShouldSample(p)
}

// Traced lets a request ask to be traced with TraceHeader, and tells the client the ID of every trace
// that's kept, in a Trace-Id header, so that a support desk can find it.
func Traced(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if sc := trace.SpanContextFromContext(r.Context()); sc.IsSampled() {
			w.Header().Set("Trace-Id", sc.TraceID().String())
		}
		next.ServeHTTP(w, r)
	})
}

// Ask marks the request to be traced if it carries TraceHeader. It goes before the handler that starts
// the trace.
func Ask(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(TraceHeader) != "" {
			r = r.WithContext(context.WithValue(r.Context(), askedKey{}, true))
		}
		next.ServeHTTP(w, r)
	})
}
