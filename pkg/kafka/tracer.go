package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NewKotelOpts builds franz-go hook options for OTel tracing and metrics.
// Pass TracerProvider and/or MeterProvider; nil providers are skipped.
//
// Usage:
//
//	opts := kafka.NewKotelOpts(tp, mp)
//	pub, _ := kafka.NewPublisher(kafka.PublisherConfig{
//	    Brokers: brokers,
//	    KgoOpts: opts,
//	}, logger)
func NewKotelOpts(tp trace.TracerProvider, mp metric.MeterProvider) []kgo.Opt {
	var kotelOpts []kotel.Opt

	if tp != nil {
		tracer := kotel.NewTracer(kotel.TracerProvider(tp))
		kotelOpts = append(kotelOpts, kotel.WithTracer(tracer))
	}
	if mp != nil {
		meter := kotel.NewMeter(kotel.MeterProvider(mp))
		kotelOpts = append(kotelOpts, kotel.WithMeter(meter))
	}

	service := kotel.NewKotel(kotelOpts...)

	return []kgo.Opt{
		kgo.WithHooks(service.Hooks()...),
	}
}
