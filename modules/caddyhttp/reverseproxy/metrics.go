package reverseproxy

import (
	"errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/caddyserver/caddy/v2"
)

var reverseProxyMetrics = struct {
	once                        sync.Once
	upstreamsHealthy            *prometheus.GaugeVec
	adaptiveActiveConnections  *prometheus.GaugeVec
	adaptiveMaxConnections     *prometheus.GaugeVec
	logger                      *zap.Logger
}{}

func initReverseProxyMetrics(handler *Handler, registry *prometheus.Registry) {
	const ns, sub = "caddy", "reverse_proxy"

	upstreamsLabels := []string{"upstream"}
	reverseProxyMetrics.once.Do(func() {
		reverseProxyMetrics.upstreamsHealthy = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "upstreams_healthy",
			Help:      "Health status of reverse proxy upstreams.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveActiveConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_active_connections",
			Help:      "Number of active HTTP/1.1 requests occupying adaptive connection slots; excludes idle keep-alive connections.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveMaxConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_max_connections",
			Help:      "Adaptive maximum number of concurrent HTTP/1.1 upstream connections.",
		}, upstreamsLabels)
	})

	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
	// there's no good way to capture having multiple sites with reverse proxy. If this happens, the metrics will be
	// registered twice, but the second registration will be ignored.
	if err := registry.Register(reverseProxyMetrics.upstreamsHealthy); err != nil &&
		!errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: reverseProxyMetrics.upstreamsHealthy,
			NewCollector:      reverseProxyMetrics.upstreamsHealthy,
		}) {
		panic(err)
	}
	if err := registry.Register(reverseProxyMetrics.adaptiveActiveConnections); err != nil &&
		!errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: reverseProxyMetrics.adaptiveActiveConnections,
			NewCollector:      reverseProxyMetrics.adaptiveActiveConnections,
		}) {
		panic(err)
	}
	if err := registry.Register(reverseProxyMetrics.adaptiveMaxConnections); err != nil &&
		!errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: reverseProxyMetrics.adaptiveMaxConnections,
			NewCollector:      reverseProxyMetrics.adaptiveMaxConnections,
		}) {
		panic(err)
	}

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
}

type metricsUpstreamsHealthyUpdater struct {
	handler *Handler
}

func newMetricsUpstreamsHealthyUpdater(handler *Handler, ctx caddy.Context) *metricsUpstreamsHealthyUpdater {
	initReverseProxyMetrics(handler, ctx.GetMetricsRegistry())
	reverseProxyMetrics.upstreamsHealthy.Reset()
	reverseProxyMetrics.adaptiveActiveConnections.Reset()
	reverseProxyMetrics.adaptiveMaxConnections.Reset()

	return &metricsUpstreamsHealthyUpdater{handler}
}

func (m *metricsUpstreamsHealthyUpdater) init() {
	go func() {
		defer func() {
			if err := recover(); err != nil {
				if c := reverseProxyMetrics.logger.Check(zapcore.ErrorLevel, "upstreams healthy metrics updater panicked"); c != nil {
					c.Write(
						zap.Any("error", err),
						zap.ByteString("stack", debug.Stack()),
					)
				}
			}
		}()

		m.update()
		m.updateAdaptiveConcurrency()

		ticker := time.NewTicker(10 * time.Second)
		adaptiveTicker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				m.update()
			case <-adaptiveTicker.C:
				m.updateAdaptiveConcurrency()
			case <-m.handler.ctx.Done():
				ticker.Stop()
				adaptiveTicker.Stop()
				return
			}
		}
	}()
}

func (m *metricsUpstreamsHealthyUpdater) update() {
	for _, upstream := range m.handler.Upstreams {
		labels := prometheus.Labels{"upstream": upstream.Dial}

		gaugeValue := 0.0
		if upstream.Healthy() {
			gaugeValue = 1.0
		}

		reverseProxyMetrics.upstreamsHealthy.With(labels).Set(gaugeValue)
	}
}

func (m *metricsUpstreamsHealthyUpdater) updateAdaptiveConcurrency() {
	for _, upstream := range m.handler.Upstreams {
		if upstream.adaptive == nil {
			continue
		}
		_, max := upstream.adaptive.snapshot()
		labels := prometheus.Labels{"upstream": upstream.Dial}
		reverseProxyMetrics.adaptiveActiveConnections.With(labels)
		reverseProxyMetrics.adaptiveMaxConnections.With(labels).Set(float64(max))
	}
}

func incAdaptiveActiveConnections(upstream string) {
	reverseProxyMetrics.adaptiveActiveConnections.WithLabelValues(upstream).Inc()
}

func decAdaptiveActiveConnections(upstream string) {
	reverseProxyMetrics.adaptiveActiveConnections.WithLabelValues(upstream).Dec()
}
