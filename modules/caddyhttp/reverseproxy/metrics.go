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
	once                      sync.Once
	upstreamsHealthy          *prometheus.GaugeVec
	adaptiveMaxConnections    *prometheus.GaugeVec
	adaptiveActiveConnections *prometheus.GaugeVec
	adaptiveObservedRPS       *prometheus.GaugeVec
	adaptiveAverageRTT        *prometheus.GaugeVec
	adaptiveRPSStop           *prometheus.GaugeVec
	logger                    *zap.Logger
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
		reverseProxyMetrics.adaptiveMaxConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_max_connections",
			Help:      "Current adaptive maximum number of concurrent reverse proxy requests.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveActiveConnections = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_active_connections",
			Help:      "Current number of active adaptive reverse proxy requests.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveObservedRPS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_observed_rps",
			Help:      "Requests per second admitted by the adaptive limiter, as measured by the controller itself.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveAverageRTT = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_average_rtt_seconds",
			Help:      "Average time requests spend after admission, measured by the controller itself.",
		}, upstreamsLabels)
		reverseProxyMetrics.adaptiveRPSStop = prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns,
			Subsystem: sub,
			Name:      "adaptive_rps_stop",
			Help:      "Throughput reference the controller must beat to keep increasing concurrency.",
		}, upstreamsLabels)
	})

	// duplicate registration could happen if multiple sites with reverse proxy are configured; so ignore the error because
	// there's no good way to capture having multiple sites with reverse proxy. If this happens, the metrics will be
	// registered twice, but the second registration will be ignored.
	for _, collector := range []prometheus.Collector{
		reverseProxyMetrics.upstreamsHealthy,
		reverseProxyMetrics.adaptiveMaxConnections,
		reverseProxyMetrics.adaptiveActiveConnections,
		reverseProxyMetrics.adaptiveObservedRPS,
		reverseProxyMetrics.adaptiveAverageRTT,
		reverseProxyMetrics.adaptiveRPSStop,
	} {
		if err := registry.Register(collector); err != nil && !errors.Is(err, prometheus.AlreadyRegisteredError{
			ExistingCollector: collector,
			NewCollector:      collector,
		}) {
			panic(err)
		}
	}

	reverseProxyMetrics.logger = handler.logger.Named("reverse_proxy.metrics")
}

type metricsUpstreamsHealthyUpdater struct {
	handler *Handler
}

func newMetricsUpstreamsHealthyUpdater(handler *Handler, ctx caddy.Context) *metricsUpstreamsHealthyUpdater {
	initReverseProxyMetrics(handler, ctx.GetMetricsRegistry())
	reverseProxyMetrics.upstreamsHealthy.Reset()

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

		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				m.update()
			case <-m.handler.ctx.Done():
				ticker.Stop()
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
