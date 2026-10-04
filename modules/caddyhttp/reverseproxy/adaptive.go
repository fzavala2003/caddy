package reverseproxy

import (
	"context"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
)

type adaptiveController struct {
	mu                  sync.Mutex
	changed             chan struct{}
	interval            time.Duration
	logger              *zap.Logger
	upstreams           []string
	maxConn             int
	maxConnCeiling      int
	activeConn          int
	upstreamResponses   int64
	totalRTT            time.Duration
	previousRequests    float64
	previousConcurrency float64
	previousRTT         float64
	hasPreviousWindow   bool

	// rpsStop es la referencia de RPS contra la que se mide la
	// recuperación. Se escribe con el RPS de la ventana en la que se
	// descubrió que el incremento dejó de ser voluntario, y después se
	// mueve con cada ventana que no mejora: así la barra sigue al RPS
	// real en vez de quedar anclada a un valor histórico inalcanzable.
	//
	// Se usa RPS y no RTT porque el RTT crece de forma ilimitada con la
	// cola: cuando el sistema se degrada, un RTT enorme vuelve la barra
	// inalcanzable y el controlador queda detenido para siempre. El RPS en
	// cambio se satura en la capacidad real del backend, así que siempre
	// queda un margen alcanzable.
	rpsStop float64

	// rpsSamples son las ventanas de RPS de la tanda en curso. La prueba
	// de si el +1 sirvió se hace sobre recoveryRequired ventanas
	// consecutivas, y basta con que el RPS supere la barra en una de
	// ellas.
	rpsSamples []float64

	// rpsBatchMaxConn es el max_conn del que partió la tanda de
	// medición. Si el +1 no se sostiene, se vuelve a este valor.
	rpsBatchMaxConn int

	// recoveryThreshold es la mejora relativa de RPS que se exige sobre
	// rpsStop para considerar que el sistema se recuperó.
	recoveryThreshold float64

	// recoveryRequired es cuántas ventanas consecutivas deben cumplir esa
	// mejora antes de permitir un nuevo experimento.
	recoveryRequired int

	// lastMaxConnAction registra si la ventana anterior terminó con un
	// aumento (+1) o sin cambio (0). Es la memoria que permite decidir si
	// un incremento debe evaluarse con elasticidad.
	lastMaxConnAction int
}

const (
	// adaptiveMinDeltaL es el cambio mínimo relativo de L para que la
	// elasticidad sea informativa. Por debajo, X y L están planos y E es
	// ruido, así que no se decide nada con ella.
	adaptiveMinDeltaL = 0.02

	// adaptiveDefaultCeiling es el techo por defecto de max_conn. Sin
	// techo el controlador puede seguir subiendo mientras la elasticidad
	// lo justifique, así que el valor por defecto es el techo más alto
	// representable: el límite, si se quiere uno, se pone explícito en la
	// configuración.
	adaptiveDefaultCeiling = math.MaxInt32

	// defaultRecoveryThreshold es la mejora relativa de RPS sobre
	// rpsStop que se exige para dar por recuperada la capacidad.
	defaultRecoveryThreshold = 0.10

	// defaultRecoveryRequired es el número de ventanas consecutivas con
	// RTT recuperado que se exigen antes de volver a experimentar.
	defaultRecoveryRequired = 2
)

func adaptiveInterval(configured time.Duration) time.Duration {
	if configured == 0 {
		return time.Second
	}
	return configured
}

func newAdaptiveController(handler *Handler, interval time.Duration) *adaptiveController {
	upstreams := make([]string, 0, len(handler.Upstreams))
	seen := make(map[string]struct{}, len(handler.Upstreams))
	for _, upstream := range handler.Upstreams {
		if _, ok := seen[upstream.Dial]; ok {
			continue
		}
		seen[upstream.Dial] = struct{}{}
		upstreams = append(upstreams, upstream.Dial)
	}

	ceiling := handler.AdaptiveMaxConnCeiling
	if ceiling <= 0 {
		ceiling = adaptiveDefaultCeiling
	}

	return &adaptiveController{
		changed:           make(chan struct{}),
		interval:          interval,
		logger:            handler.logger.Named("reverse_proxy.adaptive"),
		upstreams:         upstreams,
		maxConn:           handler.AdaptiveMaxConn,
		maxConnCeiling:    ceiling,
		recoveryThreshold: defaultRecoveryThreshold,
		recoveryRequired:  defaultRecoveryRequired,
	}
}

func (c *adaptiveController) run(done <-chan struct{}) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	lastWindow := time.Now()
	for {
		select {
		case <-ticker.C:
			now := time.Now()
			c.updateWindow(now.Sub(lastWindow))
			lastWindow = now
		case <-done:
			return
		}
	}
}

func (c *adaptiveController) acquire(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}

		c.mu.Lock()
		if c.activeConn < c.maxConn {
			c.activeConn++
			c.publishLocked()
			c.mu.Unlock()
			return true
		}
		changed := c.changed
		c.mu.Unlock()

		select {
		case <-ctx.Done():
			return false
		case <-changed:
		}
	}
}

func (c *adaptiveController) recordResponse(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	c.mu.Lock()
	c.upstreamResponses++
	c.totalRTT += rtt
	c.mu.Unlock()
}

func (c *adaptiveController) release() {
	c.mu.Lock()
	c.activeConn--
	c.publishLocked()
	c.notifyLocked()
	c.mu.Unlock()
}

func calculateAverageRTT(totalRTT time.Duration, responseCount int64) (time.Duration, bool) {
	if responseCount <= 0 || totalRTT <= 0 {
		return 0, false
	}
	return totalRTT / time.Duration(responseCount), true
}

func calculateObservedRPS(responseCount int64, window time.Duration) (float64, bool) {
	if responseCount <= 0 || window <= 0 {
		return 0, false
	}
	return float64(responseCount) / window.Seconds(), true
}

// calculateLittleLaw devuelve L = λ × W. L no viene de max_conn sino de la
// concurrencia observada, y por eso cambia con el tráfico.
func calculateLittleLaw(rps float64, averageRTT time.Duration) float64 {
	if rps <= 0 || averageRTT <= 0 {
		return 0
	}
	return rps * averageRTT.Seconds()
}

func calculateElasticity(initialX, initialN, finalX, finalN float64) (float64, bool) {
	if initialX <= 0 || initialN <= 0 || finalX <= 0 || finalN <= 0 {
		return 0, false
	}

	deltaX := finalX - initialX
	deltaN := finalN - initialN

	relativeXChange := deltaX / initialX
	relativeNChange := deltaN / initialN

	if relativeNChange == 0 {
		return 0, false
	}

	// Si X y N disminuyen al mismo tiempo,
	// la elasticidad se considera 0.
	if deltaX < 0 && deltaN < 0 {
		return 0.0, true
	}

	elasticity := relativeXChange / relativeNChange

	if math.IsNaN(elasticity) || math.IsInf(elasticity, 0) {
		return 0, false
	}

	return elasticity, true
}

func (c *adaptiveController) updateWindow(window time.Duration) {
	if window <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	maxConnBefore := c.maxConn

	// ============================================================
	// 1. Obtener mediciones de la ventana actual
	// ============================================================

	responseCount := c.upstreamResponses
	c.upstreamResponses = 0

	totalRTT := c.totalRTT
	c.totalRTT = 0

	averageRTT, valid := calculateAverageRTT(
		totalRTT,
		responseCount,
	)
	if !valid {
		return
	}

	currentRPS, valid := calculateObservedRPS(
		responseCount,
		window,
	)
	if !valid {
		return
	}

	// X:
	// cantidad de respuestas observadas en la ventana.
	currentX := float64(responseCount)

	// RTT en segundos.
	currentRTTSec := averageRTT.Seconds()

	// ============================================================
	// 2. Estimar concurrencia mediante Little's Law
	//
	// L = RPS * RTT
	// ============================================================

	currentL := calculateLittleLaw(
		currentRPS,
		averageRTT,
	)

	if currentL <= 0 {
		return
	}

	// ============================================================
	// 3. Primera ventana
	//
	// Todavía no podemos calcular elasticidad porque no existe
	// una ventana anterior.
	//
	// Por eso hacemos el primer experimento:
	//
	//     maxConn + 1
	// ============================================================

	if !c.hasPreviousWindow {

		c.previousRequests = currentX
		c.previousConcurrency = currentL
		c.previousRTT = currentRTTSec

		c.hasPreviousWindow = true

		if c.maxConn < c.maxConnCeiling {
			c.maxConn++

			// Registramos que esta ventana terminó
			// con un aumento.
			c.lastMaxConnAction = +1
		} else {
			c.lastMaxConnAction = 0
		}

		maxConnAfter := c.maxConn

		c.logWindow(
			window,
			responseCount,
			currentRPS,
			currentX,
			currentL,
			currentRTTSec,
			0, // previousRTT
			0, // deltaX
			0, // deltaL
			0, // elasticity
			maxConnBefore,
			maxConnAfter,
		)

		c.publishLocked()
		c.notifyLocked()

		return
	}

	// ============================================================
	// 4. Obtener valores de la ventana anterior
	// ============================================================

	previousX := c.previousRequests
	previousL := c.previousConcurrency
	previousRTT := c.previousRTT

	// Esta variable nos dice si en la ventana anterior
	// modificamos maxConn.
	previousAction := c.lastMaxConnAction

	// ============================================================
	// 5. Calcular variación de X
	//
	// ΔX/X
	// ============================================================

	deltaX := 0.0

	if previousX > 0 {
		deltaX =
			(currentX - previousX) /
				previousX
	}

	// ============================================================
	// 6. Calcular variación de L
	//
	// ΔL/L
	// ============================================================

	deltaL := 0.0

	if previousL > 0 {
		deltaL =
			(currentL - previousL) /
				previousL
	}

	// ============================================================
	// 7. Calcular elasticidad
	//
	// E = (ΔX/X) / (ΔL/L)
	// ============================================================

	elasticity := 0.0
	hasElasticity := false

	if previousX > 0 &&
		previousL > 0 &&
		math.Abs(deltaL) >= adaptiveMinDeltaL {

		// Evitamos el caso:
		//
		// ΔX < 0
		// ΔL < 0
		//
		// porque matemáticamente:
		//
		// (-) / (-) = positivo
		//
		// pero eso NO significa que aumentar concurrencia
		// sea beneficioso.

		if deltaX < 0 && deltaL < 0 {
			elasticity = 0
		} else {
			elasticity = deltaX / deltaL
		}

		hasElasticity = true
	}

	// ============================================================
	// 8. Control de maxConn
	// ============================================================

	if previousAction == +1 {

		// --------------------------------------------------------
		// La ventana anterior fue un experimento:
		//
		// maxConn aumentó.
		//
		// Ahora evaluamos si ese aumento produjo una respuesta
		// favorable.
		// --------------------------------------------------------

		if hasElasticity && elasticity > 0 {

			// El aumento fue beneficioso.
			//
			// Continuamos explorando capacidad.

			if c.maxConn < c.maxConnCeiling {

				c.maxConn++

				// El nuevo aumento también deberá ser
				// evaluado en la siguiente ventana.
				c.lastMaxConnAction = +1

			} else {

				c.lastMaxConnAction = 0
			}

		} else {

			// ----------------------------------------------------
			// El aumento dejó de ser beneficioso.
			//
			// Detenemos la expansión.
			// ----------------------------------------------------

			c.lastMaxConnAction = 0

			// Guardamos el RPS de referencia.
			c.rpsStop = currentRPS

			// Comenzamos una tanda nueva de medición.
			c.rpsSamples = c.rpsSamples[:0]
		}

	} else {

		// --------------------------------------------------------
		// Estado de espera.
		//
		// Aquí NO utilizamos E para aumentar maxConn.
		//
		// La recuperación se mide con RPS, y aquí la espera no es
		// pasiva. En congestión los RPS se estabilizan y no mejoran
		// solos, así que esperar quieto no produciría nunca la
		// evidencia que exigimos. En vez de eso, cada vez que el RPS
		// mejora más del 10% sumamos +1 a propósito, y comprobamos
		// si los RPS siguen subiendo: si suben, la mejora viene de
		// la concurrencia; si se estancan, no.
		// --------------------------------------------------------

		if c.rpsStop > 0 {

			// ----------------------------------------------------
			// RPS mínimo necesario para considerar que hubo
			// recuperación.
			//
			// Ejemplo:
			//
			// rpsStop = 1000 req/s
			// threshold = 0.10
			//
			// recoveryLimit = 1100 req/s
			// ----------------------------------------------------

			recoveryLimit :=
				c.rpsStop *
					(1.0 + c.recoveryThreshold)

			if len(c.rpsSamples) == 0 {
				// Guardamos el nivel del que parte la tanda: si el
				// +1 no se sostiene, se vuelve aquí.
				c.rpsBatchMaxConn = c.maxConn
			}

			c.rpsSamples = append(c.rpsSamples, currentRPS)

			if len(c.rpsSamples) < c.recoveryRequired {

				// Todavía no hay ventanas suficientes para decidir.
				// Sumamos +1 a propósito y medimos la siguiente: en
				// congestión los RPS se estabilizan y no mejoran
				// solos, así que la mejora tiene que venir de más
				// concurrencia.
				if c.maxConn < c.maxConnCeiling {
					c.maxConn++
				}

			} else {

				// ----------------------------------------------------
				// Tanda completa: recoveryRequired ventanas
				// consecutivas. Basta con que el RPS supere la barra
				// en una de ellas.
				// ----------------------------------------------------

				ventanas := c.rpsSamples
				c.rpsSamples = c.rpsSamples[:0]

				mejoradas := 0
				mayor := 0.0

				for _, muestra := range ventanas {
					if muestra > recoveryLimit {
						mejoradas++
						if muestra > mayor {
							mayor = muestra
						}
					}
				}

				switch {

				case mejoradas == 0:
					// Ninguna ventana superó el 10%: el +1 no
					// sirvió, así que se deshace y se vuelve al
					// nivel del que partió la tanda.
					//
					// rpsStop NO se toca. La referencia solo avanza
					// cuando hay una ventana que la supera; si se
					// bajara aquí al RPS actual, cada prueba
					// fallida relajaría la barra y el ciclo sería
					// un paseo aleatorio que nunca se estabiliza.
					c.maxConn = c.rpsBatchMaxConn

				case mejoradas == len(ventanas):
					// Mejoraron todas: la referencia pasa a ser el
					// mayor RPS visto, que es más exigente que la
					// anterior.
					c.rpsStop = mayor

				default:
					// Mejoró al menos una de las dos: el +1 está
					// justificado. La referencia se mantiene y el
					// ciclo vuelve a empezar con otro +1.
				}
			}
		}

		// El +1 de esta rama se valida con RPS y no con E, así que la
		// escalera de elasticidad no se consulta mientras estamos aquí.
		c.lastMaxConnAction = 0
	}

	// ============================================================
	// 9. Actualizar la ventana anterior
	//
	// IMPORTANTE:
	//
	// Aunque maxConn esté detenido, seguimos actualizando
	// previousX, previousL y previousRTT.
	//
	// Por lo tanto, la comparación nunca se congela.
	// ============================================================

	c.previousRequests = currentX
	c.previousConcurrency = currentL
	c.previousRTT = currentRTTSec

	maxConnAfter := c.maxConn

	// ============================================================
	// 10. Registrar diagnóstico
	// ============================================================

	c.logWindow(
		window,
		responseCount,
		currentRPS,
		currentX,
		currentL,
		currentRTTSec,
		previousRTT,
		deltaX,
		deltaL,
		elasticity,
		maxConnBefore,
		maxConnAfter,
	)

	c.publishLocked()
	c.notifyLocked()
}

func (c *adaptiveController) logWindow(
	window time.Duration,
	upstreamResponses int64,
	rps float64,
	finalRequests float64,
	finalConcurrency float64,
	currentRTTSec float64,
	previousRTTSec float64,
	deltaX float64,
	deltaL float64,
	elasticity float64,
	maxConnBefore int,
	maxConnAfter int,
) {
	if c.logger == nil {
		return
	}
	c.logger.Info(
		"adaptive concurrency window observed",
		zap.Duration("window", window),
		zap.Int64("upstream_responses_in_window", upstreamResponses),
		zap.Float64("rps", rps),
		zap.Float64("x_final_requests", finalRequests),
		zap.Float64("l_final", finalConcurrency),
		zap.Float64("average_rtt_sec", currentRTTSec),
		zap.Float64("previous_rtt_sec", previousRTTSec),
		zap.Float64("delta_x", deltaX),
		zap.Float64("delta_l", deltaL),
		zap.Float64("elasticity", elasticity),
		zap.Int("max_conn_before", maxConnBefore),
		zap.Int("max_conn", maxConnAfter),
		zap.Int("max_conn_ceiling", c.maxConnCeiling),
		zap.Float64("rps_stop", c.rpsStop),
		zap.Int("rps_samples", len(c.rpsSamples)),
		zap.Int("rps_batch_max_conn", c.rpsBatchMaxConn),
		zap.Int("recovery_required", c.recoveryRequired),
		zap.Float64("recovery_threshold", c.recoveryThreshold),
	)
}

func (c *adaptiveController) publishLocked() {
	if reverseProxyMetrics.adaptiveMaxConnections == nil || reverseProxyMetrics.adaptiveActiveConnections == nil {
		return
	}
	for _, upstream := range c.upstreams {
		labels := []string{upstream}
		reverseProxyMetrics.adaptiveMaxConnections.WithLabelValues(labels...).Set(float64(c.maxConn))
		reverseProxyMetrics.adaptiveActiveConnections.WithLabelValues(labels...).Set(float64(c.activeConn))
	}
}

func (c *adaptiveController) publish() {
	c.mu.Lock()
	c.publishLocked()
	c.mu.Unlock()
}

func (c *adaptiveController) notifyLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}