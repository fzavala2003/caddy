package reverseproxy

import (
	"math"
	"testing"
	"time"
)

func TestAdaptiveInterval(t *testing.T) {
	if got := adaptiveInterval(0); got != time.Second {
		t.Errorf("adaptiveInterval(0) = %s, want 1s", got)
	}
	if got := adaptiveInterval(2 * time.Second); got != 2*time.Second {
		t.Errorf("adaptiveInterval(2s) = %s, want 2s", got)
	}
}

func TestAdaptiveRecordResponseTracksUpstreamResponse(t *testing.T) {
	controller := &adaptiveController{
		changed:    make(chan struct{}),
		activeConn: 1,
	}
	controller.recordResponse(200 * time.Millisecond)
	controller.release()
	if controller.upstreamResponses != 1 || controller.totalRTT != 200*time.Millisecond || controller.activeConn != 0 {
		t.Fatalf("after response/release: responses=%d RTT=%s active=%d", controller.upstreamResponses, controller.totalRTT, controller.activeConn)
	}
}

func TestCalculateElasticity(t *testing.T) {
	tests := []struct {
		name      string
		initialX  float64
		initialL  float64
		finalX    float64
		finalL    float64
		want      float64
		wantValid bool
	}{
		{
			name:      "equal relative growth",
			initialX:  100,
			initialL:  10,
			finalX:    120,
			finalL:    12,
			want:      1,
			wantValid: true,
		},
		{
			name:      "X rises while L falls",
			initialX:  100,
			initialL:  10,
			finalX:    120,
			finalL:    8,
			want:      -1,
			wantValid: true,
		},
		{
			name:     "unchanged L invalidates denominator",
			initialX: 100,
			initialL: 10,
			finalX:   120,
			finalL:   10,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, valid := calculateElasticity(tc.initialX, tc.initialL, tc.finalX, tc.finalL)
			if valid != tc.wantValid {
				t.Fatalf("calculateElasticity() valid = %v, want %v", valid, tc.wantValid)
			}
			if valid && math.Abs(got-tc.want) > 1e-9 {
				t.Errorf("calculateElasticity() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCalculateObservedRPS(t *testing.T) {
	got, valid := calculateObservedRPS(30, 10*time.Second)
	if !valid {
		t.Fatal("calculateObservedRPS() marked valid values as invalid")
	}
	if got != 3 {
		t.Fatalf("calculateObservedRPS(30, 10s) = %v RPS, want 3", got)
	}
}

func newTestController(maxConn, ceiling int) *adaptiveController {
	return &adaptiveController{
		changed:           make(chan struct{}),
		maxConn:           maxConn,
		maxConnCeiling:    ceiling,
		recoveryThreshold: defaultRecoveryThreshold,
		recoveryRequired:  defaultRecoveryRequired,
	}
}

func feed(controller *adaptiveController, responses int, rtt time.Duration) {
	for range responses {
		controller.recordResponse(rtt)
	}
	controller.updateWindow(10 * time.Second)
}

// La primera ventana siempre sube un escalón.
func TestUpdateWindowFirstWindowIncrements(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 100, 100*time.Millisecond)

	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3", c.maxConn)
	}
	if c.lastMaxConnAction != +1 {
		t.Fatalf("lastMaxConnAction = %d, want +1", c.lastMaxConnAction)
	}
}

// La primera ventana respeta el techo.
func TestUpdateWindowFirstWindowRespectsCeiling(t *testing.T) {
	c := newTestController(20, 20)

	feed(c, 100, 100*time.Millisecond)

	if c.maxConn != 20 {
		t.Fatalf("maxConn = %d, want 20 (ceiling)", c.maxConn)
	}
	if c.lastMaxConnAction != 0 {
		t.Fatalf("lastMaxConnAction = %d, want 0", c.lastMaxConnAction)
	}
}

// E > 0 tras un incremento: la escalera de E sigue subiendo.
func TestUpdateWindowIncrementsWhenElasticityPositive(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: RPS 100, maxConn 2 -> 3
	feed(c, 2000, 200*time.Millisecond) // RPS 200, E > 0 -> maxConn 4

	if c.maxConn != 4 {
		t.Fatalf("maxConn = %d, want 4", c.maxConn)
	}
	if c.lastMaxConnAction != +1 {
		t.Fatalf("lastMaxConnAction = %d, want +1", c.lastMaxConnAction)
	}
	if c.rpsStop != 0 {
		t.Fatalf("rpsStop = %v, want 0 mientras la escalera de E sube", c.rpsStop)
	}
}

// E <= 0: congela y guarda el RPS actual como referencia.
func TestUpdateWindowNegativeElasticityFreezesWithCurrentRPS(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: RPS 100, maxConn 3
	feed(c, 500, 200*time.Millisecond)  // RPS 50, E < 0 -> congela

	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3", c.maxConn)
	}
	if c.lastMaxConnAction != 0 {
		t.Fatalf("lastMaxConnAction = %d, want 0", c.lastMaxConnAction)
	}
	if c.rpsStop != 50 {
		t.Fatalf("rpsStop = %v, want 50", c.rpsStop)
	}
	if len(c.rpsSamples) != 0 {
		t.Fatalf("rpsSamples = %v, want tanda reiniciada", c.rpsSamples)
	}
}

// ΔX<0 y ΔL<0 no cuenta como éxito: E forzado a 0 y congela.
func TestUpdateWindowBothFallingIsNotTreatedAsSuccess(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 400, 50*time.Millisecond)   // RPS 40: X cae y L cae

	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3", c.maxConn)
	}
	if c.rpsStop != 40 {
		t.Fatalf("rpsStop = %v, want 40", c.rpsStop)
	}
}

// Solo en el estado congelado se suma +1, y se mide en dos ventanas.
func TestUpdateWindowFrozenStateAddsOneAndMeasuresTwoWindows(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: maxConn 3
	feed(c, 500, 200*time.Millisecond)  // RPS 50 -> congela, rpsStop = 50
	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3", c.maxConn)
	}

	// Ventana 1 de la tanda: recoveryLimit = 55. RPS 50 no pasa, pero aun
	// así se suma +1 a propósito.
	feed(c, 500, 200*time.Millisecond)
	if c.maxConn != 4 {
		t.Fatalf("maxConn = %d, want 4 (+1 en la primera ventana)", c.maxConn)
	}
	if len(c.rpsSamples) != 1 {
		t.Fatalf("rpsSamples = %v, want 1 muestra", c.rpsSamples)
	}
	if c.lastMaxConnAction != 0 {
		t.Fatalf("lastMaxConnAction = %d, want 0 (aquí no se usa E)", c.lastMaxConnAction)
	}

	// Ventana 2: cierra la tanda. Ninguna superó 55, así que el +1 se
	// deshace y se vuelve al nivel del que partió la tanda.
	feed(c, 500, 200*time.Millisecond)
	if len(c.rpsSamples) != 0 {
		t.Fatalf("rpsSamples = %v, want tanda reiniciada", c.rpsSamples)
	}
	if c.rpsStop != 50 {
		t.Fatalf("rpsStop = %v, want 50 (RPS actual)", c.rpsStop)
	}
	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3 (el +1 fallido se revierte)", c.maxConn)
	}
}

// El +1 que no se sostiene se deshice: el nivel no se acumula.
func TestUpdateWindowFailedProbeRevertsToStartingLevel(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: 2 -> 3
	feed(c, 500, 200*time.Millisecond)  // RPS 50 -> congela en 3

	// Tres tandas seguidas que fracasan. Cada una sube a 4 y vuelve a 3.
	for i := 0; i < 3; i++ {
		antes := c.maxConn
		feed(c, 500, 200*time.Millisecond) // tanda 1/2: +1
		if c.maxConn != antes+1 {
			t.Fatalf("tanda %d: maxConn = %d, want %d", i, c.maxConn, antes+1)
		}
		feed(c, 500, 200*time.Millisecond) // cierra tanda, falla
		if c.maxConn != antes {
			t.Fatalf("tanda %d: maxConn = %d, want %d (revertido)", i, c.maxConn, antes)
		}
	}

	// Clave: tras tres fallos sigue en 3, no en 6.
	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3: los +1 fallidos no se acumulan", c.maxConn)
	}
}

// Un +1 que sí se sostiene no se revierte y la referencia sube.
func TestUpdateWindowSuccessfulProbeKeepsIncrement(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: 2 -> 3
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50

	// Tanda con ambas ventanas > 55: el +1 se queda.
	feed(c, 600, 200*time.Millisecond) // +1 -> 4, muestra 60
	feed(c, 900, 200*time.Millisecond) // muestra 90, ambas mejoran

	if c.maxConn != 4 {
		t.Fatalf("maxConn = %d, want 4 (el +1 se sostiene)", c.maxConn)
	}
	if c.rpsStop != 90 {
		t.Fatalf("rpsStop = %v, want 90", c.rpsStop)
	}

	// La siguiente tanda parte de 4, no de 3.
	feed(c, 900, 200*time.Millisecond)
	if c.maxConn != 5 {
		t.Fatalf("maxConn = %d, want 5 (parte del nivel sostenido)", c.maxConn)
	}
}

// Basta con que el RPS mejore en UNA de las dos ventanas.
func TestUpdateWindowOneOfTwoWindowsIsEnough(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50

	// Ventana 1: RPS 50 (no mejora) pero se suma +1.
	feed(c, 500, 200*time.Millisecond) // maxConn 4, muestra 50

	// Ventana 2: RPS 60 > 55 ->UNA de dos mejoró.
	feed(c, 600, 200*time.Millisecond)

	// rpsStop se mantiene en 50 porque solo mejoró una, no las dos.
	if c.rpsStop != 50 {
		t.Fatalf("rpsStop = %v, want 50 (solo una de dos mejoró)", c.rpsStop)
	}

	// El ciclo continúa: siguiente tanda vuelve a sumar +1.
	antes := c.maxConn
	feed(c, 500, 200*time.Millisecond)
	if c.maxConn != antes+1 {
		t.Fatalf("maxConn = %d, want %d (el ciclo sigue)", c.maxConn, antes+1)
	}
}

// Si las dos ventanas suben 10%, se guarda el RPS mayor.
func TestUpdateWindowBothWindowsImproveKeepsHighestRPS(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50, recoveryLimit 55

	feed(c, 600, 200*time.Millisecond) // muestra 60 (>55), +1 a maxConn 4
	feed(c, 900, 200*time.Millisecond) // muestra 90 (>55) -> las dos mejoraron

	if c.rpsStop != 90 {
		t.Fatalf("rpsStop = %v, want 90 (el mayor de la tanda)", c.rpsStop)
	}
}

// Dentro de la banda ni sube ni baja: se deshace el +1 y rpsStop no se toca.
func TestUpdateWindowNeutralBandRevertsAndKeepsReference(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50, banda = [45, 55]

	// 50 cae dentro de la banda: no mejora ni empeora.
	feed(c, 500, 200*time.Millisecond) // muestra 50, +1 -> 4
	feed(c, 500, 200*time.Millisecond) // muestra 50 -> neutro

	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3 (+1 deshecho)", c.maxConn)
	}
	if c.rpsStop != 50 {
		t.Fatalf("rpsStop = %v, want 50 (intocable sin señal real)", c.rpsStop)
	}
}

// La contra de la subida: si el RPS cae más de un 10%, se BAJA un escalón.
func TestUpdateWindowDropLowersLevelAndReference(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: mc 2 -> 3
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50, banda = [45, 55]

	// 40 y 30 están por debajo de 45: caída clara, caidas = 2.
	feed(c, 400, 200*time.Millisecond) // muestra 40, +1 -> 4
	feed(c, 300, 200*time.Millisecond) // muestra 30 -> caída

	if c.lastRpsCaidas != 2 {
		t.Fatalf("lastRpsCaidas = %d, want 2", c.lastRpsCaidas)
	}
	// Baja un escalón POR DEBAJO del nivel de partida (3), no solo lo
	// deshace: el nivel actual era demasiado alto.
	if c.maxConn != 2 {
		t.Fatalf("maxConn = %d, want 2 (un escalón bajo el de partida)", c.maxConn)
	}
	// La referencia guarda el mínimo real medido, no un umbral.
	if c.rpsStop != 30 {
		t.Fatalf("rpsStop = %v, want 30 (mínimo real medido)", c.rpsStop)
	}
}

// Dentro de la banda no hay caída: solo se deshace el +1.
func TestUpdateWindowInsideBandRevertsNotLowers(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: mc 2 -> 3
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50, banda = [45, 55]

	// 50 cae dentro de la banda: ni mejora ni cae.
	feed(c, 500, 200*time.Millisecond) // muestra 50, +1 -> 4
	feed(c, 500, 200*time.Millisecond) // muestra 50 -> neutro

	if c.lastRpsCaidas != 0 {
		t.Fatalf("lastRpsCaidas = %d, want 0 (50 está en la banda)", c.lastRpsCaidas)
	}
	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3 (se deshace, no se baja)", c.maxConn)
	}
	if c.rpsStop != 50 {
		t.Fatalf("rpsStop = %v, want 50 (intocable)", c.rpsStop)
	}
}

// La caída nunca lleva max_conn por debajo del suelo.
func TestUpdateWindowDropRespectsFloor(t *testing.T) {
	c := newTestController(1, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: 1 -> 2
	feed(c, 1000, 100*time.Millisecond) // rpsStop = 100, banda = [90, 110]

	for i := 0; i < 8; i++ {
		feed(c, 200, 200*time.Millisecond) // 20 rps, muy por debajo
		feed(c, 200, 200*time.Millisecond)
		if c.maxConn < 1 {
			t.Fatalf("iteración %d: maxConn = %d, bajo el suelo", i, c.maxConn)
		}
	}
	if c.maxConn != 1 {
		t.Fatalf("maxConn = %d, want 1 (suelo)", c.maxConn)
	}
}

// La referencia sube con una mejora real y baja con una caída real.
func TestUpdateWindowReferenceMovesBothWays(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 1000, 100*time.Millisecond) // congela con rpsStop = 100

	// Mejora clara: sube al máximo real de la tanda.
	feed(c, 1200, 200*time.Millisecond) // 120 rps > 110
	feed(c, 1500, 200*time.Millisecond) // 150 rps > 110
	if c.rpsStop != 150 {
		t.Fatalf("rpsStop = %v, want 150 (máximo real)", c.rpsStop)
	}

	// Caída clara respecto a 150 (banda [135, 165]): baja al mínimo real.
	feed(c, 1200, 200*time.Millisecond) // 120 rps < 135
	feed(c, 1100, 200*time.Millisecond) // 110 rps < 135
	if c.rpsStop != 110 {
		t.Fatalf("rpsStop = %v, want 110 (mínimo real)", c.rpsStop)
	}
}

// La referencia sube con el ciclo: exige cada vez más RPS.
func TestUpdateWindowRatchetRaisesTheBar(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50

	// Tanda con ambas ventanas por encima de 55 -> se guarda el mayor real.
	feed(c, 600, 200*time.Millisecond)
	feed(c, 900, 200*time.Millisecond)
	if c.rpsStop != 90 {
		t.Fatalf("rpsStop = %v, want 90 (el mayor real de la tanda)", c.rpsStop)
	}

	// Ahora la banda es [81, 99]. Una tanda de 85 y 95 queda dentro: ni
	// mejora ni caída, así que la referencia se queda en 90. Lo que se
	// guarda es el RPS medido, nunca el umbral sintético 90*1.1 = 99.
	feed(c, 850, 200*time.Millisecond)
	feed(c, 950, 200*time.Millisecond)
	if c.rpsStop != 90 {
		t.Fatalf("rpsStop = %v, want 90 (neutro: ni sube ni baja)", c.rpsStop)
	}
}

// El valor guardado es el RPS real medido, no rpsStop*1.10.
func TestUpdateWindowSavesMeasuredMaxNotSyntheticThreshold(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	feed(c, 500, 200*time.Millisecond)  // rpsStop = 50, barra = 55

	// Ambas superan 55, pero de forma muy distinta: 80 y 200 rps. Lo que
	// se guarda es 200 (el mayor real medido), no 55 (el umbral).
	feed(c, 800, 200*time.Millisecond)  // 80 rps
	feed(c, 2000, 200*time.Millisecond) // 200 rps

	if c.rpsStop != 200 {
		t.Fatalf("rpsStop = %v, want 200 (mayor real medido)", c.rpsStop)
	}
	if c.rpsStop == 55 {
		t.Fatal("rpsStop no debe ser el umbral sintetico 50*1.10")
	}
}

// El techo limita también el +1 optimista.
func TestUpdateWindowOptimisticIncrementRespectsCeiling(t *testing.T) {
	c := newTestController(19, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline: 19 -> 20
	if c.maxConn != 20 {
		t.Fatalf("maxConn = %d, want 20", c.maxConn)
	}

	feed(c, 500, 200*time.Millisecond) // congela
	feed(c, 600, 200*time.Millisecond) // +1 pero ya está en 20

	if c.maxConn != 20 {
		t.Fatalf("maxConn = %d, want 20 (ceiling)", c.maxConn)
	}
}

// Sin referencia de RPS no hay ciclo de recuperación.
func TestUpdateWindowNoRPSStopMeansNoRecoveryPath(t *testing.T) {
	c := newTestController(2, 20)

	feed(c, 1000, 100*time.Millisecond) // baseline
	c.lastMaxConnAction = 0
	c.rpsStop = 0

	feed(c, 5000, 100*time.Millisecond) // RPS enorme, pero sin referencia

	if c.maxConn != 3 {
		t.Fatalf("maxConn = %d, want 3 (sin rpsStop no hay salida)", c.maxConn)
	}
	if c.lastMaxConnAction != 0 {
		t.Fatalf("lastMaxConnAction = %d, want 0", c.lastMaxConnAction)
	}
}
