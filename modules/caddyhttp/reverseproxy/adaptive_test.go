// Copyright 2015 Matthew Holt and The Caddy Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package reverseproxy

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestAdaptiveConcurrencyAdjustsLimitFromRTT(t *testing.T) {
	state := newAdaptiveHostState(10)
	baseline := 20 * time.Millisecond
	for range 10 {
		state.recordRTT(baseline)
	}
	if got := state.limit(); got != initialMaxConn+1 {
		t.Fatalf("max_conn after healthy window = %d; want %d", got, initialMaxConn+1)
	}
	if state.rttBase != baseline || state.rttActual != baseline {
		t.Fatalf("RTT estimates = base %s, actual %s; want both %s", state.rttBase, state.rttActual, baseline)
	}

	state.recordRTT(100 * time.Millisecond)
	state.recordRTT(100 * time.Millisecond)
	if got := state.limit(); got >= initialMaxConn+1 {
		t.Fatalf("max_conn after degraded RTT = %d; want it to decrease", got)
	}
}

func TestAdaptiveConcurrencyWindowMustBePositive(t *testing.T) {
	config := &AdaptiveConcurrency{WindowSize: -1}
	if err := config.provision(); err == nil {
		t.Fatal("expected an error for a negative window size")
	}

	config = &AdaptiveConcurrency{}
	if err := config.provision(); err != nil {
		t.Fatalf("default window size should be valid: %v", err)
	}
	if config.WindowSize != defaultRTTWindowSize {
		t.Fatalf("default window size = %d; want %d", config.WindowSize, defaultRTTWindowSize)
	}
}

func TestAdaptiveRequestLimitIsConcurrentSafe(t *testing.T) {
	state := newAdaptiveHostState(10)
	state.maxConn = 3
	const contenders = 20
	releaseAll := make(chan struct{})
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := state.acquireRequest(context.Background(), maxAdaptiveRequestQueue, time.Second)
			if err != nil {
				t.Errorf("acquire request slot: %v", err)
				return
			}
			<-releaseAll
			release()
		}()
	}
	waitForAdaptiveRequestState(t, state, 3, contenders-3)
	close(releaseAll)
	wg.Wait()

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.activeConns != 0 || len(state.connWaiters) != 0 {
		t.Fatalf("request state after releases: active=%d waiters=%d; want 0, 0", state.activeConns, len(state.connWaiters))
	}
}

func TestAdaptiveRequestQueueIsBoundedAndWakesWaiter(t *testing.T) {
	state := newAdaptiveHostState(1)
	state.maxConn = 1
	releaseFirst, err := state.acquireRequest(context.Background(), 1, time.Second)
	if err != nil {
		t.Fatalf("acquire initial request slot: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiterResult := make(chan error, 1)
	go func() {
		release, err := state.acquireRequest(ctx, 1, time.Second)
		if err == nil {
			release()
		}
		waiterResult <- err
	}()
	waitForAdaptiveRequestState(t, state, 1, 1)

	if _, err := state.acquireRequest(context.Background(), 1, time.Second); !errors.Is(err, errAdaptiveRequestQueueFull) {
		t.Fatalf("acquire beyond queue limit error = %v; want queue full", err)
	}

	releaseFirst()
	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatalf("queued TCP slot was not granted: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request was not woken after a request completed")
	}

	state.mu.Lock()
	active := state.activeConns
	state.mu.Unlock()
	if active != 0 {
		t.Fatalf("active request slots after release = %d; want 0", active)
	}
}

func TestAdaptiveRequestQueueDrainsToReducedLimit(t *testing.T) {
	state := newAdaptiveHostState(1)
	state.maxConn = 20
	releases := make([]func(), 20)
	for i := range releases {
		release, err := state.acquireRequest(context.Background(), maxAdaptiveRequestQueue, time.Second)
		if err != nil {
			t.Fatalf("acquire active request %d: %v", i, err)
		}
		releases[i] = release
	}

	state.mu.Lock()
	state.maxConn = 5
	state.mu.Unlock()
	waiterResult := make(chan error, 1)
	go func() {
		release, err := state.acquireRequest(context.Background(), 1, time.Second)
		if err == nil {
			release()
		}
		waiterResult <- err
	}()
	waitForAdaptiveRequestState(t, state, 20, 1)

	for i := 0; i < 15; i++ {
		releases[i]()
	}
	waitForAdaptiveRequestState(t, state, 5, 1)
	select {
	case err := <-waiterResult:
		t.Fatalf("queued request was admitted at active=max_conn: %v", err)
	default:
	}

	releases[15]()
	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatalf("queued request was not admitted after active count dropped below max_conn: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued request was not admitted after an active request completed")
	}

	for i := 16; i < len(releases); i++ {
		releases[i]()
	}
}

func TestAdaptiveRequestQueueTimeout(t *testing.T) {
	state := newAdaptiveHostState(1)
	state.maxConn = 0
	_, err := state.acquireRequest(context.Background(), 1, time.Millisecond)
	if !errors.Is(err, errAdaptiveRequestQueueFull) {
		t.Fatalf("queue timeout error = %v; want queue full/timeout", err)
	}
}

func TestAdaptiveRequestReleaseIsIdempotent(t *testing.T) {
	state := newAdaptiveHostState(1)
	release, err := state.acquireRequest(context.Background(), 1, time.Second)
	if err != nil {
		t.Fatalf("acquire request slot: %v", err)
	}
	release()
	release()
	state.mu.Lock()
	active := state.activeConns
	state.mu.Unlock()
	if active != 0 {
		t.Fatalf("active request slots after release = %d; want 0", active)
	}
}

func TestAdaptiveSnapshotReportsActiveAndMaximum(t *testing.T) {
	state := newAdaptiveHostState(1)
	release, err := state.acquireRequest(context.Background(), 1, time.Second)
	if err != nil {
		t.Fatalf("acquire request slot: %v", err)
	}
	active, max := state.snapshot()
	if active != 1 || max != initialMaxConn {
		t.Fatalf("adaptive snapshot = active %d, max %d; want 1, %d", active, max, initialMaxConn)
	}

	release()
	active, max = state.snapshot()
	if active != 0 || max != initialMaxConn {
		t.Fatalf("adaptive snapshot after release = active %d, max %d; want 0, %d", active, max, initialMaxConn)
	}
}

func waitForAdaptiveRequestState(t *testing.T, state *adaptiveHostState, active, waiters int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		state.mu.Lock()
		matches := state.activeConns == active && len(state.connWaiters) == waiters
		state.mu.Unlock()
		if matches {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for request state active=%d waiters=%d", active, waiters)
		}
		runtime.Gosched()
	}
}