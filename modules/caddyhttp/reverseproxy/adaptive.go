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
	"math"
	"sort"
	"sync"
	"time"
)

const (
	defaultRTTWindowSize = 20
	initialMaxConn       = 10
	minimumMaxConn       = 2
	rttEMAAlpha          = 0.2
	maxConnDecreaseK     = 0.5
	maxAdaptiveRequestQueue = 100
	adaptiveRequestWaitTime = 2 * time.Second
)

var errAdaptiveRequestQueueFull = errors.New("adaptive request queue is full or timed out")
var errAdaptiveTCPUnsupported = errors.New("adaptive TCP connection limits require a direct TCP upstream")

// AdaptiveConcurrency configures RTT-based adaptation of an upstream's
// concurrent TCP connection limit. In a Caddyfile, enable it with
// `lb_adaptive [<window_size>]`.
type AdaptiveConcurrency struct {
	// WindowSize is the number of recent RTT samples used for the P10 baseline.
	WindowSize int `json:"window_size,omitempty"`
}

func (a *AdaptiveConcurrency) provision() error {
	if a.WindowSize == 0 {
		a.WindowSize = defaultRTTWindowSize
	}
	if a.WindowSize < 1 {
		return errors.New("RTT window size must be greater than zero")
	}
	return nil
}

type adaptiveHostState struct {
	mu           sync.Mutex
	samples      []time.Duration
	next         int
	count        int
	rttBase      time.Duration
	rttActual    time.Duration
	maxConn      int
	activeConns  int
	connWaiters  []*adaptiveConnWaiter
}

type adaptiveConnWaiter struct {
	ready   chan struct{}
	granted bool
}

func newAdaptiveHostState(windowSize int) *adaptiveHostState {
	if windowSize < 1 {
		windowSize = defaultRTTWindowSize
	}
	return &adaptiveHostState{
		samples: make([]time.Duration, windowSize),
		maxConn: initialMaxConn,
	}
}

func (s *adaptiveHostState) recordRTT(rtt time.Duration) {
	if rtt <= 0 {
		return
	}

	s.mu.Lock()

	s.samples[s.next] = rtt
	s.next = (s.next + 1) % len(s.samples)
	if s.count < len(s.samples) {
		s.count++
	}

	if s.rttActual == 0 {
		s.rttActual = rtt
	} else {
		s.rttActual = time.Duration((1-rttEMAAlpha)*float64(s.rttActual) + rttEMAAlpha*float64(rtt))
	}

	if s.count < len(s.samples) {
		s.mu.Unlock()
		return
	}

	ordered := append([]time.Duration(nil), s.samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	rank := int(math.Ceil(0.10 * float64(len(ordered))))
	if rank < 1 {
		rank = 1
	}
	s.rttBase = ordered[rank-1]

	maxConn := float64(s.maxConn)
	congestion := maxConn * (1 - float64(s.rttBase)/float64(s.rttActual))
	alpha := math.Max(3, 0.10*maxConn)
	beta := math.Max(6, 0.20*maxConn)

	switch {
	case congestion < alpha:
		s.maxConn++
	case congestion > beta:
		excess := (congestion - beta) / beta
		gamma := math.Max(0.1, 1-maxConnDecreaseK*excess)
		s.maxConn = int(math.Floor(maxConn * gamma))
		if s.maxConn < minimumMaxConn {
			s.maxConn = minimumMaxConn
		}
	}
	s.grantConnWaitersLocked()
	s.mu.Unlock()
}

func (s *adaptiveHostState) limit() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxConn
}

func (s *adaptiveHostState) snapshot() (active, max int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeConns, s.maxConn
}

func (u *Upstream) maxTCPConnections() int {
	if u.adaptive == nil {
		return 0
	}
	return u.adaptive.limit()
}

func (u *Upstream) recordRTT(rtt time.Duration) {
	if u.adaptive != nil {
		u.adaptive.recordRTT(rtt)
	}
}

func (s *adaptiveHostState) acquireRequest(ctx context.Context, maxWaiters int, waitTimeout time.Duration) (func(), error) {
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()

	s.mu.Lock()
	if s.activeConns < s.maxConn {
		s.activeConns++
		s.mu.Unlock()
		return s.newRequestRelease(), nil
	}
	if len(s.connWaiters) >= maxWaiters {
		s.mu.Unlock()
		return nil, errAdaptiveRequestQueueFull
	}
	waiter := &adaptiveConnWaiter{ready: make(chan struct{})}
	s.connWaiters = append(s.connWaiters, waiter)
	s.mu.Unlock()

	select {
	case <-waiter.ready:
		return s.newRequestRelease(), nil
	case <-ctx.Done():
		s.cancelRequestWaiter(waiter)
		return nil, ctx.Err()
	case <-timer.C:
		if s.cancelRequestWaiter(waiter) && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errAdaptiveRequestQueueFull
	}
}

func (s *adaptiveHostState) newRequestRelease() func() {
	var once sync.Once
	return func() { once.Do(s.releaseRequest) }
}

func (s *adaptiveHostState) cancelRequestWaiter(waiter *adaptiveConnWaiter) bool {
	s.mu.Lock()
	if waiter.granted {
		s.mu.Unlock()
		s.releaseRequest()
		return true
	}
	for i, queued := range s.connWaiters {
		if queued == waiter {
			copy(s.connWaiters[i:], s.connWaiters[i+1:])
			s.connWaiters[len(s.connWaiters)-1] = nil
			s.connWaiters = s.connWaiters[:len(s.connWaiters)-1]
			break
		}
	}
	s.grantConnWaitersLocked()
	s.mu.Unlock()
	return false
}

func (s *adaptiveHostState) releaseRequest() {
	s.mu.Lock()
	if s.activeConns > 0 {
		s.activeConns--
	}
	s.grantConnWaitersLocked()
	s.mu.Unlock()
}

func (s *adaptiveHostState) grantConnWaitersLocked() {
	for s.activeConns < s.maxConn && len(s.connWaiters) > 0 {
		waiter := s.connWaiters[0]
		copy(s.connWaiters, s.connWaiters[1:])
		s.connWaiters[len(s.connWaiters)-1] = nil
		s.connWaiters = s.connWaiters[:len(s.connWaiters)-1]
		s.activeConns++
		waiter.granted = true
		close(waiter.ready)
	}
}