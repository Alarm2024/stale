package measure

// Temporary PoC verification tests for the stale audit (not for merge).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func noopSleep(context.Context, time.Duration) error { return nil }

// T-A: reference BEHIND target is floored to lag 0 and reads FRESH.
func TestPOC_FloorHidesBehindReference(t *testing.T) {
	var calls int64
	cfg := Config{
		TargetURL: "http://target.invalid", RefURL: "http://ref.invalid",
		MaxLag: 5, For: 60 * time.Millisecond, Sleep: noopSleep,
		GetSlot: func(_ context.Context, ep string) (uint64, error) {
			if ep == "http://target.invalid" {
				n := atomic.AddInt64(&calls, 1)
				return uint64(1000 + n%2), nil // advances: 1001,1000,1001...
			}
			return 900, nil // frozen, 100 slots BEHIND target
		},
	}
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	last := res.Samples[len(res.Samples)-1]
	t.Logf("last sample: target=%d ref=%d reported_lag=%d (true signed lag=%d) verdict=%s",
		last.TargetSlot, last.RefSlot, last.LagSlots, int64(last.RefSlot)-int64(last.TargetSlot), res.Verdict)
	if last.LagSlots != 0 {
		t.Fatalf("expected floored lag 0, got %d", last.LagSlots)
	}
	if res.Verdict != VerdictFresh {
		t.Fatalf("frozen reference 100 slots behind target produced %s, want FRESH (that is the bug)", res.Verdict)
	}
}

// T-B: one timeout anywhere erases overwhelming STALE evidence (check mode).
func TestPOC_TimeoutMasksProvenStaleness(t *testing.T) {
	samples := make([]Sample, 10)
	for i := range samples {
		samples[i] = Sample{TargetOK: true, RefOK: true, LagSlots: 100, LagMs: 40000}
	}
	res := Result{
		Samples: samples, TargetAdvanced: true, AnyTimeout: true,
		RefAnswered: true, TargetAnswered: true, LastLagSlots: 100,
	}
	v := ComputeVerdict(res, 5)
	t.Logf("10 samples all lag=100, one timeout anywhere -> %s", v)
	if v != VerdictUnknown {
		t.Fatalf("want UNKNOWN-from-masking demonstration, got %s", v)
	}
}

// T-G: last-sample-only verdict (page discloses this; verifying code matches).
func TestPOC_LastSampleOnlyVerdict(t *testing.T) {
	samples := make([]Sample, 10)
	for i := range samples {
		samples[i] = Sample{TargetOK: true, RefOK: true, LagSlots: 100, LagMs: 40000}
	}
	samples[9].LagSlots = 0
	res := Result{
		Samples: samples, TargetAdvanced: true,
		RefAnswered: true, TargetAnswered: true, LastLagSlots: 0,
	}
	v := ComputeVerdict(res, 5)
	t.Logf("9 samples lag=100, last sample lag=0 -> %s", v)
	if v != VerdictFresh {
		t.Fatalf("want FRESH per last-sample rule, got %s", v)
	}
}

// receipt-time slot stub: a perfectly healthy endpoint reporting chain head.
func headStub(delay time.Duration, start time.Time, base uint64) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slot := base + uint64(time.Since(start)/(400*time.Millisecond)) // value at RECEIPT
		if delay > 0 {
			time.Sleep(delay)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": slot})
	}))
}

// T-D: sequential target-then-ref calls turn inter-call time into phantom lag.
// Two perfectly healthy endpoints, identical chain state, target RTT ~900ms.
func TestPOC_PhantomLagFromSequentialCalls(t *testing.T) {
	start := time.Now()
	target := headStub(900*time.Millisecond, start, 450000000)
	defer target.Close()
	ref := headStub(0, start, 450000000)
	defer ref.Close()

	cfg := DefaultConfig(target.URL, ref.URL)
	cfg.MaxLag = 1
	cfg.For = 3 * time.Second
	cfg.Sleep = noopSleep
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range res.Samples {
		t.Logf("sample %d: target=%d ref=%d lag=%d", i, s.TargetSlot, s.RefSlot, s.LagSlots)
	}
	if res.LastLagSlots < 2 {
		t.Fatalf("expected phantom lag >= 2 from 900ms inter-call gap, got %d", res.LastLagSlots)
	}
	if res.Verdict != VerdictStale {
		t.Fatalf("healthy endpoints read %s, want STALE (that is the bias)", res.Verdict)
	}
	if res.AnyTimeout {
		t.Fatal("no timeout should have occurred")
	}
}

// T-E: a slow call just under the 2s timeout is kept whole, phantom lag ~4 slots.
// Page claims "a refused or slow call makes the verdict UNKNOWN" — a 1.9s call
// does not: it is counted, and here it convicts a healthy target.
func TestPOC_SlowSubTimeoutCallKeptWhole(t *testing.T) {
	start := time.Now()
	target := headStub(1900*time.Millisecond, start, 450000000)
	defer target.Close()
	ref := headStub(0, start, 450000000)
	defer ref.Close()

	cfg := DefaultConfig(target.URL, ref.URL)
	cfg.MaxLag = 3
	cfg.For = 4500 * time.Millisecond
	cfg.Sleep = noopSleep
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range res.Samples {
		t.Logf("sample %d: target=%d ref=%d lag=%d tok=%v rok=%v", i, s.TargetSlot, s.RefSlot, s.LagSlots, s.TargetOK, s.RefOK)
	}
	if res.AnyTimeout {
		t.Fatal("1.9s is under the 2s timeout; no timeout expected")
	}
	if res.LastLagSlots < 4 {
		t.Fatalf("expected phantom lag >= 4 from 1.9s gap, got %d", res.LastLagSlots)
	}
	if res.Verdict != VerdictStale {
		t.Fatalf("slow-but-healthy target read %s, want STALE (contradicts page's 'slow call makes the verdict UNKNOWN')", res.Verdict)
	}
}
