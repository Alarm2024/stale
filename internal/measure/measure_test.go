package measure_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Alarm2024/stale/internal/measure"
)

type slotServer struct {
	slot      atomic.Uint64
	frozen    bool
	lag       int64
	hang      bool
	fail      bool
	increment bool
}

func (s *slotServer) handler(w http.ResponseWriter, r *http.Request) {
	if s.hang {
		time.Sleep(3 * time.Second)
		return
	}
	if s.fail {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}

	body, _ := io.ReadAll(r.Body)
	var req struct {
		Method string `json:"method"`
	}
	_ = json.Unmarshal(body, &req)
	if req.Method != "getSlot" {
		http.Error(w, "unexpected method", http.StatusBadRequest)
		return
	}

	slot := s.slot.Load()
	if s.increment && !s.frozen {
		slot = s.slot.Add(1) - 1
		if s.lag > 0 {
			slot -= uint64(s.lag)
		}
	}

	_ = json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"result":  slot,
	})
}

func newSlotServer(s *slotServer) *httptest.Server {
	s.slot.Store(1000)
	return httptest.NewServer(http.HandlerFunc(s.handler))
}

func runCheck(t *testing.T, target, ref string, maxLag int64) measure.Result {
	t.Helper()
	cfg := measure.DefaultConfig(target, ref)
	cfg.MaxLag = maxLag
	cfg.For = 2500 * time.Millisecond

	virtual := time.Unix(0, 0)
	cfg.Now = func() time.Time { return virtual }
	cfg.Sleep = func(ctx context.Context, d time.Duration) error {
		virtual = virtual.Add(d)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := measure.Run(ctx, cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	return result
}

func TestFrozenTargetIsStale(t *testing.T) {
	refSrv := newSlotServer(&slotServer{increment: true})
	defer refSrv.Close()

	targetSrv := newSlotServer(&slotServer{frozen: true, increment: true})
	defer targetSrv.Close()

	result := runCheck(t, targetSrv.URL, refSrv.URL, 5)
	if result.Verdict != measure.VerdictStale {
		t.Fatalf("verdict = %q, want STALE (frozen target)", result.Verdict)
	}
}

func TestLaggingTargetIsStale(t *testing.T) {
	refSrv := newSlotServer(&slotServer{increment: true})
	defer refSrv.Close()

	targetSrv := newSlotServer(&slotServer{increment: true, lag: 20})
	defer targetSrv.Close()

	result := runCheck(t, targetSrv.URL, refSrv.URL, 5)
	if result.Verdict != measure.VerdictStale {
		t.Fatalf("verdict = %q, want STALE (lagging target)", result.Verdict)
	}
}

func TestHealthyTargetIsFresh(t *testing.T) {
	refSrv := newSlotServer(&slotServer{increment: true})
	defer refSrv.Close()

	targetSrv := newSlotServer(&slotServer{increment: true, lag: 1})
	defer targetSrv.Close()

	result := runCheck(t, targetSrv.URL, refSrv.URL, 5)
	if result.Verdict != measure.VerdictFresh {
		t.Fatalf("verdict = %q, want FRESH", result.Verdict)
	}
}

func TestDeadReferenceIsUnknown(t *testing.T) {
	refSrv := newSlotServer(&slotServer{fail: true})
	defer refSrv.Close()

	targetSrv := newSlotServer(&slotServer{increment: true})
	defer targetSrv.Close()

	result := runCheck(t, targetSrv.URL, refSrv.URL, 5)
	if result.Verdict != measure.VerdictUnknown {
		t.Fatalf("verdict = %q, want UNKNOWN (dead reference)", result.Verdict)
	}
}

func TestHangingTargetIsUnknown(t *testing.T) {
	refSrv := newSlotServer(&slotServer{increment: true})
	defer refSrv.Close()

	targetSrv := newSlotServer(&slotServer{hang: true})
	defer targetSrv.Close()

	result := runCheck(t, targetSrv.URL, refSrv.URL, 5)
	if result.Verdict != measure.VerdictUnknown {
		t.Fatalf("verdict = %q, want UNKNOWN (hanging target)", result.Verdict)
	}
}

func TestComputeVerdictWithoutFrozenCheck(t *testing.T) {
	// Document the regression guard: removing the frozen-slot branch must fail TestFrozenTargetIsStale.
	samples := []measure.Sample{
		{TargetOK: true, RefOK: true, TargetSlot: 100, RefSlot: 110},
		{TargetOK: true, RefOK: true, TargetSlot: 100, RefSlot: 112, LagSlots: 12, LagMs: 4800},
	}
	result := measure.Result{
		Samples:        samples,
		TargetAdvanced: false,
		RefAnswered:    true,
		TargetAnswered: true,
		LastLagSlots:   12,
		LastLagMs:      4800,
		LastTargetSlot: 100,
		LastRefSlot:    112,
	}
	if got := measure.ComputeVerdict(result, 5); got != measure.VerdictStale {
		t.Fatalf("ComputeVerdict = %q, want STALE when ref advanced and target frozen", got)
	}
}

func TestSameEndpointIsRefused(t *testing.T) {
	same := [][2]string{
		{"https://api.mainnet-beta.solana.com", "https://api.mainnet-beta.solana.com"},
		{"https://api.mainnet-beta.solana.com", "https://API.mainnet-beta.solana.com/"},
		{"https://rpc.example.com/?api-key=a", "https://rpc.example.com:443?api-key=b"},
	}
	for _, p := range same {
		if !measure.SameEndpoint(p[0], p[1]) {
			t.Fatalf("%q and %q are one endpoint", p[0], p[1])
		}
		cfg := measure.DefaultConfig(p[0], p[1])
		if _, err := measure.Run(context.Background(), cfg); err != measure.ErrSameEndpoint {
			t.Fatalf("Run(%q vs %q) err = %v, want ErrSameEndpoint", p[0], p[1], err)
		}
	}
	different := [][2]string{
		{"https://rpc.example.com", "https://api.mainnet-beta.solana.com"},
		{"https://rpc.example.com/a", "https://rpc.example.com/b"},
		{"http://127.0.0.1:8899", "http://127.0.0.1:18899"},
	}
	for _, p := range different {
		if measure.SameEndpoint(p[0], p[1]) {
			t.Fatalf("%q and %q are different endpoints", p[0], p[1])
		}
	}
}
