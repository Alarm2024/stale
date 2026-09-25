package measure

import (
	"testing"
	"time"
)

// X-Stale-Sampled-Ms-Ago used to be stored once, as the time the sample took,
// so it read ~0 however old the sample was. It must grow as the sample ages.
func TestSampledMsAgoAgesWithTheClock(t *testing.T) {
	taken := time.Date(2026, 9, 24, 20, 0, 0, 0, time.UTC)
	snap := Snapshot{Verdict: VerdictFresh, Measured: true, HasSample: true, SampledAt: taken}
	if got := snap.at(taken.Add(250 * time.Millisecond)).SampledMsAgo; got != 250 {
		t.Fatalf("250ms after the sample: SampledMsAgo = %d", got)
	}
	if got := snap.at(taken.Add(7 * time.Second)).SampledMsAgo; got != 7000 {
		t.Fatalf("7s after the sample: SampledMsAgo = %d", got)
	}
}

func TestNoSampleYetIsUnknown(t *testing.T) {
	var s Sampler
	if got := s.Current().Verdict; got != VerdictUnknown {
		t.Fatalf("before any sample the verdict is %q, want UNKNOWN", got)
	}
}

func TestHistoryWithTimeoutKeepsStaleButDoesNotClaimMeasuredFresh(t *testing.T) {
	history := []sampleRecord{
		{Sample: Sample{TargetOK: true, RefOK: true, TargetSlot: 100, RefSlot: 200, LagSlots: 100}},
		{Sample: Sample{TargetOK: true, RefOK: true, TargetSlot: 100, RefSlot: 200, LagSlots: 100}},
		{AnyTimeout: true},
	}
	result := resultFromHistory(history)
	if got := ComputeVerdict(result, 5); got != VerdictStale || !result.AnyTimeout {
		t.Fatalf("verdict=%s timeout=%t, want STALE with degraded timeout", got, result.AnyTimeout)
	}
	if result.LastLagSlots != 0 {
		t.Fatalf("unanswered last sample must not report a known lag, got %d", result.LastLagSlots)
	}
}
