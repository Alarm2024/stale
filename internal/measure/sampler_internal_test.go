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
