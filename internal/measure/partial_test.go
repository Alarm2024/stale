package measure_test

import (
	"testing"

	"github.com/Alarm2024/stale/internal/measure"
)

// paired returns a sample in which both endpoints answered.
func paired(target, ref uint64) measure.Sample {
	lag := int64(ref) - int64(target)
	if lag < 0 {
		lag = 0
	}
	return measure.Sample{TargetOK: true, RefOK: true, TargetSlot: target, RefSlot: ref, LagSlots: lag}
}

// A target proven 100 slots behind stays STALE when a later call times out.
func TestProvenStaleSurvivesALaterTimeout(t *testing.T) {
	for name, last := range map[string]measure.Sample{
		"target timed out":    {RefOK: true, RefSlot: 1104},
		"reference timed out": {TargetOK: true, TargetSlot: 1000},
		"both timed out":      {},
	} {
		t.Run(name, func(t *testing.T) {
			r := measure.Result{
				Samples:        []measure.Sample{paired(1000, 1100), paired(1000, 1101), paired(1000, 1102), last},
				AnyTimeout:     true,
				RefAnswered:    true,
				TargetAnswered: true,
			}
			if got := measure.ComputeVerdict(r, 5); got != measure.VerdictStale {
				t.Fatalf("verdict = %s, want STALE", got)
			}
		})
	}
}

// One paired sample is not enough evidence for anything once a call failed.
func TestSinglePairedSampleWithTimeoutIsUnknown(t *testing.T) {
	r := measure.Result{
		Samples:        []measure.Sample{paired(1000, 1100), {}},
		AnyTimeout:     true,
		RefAnswered:    true,
		TargetAnswered: true,
	}
	if got := measure.ComputeVerdict(r, 5); got != measure.VerdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN", got)
	}
}

// A healthy-looking window with a timeout is never promoted to FRESH.
func TestTimeoutNeverYieldsFresh(t *testing.T) {
	r := measure.Result{
		Samples:        []measure.Sample{paired(1000, 1001), paired(1001, 1002), {TargetOK: true, TargetSlot: 1002}},
		TargetAdvanced: true,
		AnyTimeout:     true,
		RefAnswered:    true,
		TargetAnswered: true,
	}
	if got := measure.ComputeVerdict(r, 5); got != measure.VerdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN", got)
	}
}

// A final call that failed without timing out (a 429, a refused connection)
// leaves LastLagSlots as a placeholder 0. That 0 must not produce FRESH.
func TestPlaceholderLagNeverYieldsFresh(t *testing.T) {
	r := measure.Result{
		Samples:        []measure.Sample{paired(1000, 1001), paired(1001, 1002), {TargetOK: true, TargetSlot: 1002}},
		TargetAdvanced: true,
		AnyTimeout:     false,
		RefAnswered:    true,
		TargetAnswered: true,
		LastLagSlots:   0,
	}
	if got := measure.ComputeVerdict(r, 5); got != measure.VerdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN", got)
	}
}

// A target that answered advancing in an unpaired sample is not frozen, so
// "reference moved, target did not" cannot be claimed from the paired ones.
func TestTargetAdvancingOutsidePairsIsNotFrozen(t *testing.T) {
	r := measure.Result{
		Samples:        []measure.Sample{paired(1000, 1001), paired(1000, 1002), {TargetOK: true, TargetSlot: 1003}},
		TargetAdvanced: true,
		AnyTimeout:     true,
		RefAnswered:    true,
		TargetAnswered: true,
	}
	if got := measure.ComputeVerdict(r, 5); got != measure.VerdictUnknown {
		t.Fatalf("verdict = %s, want UNKNOWN (lag 2 within bound, target moved)", got)
	}
}

func TestLastLagKnown(t *testing.T) {
	if (measure.Result{}).LastLagKnown() {
		t.Fatal("no samples: lag must be unknown")
	}
	r := measure.Result{Samples: []measure.Sample{paired(1, 2), {TargetOK: true}}}
	if r.LastLagKnown() {
		t.Fatal("unpaired final sample: lag must be unknown")
	}
	r.Samples = append(r.Samples, paired(3, 4))
	if !r.LastLagKnown() {
		t.Fatal("paired final sample: lag must be known")
	}
}
