package check

import (
	"strings"
	"testing"

	"github.com/Alarm2024/stale/internal/measure"
)

func TestVerdictLinePrintsUnknownLagForUnpairedFinalSample(t *testing.T) {
	r := measure.Result{
		Verdict:  measure.VerdictStale,
		Degraded: true,
		Samples: []measure.Sample{
			{TargetOK: true, RefOK: true, TargetSlot: 1000, RefSlot: 1100, LagSlots: 100},
			{TargetOK: true, RefOK: true, TargetSlot: 1000, RefSlot: 1101, LagSlots: 101},
			{RefOK: true, RefSlot: 1102},
		},
		LastRefSlot: 1102,
	}
	line := VerdictLine(r)
	for _, want := range []string{"verdict=STALE", "lag=unknown", "degraded=true"} {
		if !strings.Contains(line, want) {
			t.Fatalf("line %q missing %q", line, want)
		}
	}
	if strings.Contains(line, "lag=0") {
		t.Fatalf("line %q prints a placeholder lag", line)
	}
}

func TestVerdictLinePrintsMeasuredLag(t *testing.T) {
	r := measure.Result{
		Verdict:      measure.VerdictStale,
		Samples:      []measure.Sample{{TargetOK: true, RefOK: true, LagSlots: 28, LagMs: 11200}},
		LastLagSlots: 28,
		LastLagMs:    11200,
	}
	line := VerdictLine(r)
	if !strings.Contains(line, "lag=28 slots (11200 ms)") || !strings.Contains(line, "degraded=false") {
		t.Fatalf("unexpected line %q", line)
	}
}
