package measure

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// A reference trailing its target is floored to lag 0; RefBehind must say so.
func TestRefBehindIsReported(t *testing.T) {
	var calls int64
	cfg := Config{
		TargetURL: "http://target.invalid", RefURL: "http://ref.invalid",
		MaxLag: 5, For: 60 * time.Millisecond,
		Sleep: func(context.Context, time.Duration) error { return nil },
		GetSlot: func(_ context.Context, ep string) (uint64, error) {
			if ep == "http://target.invalid" {
				n := atomic.AddInt64(&calls, 1)
				return uint64(1000 + n%2), nil
			}
			return 900, nil
		},
	}
	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	last := res.Samples[len(res.Samples)-1]
	if !last.RefBehind {
		t.Fatal("ref 100 slots behind target: RefBehind = false")
	}
	if last.LagSlots != 0 {
		t.Fatalf("lag floor changed: got %d", last.LagSlots)
	}
	if !res.LastRefBehind {
		t.Fatal("Result.LastRefBehind not carried from the last sample")
	}
	if line := FormatSampleLine(last); !strings.Contains(line, "ref_behind=yes") {
		t.Fatalf("sample line hides the behind reference: %q", line)
	}
}

// A reference level with or ahead of its target must not set RefBehind.
func TestRefAheadIsNotRefBehind(t *testing.T) {
	s := Sample{TargetOK: true, RefOK: true, TargetSlot: 100, RefSlot: 105, LagSlots: 5}
	if strings.Contains(FormatSampleLine(s), "ref_behind") {
		t.Fatalf("ref ahead of target still flags ref_behind: %q", FormatSampleLine(s))
	}
}
