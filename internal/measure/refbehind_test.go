package measure

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The reference reads 100 slots below a target that keeps moving. The lag
// floors to 0, which alone would look like a perfect target, so the sample,
// the Result and the printed line must all say the reference was behind.
func TestRefBehindIsReported(t *testing.T) {
	const (
		targetURL = "http://target.invalid"
		refURL    = "http://ref.invalid"
	)
	var targetCalls atomic.Int64
	cfg := Config{
		TargetURL: targetURL,
		RefURL:    refURL,
		MaxLag:    5,
		For:       60 * time.Millisecond,
		Sleep:     func(context.Context, time.Duration) error { return nil },
		GetSlot: func(_ context.Context, endpoint string) (uint64, error) {
			switch endpoint {
			case targetURL:
				// 1001, 1000, 1001, ... : the target moves, the reference sits.
				return uint64(1000 + targetCalls.Add(1)%2), nil
			case refURL:
				return 900, nil
			}
			t.Errorf("getSlot asked for an unexpected endpoint %q", endpoint)
			return 0, context.DeadlineExceeded
		},
	}

	res, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Samples) == 0 {
		t.Fatal("Run returned no samples")
	}
	last := res.Samples[len(res.Samples)-1]

	if !last.RefBehind {
		t.Fatal("reference 100 slots below the target: Sample.RefBehind = false")
	}
	if last.LagSlots != 0 {
		t.Fatalf("lag is no longer floored at 0: LagSlots = %d", last.LagSlots)
	}
	if !res.LastRefBehind {
		t.Fatal("Result.LastRefBehind does not repeat the last sample's flag")
	}
	if line := FormatSampleLine(last); !strings.Contains(line, "ref_behind=yes") {
		t.Fatalf("sample line does not mention the behind reference: %q", line)
	}
}

// A reference ahead of its target is the ordinary case: the flag stays off
// and the line does not grow a ref_behind field.
func TestRefAheadIsNotRefBehind(t *testing.T) {
	s := pairSample(time.Time{}, slotAnswer{slot: 100}, slotAnswer{slot: 105})
	if s.RefBehind {
		t.Fatal("reference 5 slots ahead of the target: RefBehind = true")
	}
	if s.LagSlots != 5 {
		t.Fatalf("LagSlots = %d, want 5", s.LagSlots)
	}
	if line := FormatSampleLine(s); strings.Contains(line, "ref_behind") {
		t.Fatalf("reference ahead of the target still prints ref_behind: %q", line)
	}
}
