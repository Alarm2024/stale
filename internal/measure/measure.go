package measure

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/solana-foundation/solana-go/v2/rpc"
)

const (
	DefaultRefURL     = "https://api.mainnet-beta.solana.com"
	SlotDuration      = 400 * time.Millisecond
	CallTimeout       = 2 * time.Second
	DefaultMaxLag     = 5
	DefaultSampleFor  = 10 * time.Second
	SampleInterval    = time.Second
	MinSamples        = 2
)

type Verdict string

const (
	VerdictFresh   Verdict = "FRESH"
	VerdictStale   Verdict = "STALE"
	VerdictUnknown Verdict = "UNKNOWN"
)

func (v Verdict) ExitCode() int {
	switch v {
	case VerdictFresh:
		return 0
	case VerdictStale:
		return 1
	default:
		return 2
	}
}

type Sample struct {
	At             time.Time
	TargetSlot     uint64
	RefSlot        uint64
	TargetOK       bool
	RefOK          bool
	LagSlots       int64
	LagMs          int64
	TargetAdvanced bool
}

type Result struct {
	Verdict          Verdict
	Samples          []Sample
	TargetAdvanced   bool
	LastTargetSlot   uint64
	LastRefSlot      uint64
	LastLagSlots     int64
	LastLagMs        int64
	AnyTimeout       bool
	RefAnswered      bool
	TargetAnswered   bool
}

type Config struct {
	TargetURL  string
	RefURL     string
	MaxLag     int64
	For        time.Duration
	Now        func() time.Time
	Sleep      func(context.Context, time.Duration) error
	GetSlot    func(ctx context.Context, endpoint string) (uint64, error)
}

func DefaultConfig(targetURL, refURL string) Config {
	if refURL == "" {
		refURL = DefaultRefURL
	}
	return Config{
		TargetURL: targetURL,
		RefURL:    refURL,
		MaxLag:    DefaultMaxLag,
		For:       DefaultSampleFor,
		Now:       time.Now,
		Sleep:     sleepContext,
		GetSlot:   getSlot,
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func getSlot(ctx context.Context, endpoint string) (uint64, error) {
	callCtx, cancel := context.WithTimeout(ctx, CallTimeout)
	defer cancel()

	client := rpc.NewWithTimeout(endpoint, CallTimeout)
	defer client.Close()

	return client.GetSlot(callCtx, rpc.CommitmentProcessed)
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.For <= 0 {
		return Result{}, errors.New("sample duration must be positive")
	}
	if cfg.MaxLag < 0 {
		return Result{}, errors.New("max lag must be non-negative")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Sleep == nil {
		cfg.Sleep = sleepContext
	}
	if cfg.GetSlot == nil {
		cfg.GetSlot = getSlot
	}

	deadline := cfg.Now().Add(cfg.For)
	var (
		samples        []Sample
		prevTarget     uint64
		hasPrevTarget  bool
		targetAdvanced bool
		anyTimeout     bool
		refAnswered    bool
		targetAnswered bool
	)

	for cfg.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}

		sample := Sample{At: cfg.Now()}

		targetSlot, targetErr := cfg.GetSlot(ctx, cfg.TargetURL)
		if targetErr != nil {
			if errors.Is(targetErr, context.DeadlineExceeded) {
				anyTimeout = true
			}
			sample.TargetOK = false
		} else {
			sample.TargetOK = true
			sample.TargetSlot = targetSlot
			targetAnswered = true
			if hasPrevTarget && targetSlot > prevTarget {
				targetAdvanced = true
				sample.TargetAdvanced = true
			}
			prevTarget = targetSlot
			hasPrevTarget = true
		}

		refSlot, refErr := cfg.GetSlot(ctx, cfg.RefURL)
		if refErr != nil {
			if errors.Is(refErr, context.DeadlineExceeded) {
				anyTimeout = true
			}
			sample.RefOK = false
		} else {
			sample.RefOK = true
			sample.RefSlot = refSlot
			refAnswered = true
		}

		if sample.TargetOK && sample.RefOK {
			lag := int64(refSlot) - int64(targetSlot)
			if lag < 0 {
				lag = 0
			}
			sample.LagSlots = lag
			sample.LagMs = lag * int64(SlotDuration/time.Millisecond)
		}

		samples = append(samples, sample)

		if err := cfg.Sleep(ctx, SampleInterval); err != nil {
			return Result{}, err
		}
	}

	result := Result{
		Samples:        samples,
		TargetAdvanced: targetAdvanced,
		AnyTimeout:     anyTimeout,
		RefAnswered:    refAnswered,
		TargetAnswered: targetAnswered,
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		result.LastTargetSlot = last.TargetSlot
		result.LastRefSlot = last.RefSlot
		result.LastLagSlots = last.LagSlots
		result.LastLagMs = last.LagMs
	}
	result.Verdict = ComputeVerdict(result, cfg.MaxLag)
	return result, nil
}

// ComputeVerdict derives a verdict from collected samples. Exported for tests
// and the background sampler used by stale serve.
func ComputeVerdict(result Result, maxLag int64) Verdict {
	if len(result.Samples) < MinSamples {
		return VerdictUnknown
	}
	if result.AnyTimeout || !result.RefAnswered || !result.TargetAnswered {
		return VerdictUnknown
	}

	refAdvanced := referenceAdvanced(result.Samples)
	if refAdvanced && !result.TargetAdvanced {
		return VerdictStale
	}
	if result.LastLagSlots > maxLag {
		return VerdictStale
	}
	if result.TargetAdvanced && result.LastLagSlots <= maxLag {
		return VerdictFresh
	}
	return VerdictUnknown
}

func referenceAdvanced(samples []Sample) bool {
	var first, last uint64
	var seen bool
	for _, s := range samples {
		if !s.RefOK {
			continue
		}
		if !seen {
			first = s.RefSlot
			last = s.RefSlot
			seen = true
			continue
		}
		last = s.RefSlot
	}
	return seen && last > first
}

func FormatSampleLine(s Sample) string {
	target := "timeout"
	if s.TargetOK {
		target = fmt.Sprintf("%d", s.TargetSlot)
	}
	ref := "timeout"
	if s.RefOK {
		ref = fmt.Sprintf("%d", s.RefSlot)
	}
	adv := "no"
	if s.TargetAdvanced {
		adv = "yes"
	}
	return fmt.Sprintf("target=%s ref=%s lag=%d slots (%d ms) advanced=%s",
		target, ref, s.LagSlots, s.LagMs, adv)
}
