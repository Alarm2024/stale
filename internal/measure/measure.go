package measure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/solana-foundation/solana-go/v2/rpc"
)

const (
	DefaultRefURL    = "https://api.mainnet-beta.solana.com"
	SlotDuration     = 400 * time.Millisecond
	CallTimeout      = 2 * time.Second
	DefaultMaxLag    = 5
	DefaultSampleFor = 10 * time.Second
	SampleInterval   = time.Second
	MinSamples       = 2
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
	// RefBehind is true when the reference's slot read below the target's in
	// this sample. Lag is floored at 0, so without this flag a reference
	// trailing its target is indistinguishable from perfect freshness.
	RefBehind bool
}

type Result struct {
	Verdict        Verdict
	Samples        []Sample
	TargetAdvanced bool
	LastTargetSlot uint64
	LastRefSlot    uint64
	LastLagSlots   int64
	LastLagMs      int64
	LastRefBehind  bool
	AnyTimeout     bool
	RefAnswered    bool
	TargetAnswered bool
	// Degraded is true when the verdict is STALE but not every sample in the
	// window got an answer from both endpoints. The STALE rests on the paired
	// samples that did; the rest of the window is unmeasured.
	Degraded bool
}

// LastLagKnown reports whether the final sample has an answer from both
// endpoints. When it does not, LastLagSlots and LastLagMs are 0 as a
// placeholder, not a measurement, and must not be printed as a lag.
func (r Result) LastLagKnown() bool {
	if len(r.Samples) == 0 {
		return false
	}
	last := r.Samples[len(r.Samples)-1]
	return last.TargetOK && last.RefOK
}

type Config struct {
	TargetURL string
	RefURL    string
	MaxLag    int64
	For       time.Duration
	Now       func() time.Time
	Sleep     func(context.Context, time.Duration) error
	GetSlot   func(ctx context.Context, endpoint string) (uint64, error)
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

// ErrSameEndpoint is returned when the target and the reference are one
// endpoint. Lag against yourself is always 0, which would print FRESH for a
// measurement that never happened.
var ErrSameEndpoint = errors.New("target and reference are the same endpoint — pass --ref with a different RPC")

// SameEndpoint reports whether two RPC URLs reach the same endpoint: same
// scheme, host and port, and path. Credentials and query strings (API keys)
// are ignored, since two keys on one host are still one server's view.
func SameEndpoint(a, b string) bool {
	norm := func(raw string) (string, bool) {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || u.Host == "" {
			return "", false
		}
		scheme := strings.ToLower(u.Scheme)
		host := strings.ToLower(u.Hostname())
		port := u.Port()
		if (scheme == "https" && port == "443") || (scheme == "http" && port == "80") {
			port = ""
		}
		return scheme + "://" + host + ":" + port + strings.TrimRight(u.Path, "/"), true
	}
	na, oka := norm(a)
	nb, okb := norm(b)
	return oka && okb && na == nb
}

func Run(ctx context.Context, cfg Config) (Result, error) {
	if cfg.For <= 0 {
		return Result{}, errors.New("sample duration must be positive")
	}
	if SameEndpoint(cfg.TargetURL, cfg.RefURL) {
		return Result{}, ErrSameEndpoint
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

		// The two calls run together: taken one after another, the first
		// call's whole round trip lands in the measured lag as phantom slots
		// the chain never produced. Concurrent calls shrink that bias to the
		// difference between the two round trips.
		type slotResult struct {
			slot uint64
			err  error
		}
		targetCh := make(chan slotResult, 1)
		refCh := make(chan slotResult, 1)
		go func() {
			slot, err := cfg.GetSlot(ctx, cfg.TargetURL)
			targetCh <- slotResult{slot, err}
		}()
		go func() {
			slot, err := cfg.GetSlot(ctx, cfg.RefURL)
			refCh <- slotResult{slot, err}
		}()
		targetRes, refRes := <-targetCh, <-refCh
		targetSlot, targetErr := targetRes.slot, targetRes.err
		refSlot, refErr := refRes.slot, refRes.err
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
				sample.RefBehind = true
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
		result.LastRefBehind = last.RefBehind
	}
	result.Verdict = ComputeVerdict(result, cfg.MaxLag)
	result.Degraded = isDegraded(result)
	return result, nil
}

// ComputeVerdict derives a verdict from collected samples. Exported for tests
// and the background sampler used by stale serve.
//
// STALE and FRESH need different evidence. STALE can be proven by the samples
// in which both endpoints answered, even if other calls in the window failed:
// a target seen 100 slots behind twice does not become "unknown" because a
// third call timed out. FRESH is a claim about the whole window, so it still
// needs every call answered and a final sample from both endpoints.
func ComputeVerdict(result Result, maxLag int64) Verdict {
	if paired := pairedSamples(result.Samples); len(paired) >= MinSamples {
		// A target seen advancing in any answered sample is not frozen.
		if referenceAdvanced(paired) && !result.TargetAdvanced {
			return VerdictStale
		}
		if paired[len(paired)-1].LagSlots > maxLag {
			return VerdictStale
		}
	}

	if len(result.Samples) < MinSamples {
		return VerdictUnknown
	}
	if result.AnyTimeout || !result.RefAnswered || !result.TargetAnswered {
		return VerdictUnknown
	}
	// A failed call that was not a timeout still leaves the final lag as a
	// placeholder 0. FRESH is never read off a placeholder.
	if !result.LastLagKnown() {
		return VerdictUnknown
	}

	// FRESH also needs a live reference: a reference that never advanced
	// over the window cannot vouch for the target, whatever the lag reads.
	if result.TargetAdvanced && result.LastLagSlots <= maxLag && referenceAdvanced(result.Samples) {
		return VerdictFresh
	}
	return VerdictUnknown
}

// pairedSamples keeps the samples in which both endpoints answered. Only
// those carry a lag.
func pairedSamples(samples []Sample) []Sample {
	var paired []Sample
	for _, s := range samples {
		if s.TargetOK && s.RefOK {
			paired = append(paired, s)
		}
	}
	return paired
}

// isDegraded is true for a STALE verdict reached with some samples unpaired.
func isDegraded(result Result) bool {
	if result.Verdict != VerdictStale {
		return false
	}
	return len(pairedSamples(result.Samples)) < len(result.Samples)
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
	line := fmt.Sprintf("target=%s ref=%s lag=%d slots (%d ms) advanced=%s",
		target, ref, s.LagSlots, s.LagMs, adv)
	if s.RefBehind {
		line += " ref_behind=yes"
	}
	return line
}
