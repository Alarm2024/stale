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
}

type Result struct {
	Verdict        Verdict
	Degraded       bool
	Samples        []Sample
	TargetAdvanced bool
	LastTargetSlot uint64
	LastRefSlot    uint64
	LastLagSlots   int64
	LastLagMs      int64
	AnyTimeout     bool
	RefAnswered    bool
	TargetAnswered bool
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
	result.Degraded = result.Verdict == VerdictStale && result.AnyTimeout
	return result, nil
}

// ComputeVerdict derives a verdict from collected samples. Exported for tests
// and the background sampler used by stale serve.
func ComputeVerdict(result Result, maxLag int64) Verdict {
	// Only paired replies establish lag. A timeout must not overwrite a
	// proven stale reading with a zero from the unanswered final sample.
	var clean []Sample
	for _, sample := range result.Samples {
		if sample.TargetOK && sample.RefOK {
			clean = append(clean, sample)
		}
	}
	if len(clean) >= MinSamples {
		if referenceAdvanced(clean) && !result.TargetAdvanced {
			return VerdictStale
		}
		if clean[len(clean)-1].LagSlots > maxLag {
			return VerdictStale
		}
	}
	// A timeout still prevents a FRESH claim. The latest sample must be
	// paired, and a live reference must vouch for an advancing target.
	if len(result.Samples) < MinSamples || result.AnyTimeout ||
		!result.RefAnswered || !result.TargetAnswered {
		return VerdictUnknown
	}
	if result.TargetAdvanced && result.LastLagSlots <= maxLag &&
		result.Samples[len(result.Samples)-1].TargetOK &&
		result.Samples[len(result.Samples)-1].RefOK &&
		referenceAdvanced(result.Samples) {
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
