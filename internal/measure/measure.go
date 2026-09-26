package measure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
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
	// RefBehind: the reference answered a lower slot than the target did.
	// LagSlots is never negative, so that case reads as a lag of 0 — the
	// same number a perfectly fresh target gets. The flag keeps the two
	// apart. It says nothing about the target; it says the reference read
	// below it in this sample.
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

		at := cfg.Now()
		target, ref := askBoth(ctx, cfg)
		sample := pairSample(at, target, ref)

		if target.timedOut() || ref.timedOut() {
			anyTimeout = true
		}
		if sample.TargetOK {
			targetAnswered = true
			if hasPrevTarget && sample.TargetSlot > prevTarget {
				targetAdvanced = true
				sample.TargetAdvanced = true
			}
			prevTarget = sample.TargetSlot
			hasPrevTarget = true
		}
		if sample.RefOK {
			refAnswered = true
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
	result.copyLastSample()
	result.Verdict = ComputeVerdict(result, cfg.MaxLag)
	result.Degraded = isDegraded(result)
	return result, nil
}

// copyLastSample lifts the final sample's readings onto the Result, where
// check and serve print them without walking Samples. With no samples the
// Last* fields stay at their zero values.
func (r *Result) copyLastSample() {
	if len(r.Samples) == 0 {
		return
	}
	last := r.Samples[len(r.Samples)-1]
	r.LastTargetSlot = last.TargetSlot
	r.LastRefSlot = last.RefSlot
	r.LastLagSlots = last.LagSlots
	r.LastLagMs = last.LagMs
	r.LastRefBehind = last.RefBehind
}

// slotAnswer is what one endpoint gave back to getSlot: a slot, or the error
// that came instead of one.
type slotAnswer struct {
	slot uint64
	err  error
}

func (a slotAnswer) ok() bool { return a.err == nil }

// timedOut is true when the call was cut off by its deadline rather than
// refused or failed outright. Only that case counts as a timeout for the
// verdict.
func (a slotAnswer) timedOut() bool { return errors.Is(a.err, context.DeadlineExceeded) }

// askBoth asks the target and the reference for their slot at the same
// moment and returns once both have answered or failed.
//
// The two must be in flight together. Asked one after the other, the chain
// keeps producing slots during the first call's round trip, and every one
// of them shows up in the difference as lag the target never had. Asked
// together, only the gap between the two round trips can leak in.
func askBoth(ctx context.Context, cfg Config) (target, ref slotAnswer) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		target.slot, target.err = cfg.GetSlot(ctx, cfg.TargetURL)
	}()
	go func() {
		defer wg.Done()
		ref.slot, ref.err = cfg.GetSlot(ctx, cfg.RefURL)
	}()
	wg.Wait()
	return target, ref
}

// pairSample turns the two answers into one Sample taken at the given time.
// An endpoint that failed leaves its OK flag false and its slot at 0. Lag
// exists only when both answered.
func pairSample(at time.Time, target, ref slotAnswer) Sample {
	s := Sample{At: at}
	if target.ok() {
		s.TargetOK, s.TargetSlot = true, target.slot
	}
	if ref.ok() {
		s.RefOK, s.RefSlot = true, ref.slot
	}
	if s.TargetOK && s.RefOK {
		s.LagSlots, s.RefBehind = slotLag(s.TargetSlot, s.RefSlot)
		s.LagMs = s.LagSlots * int64(SlotDuration/time.Millisecond)
	}
	return s
}

// slotLag is how many slots the target trails the reference by. It cannot
// be negative: a reference that reads below the target gives a lag of 0 and
// refBehind=true, so that the 0 is not mistaken for a target keeping pace.
func slotLag(targetSlot, refSlot uint64) (lag int64, refBehind bool) {
	if refSlot < targetSlot {
		return 0, true
	}
	return int64(refSlot - targetSlot), false
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

	// The reference is the witness. If it stood still for the whole window
	// it saw nothing, and a lag measured against it is a number without a
	// witness: the target may be frozen at the same height, or the reference
	// may be the stuck one. Either way there is no FRESH to hand out.
	if !referenceAdvanced(result.Samples) {
		return VerdictUnknown
	}
	if result.TargetAdvanced && result.LastLagSlots <= maxLag {
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

// referenceAdvanced reports whether the reference ended the window on a
// higher slot than it started it. Only the samples it answered count; a
// dip in the middle does not matter, and a single answer cannot show
// movement. This is the liveness the verdict asks of its witness.
func referenceAdvanced(samples []Sample) bool {
	slots := answeredRefSlots(samples)
	if len(slots) < 2 {
		return false
	}
	return slots[len(slots)-1] > slots[0]
}

// answeredRefSlots lists the reference's slot from every sample it
// answered, in sample order.
func answeredRefSlots(samples []Sample) []uint64 {
	slots := make([]uint64, 0, len(samples))
	for _, s := range samples {
		if s.RefOK {
			slots = append(slots, s.RefSlot)
		}
	}
	return slots
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
	fields := []string{
		fmt.Sprintf("target=%s ref=%s lag=%d slots (%d ms) advanced=%s",
			target, ref, s.LagSlots, s.LagMs, adv),
	}
	// Printed only when set. On an ordinary sample the line stays as it
	// always was; when the reference read behind, the lag=0 next to it gets
	// its explanation.
	if s.RefBehind {
		fields = append(fields, "ref_behind=yes")
	}
	return strings.Join(fields, " ")
}
