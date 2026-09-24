package measure

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Snapshot is the latest measured state from a background sampler.
type Snapshot struct {
	Verdict        Verdict `json:"verdict"`
	LagSlots       int64   `json:"lag_slots"`
	SampledMsAgo   int64   `json:"sampled_ms_ago"`
	TargetSlot     uint64  `json:"target_slot"`
	RefSlot        uint64  `json:"ref_slot"`
	TargetAdvanced bool    `json:"target_advanced"`
	Measured       bool    `json:"measured"`
}

type sampleRecord struct {
	Sample
	AnyTimeout bool
}

// Sampler continuously samples slot lag in the background.
type Sampler struct {
	cfg Config

	mu       sync.RWMutex
	snapshot Snapshot
	stop     context.CancelFunc
}

func NewSampler(targetURL, refURL string, maxLag int64) *Sampler {
	cfg := DefaultConfig(targetURL, refURL)
	cfg.MaxLag = maxLag
	return &Sampler{cfg: cfg}
}

func (s *Sampler) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.stop = cancel

	go func() {
		ticker := time.NewTicker(SampleInterval)
		defer ticker.Stop()

		var history []sampleRecord
		for {
			select {
			case <-runCtx.Done():
				return
			case tickAt := <-ticker.C:
				record := s.collectSample(runCtx)
				history = append(history, record)
				if len(history) > 32 {
					history = history[len(history)-32:]
				}

				result := resultFromHistory(history)
				verdict := ComputeVerdict(result, s.cfg.MaxLag)
				measured := len(result.Samples) >= MinSamples &&
					result.RefAnswered &&
					result.TargetAnswered &&
					!result.AnyTimeout

				s.mu.Lock()
				s.snapshot = Snapshot{
					Verdict:        verdict,
					LagSlots:       result.LastLagSlots,
					SampledMsAgo:   time.Since(tickAt).Milliseconds(),
					TargetSlot:     result.LastTargetSlot,
					RefSlot:        result.LastRefSlot,
					TargetAdvanced: result.TargetAdvanced,
					Measured:       measured,
				}
				if !measured && s.snapshot.Verdict == VerdictFresh {
					s.snapshot.Verdict = VerdictUnknown
				}
				s.mu.Unlock()
			}
		}
	}()
}

func (s *Sampler) Stop() {
	if s.stop != nil {
		s.stop()
	}
}

func (s *Sampler) Current() Snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshot
}

func (s *Sampler) collectSample(ctx context.Context) sampleRecord {
	sample := Sample{At: time.Now()}
	record := sampleRecord{Sample: sample}

	targetSlot, targetErr := s.cfg.GetSlot(ctx, s.cfg.TargetURL)
	if targetErr != nil {
		if errors.Is(targetErr, context.DeadlineExceeded) {
			record.AnyTimeout = true
		}
	} else {
		sample.TargetOK = true
		sample.TargetSlot = targetSlot
	}

	refSlot, refErr := s.cfg.GetSlot(ctx, s.cfg.RefURL)
	if refErr != nil {
		if errors.Is(refErr, context.DeadlineExceeded) {
			record.AnyTimeout = true
		}
	} else {
		sample.RefOK = true
		sample.RefSlot = refSlot
	}

	if sample.TargetOK && sample.RefOK {
		lag := int64(refSlot) - int64(targetSlot)
		if lag < 0 {
			lag = 0
		}
		sample.LagSlots = lag
		sample.LagMs = lag * int64(SlotDuration/time.Millisecond)
	}
	record.Sample = sample
	return record
}

func resultFromHistory(history []sampleRecord) Result {
	samples := make([]Sample, len(history))
	result := Result{Samples: samples}
	for i, record := range history {
		samples[i] = record.Sample
		if record.AnyTimeout {
			result.AnyTimeout = true
		}
		if record.TargetOK {
			result.TargetAnswered = true
		}
		if record.RefOK {
			result.RefAnswered = true
		}
	}
	if len(samples) > 0 {
		last := samples[len(samples)-1]
		result.LastTargetSlot = last.TargetSlot
		result.LastRefSlot = last.RefSlot
		result.LastLagSlots = last.LagSlots
		result.LastLagMs = last.LagMs
	}
	result.TargetAdvanced = targetAdvanced(samples)
	return result
}

func targetAdvanced(samples []Sample) bool {
	var prev uint64
	hasPrev := false
	for _, s := range samples {
		if !s.TargetOK {
			continue
		}
		if hasPrev && s.TargetSlot > prev {
			return true
		}
		prev = s.TargetSlot
		hasPrev = true
	}
	return false
}
