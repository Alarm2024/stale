package check

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Alarm2024/stale/internal/measure"
	"github.com/Alarm2024/stale/internal/redact"
)

type Options struct {
	TargetURL string
	RefURL    string
	MaxLag    int64
	For       time.Duration
	Out       io.Writer
}

func Run(ctx context.Context, opts Options) (measure.Verdict, error) {
	if opts.Out == nil {
		opts.Out = io.Discard
	}
	cfg := measure.DefaultConfig(opts.TargetURL, opts.RefURL)
	cfg.MaxLag = opts.MaxLag
	if opts.For > 0 {
		cfg.For = opts.For
	}

	result, err := measure.Run(ctx, cfg)
	if err != nil {
		return measure.VerdictUnknown, err
	}

	targetHost := redact.URL(opts.TargetURL)
	refHost := redact.URL(cfg.RefURL)

	fmt.Fprintf(opts.Out, "stale check %s ref=%s max_lag=%d for=%s\n",
		targetHost, refHost, cfg.MaxLag, cfg.For.Round(time.Second))

	for _, sample := range result.Samples {
		fmt.Fprintln(opts.Out, measure.FormatSampleLine(sample))
	}

	lag := "unknown"
	if len(result.Samples) > 0 {
		last := result.Samples[len(result.Samples)-1]
		if last.TargetOK && last.RefOK {
			lag = fmt.Sprintf("%d slots (%d ms)", result.LastLagSlots, result.LastLagMs)
		}
	}

	fmt.Fprintf(opts.Out, "verdict=%s target_slot=%d ref_slot=%d lag=%s target_advanced=%t ref_behind=%t degraded=%t\n",
		result.Verdict,
		result.LastTargetSlot,
		result.LastRefSlot,
		lag,
		result.TargetAdvanced,
		result.LastRefBehind,
		result.Degraded,
	)

	return result.Verdict, nil
}
