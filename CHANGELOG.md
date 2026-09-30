# Changelog

## v0.1.2

Notes for #11 and #12. No tag.

### #11 Keep a proven STALE through a failed call

- STALE can rest on the samples both endpoints answered, when there are at least two and either the reference moved while the target did not, or the last of those samples is past the lag bound. A later failed call does not turn that into UNKNOWN.
- FRESH still needs a clean window. An unpaired final sample leaves a placeholder lag of 0; that 0 no longer yields FRESH, including when the failure was not a timeout (a 429 or a refused connection).
- A STALE reached while some samples went unanswered is marked degraded: `degraded=true` on the `stale check` verdict line, `"degraded": true` in `/health`, and `X-Stale-Degraded: true`.
- `stale check` prints `lag=unknown` when the final sample is unpaired, instead of `lag=0 slots (0 ms)`.

### #12 Report whether the reference is behind, and pair every sample for FRESH

- `/health` `ref_behind` is `true` when the last paired sample has the reference slot below the target, and `null` when the last sample is unpaired. It had stayed `false`.
- The `stale check` verdict line prints `ref_behind=unknown` when the final sample is unpaired. `false` there would read as "the reference is not behind".
- FRESH requires every sample in the window to be paired. A non-timeout failure in the middle, on either endpoint, is UNKNOWN. STALE still rests on the paired samples alone.
- The measure path that asks both endpoints together, flags a trailing reference, and refuses FRESH against a frozen reference is rewritten. Those verdicts are unchanged.
