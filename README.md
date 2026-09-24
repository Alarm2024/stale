# stale

Measure how old a Solana RPC's answers really are — instead of trusting the RPC's own word.

```bash
go install github.com/Alarm2024/stale/cmd/stale@latest
stale check https://api.mainnet-beta.solana.com
stale serve --upstream https://api.mainnet-beta.solana.com
```

MIT · read-only · no keys, no wallet
