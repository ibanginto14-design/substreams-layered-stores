# RFC sketch: SparseFirehose — Index-Guided Seekable Execution

## Problem

Substreams can know exactly which historical blocks satisfy a block filter, but that knowledge does not currently eliminate all physical I/O. A merged bundle that contains one matching block can still be opened and walked before non-matching blocks are discarded.

For sparse workloads this creates an avoidable gap between logical selectivity and physical selectivity.

## Proposal

Push block-index selectivity through the entire historical read path:

1. **Tier1 sparse planning** — preflight block-index coverage before scheduling Tier2 work.
2. **Optional range reads in dstore** — backward-compatible byte-range capability.
3. **Seekable merged bundles** — preserve 100-block boundaries, but use independently compressed internal frames plus an offset/length/checksum table.
4. **Adaptive sparse FileSource** — range-fetch required frames at low density, sequential fallback at high density.
5. **Finalized history first** — preserve live/reorg/progress/gap-bundle semantics.

## Why it may matter

At 1% independently distributed relevant blocks, the committed simulation reads about 63.4% of the chain through the current 100-block bundle model. Four-block seekable frames reduce the modeled physical read fraction to about 3.94%, or ~16.1x less block I/O.

A deliberately bursty ~1% workload still produces ~12.1x less modeled block I/O.

These are not end-to-end speed claims.

## Acceptance benchmark

A prototype should target:

- >=5x end-to-end backfill speedup on <=1%-selective historical workloads;
- no output differences;
- no cursor/reorg regressions;
- <=5% regression on dense workloads;
- bytes downloaded/decompressed measured separately from WASM time.

## Follow-up

Once seekable reads exist, a BASE / EXTENDED projection lane can reduce payload size further for modules that do not consume traces/state changes.
