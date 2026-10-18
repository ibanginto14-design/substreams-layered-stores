# SparseFirehose: index-guided seekable execution

Reproducible simulation for a performance proposal to StreamingFast / The Graph.

## Core idea

Substreams block indexes can identify the exact historical blocks that match a filter. The proposal is to push that selectivity all the way down to storage:

Block Index -> Tier1 sparse planner -> skip empty Tier2 jobs -> exact block set -> seek table -> object-storage range reads -> matching compressed frames only -> lazy decode -> WASM

Keep the existing 100-block merged-bundle boundaries for compatibility, but make the inside of a bundle independently seekable in small compressed frames. Dense workloads keep the current sequential reader as fallback.

## Code paths

- Substreams block indexes: https://github.com/streamingfast/substreams/blob/develop/storage/index/index.go
- BlockIndexProvider: https://github.com/streamingfast/bstream/blob/develop/transform/block_index_provider.go
- FileSource: https://github.com/streamingfast/bstream/blob/develop/filesource.go
- Firehose merged bundles: https://github.com/streamingfast/firehose-core/blob/develop/merged_blocks_writer.go
- dstore Store API: https://github.com/streamingfast/dstore/blob/master/stores.go

## Simulation setup

- 500,000 blocks per run
- 30 runs per selectivity
- current bundle size: 100 blocks
- proposed seekable frame size: 4 blocks
- deterministic seeds
- Python standard library only

## Results

| Relevant blocks | Current blocks read | 4-block frames | I/O reduction |
| ---: | ---: | ---: | ---: |
| 0.1% | 9.55% | 0.40% | 23.95x |
| 0.5% | 39.25% | 1.98% | 19.81x |
| 1.0% | 63.41% | 3.94% | 16.11x |
| 5.0% | 99.40% | 18.57% | 5.35x |
| 10.0% | 100.00% | 34.36% | 2.91x |

A bursty model with about 1% relevant blocks gives about 12.06x less modeled block I/O.

These are I/O-model results, not end-to-end Substreams benchmarks.

Target for a prototype: >=5x end-to-end speedup on <=1%-selective historical workloads, while keeping dense workloads within +/-5% of the current path.

## Reproduce

Run:

python3 simulate.py

The script writes CSV results under results/.
