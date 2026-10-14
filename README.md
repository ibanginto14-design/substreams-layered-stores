# Layered stores for Substreams

Evidence for a proposal to StreamingFast: squash store partials as a binary-aligned tree instead of one sequential chain, and read the store at any segment boundary from at most 14 tree nodes instead of a per-segment full snapshot.

Two scripts, Python 3 only, no dependencies.

| Script | What it shows |
| --- | --- |
| `store_algebra.py` | For all seven update policies, the tree gives the same store as today's sequential squash, at every boundary and for every key lookup |
| `schedule_model.py` | A discrete-event model of a backfill: how long it takes and how many bytes it moves, today, with snapshot uploads taken off the chain (P1), and with layered stores |

## Run

```
python3 store_algebra.py 2000 1          # equivalence check, seed 1 (also seeds 2 and 3)
python3 store_algebra.py mutants         # four deliberately wrong combine rules, all caught
python3 schedule_model.py 20             # model at 20 workers (also 5 and 50)
python3 schedule_model.py sensitivity 20 # sensitivity table
```

Runs write their numbers to `results/`. The files committed there are the ones quoted in the proposal.

## Results

Equivalence: seeds 1 to 3 check 862,337 boundaries, with 0 differences. All four wrong rules are caught.

Model, 10,000 segments of 1,000 blocks, store growing steadily to its final size, hours:

| Workers | Store | Today | P1: uploads off the chain | Layered stores + node cache |
| --- | --- | --- | --- | --- |
| 5 | 1 GiB | 10.4 | 9.8 | 8.4 |
| 5 | 3.5 GiB | 27.0 | 13.6 | 8.6 |
| 20 | 1 GiB | 7.6 | 2.5 | 2.1 |
| 20 | 3.5 GiB | 26.3 | 3.8 | 2.3 |
| 50 | 1 GiB | 7.5 | 1.2 | 0.9 |
| 50 | 3.5 GiB | 26.2 | 3.4 | 1.0 |

For a 3.5 GiB store, bytes written fall from 18.8 TB to 0.37 TB. A 0.1 GiB store sees 1.0 to 1.2x.

## Assumptions and limits

- These are model numbers, not measurements. Storage throughput (200 MB/s per stream here) matters most, and the squash timing logs tier1 already writes would replace it.
- `store_algebra.py` models the Go code (`storage/store/merge.go` and the per-policy write files) for int64 and byte values. Float64, bigfloat and long bigdecimal sums are not associative bit for bit, so stores of those types would keep the sequential squash.
- Engine behavior is described from the Substreams source at commit [7712e95](https://github.com/streamingfast/substreams/commit/7712e950fd48ce09877a044c9f06ddd44d6fe6cd).

Apache-2.0.
