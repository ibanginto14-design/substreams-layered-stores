"""Discrete-event model of a two-stage Substreams backfill: stage 0 writes a store, stage 1 reads it.

today    : the store is squashed sequentially. Merging segment k's partial is followed by saving a
           full snapshot of the store (squash.go: singleSquash -> fullKV.Save at every segment
           boundary). A stage-1 job at segment k waits until the squash has reached k
           (stages.go: dependenciesCompleted) and starts by loading that full snapshot (tier2
           setupSubrequestStores -> FullKV.Load).
pipelined: (P1 in the proposal) today's layout, but the snapshot upload is taken off the squash chain: the in-memory
           merge stays sequential and up to R snapshot uploads run at once (needs a frozen view of
           the store per upload in flight). This is the obvious change that needs no new layout.
layered  : partials are combined pairwise into a binary-aligned tree by M parallel mergers. A
           stage-1 job at segment k waits only for the <= log2(k)+1 nodes covering [0, k) and reads
           those, optionally from a per-worker cache of nodes it has already fetched. No per-segment
           full snapshot is written.

Tier2 workers follow NextJob's order: lowest segment first, and within a segment the higher
stage first. All sizes and rates are parameters; nothing here is measured on StreamingFast.
"""
import heapq
import json
import os
import sys


class Params:
    def __init__(self, **kw):
        self.n = 10_000           # segments of 1,000 blocks
        self.W = 20               # Tier2 workers for the request
        self.t0 = 10.0            # stage-0 job compute, s
        self.t1 = 5.0             # stage-1 job compute, s
        self.S = 3.5 * 2 ** 30    # final store size, bytes
        self.u = 0.002            # share of existing keys updated per segment
        self.Bw = 200e6           # write bandwidth to object storage, B/s
        self.Br = 200e6           # read bandwidth from object storage, B/s
        self.Bm = 100e6           # merge throughput, B/s
        self.M = 4                # parallel tree mergers (layered only)
        self.cache = True         # per-worker cache of tree nodes (layered only)
        self.R = 8                # snapshot uploads in flight (pipelined only)
        self.__dict__.update(kw)

    def store_size(self, k):
        """Store size after k segments (keys added at a steady rate)."""
        return self.S * k / self.n

    def range_size(self, a, length):
        """Distinct keys written in segments [a, a+length): new keys + existing keys updated."""
        new = length * self.S / self.n
        updated = self.store_size(a) * (1 - (1 - self.u) ** length)
        return min(new + updated, self.store_size(a + length))


def decomposition(k):
    """Binary-aligned tree nodes covering segments [0, k)."""
    nodes, start = [], 0
    for bit in reversed(range(k.bit_length())):
        size = 1 << bit
        if k & size:
            nodes.append((start, size))
            start += size
    return nodes


def simulate(p, mode):
    n = p.n
    layered = mode == "layered"
    pipelined = mode == "pipelined"
    partial_done = [None] * n
    state_ready = [None] * (n + 1)   # when the store state at boundary k can be read
    state_ready[0] = 0.0
    totals = {"written": 0.0, "read": 0.0}
    events, seq = [], [0]

    def push(t, kind, data):
        heapq.heappush(events, (t, seq[0], kind, data))
        seq[0] += 1

    # stage-1 readiness ------------------------------------------------------------------------
    ready_stage1 = []

    def maybe_ready_stage1(k):
        if k < n and partial_done[k] is not None and state_ready[k] is not None:
            heapq.heappush(ready_stage1, k)

    def set_state_ready(k, now):
        if state_ready[k] is None:
            state_ready[k] = now
            maybe_ready_stage1(k)

    # today: one sequential squasher -----------------------------------------------------------
    squash = {"next": 0, "busy": False}

    uploaders = {"free": p.R}

    def try_squash(now):
        k = squash["next"]
        if squash["busy"] or k >= n or partial_done[k] is None:
            return
        if pipelined:
            if uploaders["free"] == 0:      # no frozen view free for an upload: the merge waits
                return
            uploaders["free"] -= 1
            push(now + p.range_size(k, 1) / p.Bm, "merged_seq", k)
        else:
            push(now + p.range_size(k, 1) / p.Bm + p.store_size(k + 1) / p.Bw, "squashed", k)
        totals["written"] += p.store_size(k + 1)
        squash["busy"] = True

    # layered: tree of combined partials, M mergers --------------------------------------------
    node_ready = set()
    merge_queue = []
    mergers = {"free": p.M}

    def node_done(node, now):
        node_ready.add(node)
        start, length = node
        # boundaries that use this node: k in [start+length, start+2*length), start aligned to 2*length
        if start % (2 * length) == 0:
            for k in range(start + length, min(n, start + 2 * length - 1) + 1):
                if state_ready[k] is None and all(nd in node_ready for nd in decomposition(k)):
                    set_state_ready(k, now)
        parent_len = 2 * length
        parent = (start - start % parent_len, parent_len)
        if parent[0] + parent_len <= n:
            sibling = (parent[0] + length if start == parent[0] else parent[0], length)
            if sibling in node_ready:
                heapq.heappush(merge_queue, parent)

    def try_merge(now):
        while mergers["free"] > 0 and merge_queue:
            start, length = heapq.heappop(merge_queue)
            half = length // 2
            dur = (p.range_size(start, half) + p.range_size(start + half, half)) / p.Bm \
                + p.range_size(start, length) / p.Bw
            totals["written"] += p.range_size(start, length)
            mergers["free"] -= 1
            push(now + dur, "merged", (start, length))

    # Tier2 workers ------------------------------------------------------------------------------
    next_stage0 = [0]
    free_ids = list(range(p.W))
    caches = [set() for _ in range(p.W)]

    def dispatch(now):
        while free_ids:
            cand1 = ready_stage1[0] if ready_stage1 else None
            cand0 = next_stage0[0] if next_stage0[0] < n else None
            if cand1 is None and cand0 is None:
                return
            wid = free_ids.pop()
            if cand1 is not None and (cand0 is None or cand1 <= cand0):
                k = heapq.heappop(ready_stage1)
                if not layered:   # today and pipelined: load the full snapshot
                    load = p.store_size(k)
                else:
                    load = 0.0
                    for node in decomposition(k):
                        if p.cache and node in caches[wid]:
                            continue
                        load += p.range_size(*node)
                        caches[wid].add(node)
                totals["read"] += load
                push(now + load / p.Br + p.t1, "s1", (k, wid))
            else:
                k = next_stage0[0]
                next_stage0[0] += 1
                push(now + p.t0 + p.range_size(k, 1) / p.Bw, "s0", (k, wid))

    dispatch(0.0)
    remaining, now = n, 0.0
    while events and remaining:
        now, _, kind, data = heapq.heappop(events)
        if kind == "s0":
            k, wid = data
            partial_done[k] = now
            free_ids.append(wid)
            maybe_ready_stage1(k)
            if layered:
                node_done((k, 1), now)
                try_merge(now)
            else:
                try_squash(now)
        elif kind == "squashed":
            squash["busy"] = False
            squash["next"] = data + 1
            set_state_ready(data + 1, now)
            try_squash(now)
        elif kind == "merged_seq":          # pipelined: merge done, upload starts, next merge may go
            squash["busy"] = False
            squash["next"] = data + 1
            push(now + p.store_size(data + 1) / p.Bw, "uploaded", data)
            try_squash(now)
        elif kind == "uploaded":
            uploaders["free"] += 1
            set_state_ready(data + 1, now)
            try_squash(now)
        elif kind == "merged":
            mergers["free"] += 1
            node_done(data, now)
            try_merge(now)
        elif kind == "s1":
            _, wid = data
            free_ids.append(wid)
            remaining -= 1
        dispatch(now)

    return {"makespan_h": now / 3600, "written_TB": totals["written"] / 1e12, "read_TB": totals["read"] / 1e12}


MODES = (("today", "today", {}),
         ("pipelined", "pipelined", {}),
         ("layered", "layered", {"cache": False}),
         ("layered+cache", "layered", {"cache": True}))

GiB = 2 ** 30
SCENARIOS = [
    dict(label="store 0.1 GiB", S=0.1 * GiB),
    dict(label="store 1 GiB", S=1 * GiB),        # binary default store size limit
    dict(label="store 3.5 GiB", S=3.5 * GiB),    # StreamingFast-hosted limit (change log v1.25.0)
]

SENSITIVITY = [
    ("base", {}),
    ("fast storage, 1 GB/s", dict(Bw=1e9, Br=1e9, Bm=500e6)),
    ("slow storage, 100 MB/s", dict(Bw=100e6, Br=100e6, Bm=50e6)),
    ("hot keys, 1% updated per segment", dict(u=0.01)),
    ("20,000 segments", dict(n=20_000)),
    ("1 tree merger", dict(M=1)),
    ("2 uploads in flight", dict(R=2)),
    ("tier2 jobs 4x slower", dict(t0=40.0, t1=20.0)),
]


def run(W, n, scenarios, overrides=None):
    rows = []
    for sc in scenarios:
        sc = dict(sc)
        label = sc.pop("label")
        row = {"scenario": label, "W": W, "n": (overrides or {}).get("n", n)}
        for name, mode, extra in MODES:
            params = dict(W=W, n=n, **sc, **extra)
            params.update(overrides or {})
            row[name] = simulate(Params(**params), mode)
        rows.append(row)
    return rows


def line(r):
    t = r["today"]
    parts = [f"W={r['W']:<3} {r['scenario']:<14} today {t['makespan_h']:6.1f} h"]
    for name in ("pipelined", "layered", "layered+cache"):
        x = r[name]
        parts.append(f"{name} {x['makespan_h']:5.1f} h ({t['makespan_h'] / x['makespan_h']:4.1f}x)")
    c = r["layered+cache"]
    parts.append(f"written {t['written_TB']:6.2f} -> {c['written_TB']:5.2f} TB, read {t['read_TB']:6.2f} -> {c['read_TB']:5.2f} TB")
    return " | ".join(parts)


if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    if len(sys.argv) > 1 and sys.argv[1] == "sensitivity":
        W = int(sys.argv[2]) if len(sys.argv) > 2 else 20
        out = {}
        for label, ov in SENSITIVITY:
            out[label] = run(W, 10_000, SCENARIOS, ov)
            print(f"--- {label}")
            for r in out[label]:
                print(line(r))
        json.dump(out, open(f"results/sensitivity_W{W}.json", "w"), indent=2)
    else:
        W = int(sys.argv[1]) if len(sys.argv) > 1 else 20
        n = int(sys.argv[2]) if len(sys.argv) > 2 else 10_000
        rows = run(W, n, SCENARIOS)
        for r in rows:
            print(line(r))
        json.dump(rows, open(f"results/model_W{W}_n{n}.json", "w"), indent=2)
