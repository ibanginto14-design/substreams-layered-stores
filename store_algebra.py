"""Reference model of Substreams store squashing, and a check that it can be done as a tree.

Mirrors storage/store/merge.go (merging a partial into a full store) and the in-segment write
semantics of each update policy (value_set.go, store_sum.go, store_max.go, store_min.go,
value_append.go, store_setsum.go, partial_kv.go DeletePrefix), for int64 / bytes values.

Today a full store at segment boundary k is built sequentially:
    full_k = merge(merge(merge(empty, p_0), p_1), ... p_{k-1})
The proposal builds it from a binary-aligned tree of combined partials:
    full_k = merge(empty, combine(node_1, combine(node_2, ...)))
where each node combines adjacent partials. That is correct if `combine` exists such that
    merge(merge(full, a), b) == merge(full, combine(a, b))   for every full, a, b
This file defines `combine` for each policy and checks the identity, plus the tree result and a
per-key layered lookup, against the sequential squash on many random histories.
"""
import random
import sys

POLICIES = ["set", "set_if_not_exists", "add", "max", "min", "append", "set_sum"]


class Partial:
    """What a Tier2 job writes for one segment: final key values + prefixes deleted in it."""

    def __init__(self):
        self.kv = {}
        self.dp = []  # ordered, each prefix once (PartialKV.DeletePrefix)

    def copy(self):
        p = Partial()
        p.kv = dict(self.kv)
        p.dp = list(self.dp)
        return p


# ---- in-segment writes (a module writing its store during one segment) ----------------------

def write(policy, part, op):
    kind = op[0]
    if kind == "delete_prefix":
        prefix = op[1]
        for k in [k for k in part.kv if k.startswith(prefix)]:
            del part.kv[k]
        if prefix not in part.dp:
            part.dp.append(prefix)
        return
    _, key, val = op
    cur = part.kv.get(key)
    if policy == "set":
        part.kv[key] = val
    elif policy == "set_if_not_exists":
        if cur is None:
            part.kv[key] = val
    elif policy == "add":
        part.kv[key] = val if cur is None else cur + val
    elif policy == "max":
        part.kv[key] = val if cur is None or val > cur else cur
    elif policy == "min":
        part.kv[key] = val if cur is None or val < cur else cur
    elif policy == "append":
        part.kv[key] = val if cur is None else cur + val
    elif policy == "set_sum":
        # val is ("set", n) or ("sum", n); a 'sum' on top of a 'set' keeps the 'set' tag
        if cur is None or val[0] == "set":
            part.kv[key] = val
        else:
            part.kv[key] = (cur[0], cur[1] + val[1])
    else:
        raise ValueError(policy)


# ---- merge.go: partial into full ------------------------------------------------------------

def merge_value(policy, existing, incoming):
    """Value in the full store after merging one partial value into it (merge.go, step 3)."""
    if policy == "set":
        return incoming
    if policy == "set_if_not_exists":
        return existing if existing is not None else incoming
    if policy == "add":
        return (existing or 0) + incoming
    if policy == "max":
        return incoming if existing is None else max(existing, incoming)
    if policy == "min":
        return incoming if existing is None else min(existing, incoming)
    if policy == "append":
        return incoming if existing is None else existing + incoming
    if policy == "set_sum":
        # full store values are always 'sum:' tagged; a 'set:' partial value resets it
        if incoming[0] == "set":
            return ("sum", incoming[1])
        return ("sum", (existing[1] if existing is not None else 0) + incoming[1])
    raise ValueError(policy)


def merge_into_full(policy, full, part):
    full = dict(full)
    for prefix in part.dp:  # deletes first (Merge -> DeletePrefix), then the key merge
        for k in [k for k in full if k.startswith(prefix)]:
            del full[k]
    for k, v in part.kv.items():
        full[k] = merge_value(policy, full.get(k), v)
    return full


# ---- the proposal: combine two adjacent partials (a older, b newer) into one ---------------

def combine_value(policy, older, newer):
    if older is None:
        return newer
    if policy == "set":
        return newer
    if policy == "set_if_not_exists":
        return older
    if policy in ("add", "append"):
        return older + newer
    if policy == "max":
        return max(older, newer)
    if policy == "min":
        return min(older, newer)
    if policy == "set_sum":
        if newer[0] == "set":
            return newer
        return (older[0], older[1] + newer[1])
    raise ValueError(policy)


def combine(policy, a, b):
    out = Partial()
    # keys of the older partial that the newer one deleted by prefix are gone
    kept = {k: v for k, v in a.kv.items() if not any(k.startswith(p) for p in b.dp)}
    out.kv = dict(kept)
    for k, v in b.kv.items():
        out.kv[k] = combine_value(policy, kept.get(k), v)
    out.dp = list(a.dp) + [p for p in b.dp if p not in a.dp]
    return out


# ---- tree layout and lookups ----------------------------------------------------------------

def decomposition(k):
    """Binary-aligned nodes covering segments [0, k), oldest first: (start, length)."""
    nodes, start = [], 0
    for bit in reversed(range(k.bit_length())):
        size = 1 << bit
        if k & size:
            nodes.append((start, size))
            start += size
    return nodes


def build_tree(policy, partials):
    """node[(start, length)] for every aligned power-of-two range; leaves are the partials."""
    n = len(partials)
    nodes = {(i, 1): partials[i] for i in range(n)}
    length = 1
    while length * 2 <= n:
        for start in range(0, n - length * 2 + 1, length * 2):
            nodes[(start, length * 2)] = combine(policy, nodes[(start, length)], nodes[(start + length, length)])
        length *= 2
    return nodes


def layered_full(policy, nodes, k):
    acc = Partial()
    for node in decomposition(k):
        acc = combine(policy, acc, nodes[node])
    return merge_into_full(policy, {}, acc)


def layered_get(policy, nodes, k, key):
    """Point lookup of one key at boundary k, touching only the O(log k) nodes."""
    value = None
    for node in decomposition(k):
        part = nodes[node]
        if any(key.startswith(p) for p in part.dp):
            value = None
        if key in part.kv:
            value = merge_value(policy, value, part.kv[key])
    return value


# ---- random histories -----------------------------------------------------------------------

KEYS = ["a:1", "a:2", "a:3", "b:1", "b:2", "c:1", "c:22", "d"]
PREFIXES = ["a:", "b:", "c:2", "c:", "d"]


def random_value(policy, rnd):
    if policy in ("set", "set_if_not_exists", "append"):
        return bytes([rnd.randrange(97, 123)])
    if policy == "set_sum":
        return (rnd.choice(["set", "sum", "sum"]), rnd.randint(-50, 50))
    return rnd.randint(-1000, 1000)


def random_partial(policy, rnd):
    part = Partial()
    for _ in range(rnd.randint(0, 6)):
        if rnd.random() < 0.12:
            write(policy, part, ("delete_prefix", rnd.choice(PREFIXES)))
        else:
            write(policy, part, ("put", rnd.choice(KEYS), random_value(policy, rnd)))
    return part


def check(cases_per_policy, seed):
    rnd = random.Random(seed)
    totals = {}
    for policy in POLICIES:
        checks = 0
        for _ in range(cases_per_policy):
            n = rnd.randint(1, 40)
            partials = [random_partial(policy, rnd) for _ in range(n)]

            # 1. the algebraic identity merge(merge(F, a), b) == merge(F, combine(a, b))
            full = merge_into_full(policy, {}, random_partial(policy, rnd))
            a, b = partials[0], random_partial(policy, rnd)
            assert merge_into_full(policy, merge_into_full(policy, full, a), b) == \
                merge_into_full(policy, full, combine(policy, a, b)), (policy, "identity")

            # 2. sequential squash (today) vs tree (proposal), at every segment boundary
            nodes = build_tree(policy, partials)
            seq = {}
            for k in range(1, n + 1):
                seq = merge_into_full(policy, seq, partials[k - 1])
                tree = layered_full(policy, nodes, k)
                assert seq == tree, (policy, "tree", k)
                for key in KEYS:
                    assert seq.get(key) == layered_get(policy, nodes, k, key), (policy, "lookup", k, key)
                checks += 1
        totals[policy] = checks
    return totals


# ---- mutation check: a wrong `combine` must be caught -----------------------------------------

def _mutants():
    """Plausible mistakes in `combine`; each must make `check` fail."""
    good_value, good_combine = combine_value, combine

    def keep_deleted_keys(policy, a, b):          # forgets that b's delete_prefix hides a's keys
        out = Partial()
        out.kv = dict(a.kv)
        for k, v in b.kv.items():
            out.kv[k] = good_value(policy, a.kv.get(k), v)
        out.dp = list(a.dp) + [p for p in b.dp if p not in a.dp]
        return out

    def drop_older_prefixes(policy, a, b):        # keeps only the newer partial's deleted prefixes
        out = good_combine(policy, a, b)
        out.dp = list(b.dp)
        return out

    def newer_wins_if_not_exists(policy, older, newer):
        return newer if policy == "set_if_not_exists" else good_value(policy, older, newer)

    def setsum_ignores_tag(policy, older, newer):
        if policy == "set_sum" and older is not None:
            return ("sum", older[1] + newer[1])
        return good_value(policy, older, newer)

    return [("keep keys deleted by a newer prefix", "combine", keep_deleted_keys),
            ("drop the older partial's deleted prefixes", "combine", drop_older_prefixes),
            ("set_if_not_exists: newer value wins", "combine_value", newer_wins_if_not_exists),
            ("set_sum: a newer 'set' does not reset", "combine_value", setsum_ignores_tag)]


def mutation_check(cases, seed):
    g = globals()
    caught = []
    for name, target, fn in _mutants():
        saved = g[target]
        g[target] = fn
        try:
            check(cases, seed)
            caught.append((name, False))
        except AssertionError:
            caught.append((name, True))
        finally:
            g[target] = saved
    return caught


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "mutants":
        for name, ok in mutation_check(300, seed=1):
            print(f"{'caught' if ok else 'MISSED'}: {name}")
        sys.exit(0)
    cases = int(sys.argv[1]) if len(sys.argv) > 1 else 2000
    totals = check(cases, seed=int(sys.argv[2]) if len(sys.argv) > 2 else 1)
    for policy, n in totals.items():
        print(f"{policy:<18} {n:7d} boundaries checked (full store and every key lookup): identical")
