"""Kafka-style range partition assignor (matches volant_broker::range_assign)."""

from __future__ import annotations


def range_assign(num_partitions: int, member_ids: list[str]) -> list[list[int]]:
    """Assign ``num_partitions`` to ``member_ids`` using the range algorithm.

    Members are sorted by id. For ``n`` partitions and ``m`` members:
    ``base = n // m``, ``extra = n % m``; sorted member ``i`` gets
    ``base + (1 if i < extra else 0)`` consecutive partitions.

    Returns a parallel list of partition lists in the original member order.
    Empty members or ``num_partitions <= 0`` yields an empty list per member.
    """
    ids = list(member_ids)
    m = len(ids)
    if m == 0 or num_partitions <= 0:
        return [[] for _ in range(m)]

    indexed = list(enumerate(ids))
    indexed.sort(key=lambda item: item[1])

    base = num_partitions // m
    extra = num_partitions % m

    result: list[list[int]] = [[] for _ in range(m)]
    nxt = 0
    for rank, (orig_idx, _) in enumerate(indexed):
        count = base + (1 if rank < extra else 0)
        result[orig_idx] = list(range(nxt, nxt + count))
        nxt += count
    return result


def range_assign_multi(
    member_ids: list[str],
    member_topics: list[list[str]],
    partition_counts: dict[str, int],
) -> list[list[tuple[str, int]]]:
    """Range-assign each topic independently to members subscribed to it.

    ``member_topics[i]`` is the subscription list for ``member_ids[i]``.
    ``partition_counts`` maps topic name → partition count. Topics missing
    from ``partition_counts`` are skipped. Returns ``assignments[i]`` as
    ``(topic, partition)`` pairs for member ``i``, sorted by topic then
    partition.
    """
    ids = list(member_ids)
    if len(ids) != len(member_topics):
        raise ValueError("member_ids and member_topics must have the same length")
    m = len(ids)
    out: list[list[tuple[str, int]]] = [[] for _ in range(m)]
    if m == 0:
        return out

    topics: list[str] = []
    for subscribed in member_topics:
        topics.extend(subscribed)
    topics.sort()
    deduped: list[str] = []
    for topic in topics:
        if not deduped or deduped[-1] != topic:
            deduped.append(topic)
    topics = deduped

    counts = partition_counts if partition_counts is not None else {}
    for topic in topics:
        n = counts.get(topic)
        if n is None:
            continue
        sub_ids: list[str] = []
        sub_orig: list[int] = []
        for i, subscribed in enumerate(member_topics):
            if any(t == topic for t in subscribed):
                sub_ids.append(ids[i])
                sub_orig.append(i)
        if not sub_ids:
            continue
        parts = range_assign(int(n), sub_ids)
        for j, ps in enumerate(parts):
            orig = sub_orig[j]
            for p in ps:
                out[orig].append((topic, int(p)))

    for assignment in out:
        assignment.sort()
    return out
