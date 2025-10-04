from __future__ import annotations

from collections import deque
from typing import Dict, List, Tuple

from .backends import GraphBackend


PairStats = Tuple[int, int]
CandidateDetail = Tuple[str, int, int, Dict[str, PairStats]]


def get_related_tracks(track_id: str, backend: GraphBackend, k: int | None = None) -> List[str]:
    """Return up to ``k`` track ids connected to ``track_id`` ordered by edge weight."""
    if k is not None and k < 0:
        raise ValueError("k must be a non-negative integer or None")

    results = backend.get_related_tracks(track_id, limit=k)
    return [track for track, _ in results]


def get_related_tracks_for_multiple(
    track_ids: List[str],
    backend: GraphBackend,
    k: int | None = None,
    *,
    max_hops: int = 1,
) -> List[str]:
    """Return track ids connected to *all* seeds within ``max_hops`` hops.

    The result is ordered by the total hop count (ascending), then by the summed edge
    weight along the selected paths (descending), and finally by track id.
    """
    details = get_related_tracks_for_multiple_details(
        track_ids,
        backend,
        k=k,
        max_hops=max_hops,
    )
    return [track_id for track_id, _, _, _ in details]


def get_related_tracks_for_multiple_details(
    track_ids: List[str],
    backend: GraphBackend,
    k: int | None = None,
    *,
    max_hops: int = 1,
) -> List[CandidateDetail]:
    """Return candidates reachable from all seeds along with hop/weight metadata."""
    if k is not None and k < 0:
        raise ValueError("k must be a non-negative integer or None")
    if max_hops < 1:
        raise ValueError("max_hops must be at least 1")
    if not track_ids:
        return []

    unique_track_ids = list(dict.fromkeys(track_ids))
    seed_set = set(unique_track_ids)

    reachable_per_seed: Dict[str, Dict[str, PairStats]] = {}

    for seed_id in unique_track_ids:
        reachable = _collect_reachable(seed_id, backend, max_hops)
        if not reachable:
            reachable_per_seed[seed_id] = {}
            continue
        # Ensure we never recommend seed ids
        for seed in seed_set:
            reachable.pop(seed, None)
        reachable_per_seed[seed_id] = reachable

    if not reachable_per_seed:
        return []

    shared_candidates: set[str] | None = None
    for reachable in reachable_per_seed.values():
        keys = set(reachable.keys())
        if shared_candidates is None:
            shared_candidates = keys
        else:
            shared_candidates &= keys

    if not shared_candidates:
        return []

    results: List[CandidateDetail] = []
    for candidate in shared_candidates:
        per_seed = {
            seed_id: reachable_per_seed[seed_id][candidate]
            for seed_id in unique_track_ids
        }
        total_distance = sum(distance for distance, _ in per_seed.values())
        total_weight = sum(weight for _, weight in per_seed.values())
        results.append((candidate, total_distance, total_weight, per_seed))

    results.sort(key=lambda item: (item[1], -item[2], item[0]))

    if k is not None:
        results = results[:k]

    return results


def _collect_reachable(
    seed_id: str, backend: GraphBackend, max_hops: int
) -> Dict[str, PairStats]:
    """Breadth-first traversal capturing shortest hop distance and weight sums."""
    visited: Dict[str, PairStats] = {}
    queue = deque([(seed_id, 0, 0)])

    while queue:
        current_id, depth, cumulative_weight = queue.popleft()
        if depth == max_hops:
            continue

        for neighbor_id, weight in backend.get_related_tracks(current_id):
            next_depth = depth + 1
            next_weight = cumulative_weight + weight

            if neighbor_id == seed_id:
                continue

            best = visited.get(neighbor_id)
            if best is None or next_depth < best[0] or (
                next_depth == best[0] and next_weight > best[1]
            ):
                visited[neighbor_id] = (next_depth, next_weight)
                queue.append((neighbor_id, next_depth, next_weight))

    return visited


__all__ = [
    "get_related_tracks",
    "get_related_tracks_for_multiple",
    "get_related_tracks_for_multiple_details",
]
