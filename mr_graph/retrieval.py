from __future__ import annotations

from collections import defaultdict
from typing import List

from .backends import GraphBackend


def get_related_tracks(track_id: str, backend: GraphBackend, k: int | None = None) -> List[str]:
    """Return up to ``k`` track ids connected to ``track_id`` ordered by edge weight."""
    if k is not None and k < 0:
        raise ValueError("k must be a non-negative integer or None")

    results = backend.get_related_tracks(track_id, limit=k)
    return [track for track, _ in results]


def get_related_tracks_for_multiple(track_ids: List[str], backend: GraphBackend, k: int | None = None) -> List[str]:
    """Return related tracks ranked by coverage across ``track_ids`` and edge weight.

    Tracks connected to more of the seed tracks appear first, breaking ties by the
    summed edge weight back to the seeds and finally by track id. Original seed
    track ids are excluded from the result.
    """
    if k is not None and k < 0:
        raise ValueError("k must be a non-negative integer or None")

    if not track_ids:
        return []

    unique_track_ids = list(dict.fromkeys(track_ids))
    seed_set = set(unique_track_ids)

    aggregated_weights: dict[str, int] = defaultdict(int)
    seed_hits: dict[str, int] = defaultdict(int)

    for seed_id in unique_track_ids:
        related_tracks = backend.get_related_tracks(seed_id)
        seen_for_seed: set[str] = set()

        for related_id, weight in related_tracks:
            if related_id in seed_set:
                continue

            aggregated_weights[related_id] += weight

            if related_id not in seen_for_seed:
                seed_hits[related_id] += 1
                seen_for_seed.add(related_id)

    if not aggregated_weights:
        return []

    ranked_tracks = sorted(
        ((track_id, seed_hits[track_id], aggregated_weights[track_id])
         for track_id in aggregated_weights),
        key=lambda item: (-item[1], -item[2], item[0]),
    )

    if k is not None:
        ranked_tracks = ranked_tracks[:k]

    return [track_id for track_id, _, _ in ranked_tracks]


__all__ = ["get_related_tracks", "get_related_tracks_for_multiple"]
