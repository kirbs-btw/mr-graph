from __future__ import annotations

from typing import List

from .backends import GraphBackend


def get_related_tracks(track_id: str, backend: GraphBackend, k: int | None = None) -> List[str]:
    """Return up to ``k`` track ids connected to ``track_id`` ordered by edge weight."""
    if k is not None and k < 0:
        raise ValueError("k must be a non-negative integer or None")

    results = backend.get_related_tracks(track_id, limit=k)
    return [track for track, _ in results]


def get_related_tracks_for_multiple(track_ids: List[str], backend: GraphBackend, k: int | None = None) -> List[str]:
    """Placeholder for multi-track retrieval strategy."""
    raise NotImplementedError(
        "Multi-track retrieval is not defined yet. Decide on a strategy before implementing."
    )


__all__ = ["get_related_tracks", "get_related_tracks_for_multiple"]
