from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from mr_graph.backends import GraphBackend, NetworkXGraphBackend
from mr_graph.retrieval import (
    get_related_tracks,
    get_related_tracks_for_multiple,
)


@dataclass
class StubBackend(GraphBackend):
    response: List[Tuple[str, int]]
    last_call: Tuple[str, Optional[int]] | None = None

    def upsert_track(self, track_id: str, **properties) -> None:
        return None

    def upsert_connection(self, source_id: str, target_id: str, weight: int) -> None:
        return None

    def get_related_tracks(
        self, track_id: str, limit: int | None = None
    ) -> List[Tuple[str, int]]:
        self.last_call = (track_id, limit)
        if limit is None:
            return list(self.response)
        return list(self.response[:limit])

    def close(self) -> None:
        return None


def test_get_related_tracks_returns_ids_in_backend_order() -> None:
    backend = StubBackend(
        response=[("track_d", 4), ("track_b", 2), ("track_c", 1)]
    )
    result = get_related_tracks("seed", backend)

    assert result == ["track_d", "track_b", "track_c"]
    assert backend.last_call == ("seed", None)


def test_get_related_tracks_honours_k_limit() -> None:
    backend = StubBackend(
        response=[("track_a", 5), ("track_b", 3), ("track_c", 2)]
    )
    result = get_related_tracks("seed", backend, k=2)

    assert result == ["track_a", "track_b"]
    assert backend.last_call == ("seed", 2)


def test_get_related_tracks_negative_k_raises() -> None:
    backend = StubBackend(response=[])

    with pytest.raises(ValueError):
        get_related_tracks("seed", backend, k=-1)


def test_placeholder_multi_track_retrieval_not_implemented() -> None:
    backend = StubBackend(response=[])

    with pytest.raises(NotImplementedError):
        get_related_tracks_for_multiple(["seed"], backend)


@pytest.mark.networkx
def test_networkx_backend_returns_sorted_neighbors() -> None:
    pytest.importorskip("networkx")

    backend = NetworkXGraphBackend()

    backend.upsert_connection("seed", "track_b", 3)
    backend.upsert_connection("seed", "track_a", 3)
    backend.upsert_connection("seed", "track_c", 1)
    backend.upsert_connection("seed", "track_d", 5)

    result = backend.get_related_tracks("seed")

    assert result == [
        ("track_d", 5),
        ("track_a", 3),
        ("track_b", 3),
        ("track_c", 1),
    ]

    limited = backend.get_related_tracks("seed", limit=2)
    assert limited == [("track_d", 5), ("track_a", 3)]

    missing = backend.get_related_tracks("unknown")
    assert missing == []

    backend.close()
