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


# Backend stub that returns neighbors per seed id
@dataclass
class MapBackend(GraphBackend):
    neighbors_map: dict[str, List[Tuple[str, int]]]

    def upsert_track(self, track_id: str, **properties) -> None:
        return None

    def upsert_connection(self, source_id: str, target_id: str, weight: int) -> None:
        return None

    def get_related_tracks(
        self, track_id: str, limit: int | None = None
    ) -> List[Tuple[str, int]]:
        results = list(self.neighbors_map.get(track_id, []))
        if limit is not None:
            return results[:limit]
        return results

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


def test_get_related_tracks_for_multiple_ranks_by_coverage_then_weight_then_id() -> None:
    backend = MapBackend(
        neighbors_map={
            # seed_a connects strongly to x, weakly to y, and to z
            "seed_a": [("x", 5), ("y", 1), ("z", 3)],
            # seed_b connects to x and y, making x and y have coverage 2 overall
            "seed_b": [("x", 2), ("y", 4), ("w", 2)],
            # seed_c connects only to y making y coverage 3
            "seed_c": [("y", 2), ("t", 10)],
        }
    )

    # Coverage counts:
    # y: 3 seeds, agg weight = 1 + 4 + 2 = 7
    # x: 2 seeds, agg weight = 5 + 2 = 7
    # z: 1 seed, agg weight = 3
    # w: 1 seed, agg weight = 2
    # t: 1 seed, agg weight = 10
    result = get_related_tracks_for_multiple(["seed_a", "seed_b", "seed_c"], backend)

    # Expect order by coverage desc, then sum weight desc, then id asc
    # First all coverage 3: only y
    # Then coverage 2: only x
    # Then coverage 1 by weight desc: t (10), z (3), w (2)
    assert result == ["y", "x", "t", "z", "w"]


def test_get_related_tracks_for_multiple_excludes_seeds_and_dedups_input() -> None:
    backend = MapBackend(
        neighbors_map={
            "a": [("b", 2), ("c", 1)],
            "b": [("a", 2), ("d", 5)],
        }
    )

    # Include duplicates and ensure seeds don't appear in result
    result = get_related_tracks_for_multiple(["a", "a", "b"], backend)
    assert result == ["d", "c"]  # d (coverage 1, weight 5) before c (coverage 1, weight 1)


def test_get_related_tracks_for_multiple_empty_input_returns_empty_list() -> None:
    backend = MapBackend(neighbors_map={})
    assert get_related_tracks_for_multiple([], backend) == []


def test_get_related_tracks_for_multiple_honours_k_limit() -> None:
    backend = MapBackend(
        neighbors_map={
            "s1": [("x", 5), ("y", 4), ("z", 3)],
            "s2": [("x", 1), ("y", 10)],
        }
    )

    # Overall ranking without k would be: y (2,14), x (2,6), z (1,3)
    limited = get_related_tracks_for_multiple(["s1", "s2"], backend, k=2)
    assert limited == ["y", "x"]


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
