from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

from mr_graph.backends import GraphBackend, NetworkXGraphBackend
from mr_graph.retrieval import (
    get_related_tracks,
    get_related_tracks_for_multiple,
    get_related_tracks_for_multiple_details,
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


def test_get_related_tracks_for_multiple_requires_intersection() -> None:
    backend = MapBackend(
        neighbors_map={
            "seed_a": [("common", 5), ("only_a", 1)],
            "seed_b": [("common", 4), ("only_b", 2)],
        }
    )

    result = get_related_tracks_for_multiple(["seed_a", "seed_b", "seed_a"], backend)

    assert result == ["common"]


def test_get_related_tracks_for_multiple_orders_by_hops_then_weight() -> None:
    backend = MapBackend(
        neighbors_map={
            "seed_a": [("direct", 2), ("mid_a", 4)],
            "seed_b": [("direct", 1), ("mid_b", 3)],
            "mid_a": [("far", 500)],
            "mid_b": [("far", 500)],
        }
    )

    result = get_related_tracks_for_multiple(
        ["seed_a", "seed_b"], backend, max_hops=2
    )

    assert result == ["direct", "far"]


def test_get_related_tracks_for_multiple_respects_max_hops() -> None:
    backend = MapBackend(
        neighbors_map={
            "seed_a": [("direct", 2), ("mid_a", 4)],
            "seed_b": [("direct", 1), ("mid_b", 3)],
            "mid_a": [("far", 500)],
            "mid_b": [("far", 500)],
        }
    )

    result_one_hop = get_related_tracks_for_multiple(
        ["seed_a", "seed_b"], backend, max_hops=1
    )
    assert result_one_hop == ["direct"]

    result_two_hops = get_related_tracks_for_multiple(
        ["seed_a", "seed_b"], backend, max_hops=2
    )
    assert result_two_hops == ["direct", "far"]


def test_get_related_tracks_for_multiple_details_returns_metadata() -> None:
    backend = MapBackend(
        neighbors_map={
            "seed_a": [("direct", 2), ("mid_a", 4)],
            "seed_b": [("direct", 1), ("mid_b", 3)],
            "mid_a": [("far", 500)],
            "mid_b": [("far", 500)],
        }
    )

    details = get_related_tracks_for_multiple_details(
        ["seed_a", "seed_b"], backend, max_hops=2
    )

    assert details[0][0] == "direct"
    assert details[0][1] == 2  # 1 hop from each seed
    assert details[0][2] == 3  # weights 2 and 1
    per_seed_stats = details[0][3]
    assert per_seed_stats["seed_a"] == (1, 2)
    assert per_seed_stats["seed_b"] == (1, 1)


def test_get_related_tracks_for_multiple_empty_input_returns_empty_list() -> None:
    backend = MapBackend(neighbors_map={})
    assert get_related_tracks_for_multiple([], backend) == []


def test_get_related_tracks_for_multiple_invalid_arguments() -> None:
    backend = MapBackend(neighbors_map={"seed": []})

    with pytest.raises(ValueError):
        get_related_tracks_for_multiple(["seed"], backend, k=-1)

    with pytest.raises(ValueError):
        get_related_tracks_for_multiple(["seed"], backend, max_hops=0)


def test_get_related_tracks_for_multiple_honours_k_limit() -> None:
    backend = MapBackend(
        neighbors_map={
            "seed_a": [("direct", 2), ("mid_a", 4)],
            "seed_b": [("direct", 1), ("mid_b", 3)],
            "mid_a": [("far", 500)],
            "mid_b": [("far", 500)],
        }
    )

    limited = get_related_tracks_for_multiple(
        ["seed_a", "seed_b"], backend, k=1, max_hops=2
    )
    assert limited == ["direct"]


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
