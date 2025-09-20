from __future__ import annotations

import csv
import logging
from collections import Counter, defaultdict
from itertools import combinations
from pathlib import Path
from typing import Dict, List, Set

from .backends import GraphBackend

logger = logging.getLogger(__name__)


def build_track_graph_from_csv(csv_path: str | Path, backend: GraphBackend) -> None:
    """Load a CSV file and populate the given graph backend with tracks and co-occurrence edges."""
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    playlist_tracks: Dict[str, List[str]] = defaultdict(list)
    track_properties: Dict[str, Dict[str, object]] = {}
    track_playlists: Dict[str, Set[str]] = defaultdict(set)

    with csv_path.open(mode="r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            track_id = (row.get("track_id") or "").strip()
            playlist_id = (row.get("playlist_id") or "").strip()
            if not track_id or not playlist_id:
                continue

            playlist_tracks[playlist_id].append(track_id)
            track_playlists[track_id].add(playlist_id)
            props = track_properties.setdefault(track_id, {})

            _maybe_store_field(props, "track_name", row.get("track_name"))
            _maybe_store_field(props, "track_external_urls", row.get("track_external_urls"))
            _maybe_store_field(props, "release_date", row.get("release_date"))
            _maybe_store_field(props, "artist_name", row.get("artist_name"))

            relevance = _safe_float(row.get("relevance"))
            if relevance is not None:
                props.setdefault("relevance", relevance)

    for track_id, playlists in track_playlists.items():
        track_properties.setdefault(track_id, {})["playlist_count"] = len(playlists)

    logger.info("Loaded %d tracks across %d playlists", len(track_properties), len(playlist_tracks))

    for track_id, properties in track_properties.items():
        backend.upsert_track(track_id, **properties)

    edge_counter: Counter[tuple[str, str]] = Counter()
    for tracks in playlist_tracks.values():
        unique_tracks = sorted(set(tracks))
        for left, right in combinations(unique_tracks, 2):
            edge = (left, right) if left < right else (right, left)
            edge_counter[edge] += 1

    logger.info("Computed %d co-occurrence edges", len(edge_counter))

    for (source_id, target_id), weight in edge_counter.items():
        backend.upsert_connection(source_id, target_id, weight)


def _maybe_store_field(target: Dict[str, object], key: str, raw_value: object) -> None:
    if raw_value is None:
        return
    if not isinstance(raw_value, str):
        target.setdefault(key, raw_value)
        return
    cleaned = raw_value.strip()
    if cleaned:
        target.setdefault(key, cleaned)


def _safe_float(raw_value: object) -> float | None:
    if raw_value in (None, ""):
        return None
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return None
