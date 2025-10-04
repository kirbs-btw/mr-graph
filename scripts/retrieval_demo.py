from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from mr_graph.backends import GraphBackend
from mr_graph.builder import build_track_graph_from_csv
from mr_graph.cli import configure_logging, create_backend
from mr_graph.retrieval import (
    get_related_tracks,
    get_related_tracks_for_multiple_details,
)


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load the track co-occurrence graph and run retrieval queries.",
    )
    parser.add_argument(
        "--csv-path",
        default=Path("data/example_data/songs.csv"),
        type=Path,
        help="CSV file used to build the graph (ignored when --skip-build is set).",
    )
    parser.add_argument(
        "--backend",
        choices=("neo4j", "networkx"),
        default="networkx",
        help="Graph backend to query.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (e.g. INFO, DEBUG).",
    )

    # Neo4j configuration mirrors the CLI module so we can reuse create_backend.
    parser.add_argument(
        "--neo4j-uri",
        default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        help="Neo4j connection URI.",
    )
    parser.add_argument(
        "--neo4j-user",
        default=os.environ.get("NEO4J_USER", "neo4j"),
        help="Neo4j user name.",
    )
    parser.add_argument(
        "--neo4j-password",
        default=os.environ.get("NEO4J_PASSWORD"),
        help="Neo4j user password.",
    )
    parser.add_argument(
        "--neo4j-database",
        default=os.environ.get("NEO4J_DATABASE"),
        help="Optional target Neo4j database name.",
    )
    parser.add_argument(
        "--neo4j-create-constraint",
        action="store_true",
        help="Create the Track.track_id uniqueness constraint when using Neo4j.",
    )

    parser.add_argument(
        "--skip-build",
        action="store_true",
        help="Do not rebuild the graph from CSV; assume the backend already has data.",
    )
    parser.add_argument(
        "--k",
        type=int,
        default=None,
        help="Limit the number of related tracks returned (default: no limit).",
    )
    parser.add_argument(
        "--max-hops",
        type=int,
        default=1,
        help="Maximum hop distance when searching for shared tracks (>=1).",
    )

    parser.add_argument(
        "track_ids",
        nargs="+",
        help="One or more seed track ids to query.",
    )

    return parser.parse_args(argv)


def _format_pairs(pairs: List[tuple[str, int]]) -> str:
    lines = []
    for index, (track_id, weight) in enumerate(pairs, start=1):
        lines.append(f"{index:>2}. {track_id} (weight={weight})")
    return "\n".join(lines)


def run_single_query(track_id: str, backend: GraphBackend, k: int | None) -> None:
    pairs = backend.get_related_tracks(track_id, limit=k)
    if not pairs:
        print(f"No related tracks found for '{track_id}'.")
        return

    print(f"Top related tracks (1 hop) for '{track_id}':")
    print(_format_pairs(pairs))


def run_multi_query(
    track_ids: List[str],
    backend: GraphBackend,
    k: int | None,
    max_hops: int,
) -> None:
    details = get_related_tracks_for_multiple_details(
        track_ids,
        backend,
        k=k,
        max_hops=max_hops,
    )
    if not details:
        print("No shared related tracks found within the hop limit.")
        return

    header = (
        "Shared related tracks (total_hops, total_weight, per-seed distance/weight):"
    )
    print(header)
    for index, (track_id, hop_sum, weight_sum, per_seed) in enumerate(details, start=1):
        per_seed_fmt = ", ".join(
            f"{seed}:{distance}h/{weight}w" for seed, (distance, weight) in per_seed.items()
        )
        print(
            f"{index:>2}. {track_id} (total_hops={hop_sum}, total_weight={weight_sum})"
            f" [{per_seed_fmt}]"
        )


def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)
    if args.max_hops < 1:
        raise SystemExit("--max-hops must be at least 1")

    configure_logging(args.log_level)

    with create_backend(args) as backend:
        if not args.skip_build:
            build_track_graph_from_csv(args.csv_path, backend)

        if len(args.track_ids) == 1:
            run_single_query(args.track_ids[0], backend, args.k)
        else:
            run_multi_query(args.track_ids, backend, args.k, args.max_hops)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
