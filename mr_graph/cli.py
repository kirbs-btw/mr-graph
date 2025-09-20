from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path

from .backends import GraphBackend, Neo4jGraphBackend, NetworkXGraphBackend
from .builder import build_track_graph_from_csv


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a track co-occurrence graph from a CSV export."
    )
    parser.add_argument(
        "--csv-path",
        default=Path("data/example_data/songs.csv"),
        type=Path,
        help="Path to the tracks CSV file.",
    )
    parser.add_argument(
        "--backend",
        choices=("neo4j", "networkx"),
        default="neo4j",
        help="Select the graph backend to use.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (e.g. INFO, DEBUG).",
    )

    parser.add_argument("--neo4j-uri", default=os.environ.get("NEO4J_URI", "bolt://localhost:7687"))
    parser.add_argument("--neo4j-user", default=os.environ.get("NEO4J_USER", "neo4j"))
    parser.add_argument("--neo4j-password", default=os.environ.get("NEO4J_PASSWORD"))
    parser.add_argument("--neo4j-database", default=os.environ.get("NEO4J_DATABASE"))
    parser.add_argument(
        "--neo4j-create-constraint",
        action="store_true",
        help="Create a Track.track_id uniqueness constraint if it does not exist.",
    )

    parser.add_argument(
        "--networkx-output",
        type=Path,
        help="Optional path to export the graph as GraphML when using the NetworkX backend.",
    )

    return parser.parse_args(argv)


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )


def create_backend(args: argparse.Namespace) -> GraphBackend:
    if args.backend == "neo4j":
        if not args.neo4j_password:
            raise SystemExit(
                "Neo4j password not provided. Use --neo4j-password or set the NEO4J_PASSWORD environment variable."
            )
        return Neo4jGraphBackend(
            uri=args.neo4j_uri,
            user=args.neo4j_user,
            password=args.neo4j_password,
            database=args.neo4j_database,
            create_unique_constraint=args.neo4j_create_constraint,
        )

    if args.backend == "networkx":
        return NetworkXGraphBackend()

    raise SystemExit(f"Unsupported backend: {args.backend}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    with create_backend(args) as backend:
        build_track_graph_from_csv(args.csv_path, backend)
        if args.backend == "networkx" and args.networkx_output:
            backend.export_graphml(str(args.networkx_output))
            logging.getLogger(__name__).info(
                "Exported NetworkX graph to %s", args.networkx_output
            )

    return 0


__all__ = ["main", "parse_args", "create_backend", "configure_logging"]
