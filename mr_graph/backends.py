from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Optional


class GraphBackend(ABC):
    """Common interface to upsert track nodes and their co-occurrence edges."""

    def initialize(self) -> None:
        """Prepare the backend (e.g. create indexes). Default no-op."""
        return None

    def __enter__(self) -> "GraphBackend":
        self.initialize()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.close()

    @abstractmethod
    def upsert_track(self, track_id: str, **properties: Any) -> None:
        """Create or update a track node."""
        raise NotImplementedError

    @abstractmethod
    def upsert_connection(self, source_id: str, target_id: str, weight: int) -> None:
        """Create or update an edge between two track nodes."""
        raise NotImplementedError

    def close(self) -> None:
        """Release resources held by the backend."""
        return None


class Neo4jGraphBackend(GraphBackend):
    """Graph backend that persists data to a Neo4j database."""

    def __init__(
        self,
        uri: str,
        user: str,
        password: str,
        database: Optional[str] = None,
        *,
        create_unique_constraint: bool = False,
    ) -> None:
        try:
            from neo4j import GraphDatabase  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "The neo4j Python driver is required to use Neo4jGraphBackend. "
                "Install it with 'pip install neo4j'."
            ) from exc

        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self._database = database
        self._create_unique_constraint = create_unique_constraint

    def initialize(self) -> None:
        if not self._create_unique_constraint:
            return

        def _create_constraint(tx) -> None:
            tx.run(
                "CREATE CONSTRAINT track_id_unique IF NOT EXISTS "
                "FOR (t:Track) REQUIRE t.track_id IS UNIQUE"
            )

        try:
            self._execute_write(_create_constraint)
        except Exception as exc:
            raise RuntimeError(
                "Failed to create the Track.track_id uniqueness constraint. "
                "Disable constraint creation or verify your Neo4j version."
            ) from exc

    def close(self) -> None:
        if hasattr(self, "_driver") and self._driver is not None:
            self._driver.close()

    def upsert_track(self, track_id: str, **properties: Any) -> None:
        property_map = {k: v for k, v in properties.items() if v is not None}

        def _upsert(tx) -> None:
            tx.run(
                "MERGE (t:Track {track_id: $track_id}) "
                "SET t += $properties",
                track_id=track_id,
                properties=property_map,
            )

        self._execute_write(_upsert)

    def upsert_connection(self, source_id: str, target_id: str, weight: int) -> None:
        def _upsert(tx) -> None:
            tx.run(
                "MERGE (source:Track {track_id: $source_id}) "
                "MERGE (target:Track {track_id: $target_id}) "
                "MERGE (source)-[r:CO_OCCURS_WITH]->(target) "
                "SET r.weight = $weight",
                source_id=source_id,
                target_id=target_id,
                weight=weight,
            )

        self._execute_write(_upsert)

    def _execute_write(self, callback) -> None:
        with self._driver.session(database=self._database) as session:
            try:
                session.execute_write(callback)
            except AttributeError:
                session.write_transaction(callback)


class NetworkXGraphBackend(GraphBackend):
    """Graph backend that stores the result in a NetworkX Graph."""

    def __init__(self) -> None:
        try:
            import networkx as nx  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "NetworkX is required for NetworkXGraphBackend. "
                "Install it with 'pip install networkx'."
            ) from exc

        self._nx = nx
        self.graph = nx.Graph()

    def upsert_track(self, track_id: str, **properties: Any) -> None:
        property_map = {k: v for k, v in properties.items() if v is not None}
        if track_id not in self.graph:
            self.graph.add_node(track_id, **property_map)
        else:
            self.graph.nodes[track_id].update(property_map)

    def upsert_connection(self, source_id: str, target_id: str, weight: int) -> None:
        if self.graph.has_edge(source_id, target_id):
            self.graph[source_id][target_id]["weight"] = weight
        else:
            self.graph.add_edge(source_id, target_id, weight=weight)

    def close(self) -> None:
        return None

    def export_graphml(self, path: str) -> None:
        """Persist the graph to GraphML format."""
        self._nx.write_graphml(self.graph, path)


__all__ = [
    "GraphBackend",
    "Neo4jGraphBackend",
    "NetworkXGraphBackend",
]
