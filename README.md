# mr-graph

Utilities to build a weighted co-occurrence graph of tracks that share Spotify playlists.

## Project layout

```
mr_graph/           Python package with the reusable graph logic
  backends.py       Backend implementations (Neo4j, NetworkX)
  builder.py        CSV ingestion and edge computation helpers
  cli.py            Command-line interface utilities
main.py             Thin entry-point forwarding to `mr_graph.cli`
data/               Example CSV source data
```

## Usage

```bash
python main.py --neo4j-password <your-password>
```

Switch to the NetworkX backend when you want a local GraphML file:

```bash
python main.py --backend networkx --networkx-output songs.graphml
```

Set `NEO4J_URI`, `NEO4J_USER`, and `NEO4J_PASSWORD` environment variables to avoid passing credentials via CLI flags.
